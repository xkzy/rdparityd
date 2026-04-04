package journal

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	RecordMagic   = "RTPJ"
	RecordVersion = 1
)

type Record struct {
	Magic             string               `json:"magic"`
	Version           int                  `json:"version"`
	TxID              string               `json:"tx_id"`
	State             State                `json:"state"`
	Timestamp         time.Time            `json:"timestamp"`
	PoolName          string               `json:"pool_name,omitempty"`
	LogicalPath       string               `json:"logical_path,omitempty"`
	File              *metadata.FileRecord `json:"file,omitempty"`
	Extents           []metadata.Extent    `json:"extents,omitempty"`
	OldGeneration     int64                `json:"old_generation,omitempty"`
	NewGeneration     int64                `json:"new_generation,omitempty"`
	AffectedExtentIDs []string             `json:"affected_extent_ids,omitempty"`
	PayloadChecksum   string               `json:"payload_checksum"`
	RecordChecksum    string               `json:"record_checksum"`
}

type ReplayAction struct {
	TxID           string `json:"tx_id"`
	LastState      State  `json:"last_state"`
	Outcome        State  `json:"outcome"`
	Recommendation string `json:"recommendation"`
}

type ReplaySummary struct {
	TotalRecords    int            `json:"total_records"`
	RequiresReplay  bool           `json:"requires_replay"`
	IncompleteTxIDs []string       `json:"incomplete_tx_ids"`
	Actions         []ReplayAction `json:"actions"`
	LastCommittedTx string         `json:"last_committed_tx,omitempty"`
	LastAbortedTx   string         `json:"last_aborted_tx,omitempty"`
}

type Store struct {
	path string
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Append(record Record) (Record, error) {
	sealed, err := sealRecord(record)
	if err != nil {
		return Record{}, err
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return Record{}, fmt.Errorf("create journal directory: %w", err)
	}

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return Record{}, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(sealed); err != nil {
		return Record{}, fmt.Errorf("encode journal record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Record{}, fmt.Errorf("sync journal: %w", err)
	}

	return sealed, nil
}

func (s *Store) Load() ([]Record, error) {
	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	var records []Record
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		var record Record
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			return nil, fmt.Errorf("decode journal record %d: %w", line, err)
		}
		if err := validateRecord(record); err != nil {
			return nil, fmt.Errorf("validate journal record %d: %w", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan journal: %w", err)
	}

	return records, nil
}

func (s *Store) Replay() (ReplaySummary, error) {
	records, err := s.Load()
	if err != nil {
		return ReplaySummary{}, err
	}
	return ReplayRecords(records)
}

func ReplayRecords(records []Record) (ReplaySummary, error) {
	summary := ReplaySummary{TotalRecords: len(records)}
	if len(records) == 0 {
		return summary, nil
	}

	grouped := make(map[string][]Record)
	order := make([]string, 0)
	for _, record := range records {
		if _, exists := grouped[record.TxID]; !exists {
			order = append(order, record.TxID)
		}
		grouped[record.TxID] = append(grouped[record.TxID], record)
	}

	for _, txID := range order {
		txRecords := grouped[txID]
		states := make([]State, 0, len(txRecords))
		for _, record := range txRecords {
			states = append(states, record.State)
		}

		lastState := states[len(states)-1]
		if err := ValidateSequence(states); err != nil {
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateAborted,
				Recommendation: fmt.Sprintf("invalid journal sequence detected: %v; abort the transaction and inspect the journal tail", err),
			})
			continue
		}

		switch lastState {
		case StateCommitted:
			summary.LastCommittedTx = txID
		case StateAborted:
			summary.LastAbortedTx = txID
		case StatePrepared:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateAborted,
				Recommendation: "discard the pending write and keep the previous generation visible",
			})
		case StateDataWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "verify the new data extent and recompute or roll forward parity before committing",
			})
		case StateParityWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "parity is durable; verify checksum and complete the metadata update",
			})
		case StateMetadataWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "metadata is durable; verify checksums and write the final commit marker",
			})
		case StateReplayRequired:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "resume startup replay for this transaction and only clear the dirty flag after reconciliation",
			})
		default:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "unknown journal state encountered; inspect manually before proceeding",
			})
		}
	}

	return summary, nil
}

func sealRecord(record Record) (Record, error) {
	record = withDefaults(record)
	if err := validateBasic(record); err != nil {
		return Record{}, err
	}

	record.PayloadChecksum = checksumJSON(payloadView(record))
	record.RecordChecksum = checksumJSON(envelopeView(record))
	return record, nil
}

func validateRecord(record Record) error {
	if err := validateBasic(record); err != nil {
		return err
	}
	if record.PayloadChecksum == "" || record.RecordChecksum == "" {
		return fmt.Errorf("missing journal checksum")
	}
	if got := checksumJSON(payloadView(record)); got != record.PayloadChecksum {
		return fmt.Errorf("payload checksum mismatch")
	}
	if got := checksumJSON(envelopeView(record)); got != record.RecordChecksum {
		return fmt.Errorf("record checksum mismatch")
	}
	return nil
}

func validateBasic(record Record) error {
	if record.Magic != RecordMagic {
		return fmt.Errorf("unexpected record magic %q", record.Magic)
	}
	if record.Version != RecordVersion {
		return fmt.Errorf("unexpected record version %d", record.Version)
	}
	if record.TxID == "" {
		return fmt.Errorf("missing tx_id")
	}
	if record.State == "" {
		return fmt.Errorf("missing state")
	}
	if record.Timestamp.IsZero() {
		return fmt.Errorf("missing timestamp")
	}
	return nil
}

func withDefaults(record Record) Record {
	if record.Magic == "" {
		record.Magic = RecordMagic
	}
	if record.Version == 0 {
		record.Version = RecordVersion
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}
	return record
}

func payloadView(record Record) any {
	return struct {
		TxID              string               `json:"tx_id"`
		State             State                `json:"state"`
		Timestamp         time.Time            `json:"timestamp"`
		PoolName          string               `json:"pool_name,omitempty"`
		LogicalPath       string               `json:"logical_path,omitempty"`
		File              *metadata.FileRecord `json:"file,omitempty"`
		Extents           []metadata.Extent    `json:"extents,omitempty"`
		OldGeneration     int64                `json:"old_generation,omitempty"`
		NewGeneration     int64                `json:"new_generation,omitempty"`
		AffectedExtentIDs []string             `json:"affected_extent_ids,omitempty"`
	}{
		TxID:              record.TxID,
		State:             record.State,
		Timestamp:         record.Timestamp.UTC(),
		PoolName:          record.PoolName,
		LogicalPath:       record.LogicalPath,
		File:              record.File,
		Extents:           record.Extents,
		OldGeneration:     record.OldGeneration,
		NewGeneration:     record.NewGeneration,
		AffectedExtentIDs: record.AffectedExtentIDs,
	}
}

func envelopeView(record Record) any {
	return struct {
		Magic             string               `json:"magic"`
		Version           int                  `json:"version"`
		TxID              string               `json:"tx_id"`
		State             State                `json:"state"`
		Timestamp         time.Time            `json:"timestamp"`
		PoolName          string               `json:"pool_name,omitempty"`
		LogicalPath       string               `json:"logical_path,omitempty"`
		File              *metadata.FileRecord `json:"file,omitempty"`
		Extents           []metadata.Extent    `json:"extents,omitempty"`
		OldGeneration     int64                `json:"old_generation,omitempty"`
		NewGeneration     int64                `json:"new_generation,omitempty"`
		AffectedExtentIDs []string             `json:"affected_extent_ids,omitempty"`
		PayloadChecksum   string               `json:"payload_checksum"`
	}{
		Magic:             record.Magic,
		Version:           record.Version,
		TxID:              record.TxID,
		State:             record.State,
		Timestamp:         record.Timestamp.UTC(),
		PoolName:          record.PoolName,
		LogicalPath:       record.LogicalPath,
		File:              record.File,
		Extents:           record.Extents,
		OldGeneration:     record.OldGeneration,
		NewGeneration:     record.NewGeneration,
		AffectedExtentIDs: record.AffectedExtentIDs,
		PayloadChecksum:   record.PayloadChecksum,
	}
}

func checksumJSON(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
