package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type RecoveryResult struct {
	AttemptedTxIDs []string      `json:"attempted_tx_ids"`
	RecoveredTxIDs []string      `json:"recovered_tx_ids"`
	AbortedTxIDs   []string      `json:"aborted_tx_ids"`
	FinalSummary   ReplaySummary `json:"final_summary"`
}

func (c *Coordinator) Recover() (RecoveryResult, error) {
	return c.RecoverWithState(metadata.PrototypeState("demo"))
}

func (c *Coordinator) RecoverWithState(defaultState metadata.SampleState) (RecoveryResult, error) {
	if c == nil {
		return RecoveryResult{}, fmt.Errorf("coordinator is nil")
	}

	state, err := c.metadata.LoadOrCreate(defaultState)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("load metadata state for recovery: %w", err)
	}

	records, err := c.journal.Load()
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("load journal for recovery: %w", err)
	}

	grouped := make(map[string][]Record)
	order := make([]string, 0)
	for _, record := range records {
		if _, ok := grouped[record.TxID]; !ok {
			order = append(order, record.TxID)
		}
		grouped[record.TxID] = append(grouped[record.TxID], record)
	}

	result := RecoveryResult{
		AttemptedTxIDs: []string{},
		RecoveredTxIDs: []string{},
		AbortedTxIDs:   []string{},
	}

	for _, txID := range order {
		txRecords := grouped[txID]
		if len(txRecords) == 0 {
			continue
		}

		last := effectiveRecoveryRecord(txRecords)
		switch last.State {
		case StateCommitted, StateAborted:
			continue
		case StatePrepared:
			result.AttemptedTxIDs = append(result.AttemptedTxIDs, txID)
			if _, err := c.journal.Append(withState(last, StateAborted)); err != nil {
				return result, fmt.Errorf("append aborted recovery record for %s: %w", txID, err)
			}
			result.AbortedTxIDs = append(result.AbortedTxIDs, txID)
		case StateDataWritten, StateParityWritten, StateMetadataWritten, StateReplayRequired:
			result.AttemptedTxIDs = append(result.AttemptedTxIDs, txID)
			if err := c.rollForwardWrite(&state, txRecords); err != nil {
				return result, fmt.Errorf("recover %s: %w", txID, err)
			}
			result.RecoveredTxIDs = append(result.RecoveredTxIDs, txID)
		default:
			return result, fmt.Errorf("unsupported recovery state %q for tx %s", last.State, txID)
		}
	}

	summary, err := c.journal.Replay()
	if err != nil {
		return result, fmt.Errorf("replay after recovery: %w", err)
	}
	result.FinalSummary = summary
	return result, nil
}

func (c *Coordinator) rollForwardWrite(state *metadata.SampleState, txRecords []Record) error {
	if len(txRecords) == 0 {
		return fmt.Errorf("empty transaction record set")
	}

	last := txRecords[len(txRecords)-1]
	if last.File == nil {
		return fmt.Errorf("missing file payload in journal for tx %s", last.TxID)
	}
	if len(last.Extents) == 0 {
		return fmt.Errorf("missing extent payload in journal for tx %s", last.TxID)
	}

	mergeRecoveredFile(state, *last.File, last.Extents)

	// For states where parity was already durably written, merge parity group
	// metadata into state BEFORE verifying extent files. This is required so
	// that ensureExtentFiles can use parity reconstruction as a fallback when
	// an extent file is missing or corrupted.
	switch last.State {
	case StateParityWritten, StateMetadataWritten:
		mergeParityGroups(state, last.Extents)
	}

	rootDir := filepath.Dir(c.metadataPath)

	// Verify that each extent file exists on disk and has the correct checksum.
	// Unlike the original write path we must NOT overwrite existing files with
	// synthetic data — the real payload bytes were already written before the
	// crash and must be left untouched.
	if err := ensureExtentFiles(rootDir, *state, last.Extents); err != nil {
		return fmt.Errorf("ensure extent files during recovery: %w", err)
	}

	if last.State == StateDataWritten || last.State == StateReplayRequired {
		mergeParityGroups(state, last.Extents)
		if err := writeParityFiles(rootDir, state, last.Extents); err != nil {
			return err
		}
		if _, err := c.journal.Append(withState(last, StateParityWritten)); err != nil {
			return fmt.Errorf("append parity-written recovery record: %w", err)
		}
		last.State = StateParityWritten
	}

	if last.State == StateParityWritten {
		// Re-run parity computation so that the checksum stored in metadata
		// accurately reflects the current on-disk parity file. The parity file
		// was written by the original write path (which may have included extents
		// from other committed transactions in the same parity group), but the
		// committed metadata snapshot may still carry an older parity checksum.
		// writeParityFiles reads all member extents, recomputes the XOR, and
		// updates state.ParityGroups[i].ParityChecksum in place before we save.
		if err := writeParityFiles(rootDir, state, last.Extents); err != nil {
			return fmt.Errorf("recompute parity during recovery: %w", err)
		}
		upsertTransaction(state, last, StateMetadataWritten, false, nil)
		if _, err := c.metadata.Save(*state); err != nil {
			return fmt.Errorf("save metadata during recovery: %w", err)
		}
		if _, err := c.journal.Append(withState(last, StateMetadataWritten)); err != nil {
			return fmt.Errorf("append metadata-written recovery record: %w", err)
		}
		last.State = StateMetadataWritten
	}

	if last.State == StateMetadataWritten {
		committedAt := time.Now().UTC()
		upsertTransaction(state, last, StateCommitted, false, &committedAt)
		if _, err := c.metadata.Save(*state); err != nil {
			return fmt.Errorf("save committed metadata during recovery: %w", err)
		}
		if _, err := c.journal.Append(withState(last, StateCommitted)); err != nil {
			return fmt.Errorf("append committed recovery record: %w", err)
		}
	}

	return nil
}

// ensureExtentFiles verifies that each extent file exists on disk and matches
// the checksum recorded in the extent's metadata. If a file is correct it is
// left untouched. If a file is missing or corrupted and parity data is
// available, parity reconstruction is attempted. This function must be used
// during crash recovery instead of writeExtentFiles so that real payload data
// written before the crash is never overwritten with synthetic content.
func ensureExtentFiles(rootDir string, state metadata.SampleState, extents []metadata.Extent) error {
	for _, extent := range extents {
		path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		if data, err := os.ReadFile(path); err == nil {
			// normalizeExtentLength truncates data longer than extent.Length or
			// zero-pads data shorter than it, ensuring a consistent byte length
			// for checksum comparison regardless of OS-level file buffering.
			normalized := normalizeExtentLength(data, extent.Length)
			if extent.Checksum == "" || digestBytes(normalized) == extent.Checksum {
				continue // file is present and correct (or has no expected checksum)
			}
		}
		// File is missing or its checksum does not match.
		if extent.Checksum == "" {
			return fmt.Errorf("extent %s has no checksum and its file is missing or corrupt; cannot recover without original payload", extent.ExtentID)
		}
		rebuilt, err := reconstructExtent(rootDir, state, extent)
		if err != nil {
			return fmt.Errorf("extent %s file is missing or corrupt and parity reconstruction failed: %w", extent.ExtentID, err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create extent directory for %s: %w", extent.ExtentID, err)
		}
		if err := os.WriteFile(path, rebuilt, 0o600); err != nil {
			return fmt.Errorf("write recovered extent %s: %w", extent.ExtentID, err)
		}
	}
	return nil
}

func mergeRecoveredFile(state *metadata.SampleState, file metadata.FileRecord, extents []metadata.Extent) {
	fileFound := false
	for i := range state.Files {
		if state.Files[i].FileID == file.FileID {
			state.Files[i] = file
			fileFound = true
			break
		}
	}
	if !fileFound {
		state.Files = append(state.Files, file)
	}

	extentIndex := make(map[string]int)
	for i, extent := range state.Extents {
		extentIndex[extent.ExtentID] = i
	}
	for _, extent := range extents {
		if idx, ok := extentIndex[extent.ExtentID]; ok {
			state.Extents[idx] = extent
			continue
		}
		state.Extents = append(state.Extents, extent)
		extentIndex[extent.ExtentID] = len(state.Extents) - 1
	}
}

func effectiveRecoveryRecord(txRecords []Record) Record {
	if len(txRecords) == 0 {
		return Record{}
	}

	last := txRecords[len(txRecords)-1]
	if last.State != StateReplayRequired {
		return last
	}
	for i := len(txRecords) - 1; i >= 0; i-- {
		if txRecords[i].State != StateReplayRequired {
			return txRecords[i]
		}
	}
	return last
}

func upsertTransaction(state *metadata.SampleState, record Record, txState State, replayRequired bool, committedAt *time.Time) {
	for i := range state.Transactions {
		if state.Transactions[i].TxID == record.TxID {
			state.Transactions[i].State = string(txState)
			state.Transactions[i].StartedAt = record.Timestamp
			state.Transactions[i].AffectedExtentIDs = append([]string(nil), record.AffectedExtentIDs...)
			state.Transactions[i].OldGeneration = record.OldGeneration
			state.Transactions[i].NewGeneration = record.NewGeneration
			state.Transactions[i].ReplayRequired = replayRequired
			state.Transactions[i].CommittedAt = committedAt
			return
		}
	}

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              record.TxID,
		State:             string(txState),
		StartedAt:         record.Timestamp,
		CommittedAt:       committedAt,
		AffectedExtentIDs: append([]string(nil), record.AffectedExtentIDs...),
		OldGeneration:     record.OldGeneration,
		NewGeneration:     record.NewGeneration,
		ReplayRequired:    replayRequired,
	})
}
