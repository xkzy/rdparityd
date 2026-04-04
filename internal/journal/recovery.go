package journal

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/rtparityd/rtparityd/internal/metadata"
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
	rootDir := filepath.Dir(c.metadataPath)
	if err := writeExtentFiles(rootDir, last.Extents); err != nil {
		return fmt.Errorf("restore extent files during recovery: %w", err)
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
