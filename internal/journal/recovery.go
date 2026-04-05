package journal

import (
	"errors"
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.RecoverWithState(metadata.PrototypeState("demo"))
}

func (c *Coordinator) RecoverWithState(defaultState metadata.SampleState) (RecoveryResult, error) {
	if c == nil {
		return RecoveryResult{}, fmt.Errorf("coordinator is nil")
	}

	// I12 (Single Authoritative Recovery): distinguish three metadata cases:
	//   (a) missing (first boot)     — safe to use defaultState
	//   (b) corrupt (torn/checksum)  — use journal as sole source; refuse if
	//                                   journal is also empty
	//   (c) valid                    — use as base, reconcile with journal
	state, metaErr := c.metadata.Load()
	metaCorrupt := false
	if metaErr != nil {
		if errors.Is(metaErr, os.ErrNotExist) {
			// Case (a): first boot or metadata file never created.
			state = defaultState
		} else {
			// Case (b): file exists but is unreadable (torn write, wrong magic,
			// checksum mismatch). Do NOT silently use defaultState here;
			// we may have committed data that the prototype knows nothing about.
			metaCorrupt = true
			state = defaultState // tentative; may be overridden by journal below
		}
	}
	_ = metaCorrupt // used below after journal load
	// Invalidate cache: recovery may modify state multiple times. The final
	// commitState calls below will repopulate the cache with the recovered state.
	c.invalidateCache()

	records, err := c.journal.Load()
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("load journal for recovery: %w", err)
	}

	// I12: if metadata was corrupt AND the journal is empty, we cannot
	// determine authoritative state. Refuse to start rather than silently
	// using the prototype (which would claim an empty pool over possibly
	// committed data).
	if metaCorrupt && len(records) == 0 {
		return RecoveryResult{}, fmt.Errorf(
			"I12: metadata is corrupt and journal is empty; cannot determine " +
				"authoritative state — manual recovery required")
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
	metadataDirty := false

	for _, txID := range order {
		txRecords := grouped[txID]
		if len(txRecords) == 0 {
			continue
		}

		last := effectiveRecoveryRecord(txRecords)
		if isRepairRecord(last) {
			if last.State == StateCommitted || last.State == StateAborted {
				continue
			}
			result.AttemptedTxIDs = append(result.AttemptedTxIDs, txID)
			changed, err := rollForwardRepair(c.metadataPath, c.journal, &state, txRecords)
			if err != nil {
				return result, fmt.Errorf("recover repair tx %s: %w", txID, err)
			}
			metadataDirty = metadataDirty || changed
			result.RecoveredTxIDs = append(result.RecoveredTxIDs, txID)
			continue
		}
		switch last.State {
		case StateCommitted:
			changed, err := reconcileCommittedTransaction(&state, txRecords)
			if err != nil {
				return result, fmt.Errorf("reconcile committed tx %s: %w", txID, err)
			}
			metadataDirty = metadataDirty || changed
			continue
		case StateAborted:
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

	if metadataDirty {
		if _, err := c.commitState(state); err != nil {
			return result, fmt.Errorf("save reconciled metadata state: %w", err)
		}
	}

	// Before declaring recovery complete, check that the on-disk state is
	// actually recoverable. If multiple disks have failed in the same parity
	// group the data is permanently lost; fail loudly rather than returning a
	// clean result that masks the loss.
	analysis := AnalyzeMultiDiskFailures(filepath.Dir(c.metadataPath), state)
	if !analysis.RecoveryIsPossible {
		return result, fmt.Errorf("unrecoverable multi-disk failure detected: %s", analysis.RecommendedAction)
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
		if err := writeParityFiles(rootDir, state, last.Extents, 0); err != nil {
			return err
		}
		if _, err := c.journal.Append(withState(last, StateParityWritten)); err != nil {
			return fmt.Errorf("append parity-written recovery record: %w", err)
		}
		last.State = StateParityWritten
	}

	if last.State == StateParityWritten {
		// Re-run parity computation to ensure the checksum stored in metadata
		// accurately reflects the current on-disk parity file. The parity file
		// may have been written with additional extents from other committed
		// transactions, leaving the committed metadata snapshot with a stale
		// checksum. writeParityFiles recomputes the XOR from all current member
		// extents, rewrites the parity file only if the checksum has changed, and
		// updates state.ParityGroups[i].ParityChecksum in place.
		//
		// Optimisation: if every parity group already has the correct checksum
		// (common for the metadata-written case where we fell through from
		// data-written in the same recovery pass), skip the disk IO.
		if parityChecksumStale(rootDir, *state, last.Extents) {
			if err := writeParityFiles(rootDir, state, last.Extents, 0); err != nil {
				return fmt.Errorf("recompute parity during recovery: %w", err)
			}
		}
		upsertTransaction(state, last, StateMetadataWritten, false, nil)
		if _, err := c.commitState(*state); err != nil {
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
		if _, err := c.commitState(*state); err != nil {
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
		if err := replaceSyncFile(path, rebuilt, 0o600); err != nil {
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

// parityChecksumStale returns true if any parity group referenced by extents
// has an on-disk parity file whose checksum differs from the stored metadata
// checksum. A mismatch means the metadata is stale and writeParityFiles must
// be called to reconcile them.
func parityChecksumStale(rootDir string, state metadata.SampleState, extents []metadata.Extent) bool {
	seen := make(map[string]bool)
	for _, extent := range extents {
		if seen[extent.ParityGroupID] {
			continue
		}
		seen[extent.ParityGroupID] = true

		var storedChecksum string
		for _, group := range state.ParityGroups {
			if group.ParityGroupID == extent.ParityGroupID {
				storedChecksum = group.ParityChecksum
				break
			}
		}
		if storedChecksum == "" {
			return true // group not yet in metadata
		}

		parityPath := filepath.Join(rootDir, "parity", extent.ParityGroupID+".bin")
		data, err := os.ReadFile(parityPath)
		if err != nil {
			return true // file missing; must recompute
		}
		if digestBytes(data) != storedChecksum {
			return true
		}
	}
	return false
}

func reconcileCommittedTransaction(state *metadata.SampleState, txRecords []Record) (bool, error) {
	if len(txRecords) == 0 {
		return false, fmt.Errorf("empty transaction record set")
	}
	last := txRecords[len(txRecords)-1]
	if last.File == nil {
		return false, fmt.Errorf("missing file payload in journal for tx %s", last.TxID)
	}
	if len(last.Extents) == 0 {
		return false, fmt.Errorf("missing extent payload in journal for tx %s", last.TxID)
	}

	mergeRecoveredFile(state, *last.File, last.Extents)
	mergeParityGroups(state, last.Extents)
	committedAt := last.Timestamp
	upsertTransaction(state, last, StateCommitted, false, &committedAt)
	return true, nil
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
