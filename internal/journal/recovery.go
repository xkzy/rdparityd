/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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
	return c.RecoverWithState(metadata.PrototypeState("demo"))
}

func (c *Coordinator) RecoverWithState(defaultState metadata.SampleState) (RecoveryResult, error) {
	if c == nil {
		return RecoveryResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return RecoveryResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoverWithStateLocked(defaultState)
}

func (c *Coordinator) recoverWithStateLocked(defaultState metadata.SampleState) (RecoveryResult, error) {
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
			// we may have committed data that prototype knows nothing about.
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

	// I12: if metadata was corrupt AND to journal is empty, we cannot
	// determine authoritative state. Refuse to start rather than silently
	// using a prototype (which would claim an empty pool over possibly
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

	// I6 (Replay Idempotence): Track which transactions have already
	// been applied to prevent duplicate application on replay. This is critical
	// because the journal may contain duplicate committed records (e.g., from
	// previous incomplete recovery attempts) and applying the same transaction
	// twice would corrupt state.
	appliedTxIDs := make(map[string]bool)
	for _, txID := range order {
		// Skip this transaction if it was already applied in a previous iteration.
		// This ensures idempotence: applying the same journal multiple
		// times produces the same final state.
		if appliedTxIDs[txID] {
			continue
		}
		appliedTxIDs[txID] = true

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
				// Mark transaction as not applied on error so it can be retried
				appliedTxIDs[txID] = false
				return result, fmt.Errorf("recover repair tx %s: %w", txID, err)
			}
			metadataDirty = metadataDirty || changed
			result.RecoveredTxIDs = append(result.RecoveredTxIDs, txID)
			continue
		}
		switch last.State {
		case StateCommitted:
			changed, err := reconcileCommittedTransaction(c.metadataPath, &state, txRecords)
			if err != nil {
				// Mark transaction as not applied on error so it can be retried
				appliedTxIDs[txID] = false
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
				var ua errUnrecoverableAbort
				if errors.As(err, &ua) {
					result.AbortedTxIDs = append(result.AbortedTxIDs, txID)
					continue
				}
				return result, fmt.Errorf("recover %s: %w", txID, err)
			}
			result.RecoveredTxIDs = append(result.RecoveredTxIDs, txID)
			metadataDirty = true
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
	// group, data is permanently lost; fail loudly rather than returning a
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

// parityGroupsBeforeMerge captures parity group membership before merging new extents.
// Used during recovery to detect when new extents are added to existing groups,
// which requires invalidating the stored ParityChecksum and forcing recomputation.
func parityGroupsBeforeMerge(state *metadata.SampleState) map[string]map[string]bool {
	body := make(map[string]map[string]bool)
	for _, group := range state.ParityGroups {
		members := make(map[string]bool)
		for _, memberID := range group.MemberExtentIDs {
			members[memberID] = true
		}
		body[group.ParityGroupID] = members
	}
	return body
}

// invalidateParityChecksumsForChangedGroups resets ParityChecksum to "" for any
// parity group whose member list changed during merge. This forces recomputation
// and prevents staleness corruption where a group's parity file contains only
// old extents but metadata claims it covers new extents.
//
// CRITICAL for recovery: When transaction T1 commits E1,E2→G1, then T2 adds E3→G1
// and crashes at StateParityWritten, recovery must detect that G1 now has 3 members
// but on-disk parity only XORs 2. The only way to detect this is to track that
// the member list changed, then force recomputation.
func invalidateParityChecksumsForChangedGroups(state *metadata.SampleState, before map[string]map[string]bool) {
	for i := range state.ParityGroups {
		groupID := state.ParityGroups[i].ParityGroupID
		newMembers := make(map[string]bool)
		for _, memberID := range state.ParityGroups[i].MemberExtentIDs {
			newMembers[memberID] = true
		}

		// If this group didn't exist before (new group in this transaction),
		// or its member list changed, invalidate the checksum.
		if oldMembers, existed := before[groupID]; !existed || !mapsEqual(oldMembers, newMembers) {
			state.ParityGroups[i].ParityChecksum = ""
		}
	}
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// errUnrecoverableAbort is a sentinel returned by rollForwardWrite when a
// transaction was explicitly aborted because its parity peers were unrecoverable.
// RecoverWithState uses this to route it to AbortedTxIDs instead of RecoveredTxIDs.
type errUnrecoverableAbort struct{ txID string }

func (e errUnrecoverableAbort) Error() string {
	return fmt.Sprintf("tx %s aborted: parity peers unrecoverable", e.txID)
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
	//
	// CRITICAL: capture parity groups BEFORE merge so we can detect when
	// new extents are added to existing groups (requires parity recomputation).
	parityGroupsBefore := parityGroupsBeforeMerge(state)
	switch last.State {
	case StateParityWritten, StateMetadataWritten:
		mergeParityGroups(state, last.Extents)
		// I2: invalidate checksums for any group that gained new members.
		// This is CRITICAL for correctness — if we don't do this, parityChecksumStale
		// will see a matching checksum (from old member list) and skip recomputation,
		// resulting in parity data that doesn't match the new member list.
		invalidateParityChecksumsForChangedGroups(state, parityGroupsBefore)
	}

	rootDir := filepath.Dir(c.metadataPath)

	// Verify that each extent file exists on disk and has the correct checksum.
	// Unlike the original write path we must NOT overwrite existing files with
	// synthetic data — real payload bytes were already written before the
	// crash and must be left untouched.
	if err := ensureExtentFiles(rootDir, *state, last.Extents); err != nil {
		return fmt.Errorf("ensure extent files during recovery: %w", err)
	}

	if last.State == StateDataWritten || last.State == StateReplayRequired {
		// If existing committed parity group members are unrecoverable, we
		// cannot safely recompute parity for this interrupted write. Aborting
		// the transaction is safer than blocking recovery forever or producing
		// poisoned parity. The extent data for the new write remains on disk
		// but is orphaned (not referenced by committed metadata).
		if err := ensureHealthyCommittedParityInputs(c.metadataPath, c.journal, *state, last.Extents); err != nil {
			if _, abortErr := c.journal.Append(withState(last, StateAborted)); abortErr != nil {
				return fmt.Errorf("abort unrecoverable write %s: %w", last.TxID, abortErr)
			}
			return errUnrecoverableAbort{txID: last.TxID}
		}
		mergeParityGroups(state, last.Extents)
		if err := writeParityFiles(rootDir, state, last.Extents, 0); err != nil {
			return err
		}
		if _, err := c.journal.Append(withState(last, StateParityWritten)); err != nil {
			return fmt.Errorf("append parity-written recovery record for %s: %w", last.TxID, err)
		}
		last.State = StateParityWritten
	}

	if last.State == StateParityWritten {
		// Re-run parity computation to ensure that the checksum stored in metadata
		// accurately reflects the current on-disk parity file. The parity file
		// may have been written with additional extents from other committed
		// transactions, leaving the committed metadata snapshot with a stale
		// checksum. writeParityFiles recomputes XOR from all current member
		// extents, rewrites the parity file only if the checksum has changed, and
		// updates state.ParityGroups[i].ParityChecksum in place.
		//
		// Optimization: if every parity group already has the correct checksum
		// (common for metadata-written case where we fell through from
		// data-written in the same recovery pass), skip the disk IO.
		if parityChecksumStale(rootDir, *state, last.Extents) {
			// Parity must be recomputed — apply the same guard as in
			// DataWritten path: validate committed members first to prevent
			// a corrupt peer from poisoning the recomputed parity.
			if err := ensureHealthyCommittedParityInputs(c.metadataPath, c.journal, *state, last.Extents); err != nil {
				if _, abortErr := c.journal.Append(withState(last, StateAborted)); abortErr != nil {
					return fmt.Errorf("abort unrecoverable stale-parity recompute %s: %w", last.TxID, abortErr)
				}
				return errUnrecoverableAbort{txID: last.TxID}
			}
			if err := writeParityFiles(rootDir, state, last.Extents, 0); err != nil {
				return fmt.Errorf("recompute parity during recovery: %w", err)
			}
		}
		upsertTransaction(state, last, StateMetadataWritten, false, nil)
		if _, err := c.commitState(*state); err != nil {
			return fmt.Errorf("save metadata during recovery: %w", err)
		}
		if _, err := c.journal.Append(withState(last, StateMetadataWritten)); err != nil {
			return fmt.Errorf("append metadata-written recovery record for %s: %w", last.TxID, err)
		}
		last.State = StateMetadataWritten
	}

	if last.State == StateMetadataWritten {
		committedAt := time.Now().UTC()
		upsertTransaction(state, last, StateCommitted, false, &committedAt)
		// Metadata is already durable at StateMetadataWritten.
		// Append committed record, then publish cached state.
		if _, err := c.journal.Append(withState(last, StateCommitted)); err != nil {
			return fmt.Errorf("append committed recovery record for %s: %w", last.TxID, err)
		}
		if _, err := c.saveStateSnapshot(*state); err != nil {
			return fmt.Errorf("save committed metadata during recovery: %w", err)
		}
		c.publishCommittedState(*state)
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

	// Return the last record with a valid state before StateReplayRequired.
	// This ensures that rollForwardWrite sees the correct state to resume from.
	for i := len(txRecords) - 1; i >= 0; i-- {
		if txRecords[i].State != StateReplayRequired {
			return txRecords[i]
		}
	}
	return Record{}
}

// parityChecksumStale returns true if any parity group referenced by extents
// has an on-disk parity file whose checksum differs from the stored metadata
// checksum. A mismatch means that metadata is stale and writeParityFiles must
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

func reconcileCommittedTransaction(metadataPath string, state *metadata.SampleState, txRecords []Record) (bool, error) {
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

	// Track parity checksums before merge to detect changes.
	beforeChecksums := make(map[string]string)
	for _, group := range state.ParityGroups {
		beforeChecksums[group.ParityGroupID] = group.ParityChecksum
	}

	mergeParityGroups(state, last.Extents)

	// I2/I5: if transaction added new member extents to an existing
	// parity group, the merged group will have an updated member list
	// but ParityChecksum will still reflect the old list.
	// We recompute it directly from the parity file since this transaction
	// was fully committed before the crash.
	for i, group := range state.ParityGroups {
		if before, ok := beforeChecksums[group.ParityGroupID]; ok {
			// If it's the same group but generation changed, it was modified
			if before != "" && group.Generation > 0 {
				// Recompute from disk
				parityPath := filepath.Join(filepath.Dir(metadataPath), "parity", group.ParityGroupID+".bin")
				b, err := os.ReadFile(parityPath)
				if err == nil {
					state.ParityGroups[i].ParityChecksum = blake3Hex(b)
				} else {
					// We couldn't read the parity file. This is highly unexpected for a committed
					// transaction, but if it happens, we clear the checksum so scrub will
					// correctly identify it as missing and repair it.
					state.ParityGroups[i].ParityChecksum = ""
				}
			}
		}
	}

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
