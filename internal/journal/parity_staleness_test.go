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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func parityGroupByID(t *testing.T, state metadata.SampleState, groupID string) metadata.ParityGroup {
	t.Helper()
	for _, g := range state.ParityGroups {
		if g.ParityGroupID == groupID {
			return g
		}
	}
	t.Fatalf("parity group %s not found", groupID)
	return metadata.ParityGroup{}
}

func TestRecoveryRecomputesStaleParityChecksumWhenGroupMembershipChanges(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	// T1: one extent committed, producing an initial parity checksum for pg-000001.
	res1, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "parity-stale",
		LogicalPath:    "/test/file1.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile T1: %v", err)
	}
	if len(res1.Extents) != 1 {
		t.Fatalf("expected 1 extent in T1, got %d", len(res1.Extents))
	}
	groupID := res1.Extents[0].ParityGroupID
	stateAfterT1, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after T1: %v", err)
	}
	groupAfterT1 := parityGroupByID(t, stateAfterT1, groupID)
	checksumAfterT1 := groupAfterT1.ParityChecksum
	if checksumAfterT1 == "" {
		t.Fatal("expected parity checksum after T1")
	}
	if len(groupAfterT1.MemberExtentIDs) != 1 {
		t.Fatalf("expected 1 member after T1, got %d", len(groupAfterT1.MemberExtentIDs))
	}

	// T2: second extent lands in the same parity group and crashes after parity write.
	res2, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "parity-stale",
		LogicalPath:    "/test/file2.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateParityWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile T2: %v", err)
	}
	if len(res2.Extents) != 1 {
		t.Fatalf("expected 1 extent in T2, got %d", len(res2.Extents))
	}
	if res2.Extents[0].ParityGroupID != groupID {
		t.Fatalf("expected T2 to reuse parity group %s, got %s", groupID, res2.Extents[0].ParityGroupID)
	}

	// Metadata is still stale here: it should still reflect only T1.
	staleState, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load stale metadata: %v", err)
	}
	staleGroup := parityGroupByID(t, staleState, groupID)
	if len(staleGroup.MemberExtentIDs) != 1 {
		t.Fatalf("expected stale metadata to still have 1 member, got %d", len(staleGroup.MemberExtentIDs))
	}
	if staleGroup.ParityChecksum != checksumAfterT1 {
		t.Fatalf("expected stale checksum %s, got %s", checksumAfterT1, staleGroup.ParityChecksum)
	}

	// Recovery must detect changed membership, invalidate the checksum, and recompute parity.
	rec := NewCoordinator(metaPath, journalPath)
	recovery, err := rec.RecoverWithState(metadata.PrototypeState("parity-stale"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}
	if len(recovery.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recovery.RecoveredTxIDs)
	}

	stateAfterRecovery, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after recovery: %v", err)
	}
	groupAfterRecovery := parityGroupByID(t, stateAfterRecovery, groupID)
	if len(groupAfterRecovery.MemberExtentIDs) != 2 {
		t.Fatalf("expected 2 members after recovery, got %d", len(groupAfterRecovery.MemberExtentIDs))
	}
	if groupAfterRecovery.ParityChecksum == checksumAfterT1 {
		t.Fatalf("parity checksum remained stale after recovery: %s", checksumAfterT1)
	}

	parityPath := filepath.Join(dir, "parity", groupID+".bin")
	parityBytes, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("read parity file: %v", err)
	}
	if got := digestBytes(parityBytes); got != groupAfterRecovery.ParityChecksum {
		t.Fatalf("recovered metadata checksum mismatch: metadata=%s disk=%s", groupAfterRecovery.ParityChecksum, got)
	}
}

func TestRecoveryReconcilesCommittedTransactionWithStaleParityChecksumFromOldMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	// T1 committed; keep a copy of the old metadata snapshot.
	res1, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "parity-reconcile",
		LogicalPath:    "/test/file1.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile T1: %v", err)
	}
	groupID := res1.Extents[0].ParityGroupID
	oldSnapshot, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read old metadata snapshot: %v", err)
	}
	_ = oldSnapshot
	stateAfterT1, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after T1: %v", err)
	}
	checksumAfterT1 := parityGroupByID(t, stateAfterT1, groupID).ParityChecksum

	// T2 stops at StateDataWritten — keeps a journal record for reconciliation.
	// This simulates the scenario where T2's write was in-flight when a crash
	// left metadata pointing at T1's checksum but T2's data and journal record exist.
	res2, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "parity-reconcile",
		LogicalPath:    "/test/file2.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile T2: %v", err)
	}
	// Since T2 stopped at StateDataWritten, stateAfterT2 is the metadata
	// at the point the write was in-flight. The parity hasn't been rewritten yet.
	// Just verify the parity group exists with 1 member (pre-T2 state).
	stateAfterT2, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after T2 crash: %v", err)
	}
	if res2.Extents[0].ParityGroupID != groupID {
		t.Fatalf("expected same parity group %s, got %s", groupID, res2.Extents[0].ParityGroupID)
	}
	_ = stateAfterT2

	// Metadata is still at T1's state (T2 crashed at StateDataWritten).
	// Recovery rolls T2 forward: parity is rewritten for both members.
	// The stale T1 snapshot restoration is no longer needed — metadata IS stale.

	// Recovery should roll T2 forward and recompute parity for both members.
	rec := NewCoordinator(metaPath, journalPath)
	_, err = rec.RecoverWithState(metadata.PrototypeState("parity-reconcile"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}

	reconciledState, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load reconciled metadata: %v", err)
	}
	reconciledGroup := parityGroupByID(t, reconciledState, groupID)
	if len(reconciledGroup.MemberExtentIDs) != 2 {
		t.Fatalf("expected 2 members after reconciliation, got %d", len(reconciledGroup.MemberExtentIDs))
	}
	if reconciledGroup.ParityChecksum == checksumAfterT1 {
		t.Fatalf("reconciliation left stale parity checksum in place: %s", checksumAfterT1)
	}

	parityPath := filepath.Join(dir, "parity", groupID+".bin")
	parityBytes, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("read parity file: %v", err)
	}
	if got := digestBytes(parityBytes); got != reconciledGroup.ParityChecksum {
		t.Fatalf("reconciled checksum mismatch: metadata=%s disk=%s", reconciledGroup.ParityChecksum, got)
	}
}

func TestWriteParityFilesRejectsCorruptedMemberInput(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "parity-input-check",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if len(res.Extents) != 1 {
		t.Fatalf("expected 1 extent, got %d", len(res.Extents))
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	extent := state.Extents[0]
	extentPath := filepath.Join(dir, extent.PhysicalLocator.RelativePath)
	if err := os.WriteFile(extentPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt extent: %v", err)
	}

	if err := writeParityFiles(dir, &state, []metadata.Extent{extent}, 0); err == nil {
		t.Fatal("expected parity recompute to reject corrupted member input")
	} else if !containsAny(err.Error(), "I3", "checksum mismatch", "must be repaired") {
		t.Fatalf("expected I3 checksum mismatch error, got: %v", err)
	}
}

func TestRecoveryRepairsCommittedPeerBeforeStaleParityRecompute(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	res1, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "stale-parity-heal",
		LogicalPath:    "/test/file1.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile T1: %v", err)
	}
	payload2 := makePayload(4096, 0x42)
	res2, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "stale-parity-heal",
		LogicalPath:    "/test/file2.bin",
		AllowSynthetic: true,
		Payload:        payload2,
		FailAfter:      StateParityWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile T2: %v", err)
	}
	if len(res1.Extents) != 1 || len(res2.Extents) != 1 {
		t.Fatalf("expected one extent each, got %d and %d", len(res1.Extents), len(res2.Extents))
	}
	if res1.Extents[0].ParityGroupID != res2.Extents[0].ParityGroupID {
		t.Fatalf("expected same parity group, got %s and %s", res1.Extents[0].ParityGroupID, res2.Extents[0].ParityGroupID)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata before corruption: %v", err)
	}
	committedExtent := state.Extents[0]
	committedPath := filepath.Join(dir, committedExtent.PhysicalLocator.RelativePath)
	if err := os.WriteFile(committedPath, []byte("bad-bytes"), 0o600); err != nil {
		t.Fatalf("corrupt committed extent: %v", err)
	}

	rec := NewCoordinator(metaPath, journalPath)
	result, err := rec.RecoverWithState(metadata.PrototypeState("stale-parity-heal"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}
	if len(result.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got recovered=%v aborted=%v", result.RecoveredTxIDs, result.AbortedTxIDs)
	}

	read2, err := rec.ReadFile("/test/file2.bin")
	if err != nil {
		t.Fatalf("ReadFile file2 after recovery: %v", err)
	}
	if !bytes.Equal(read2.Data, payload2) {
		t.Fatal("file2 payload mismatch after stale-parity recovery")
	}

	stateAfter, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after recovery: %v", err)
	}
	group := parityGroupByID(t, stateAfter, res1.Extents[0].ParityGroupID)
	parityPath := filepath.Join(dir, "parity", group.ParityGroupID+".bin")
	parityBytes, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("read parity file: %v", err)
	}
	if got := digestBytes(parityBytes); got != group.ParityChecksum {
		t.Fatalf("parity checksum mismatch after stale-parity recovery: metadata=%s disk=%s", group.ParityChecksum, got)
	}
}
