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

// invariant_enforcement_test.go — Tests for each enforcement gap identified
// in docs/invariants.md. Every test is labelled with the invariant ID it
// exercises (I3, I5, I6, I8, I11, I12, I13, I15) and the gap description.
//
// These tests are distinct from the failure-matrix tests: they target the
// specific code guards added to coordinator.go, repair.go, reader.go, and
// recovery.go rather than end-to-end crash scenarios.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── I3/I9: Post-write readback in runExtentRepair ───────────────────────────

// TestI3_RepairReadbackCatchesTornWrite verifies that if the extent file on
// disk contains wrong bytes after replaceSyncFile (simulated by corrupting the
// file between write and readback), runExtentRepair returns an error rather
// than journaling a false "committed" repair record.
//
// Implementation note: we cannot hook between replaceSyncFile and the readback
// in a unit test without side-channel corruption, so this test instead verifies
// the sunny-day path: a correctly repaired extent passes the readback.
func TestI3_PostWriteReadbackRejectsLengthMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "extent.bin")
	extent := metadata.Extent{
		ExtentID:    "extent-i3-length",
		Length:      4,
		ChecksumAlg: ChecksumAlgorithm,
		Checksum:    digestBytes([]byte("ABCD")),
	}
	if err := os.WriteFile(path, []byte("ABCDTRAILER"), 0o600); err != nil {
		t.Fatalf("write extent: %v", err)
	}
	if err := verifyOnDiskExtentBytes(path, extent); err == nil {
		t.Fatal("expected post-write verifier to reject trailing bytes")
	} else if !containsAny(err.Error(), "length mismatch") {
		t.Fatalf("expected length mismatch error, got: %v", err)
	}
}

func TestI3_RepairReadbackVerifiesSunnyDay(t *testing.T) {
	dir := t.TempDir()
	meta, journal, payload := writeAndCommit(t, dir, "/test/i3.bin", 1<<20, 3)

	// Remove the first extent to force repair on next read.
	state, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("no extents")
	}
	extentPath := filepath.Join(dir, state.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("remove extent: %v", err)
	}

	// Read must succeed via self-heal (repair readback passes).
	coord := NewCoordinator(meta, journal)
	result, err := coord.ReadFile("/test/i3.bin")
	if err != nil {
		t.Fatalf("ReadFile after extent removal: %v", err)
	}
	if !bytes.Equal(result.Data, payload) {
		t.Fatal("repaired data does not match original payload")
	}
	// The extent file must be back on disk (repair wrote it back).
	if _, err := os.Stat(extentPath); err != nil {
		t.Fatal("repaired extent file not present on disk after self-heal")
	}
}

// ─── I5: commitState rejects structurally invalid state ──────────────────────

// TestI5_CommitStateRejectsDuplicateExtentID verifies that commitState returns
// an error (M3 violation) when the state contains two extents with the same ID.
// This prevents corrupt metadata from ever being written to disk.
func TestI5_CommitStateRejectsDuplicateExtentID(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	state := metadata.PrototypeState("i5-test")
	// Add two extents with the same ExtentID.
	dup := metadata.Extent{
		ExtentID:        "duplicate-extent",
		FileID:          "file-1",
		DataDiskID:      "disk-01",
		ChecksumAlg:     ChecksumAlgorithm,
		Checksum:        "abc",
		Length:          1024,
		LogicalOffset:   0,
		PhysicalLocator: metadata.Locator{RelativePath: "data/duplicate.bin"},
	}
	state.Extents = append(state.Extents, dup, dup)

	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.commitState(state)
	if err == nil {
		t.Fatal("expected I5 error for duplicate extent ID, got nil")
	}
	if !containsAny(err.Error(), "I5", "M3", "duplicate") {
		t.Fatalf("expected I5/M3 error, got: %v", err)
	}
}

// TestI5_CommitStateRejectsDanglingFileRef verifies that commitState returns
// an error (M1 violation) when an extent references a file_id that does not
// exist in state.Files.
func TestI5_CommitStateRejectsDanglingFileRef(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	state := metadata.PrototypeState("i5-m1-test")
	state.Extents = append(state.Extents, metadata.Extent{
		ExtentID:        "orphan-extent",
		FileID:          "nonexistent-file-id", // dangling reference
		DataDiskID:      "disk-01",
		ChecksumAlg:     ChecksumAlgorithm,
		Checksum:        "abc",
		Length:          1024,
		LogicalOffset:   0,
		PhysicalLocator: metadata.Locator{RelativePath: "data/orphan.bin"},
	})

	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.commitState(state)
	if err == nil {
		t.Fatal("expected I5 error for dangling file ref, got nil")
	}
	if !containsAny(err.Error(), "I5", "M1", "file_id") {
		t.Fatalf("expected I5/M1 error, got: %v", err)
	}
}

// TestI5_CommitStateAllowsReplayRequired verifies that commitState does NOT
// reject state that has a transaction with ReplayRequired=true. This is the
// intentional write-path state at StateMetadataWritten; rejecting it would
// break the crash-recovery breadcrumb mechanism (J3 is post-recovery only).
func TestI5_CommitStateAllowsReplayRequired(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	state := metadata.PrototypeState("i5-j3-test")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:           "tx-in-flight",
		State:          "replay-required",
		ReplayRequired: true,
		OldGeneration:  1,
		NewGeneration:  2,
	})

	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.commitState(state)
	// Must succeed: J3 is not checked in the pre-commit guard.
	if err != nil {
		t.Fatalf("commitState must allow ReplayRequired=true (J3 is post-recovery only): %v", err)
	}
}

// ─── I11: Monotonic generation enforcement ───────────────────────────────────

// TestI11_CommitStateRejectsNonMonotonicGeneration verifies that commitState
// refuses a state whose transaction count is strictly less than the cached
// committed state's transaction count.
func TestI11_CommitStateRejectsNonMonotonicGeneration(t *testing.T) {
	dir := t.TempDir()
	meta, journal, _ := writeAndCommit(t, dir, "/test/i11.bin", 1<<20, 11)

	// Load the committed state (generation = 1 transaction).
	coord := NewCoordinator(meta, journal)
	state, err := coord.loadState(metadata.PrototypeState("i11"))
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}

	// Attempt to commit a state with fewer transactions (generation rollback).
	olderState := metadata.PrototypeState("i11")
	olderState.Transactions = state.Transactions[:0] // 0 transactions < 1

	_, err = coord.commitState(olderState)
	if err == nil {
		t.Fatal("expected I11 error for non-monotonic generation, got nil")
	}
	if !containsAny(err.Error(), "I11", "non-monotonic", "generation") {
		t.Fatalf("expected I11 error, got: %v", err)
	}
}

// TestI11_CommitStateSameGenerationIsAllowed verifies that commitState allows
// the same transaction count (equal generation) because recovery may replay
// and re-commit the same generation without incrementing.
func TestI11_CommitStateSameGenerationIsAllowed(t *testing.T) {
	dir := t.TempDir()
	meta, journal, _ := writeAndCommit(t, dir, "/test/i11same.bin", 512, 111)

	coord := NewCoordinator(meta, journal)
	state, err := coord.loadState(metadata.PrototypeState("i11"))
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}
	// Re-commit the exact same state (generation unchanged).
	_, err = coord.commitState(state)
	if err != nil {
		t.Fatalf("commitState with same generation must succeed: %v", err)
	}
}

// ─── I12: Single authoritative recovery ──────────────────────────────────────

// TestI12_CorruptMetadataAndEmptyJournalRefusesStart verifies that Recover()
// returns an error when metadata is corrupt AND the journal is empty. The
// system must not silently start with the prototype state over committed data.
func TestI12_CorruptMetadataAndEmptyJournalRefusesStart(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Write corrupt (non-empty) metadata: something that is not a valid snapshot.
	if err := os.WriteFile(metaPath, []byte("this is not a valid metadata snapshot"), 0o600); err != nil {
		t.Fatalf("write corrupt metadata: %v", err)
	}
	// Journal does not exist (first boot would have no journal either, but
	// here we explicitly test that a corrupt metadata file + empty journal
	// is rejected, not silently treated as "first boot").
	// Create an empty journal file so the journal load succeeds with 0 records.
	if err := os.WriteFile(journalPath, []byte{}, 0o600); err != nil {
		t.Fatalf("create empty journal: %v", err)
	}

	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.Recover()
	if err == nil {
		t.Fatal("expected I12 refusal for corrupt metadata + empty journal, got nil")
	}
	if !containsAny(err.Error(), "I12", "corrupt", "authoritative", "manual") {
		t.Fatalf("expected I12 error, got: %v", err)
	}
}

// TestI12_MissingMetadataAndEmptyJournalIsFirstBoot verifies that Recover()
// succeeds (first-boot scenario) when neither the metadata file nor the
// journal file exist. This is distinct from the corrupt-metadata case.
func TestI12_MissingMetadataAndEmptyJournalIsFirstBoot(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	// Neither file exists — true first boot.

	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.Recover()
	// First boot: no data ever committed, safe to start with empty state.
	if err != nil {
		t.Fatalf("first-boot recovery must succeed: %v", err)
	}
}

// TestI12_CorruptMetadataWithJournalSucceeds verifies that Recover() succeeds
// when metadata is corrupt BUT the journal contains incomplete records.
// The journal takes precedence for in-flight transactions. Post-compaction,
// committed transactions are in metadata only; the journal carries the pending
// record (left by writeAndCommit helper).
func TestI12_CorruptMetadataWithJournalSucceeds(t *testing.T) {
	dir := t.TempDir()
	meta, journal, _ := writeAndCommit(t, dir, "/test/i12.bin", 512, 12)

	// Corrupt the metadata file.
	if err := os.WriteFile(meta, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt metadata: %v", err)
	}

	coord := NewCoordinator(meta, journal)
	recResult, err := coord.Recover()
	if err != nil {
		t.Fatalf("Recover with corrupt metadata + valid journal: %v", err)
	}
	// Recovery must complete and clean up the journal (pending record resolved).
	t.Logf("I12: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journal).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after I12 recovery: %+v", summary)
	}
}

// ─── I13: Read correctness — length assertion ─────────────────────────────────

// TestI13_ReadReturnsExactlyCommittedBytes verifies that ReadFile returns
// exactly file.SizeBytes of data, not more and not less.
func TestI13_ReadReturnsExactlyCommittedBytes(t *testing.T) {
	for _, size := range []int{512, 1 << 20, (1 << 20) + 7, 3 * (1 << 20)} {
		size := size
		t.Run("size", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			meta, journal, payload := writeAndCommit(t, dir, "/test/i13.bin", size, 13)
			coord := NewCoordinator(meta, journal)
			result, err := coord.ReadFile("/test/i13.bin")
			if err != nil {
				t.Fatalf("ReadFile size=%d: %v", size, err)
			}
			if len(result.Data) != size {
				t.Fatalf("I13 length mismatch: expected %d got %d", size, len(result.Data))
			}
			if !bytes.Equal(result.Data, payload) {
				t.Fatal("I13 data content mismatch")
			}
		})
	}
}

// TestI13_ReadRejectsOverlongExtentFile verifies that an extent with the right
// checksum prefix but extra trailing bytes is treated as corruption and healed,
// rather than being silently accepted after truncation.
func TestI13_ReadRejectsOverlongExtentFile(t *testing.T) {
	dir := t.TempDir()
	meta, journal, payload := writeAndCommit(t, dir, "/test/i13-overlong.bin", 1<<20, 14)

	state, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("no extents")
	}
	target := state.Extents[0]
	path := filepath.Join(dir, target.PhysicalLocator.RelativePath)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open extent for append: %v", err)
	}
	if _, err := f.Write([]byte("TRAILING-GARBAGE")); err != nil {
		f.Close()
		t.Fatalf("append garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close appended extent: %v", err)
	}

	coord := NewCoordinator(meta, journal)
	result, err := coord.ReadFile("/test/i13-overlong.bin")
	if err != nil {
		t.Fatalf("ReadFile overlong extent: %v", err)
	}
	if !bytes.Equal(result.Data, payload) {
		t.Fatal("repaired data mismatch after overlong extent corruption")
	}
	// The extent may be healed either during the read itself or during the
	// forced pre-read recovery pass if another pending journal transaction was
	// present. The correctness requirement is that the file bytes are right and
	// the on-disk extent is restored to its committed length.

	restored, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read healed extent: %v", err)
	}
	if int64(len(restored)) != target.Length {
		t.Fatalf("healed extent length mismatch: expected %d got %d", target.Length, len(restored))
	}
}

// ─── I8: Post-rebuild integrity check ────────────────────────────────────────

// TestI8_RebuildProducesVerifiedExtents verifies that after RebuildDataDisk(),
// all rebuilt extents satisfy E1 (disk bytes match stored checksum) and the
// data round-trips correctly.
func TestI8_RebuildProducesVerifiedExtents(t *testing.T) {
	dir := t.TempDir()
	meta, journal, payload := writeAndCommit(t, dir, "/test/i8.bin", 1<<20, 8)

	// Identify the first extent and which disk it lives on.
	state, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("no extents")
	}
	targetDisk := state.Extents[0].DataDiskID

	// Remove the extent file to simulate disk failure.
	extentPath := filepath.Join(dir, state.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("remove extent: %v", err)
	}

	// Rebuild the disk.
	coord := NewCoordinator(meta, journal)
	_, err = coord.RebuildDataDisk(targetDisk)
	if err != nil {
		t.Fatalf("RebuildDataDisk: %v", err)
	}

	// I8: verify all extents pass E1 after rebuild.
	reloadedState, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load metadata post-rebuild: %v", err)
	}
	if vs := CheckIntegrityInvariants(dir, reloadedState); len(vs) > 0 {
		for _, v := range vs {
			t.Errorf("I8 post-rebuild invariant violation: %v", v)
		}
	}

	// Data must round-trip correctly.
	result, err := coord.ReadFile("/test/i8.bin")
	if err != nil {
		t.Fatalf("ReadFile after rebuild: %v", err)
	}
	if !bytes.Equal(result.Data, payload) {
		t.Fatal("data mismatch after rebuild")
	}
}

// ─── I6: Replay idempotence ───────────────────────────────────────────────────

// TestI6_MergeRecoveredFileIsUpsertNotAppend verifies that calling Recover()
// twice on the same journal produces the same file count, not a doubled list.
func TestI6_MergeRecoveredFileIsUpsertNotAppend(t *testing.T) {
	dir := t.TempDir()
	meta, journal, _ := writeAndCommit(t, dir, "/test/i6.bin", 512, 6)

	// First recovery.
	coord1 := NewCoordinator(meta, journal)
	_, err := coord1.Recover()
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	state1, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load after first recover: %v", err)
	}

	// Second recovery.
	coord2 := NewCoordinator(meta, journal)
	_, err = coord2.Recover()
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	state2, err := metadata.NewStore(meta).Load()
	if err != nil {
		t.Fatalf("load after second recover: %v", err)
	}

	// File count must be identical (upsert, not append).
	if len(state1.Files) != len(state2.Files) {
		t.Fatalf("I6: file count differs after double recovery: first=%d second=%d",
			len(state1.Files), len(state2.Files))
	}

	// Extent count must be identical.
	if len(state1.Extents) != len(state2.Extents) {
		t.Fatalf("I6: extent count differs after double recovery: first=%d second=%d",
			len(state1.Extents), len(state2.Extents))
	}
}

// ─── Helper ───────────────────────────────────────────────────────────────────

// containsAny returns true if s contains any of the given substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
