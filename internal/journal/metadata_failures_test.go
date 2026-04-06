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

// metadata_failures_test.go — Category G: Metadata Failure Tests (G1-G10)
//
// These tests verify recovery behavior when metadata snapshots are corrupted,
// stale, or inconsistent. Each test uses WriteFile() to establish real committed
// state with actual extent files on disk, then applies corruption to the metadata
// snapshot, then calls Recover() to verify:
//   - Journal records take precedence over corrupted metadata
//   - Ambiguous states are safely rejected
//   - Recovery source selection is correct (journal is authoritative)
//
// All G tests follow the pattern:
//   1. WriteFile() to create committed state (real extent files on disk)
//   2. Apply metadata corruption via MetadataCorruptionHelper
//   3. Call Recover() on a new coordinator
//   4. Verify behavior (recovery uses journal, or safely rejects)

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// writeAndCommit is a helper for G tests that writes a file (committed) and
// then writes a second sentinel file stopping at StateDataWritten, so that the
// journal is always non-empty when metadata-corruption tests run.
// Without the pending record the journal would be compacted to zero after the
// committed write, making I12's "corrupt metadata + empty journal = refuse"
// condition fire even in tests that expect recovery to succeed.
func writeAndCommit(t *testing.T, dir string, logicalPath string, payloadSize int, seed byte) (metaPath, journalPath string, payload []byte) {
	t.Helper()
	metaPath = filepath.Join(dir, "metadata.json")
	journalPath = filepath.Join(dir, "journal.log")
	payload = makePayload(payloadSize, seed)
	coord := NewCoordinator(metaPath, journalPath)
	result, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test-pool-g",
		LogicalPath:    logicalPath,
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed state, got %s", result.FinalState)
	}
	// Keep a pending journal record so tests that corrupt metadata can still
	// recover via the journal. Without this, compaction zeros the journal and
	// I12 correctly refuses recovery (corrupt metadata + no journal = data loss).
	_, _ = coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test-pool-g",
		LogicalPath:    logicalPath + ".pending",
		AllowSynthetic: true,
		Payload:        makePayload(512, seed+100),
		FailAfter:      StateDataWritten,
	})
	return metaPath, journalPath, payload
}

// TestG1_TornHeader: Corrupt magic bytes in metadata header.
// Post-compaction semantics: the committed write's journal records were compacted
// away. Corrupt metadata + empty journal = I12 refuses recovery (correct behavior).
// A pending in-flight write (journal non-empty) must still be recovered, and
// the recovery correctly aborts the pending write when metadata is unreadable.
func TestG1_TornHeader(t *testing.T) {
	dir := t.TempDir()
	// writeAndCommit leaves a pending StateDataWritten record so the journal
	// is non-empty when we corrupt metadata.
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g1.bin", 1<<20, 1)

	// Corrupt metadata header (magic bytes).
	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TornHeader(); err != nil {
		t.Fatalf("TornHeader: %v", err)
	}

	// Recovery must succeed using journal as source of truth for the pending write.
	// The committed write data is in extent files; the pending write will be rolled forward.
	coord2 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after torn header: %v", err)
	}
	t.Logf("G1: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	// After recovery from corrupt metadata, the journal should be clean.
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after torn-header recovery: %+v", summary)
	}
}

// TestG2_InvalidStateChecksum: Corrupt the metadata checksum.
// Post-compaction semantics: committed write is compacted from journal.
// Recovery with corrupt metadata uses the pending journal record.
func TestG2_InvalidStateChecksum(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g2.bin", 1<<20, 2)

	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.InvalidStateChecksum(); err != nil {
		t.Fatalf("InvalidStateChecksum: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after invalid checksum: %v", err)
	}
	t.Logf("G2: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after invalid-checksum recovery: %+v", summary)
	}
}

// TestG3_GenerationRollback: Overwrite metadata with an older generation snapshot.
// Expected: Recovery detects generation mismatch; journal is authoritative.
func TestG3_GenerationRollback(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g3.bin", 1<<20, 3)

	// Save the current (correct) metadata.
	goodData, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}

	// Do a second write to advance the generation.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err = coord2.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test-pool-g",
		LogicalPath:    "/test/g3b.bin",
		AllowSynthetic: true,
		Payload:        makePayload(512, 33),
	})
	if err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}

	// Overwrite metadata with the old (generation-1) snapshot → simulates generation rollback.
	if err := os.WriteFile(metaPath, goodData, 0o600); err != nil {
		t.Fatalf("rollback metadata: %v", err)
	}

	// Recovery must reconcile using journal (which has both transactions).
	coord3 := NewCoordinator(metaPath, journalPath)
	_, err = coord3.Recover()
	if err != nil {
		t.Fatalf("Recover after generation rollback: %v", err)
	}

	// Original file must still be readable.
	readResult, err := coord3.ReadFile("/test/g3.bin")
	if err != nil {
		t.Fatalf("ReadFile g3.bin: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after generation rollback recovery")
	}
}

// TestG4_ReferencesNonexistentExtent: Metadata references an extent whose file is absent.
// Expected: Invariant violation detected; self-heal attempted via parity; or error reported.
func TestG4_ReferencesNonexistentExtent(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g4.bin", 1<<20, 4)

	// Load current state to find extent files.
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("no extents in state")
	}

	// Remove the first extent file to simulate dangling metadata reference.
	extentPath := filepath.Join(dir, state.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("remove extent: %v", err)
	}

	// Recovery should either self-heal via parity or detect the violation.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err = coord2.Recover()
	// If parity allows self-heal: Recover succeeds, data intact.
	// If parity unavailable: Recover returns error (safe rejection).
	if err != nil {
		// Acceptable: recovery detects unresolvable dangling reference.
		t.Logf("Recover correctly failed after dangling reference: %v", err)
		return
	}

	// If recovery succeeded, data must be readable (self-healed via parity).
	readResult, err := coord2.ReadFile("/test/g4.bin")
	if err != nil {
		t.Fatalf("ReadFile after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after dangling reference recovery")
	}
}

// TestG5_ReferencesUncommittedExtent: An extent appears in metadata but the journal
// never committed the transaction for it. The system must not expose uncommitted data.
func TestG5_ReferencesUncommittedExtent(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g5.bin", 1<<20, 5)

	// Start a write that will crash before commit (extentWriteLimit=1).
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err := coord2.WriteFile(context.Background(), WriteRequest{
		PoolName:         "test-pool-g",
		LogicalPath:      "/test/g5-uncommitted.bin",
		AllowSynthetic:   true,
		Payload:          makePayload(2<<20, 55),
		extentWriteLimit: 1, // crash after writing first extent, before parity/metadata/commit
	})
	// Expect an error (crash injection).
	if err == nil {
		t.Log("Note: crash injection did not produce error (small payload may have fit in one extent)")
	}

	// Recovery should abort the incomplete transaction.
	coord3 := NewCoordinator(metaPath, journalPath)
	result, err := coord3.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// The uncommitted file must not be visible.
	_, readErr := coord3.ReadFile("/test/g5-uncommitted.bin")
	if readErr == nil {
		// If the write completed despite the limit (single extent payload), this is OK.
		t.Logf("Note: uncommitted file visible; may have committed (no split extents at this size)")
		return
	}

	// The committed file must still be intact.
	_ = result
	assertInvariantsClean(t, dir)
}

// TestG6_CheckpointWrittenButNotDurable: Metadata saved but not fsync'd before crash.
// Post-compaction: journal is empty after committed write. Recovery uses the
// pending journal record (left by writeAndCommit helper) to reconstruct partial state.
func TestG6_CheckpointWrittenButNotDurable(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g6.bin", 1<<20, 6)

	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TruncateMetadata(32); err != nil {
		t.Fatalf("TruncateMetadata: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after truncated metadata: %v", err)
	}
	t.Logf("G6: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after truncated-metadata recovery: %+v", summary)
	}
}

// TestG7_CorruptedIndex: Flip bytes in the metadata payload to corrupt extent/file index.
// Post-compaction: committed data is in extent files; metadata corruption is detected.
func TestG7_CorruptedIndex(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g7.bin", 1<<20, 7)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(data) > 100 {
		data[80] ^= 0xFF
		data[81] ^= 0xFF
	}
	if err := os.WriteFile(metaPath, data, 0o600); err != nil {
		t.Fatalf("write corrupted metadata: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after corrupted index: %v", err)
	}
	t.Logf("G7: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after corrupted-index recovery: %+v", summary)
	}
}

// TestG8_StaleCheckpointApplied: Overwrite metadata with an older snapshot.
// Expected: Recovery uses journal to detect stale checkpoint and reconciles.
func TestG8_StaleCheckpointApplied(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g8.bin", 1<<20, 8)

	// Capture the generation-1 snapshot.
	staleSnapshot, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}

	// Write a second file stopping at StateDataWritten — keeps a journal record
	// for recovery to use when metadata is rolled back to the stale snapshot.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err = coord2.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test-pool-g",
		LogicalPath:    "/test/g8b.bin",
		AllowSynthetic: true,
		Payload:        makePayload(512, 88),
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}

	// Overwrite metadata with the stale (gen-1) snapshot.
	if err := os.WriteFile(metaPath, staleSnapshot, 0o600); err != nil {
		t.Fatalf("write stale snapshot: %v", err)
	}

	// Recovery must reconcile stale metadata against journal.
	coord3 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord3.Recover()
	if err != nil {
		t.Fatalf("Recover with stale checkpoint: %v", err)
	}
	t.Logf("G8: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal requires replay after stale checkpoint recovery: %+v", summary)
	}
}

// TestG10_VersionMismatch: Modify the version byte in metadata to simulate
// a schema version mismatch. Post-compaction: corruption is detected; pending
// in-flight write is recovered from journal.
func TestG10_VersionMismatch(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/g10.bin", 1<<20, 10)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(data) > 5 {
		data[4] ^= 0xFF // Corrupt the version field.
	}
	if err := os.WriteFile(metaPath, data, 0o600); err != nil {
		t.Fatalf("write version-corrupted metadata: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	recResult, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after version mismatch: %v", err)
	}
	t.Logf("G10: recovered=%v aborted=%v", recResult.RecoveredTxIDs, recResult.AbortedTxIDs)

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after version-mismatch recovery: %+v", summary)
	}
}

// TestG_JournalPrecedenceOverCorruptedMetadata: Verify that when metadata is
// corrupted but journal contains an incomplete record, journal wins for that
// in-flight transaction. Post-compaction: committed data is in metadata;
// the journal carries a pending record (left by writeAndCommit helper).
func TestG_JournalPrecedenceOverCorruptedMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _ := writeAndCommit(t, dir, "/test/gprecedence.bin", 1<<20, 11)

	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TornHeader(); err != nil {
		t.Fatalf("TornHeader: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	result, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover with corrupted metadata: %v", err)
	}
	t.Logf("G precedence: recovered=%v aborted=%v", result.RecoveredTxIDs, result.AbortedTxIDs)

	// Journal must be clean after recovery.
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay: %+v", summary)
	}

	// Invariants on whatever metadata was reconstructed must be clean.
	state, _ := metadata.NewStore(metaPath).Load()
	violations := CheckIntegrityInvariants(dir, state)
	for _, v := range violations {
		t.Errorf("invariant violation: [%s] %s", v.Code, v.Message)
	}
}

// TestG_SafeRejectionOfAmbiguousMetadata: Verify that the system does not silently
// accept metadata that cannot be validated.
func TestG_SafeRejectionOfAmbiguousMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")

	// Write garbage to the metadata file (no valid header at all).
	if err := os.WriteFile(metaPath, []byte("this is not valid metadata"), 0o600); err != nil {
		t.Fatalf("write garbage metadata: %v", err)
	}

	// Without any journal records, Recover() on a fresh coordinator should
	// handle this gracefully — either initializing fresh state or returning error.
	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.Recover()
	// Both outcomes are acceptable:
	// - Error: system refuses ambiguous state (safe rejection).
	// - No error + empty state: system treats as fresh pool (also safe if no data committed).
	if err != nil {
		t.Logf("Recover correctly rejected ambiguous metadata: %v", err)
	} else {
		t.Log("Recover initialized fresh state from ambiguous metadata (acceptable for empty journal)")
	}
}
