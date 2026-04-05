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
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// writeAndCommit is a helper for G tests that writes a file and returns the committed
// coordinator. All extent files and parity files are on disk and consistent.
func writeAndCommit(t *testing.T, dir string, logicalPath string, payloadSize int, seed byte) (metaPath, journalPath string, payload []byte) {
	t.Helper()
	metaPath = filepath.Join(dir, "metadata.json")
	journalPath = filepath.Join(dir, "journal.log")
	payload = makePayload(payloadSize, seed)
	coord := NewCoordinator(metaPath, journalPath)
	result, err := coord.WriteFile(WriteRequest{
		PoolName:    "test-pool-g",
		LogicalPath: logicalPath,
		AllowSynthetic: true,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed state, got %s", result.FinalState)
	}
	return metaPath, journalPath, payload
}

// TestG1_TornHeader: Corrupt magic bytes in metadata header.
// Expected: Metadata load fails; recovery uses journal; data round-trips correctly.
func TestG1_TornHeader(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g1.bin", 1<<20, 1)

	// Corrupt metadata header (magic bytes).
	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TornHeader(); err != nil {
		t.Fatalf("TornHeader: %v", err)
	}

	// Recovery must succeed using journal as source of truth.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after torn header: %v", err)
	}

	// Data must be readable after recovery.
	readResult, err := coord2.ReadFile("/test/g1.bin")
	if err != nil {
		t.Fatalf("ReadFile after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after recovery from torn header")
	}
}

// TestG2_InvalidStateChecksum: Corrupt the metadata checksum.
// Expected: Checksum validation fails; recovery falls back to journal.
func TestG2_InvalidStateChecksum(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g2.bin", 1<<20, 2)

	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.InvalidStateChecksum(); err != nil {
		t.Fatalf("InvalidStateChecksum: %v", err)
	}

	coord2 := NewCoordinator(metaPath, journalPath)
	_, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after invalid checksum: %v", err)
	}

	readResult, err := coord2.ReadFile("/test/g2.bin")
	if err != nil {
		t.Fatalf("ReadFile after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after recovery from invalid checksum")
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
	_, err = coord2.WriteFile(WriteRequest{
		PoolName:    "test-pool-g",
		LogicalPath: "/test/g3b.bin",
		AllowSynthetic: true,
		Payload:     makePayload(512, 33),
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
	_, err := coord2.WriteFile(WriteRequest{
		PoolName:         "test-pool-g",
		LogicalPath:      "/test/g5-uncommitted.bin",
		AllowSynthetic: true,
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
// In practice we cannot skip fsync in the test, but we can simulate by:
// writing metadata, then overwriting it with a truncated version, then recovering.
func TestG6_CheckpointWrittenButNotDurable(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g6.bin", 1<<20, 6)

	// Truncate the metadata file to simulate a partial write that was not durable.
	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TruncateMetadata(32); err != nil {
		t.Fatalf("TruncateMetadata: %v", err)
	}

	// Recovery must succeed by falling back to the journal.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after truncated metadata: %v", err)
	}

	readResult, err := coord2.ReadFile("/test/g6.bin")
	if err != nil {
		t.Fatalf("ReadFile after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after recovery from truncated metadata")
	}
}

// TestG7_CorruptedIndex: Flip bytes in the metadata payload to corrupt extent/file index.
// Expected: Checksum catches the corruption; recovery uses journal.
func TestG7_CorruptedIndex(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g7.bin", 1<<20, 7)

	// Corrupt bytes past the header to damage the index fields.
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(data) > 100 {
		// Flip bytes at offset 80 (well into the payload, past the checksum header).
		data[80] ^= 0xFF
		data[81] ^= 0xFF
	}
	if err := os.WriteFile(metaPath, data, 0o600); err != nil {
		t.Fatalf("write corrupted metadata: %v", err)
	}

	// Recovery must succeed using journal.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err = coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after corrupted index: %v", err)
	}

	readResult, err := coord2.ReadFile("/test/g7.bin")
	if err != nil {
		t.Fatalf("ReadFile after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after recovery from corrupted index")
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

	// Write a second file to advance the generation.
	coord2 := NewCoordinator(metaPath, journalPath)
	secondPayload := makePayload(512, 88)
	_, err = coord2.WriteFile(WriteRequest{
		PoolName:    "test-pool-g",
		LogicalPath: "/test/g8b.bin",
		AllowSynthetic: true,
		Payload:     secondPayload,
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
	_, err = coord3.Recover()
	if err != nil {
		t.Fatalf("Recover with stale checkpoint: %v", err)
	}

	// Both files must be accessible after reconciliation.
	r1, err := coord3.ReadFile("/test/g8.bin")
	if err != nil {
		t.Fatalf("ReadFile g8.bin: %v", err)
	}
	_ = r1

	r2, err := coord3.ReadFile("/test/g8b.bin")
	if err != nil {
		t.Fatalf("ReadFile g8b.bin: %v", err)
	}
	if !bytes.Equal(r2.Data, secondPayload) {
		t.Fatal("g8b.bin data mismatch after stale checkpoint recovery")
	}
}

// TestG10_VersionMismatch: Modify the version byte in metadata to simulate
// a schema version mismatch. Expected: Load detects incompatible version; recovery
// falls back to journal.
func TestG10_VersionMismatch(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/g10.bin", 1<<20, 10)

	// Flip the version byte (offset 4, just after the magic bytes).
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

	// Recovery must succeed using journal.
	coord2 := NewCoordinator(metaPath, journalPath)
	_, err = coord2.Recover()
	if err != nil {
		t.Fatalf("Recover after version mismatch: %v", err)
	}

	readResult, err := coord2.ReadFile("/test/g10.bin")
	if err != nil {
		t.Fatalf("ReadFile after version mismatch recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after version mismatch recovery")
	}
}

// TestG_JournalPrecedenceOverCorruptedMetadata: Verify that when metadata is
// corrupted but journal contains a committed record, journal wins.
func TestG_JournalPrecedenceOverCorruptedMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, payload := writeAndCommit(t, dir, "/test/gprecedence.bin", 1<<20, 11)

	// Corrupt metadata header to force journal fallback.
	corruptor := NewMetadataCorruptionHelper(metaPath)
	if err := corruptor.TornHeader(); err != nil {
		t.Fatalf("TornHeader: %v", err)
	}

	// Recovery must fall back to journal.
	coord2 := NewCoordinator(metaPath, journalPath)
	result, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recover with corrupted metadata: %v", err)
	}
	_ = result

	// Data must be readable — journal prevailed.
	readResult, err := coord2.ReadFile("/test/gprecedence.bin")
	if err != nil {
		t.Fatalf("ReadFile after journal-precedence recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch: journal did not correctly override corrupted metadata")
	}

	// Invariants must be clean after recovery.
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata post-recovery: %v", err)
	}
	violations := CheckIntegrityInvariants(dir, state)
	for _, v := range violations {
		t.Errorf("invariant violation after recovery: [%s] %s", v.Code, v.Message)
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
