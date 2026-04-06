package journal

// crash_during_preparity_heal_test.go
//
// Crash simulation matrix for the "repair-before-parity-rewrite" subphase
// introduced by ensureHealthyCommittedParityInputs.
//
// Scenario:
//   1. File A committed in parity group G
//   2. A's extent corrupted on disk
//   3. File B write starts — touches parity group G
//   4. ensureHealthyCommittedParityInputs repairs A (appending tx-repair records)
//   5. Crash at various points within/around the embedded repair
//
// All crash states must satisfy:
//   - recovery completes without error
//   - metadata/journal invariants hold
//   - A is readable after recovery
//   - journal is clean (no RequiresReplay)
//   - pool can accept new writes afterwards

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func timeNow() time.Time { return time.Now().UTC() }

func setupCorruptedGroupAndStartWrite(t *testing.T, dir string) (metaPath, journalPath, groupID, aPath string) {
	t.Helper()
	metaPath = filepath.Join(dir, "metadata.bin")
	journalPath = filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	resA, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "crash-heal-test",
		LogicalPath:    "/test/a.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile A: %v", err)
	}
	if len(resA.Extents) != 1 {
		t.Fatalf("expected 1 extent for A, got %d", len(resA.Extents))
	}
	groupID = resA.Extents[0].ParityGroupID
	aPath = filepath.Join(dir, resA.Extents[0].PhysicalLocator.RelativePath)

	data, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("read A: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(aPath, data, 0o600); err != nil {
		t.Fatalf("corrupt A: %v", err)
	}
	return metaPath, journalPath, groupID, aPath
}

func recoverAndVerifyClean(t *testing.T, metaPath, journalPath string) {
	t.Helper()
	rec := NewCoordinator(metaPath, journalPath)
	_, err := rec.RecoverWithState(metadata.PrototypeState("crash-heal-test"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after recovery: %+v", summary)
	}
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load: %v", err)
	}
	if vs := CheckStateInvariants(state); len(vs) > 0 {
		t.Fatalf("state invariants violated: %v", vs[0])
	}
}

// TestCrashBeforeAnyRepair_ThenWriteB simulates a write for B starting in the
// same parity group as corrupted A, then crashing before the embedded repair
// records are appended.  Recovery must abort the B write safely.
func TestCrashBeforeAnyRepair_ThenWriteB(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, groupID, _ := setupCorruptedGroupAndStartWrite(t, dir)
	_ = groupID

	// Crash B at StatePrepared — before ensureHealthyCommittedParityInputs runs.
	coord := NewCoordinator(metaPath, journalPath)
	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "crash-heal-test",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StatePrepared,
	})
	if err != nil {
		t.Fatalf("WriteFile B (crash at prepared): %v", err)
	}

	recoverAndVerifyClean(t, metaPath, journalPath)

	// A must still be readable — parity was never touched, so A is still
	// reconstructable from the original (pre-B) parity file.
	readA, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/a.bin")
	if err != nil {
		t.Fatalf("ReadFile A after crash-at-prepared recovery: %v", err)
	}
	if !readA.Verified {
		t.Fatal("A not verified after recovery")
	}
}

// TestCrashDuringEmbeddedRepair_RepairDataWritten simulates the embedded repair
// (for corrupted A) reaching StateDataWritten before a crash. The original write
// for B was still at StatePrepared. Recovery must:
//   - roll forward the repair (A is healed)
//   - abort the in-flight write for B
//   - leave A readable
func TestCrashDuringEmbeddedRepair_RepairDataWritten(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _, _ := setupCorruptedGroupAndStartWrite(t, dir)

	// We cannot inject a crash at the midpoint of ensureHealthyCommittedParityInputs
	// directly without a separate failAfter hook in that path.  Instead we
	// reproduce the exact on-disk state by manually appending the journal records
	// that would exist at that crash point.
	coord := NewCoordinator(metaPath, journalPath)
	jstore := NewStore(journalPath)

	// Load state to build the repair record from real extent metadata.
	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	var targetExtent metadata.Extent
	for _, e := range state.Extents {
		if e.FileID == "file-000001" {
			targetExtent = e
			break
		}
	}
	if targetExtent.ExtentID == "" {
		t.Fatal("could not find A's extent in metadata")
	}

	// Append: write tx for B @ prepared.
	bRecord := Record{
		TxID:          "tx-write-b-sim",
		Timestamp:     timeNow(),
		PoolName:      "crash-heal-test",
		LogicalPath:   "/test/b.bin",
		Extents:       []metadata.Extent{},
		File:          &metadata.FileRecord{FileID: "file-000002", Path: "/test/b.bin"},
		OldGeneration: int64(len(state.Transactions)),
		NewGeneration: int64(len(state.Transactions)) + 1,
	}
	if _, err := jstore.Append(withState(bRecord, StatePrepared)); err != nil {
		t.Fatalf("append write-b prepared: %v", err)
	}

	// Append: embedded repair tx for A @ data-written (crash happens here).
	repRecord := repairBaseRecord(state.Pool.Name, extentRepairPath(targetExtent.ExtentID), []metadata.Extent{targetExtent})
	if _, err := jstore.Append(withState(repRecord, StatePrepared)); err != nil {
		t.Fatalf("append repair prepared: %v", err)
	}
	// Simulate that the extent byte was already rewritten to healed content.
	// Use parity reconstruction (from the still-valid original parity).
	rootDir := filepath.Dir(metaPath)
	healed, err := reconstructExtent(rootDir, state, targetExtent)
	if err != nil {
		t.Fatalf("reconstructExtent for simulation: %v", err)
	}
	if err := replaceSyncFile(filepath.Join(rootDir, targetExtent.PhysicalLocator.RelativePath), healed, 0o600); err != nil {
		t.Fatalf("write healed extent: %v", err)
	}
	// crash: append StateDataWritten for repair but NOT StateCommitted.
	if _, err := jstore.Append(withState(repRecord, StateDataWritten)); err != nil {
		t.Fatalf("append repair data-written: %v", err)
	}

	// Now recover.
	recoverAndVerifyClean(t, metaPath, journalPath)

	// A must be readable (repair was rolled forward).
	readA, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/a.bin")
	if err != nil {
		t.Fatalf("ReadFile A after mid-repair crash: %v", err)
	}
	if !readA.Verified {
		t.Fatal("A not verified after mid-repair crash recovery")
	}

	// B must NOT exist (its write was aborted).
	if _, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/b.bin"); err == nil {
		t.Fatal("B should not exist after its write was aborted mid-repair")
	}
}

// TestCrashAfterEmbeddedRepairBeforeParityWrite verifies that if the embedded
// repair for A commits but the original write for B crashes at StateDataWritten
// (extent files written, parity not yet computed), recovery rolls forward B
// correctly using the now-healed A as a peer.
func TestCrashAfterEmbeddedRepairBeforeParityWrite(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _, _ := setupCorruptedGroupAndStartWrite(t, dir)
	coord := NewCoordinator(metaPath, journalPath)

	// Write B, stopping at StateDataWritten. ensureHealthyCommittedParityInputs
	// will run and repair A before writing B's extent; then B's extent is written
	// and journaled, then the crash is injected.
	resB, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "crash-heal-test",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile B (crash at data-written): %v", err)
	}
	if resB.FinalState != StateDataWritten {
		t.Fatalf("expected data-written, got %s", resB.FinalState)
	}

	recoverAndVerifyClean(t, metaPath, journalPath)

	// Both A and B must be readable.
	readA, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/a.bin")
	if err != nil {
		t.Fatalf("ReadFile A: %v", err)
	}
	if !readA.Verified {
		t.Fatal("A not verified after B's crash-at-data-written recovery")
	}
	readB, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/b.bin")
	if err != nil {
		t.Fatalf("ReadFile B: %v", err)
	}
	if !readB.Verified {
		t.Fatal("B not verified after crash-at-data-written recovery")
	}

	// Final scrub must be healthy.
	scrub, err := NewCoordinator(metaPath, journalPath).Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false): %v", err)
	}
	if !scrub.Healthy {
		t.Fatalf("pool not healthy after full recovery: %+v", scrub)
	}
}

// TestDoubleRecoveryAfterPreParityHealCrash verifies that running recovery
// twice after a crash during the embedded heal is fully idempotent.
func TestDoubleRecoveryAfterPreParityHealCrash(t *testing.T) {
	dir := t.TempDir()
	metaPath, journalPath, _, _ := setupCorruptedGroupAndStartWrite(t, dir)
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "crash-heal-test",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile B: %v", err)
	}

	// First recovery.
	rec1 := NewCoordinator(metaPath, journalPath)
	r1, err := rec1.RecoverWithState(metadata.PrototypeState("crash-heal-test"))
	if err != nil {
		t.Fatalf("First RecoverWithState: %v", err)
	}
	state1, _ := metadata.NewStore(metaPath).Load()

	// Second recovery (idempotent check).
	rec2 := NewCoordinator(metaPath, journalPath)
	r2, err := rec2.RecoverWithState(metadata.PrototypeState("crash-heal-test"))
	if err != nil {
		t.Fatalf("Second RecoverWithState: %v", err)
	}
	state2, _ := metadata.NewStore(metaPath).Load()

	if len(state2.Transactions) != len(state1.Transactions) {
		t.Fatalf("idempotency violated: transactions %d → %d", len(state1.Transactions), len(state2.Transactions))
	}
	if len(state2.Files) != len(state1.Files) {
		t.Fatalf("idempotency violated: files %d → %d", len(state1.Files), len(state2.Files))
	}
	if len(state2.ParityGroups) != len(state1.ParityGroups) {
		t.Fatalf("idempotency violated: parity groups %d → %d", len(state1.ParityGroups), len(state2.ParityGroups))
	}
	_ = r1
	_ = r2
}
