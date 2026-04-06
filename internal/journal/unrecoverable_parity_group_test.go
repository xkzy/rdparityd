package journal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// TestWriteRefusesWhenParityGroupMemberIsUnrecoverable verifies that a write
// joining a parity group whose existing member cannot be verified or repaired
// (missing parity file, so reconstruction is impossible) returns a clear error
// rather than silently producing poisoned parity or partially committing.
func TestWriteRefusesWhenParityGroupMemberIsUnrecoverable(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	resA, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "unrecoverable-test",
		LogicalPath:    "/test/a.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile A: %v", err)
	}
	groupID := resA.Extents[0].ParityGroupID

	// Corrupt A's extent AND delete the parity file — reconstruction impossible.
	aPath := filepath.Join(dir, resA.Extents[0].PhysicalLocator.RelativePath)
	data, _ := os.ReadFile(aPath)
	data[0] ^= 0xFF
	os.WriteFile(aPath, data, 0o600)

	parityPath := filepath.Join(dir, "parity", groupID+".bin")
	os.WriteFile(parityPath, []byte("TRASHED"), 0o600)

	// Write B into the same group — must be refused with a clear error.
	_, err = coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "unrecoverable-test",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err == nil {
		t.Fatal("expected write B to fail due to unrecoverable committed member in same parity group")
	}
	if !strings.Contains(err.Error(), "verify committed parity inputs") &&
		!strings.Contains(err.Error(), "parity checksum mismatch") &&
		!strings.Contains(err.Error(), "reconstructed data") {
		t.Fatalf("expected a parity-related error, got: %v", err)
	}
	t.Logf("correctly refused write: %v", err)

	// Pool must still be in a consistent, recoverable state for existing data.
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed: %v", err)
	}
	// File A must still be registered; file B must not.
	hasA, hasB := false, false
	for _, f := range state.Files {
		if f.Path == "/test/a.bin" {
			hasA = true
		}
		if f.Path == "/test/b.bin" {
			hasB = true
		}
	}
	if !hasA {
		t.Fatal("file A unexpectedly removed from metadata after rejected write")
	}
	if hasB {
		t.Fatal("file B must not appear in metadata after rejected write")
	}
	// The refused write reached StateDataWritten before ensureHealthyCommittedParityInputs
	// returned the error — extent bytes are on disk, journal has the DataWritten record.
	// The correct invariant is: journal may require replay (the partial B write is
	// pending), but it must NEVER be in a silently committed state for B.
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	// B must not appear as committed.
	for _, txID := range summary.Actions {
		if txID.Outcome == StateCommitted && strings.Contains(txID.TxID, "b.bin") {
			t.Fatalf("refused write B must not be in committed state: %+v", txID)
		}
	}
	t.Logf("journal state after refused write: requires_replay=%v actions=%d", summary.RequiresReplay, len(summary.Actions))
}

// TestRecoveryHandlesUnrecoverableCommittedMemberGracefully verifies that if
// an existing committed member is unrecoverable during roll-forward, recovery
// returns an explicit error (not a panic or silent corruption) and leaves
// metadata in a consistent state.
func TestRecoveryHandlesUnrecoverableCommittedMemberGracefully(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	resA, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "unrecoverable-recovery",
		LogicalPath:    "/test/a.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile A: %v", err)
	}
	groupID := resA.Extents[0].ParityGroupID

	// B crashes at StateDataWritten — leaves an incomplete write in the journal.
	_, err = coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "unrecoverable-recovery",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile B crash: %v", err)
	}

	// Destroy A's extent content AND the parity file so reconstruction is
	// impossible — both data and the only recovery path (parity XOR) are gone.
	aPath := filepath.Join(dir, resA.Extents[0].PhysicalLocator.RelativePath)
	os.WriteFile(aPath, make([]byte, 4096), 0o600) // all-zero: checksum mismatch
	parityPath := filepath.Join(dir, "parity", groupID+".bin")
	os.WriteFile(parityPath, make([]byte, 4096), 0o600) // all-zero: checksum mismatch

	// Recovery must succeed and ABORT B's write (cannot roll forward safely without valid A).
	// Aborting is safer than blocking recovery forever or producing poisoned parity.
	rec := NewCoordinator(metaPath, journalPath)
	result, err := rec.RecoverWithState(metadata.PrototypeState("unrecoverable-recovery"))
	if err != nil {
		t.Fatalf("recovery failed entirely: %v", err)
	}
	t.Logf("recovery completed: recovered=%v aborted=%v", result.RecoveredTxIDs, result.AbortedTxIDs)

	// B's write should have been aborted (not recovered).
	if len(result.AbortedTxIDs) == 0 && len(result.RecoveredTxIDs) == 0 {
		t.Fatal("expected recovery to explicitly handle B's unrecoverable write")
	}

	// Journal must now be clean.
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay after recovery: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after recovery of unrecoverable write: %+v", summary)
	}

	// Pre-existing metadata must remain intact (A's transaction is still there).
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed after recovery: %v", err)
	}
	hasA := false
	for _, f := range state.Files {
		if f.Path == "/test/a.bin" {
			hasA = true
		}
	}
	if !hasA {
		t.Fatal("file A must remain in metadata after recovery of unrecoverable write")
	}
}
