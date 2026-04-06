package journal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// TestRecoveryDoesNotPoisonParityFromCorruptedCommittedExtent verifies that
// roll-forward from StateDataWritten repairs/validates committed peer extents
// before recomputing parity for newly-added members.
func TestRecoveryDoesNotPoisonParityFromCorruptedCommittedExtent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	resA, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "recovery-poison-guard",
		LogicalPath:    "/test/a.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile A: %v", err)
	}
	if len(resA.Extents) != 1 {
		t.Fatalf("expected one extent for A, got %d", len(resA.Extents))
	}
	groupID := resA.Extents[0].ParityGroupID

	aPath := filepath.Join(dir, resA.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("read A extent: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(aPath, data, 0o600); err != nil {
		t.Fatalf("corrupt A extent: %v", err)
	}

	resB, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "recovery-poison-guard",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile B: %v", err)
	}
	if len(resB.Extents) != 1 {
		t.Fatalf("expected one extent for B, got %d", len(resB.Extents))
	}
	if resB.Extents[0].ParityGroupID != groupID {
		t.Fatalf("expected B to join parity group %s, got %s", groupID, resB.Extents[0].ParityGroupID)
	}

	recovery, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("recovery-poison-guard"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}
	if len(recovery.RecoveredTxIDs) == 0 {
		t.Fatal("expected interrupted write B to be recovered")
	}

	readA, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/a.bin")
	if err != nil {
		t.Fatalf("ReadFile A after recovery: %v", err)
	}
	if !readA.Verified {
		t.Fatal("A read not verified after recovery")
	}

	scrub, err := NewCoordinator(metaPath, journalPath).Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false): %v", err)
	}
	if !scrub.Healthy {
		t.Fatalf("expected healthy pool after guarded recovery parity rewrite, got %+v", scrub)
	}
}
