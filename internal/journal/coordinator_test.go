package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestCoordinatorWriteFileCommitsMetadataAndJournal(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	result, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/write.bin",
		SizeBytes:   (1 << 20) + 42,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed final state, got %q", result.FinalState)
	}
	if len(result.Extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(result.Extents))
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("expected clean journal, got %#v", summary)
	}

	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata load returned error: %v", err)
	}
	if len(state.Files) != 1 || len(state.Extents) != 2 {
		t.Fatalf("unexpected state sizes: files=%d extents=%d", len(state.Files), len(state.Extents))
	}
	for _, extent := range result.Extents {
		if extent.Checksum == "" {
			t.Fatalf("expected checksum for extent %s", extent.ExtentID)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)); err != nil {
			t.Fatalf("expected extent file %s to exist: %v", extent.PhysicalLocator.RelativePath, err)
		}
	}
	if len(state.ParityGroups) == 0 {
		t.Fatal("expected at least one parity group to be persisted")
	}
	for _, group := range state.ParityGroups {
		if group.ParityChecksum == "" {
			t.Fatalf("expected parity checksum for group %s", group.ParityGroupID)
		}
		parityPath := filepath.Join(filepath.Dir(metadataPath), "parity", group.ParityGroupID+".bin")
		if _, err := os.Stat(parityPath); err != nil {
			t.Fatalf("expected parity file %s to exist: %v", parityPath, err)
		}
	}
}

func TestCoordinatorWriteFileCanStopBeforeCommit(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	result, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/crash.bin",
		SizeBytes:   4096,
		FailAfter:   StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if result.FinalState != StateDataWritten {
		t.Fatalf("expected data-written final state, got %q", result.FinalState)
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if !summary.RequiresReplay {
		t.Fatal("expected replay to be required after stopping early")
	}

	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata load returned error: %v", err)
	}
	if len(state.Files) != 0 || len(state.Extents) != 0 {
		t.Fatalf("expected metadata snapshot to remain unchanged before metadata-written stage, got files=%d extents=%d", len(state.Files), len(state.Extents))
	}
	for _, extent := range result.Extents {
		if _, err := os.Stat(filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)); err != nil {
			t.Fatalf("expected data-written extent file %s to exist: %v", extent.PhysicalLocator.RelativePath, err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(metadataPath), "parity", "pg-000001.bin")); err == nil {
		t.Fatal("did not expect parity file before parity-written stage")
	}
}

func TestCoordinatorRecoverRollsForwardDataWrittenTransaction(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	result, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/recover.bin",
		SizeBytes:   (1 << 20) + 99,
		FailAfter:   StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if result.FinalState != StateDataWritten {
		t.Fatalf("expected data-written final state, got %q", result.FinalState)
	}

	recovery, err := coordinator.Recover()
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(recovery.RecoveredTxIDs) != 1 {
		t.Fatalf("expected exactly one recovered tx, got %#v", recovery.RecoveredTxIDs)
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("expected clean journal after recovery, got %#v", summary)
	}

	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata load returned error: %v", err)
	}
	if len(state.Files) != 1 || len(state.Extents) != len(result.Extents) {
		t.Fatalf("expected recovered metadata to contain the written file and extents, got files=%d extents=%d", len(state.Files), len(state.Extents))
	}
	if len(state.ParityGroups) == 0 || state.ParityGroups[0].ParityChecksum == "" {
		t.Fatalf("expected parity metadata after recovery, got %#v", state.ParityGroups)
	}
}

func TestCoordinatorWriteFileWithRealPayload(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	payload := make([]byte, (1<<20)+512)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	result, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/real-data.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile with real payload returned error: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed state, got %q", result.FinalState)
	}
	if len(result.Extents) != 2 {
		t.Fatalf("expected 2 extents for payload, got %d", len(result.Extents))
	}
	for _, extent := range result.Extents {
		if extent.Checksum == "" {
			t.Fatalf("expected checksum for extent %s", extent.ExtentID)
		}
		if extent.ChecksumAlg != "sha256" {
			t.Fatalf("expected sha256 checksum alg for extent %s, got %q", extent.ExtentID, extent.ChecksumAlg)
		}
	}

	readResult, err := coordinator.ReadFile("/shares/demo/real-data.bin")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !readResult.Verified {
		t.Fatal("expected verified read")
	}
	if readResult.BytesRead != int64(len(payload)) {
		t.Fatalf("expected %d bytes read, got %d", len(payload), readResult.BytesRead)
	}

	// Verify the actual bytes round-trip correctly.
	for i := range payload {
		if readResult.Data[i] != payload[i] {
			t.Fatalf("data mismatch at byte %d: expected %d, got %d", i, payload[i], readResult.Data[i])
		}
	}
}
