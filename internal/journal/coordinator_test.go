package journal

import (
	"path/filepath"
	"testing"

	"github.com/rtparityd/rtparityd/internal/metadata"
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
}
