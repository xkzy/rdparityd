package journal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReadMetaForcesRecoveryWhenJournalReplayIsPending(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "meta-view-recovery",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	}); err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}

	reader := NewCoordinator(metaPath, journalPath)
	state, err := reader.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta should force recovery and succeed: %v", err)
	}
	found := false
	for _, f := range state.Files {
		if f.Path == "/test/pending.bin" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("ReadMeta did not expose recovered pending file")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after ReadMeta-triggered recovery: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after ReadMeta-triggered recovery: %+v", summary)
	}
}

func TestPoolNameForcesRecoveryWhenJournalReplayIsPending(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "poolname-recovery",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	}); err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}

	reader := NewCoordinator(metaPath, journalPath)
	name, err := reader.PoolName()
	if err != nil {
		t.Fatalf("PoolName should force recovery and succeed: %v", err)
	}
	if name != "poolname-recovery" {
		t.Fatalf("expected recovered pool name %q, got %q", "poolname-recovery", name)
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after PoolName-triggered recovery: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after PoolName-triggered recovery: %+v", summary)
	}
}
