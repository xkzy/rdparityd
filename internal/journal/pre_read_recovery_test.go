package journal

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestReadFileForcesRecoveryWhenJournalReplayIsPending(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := makePayload(4096, 0x5a)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "pre-read-recovery",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        payload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if res.FinalState != StateDataWritten || !res.ReplayRequired {
		t.Fatalf("expected StateDataWritten + replay required, got state=%s replay=%v", res.FinalState, res.ReplayRequired)
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay before read: %v", err)
	}
	if !summary.RequiresReplay {
		t.Fatalf("expected journal to require replay before read, got %+v", summary)
	}

	reader := NewCoordinator(metaPath, journalPath)
	read, err := reader.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile should force recovery and succeed: %v", err)
	}
	if !bytes.Equal(read.Data, payload) {
		t.Fatal("ReadFile returned wrong data after pre-read recovery")
	}

	summary, err = NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after read-triggered recovery: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after read-triggered recovery: %+v", summary)
	}
}
