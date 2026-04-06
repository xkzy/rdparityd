package journal

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestRenameFileSamePathForcesRecoveryAndPreservesRecoveredFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := makePayload(4096, 0x44)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "rename-noop-recovery",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        payload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if !res.ReplayRequired {
		t.Fatal("expected replay-required pending write")
	}

	mut := NewCoordinator(metaPath, journalPath)
	result, err := mut.RenameFile("/test/pending.bin", "/test/pending.bin")
	if err != nil {
		t.Fatalf("same-path RenameFile should force recovery then succeed: %v", err)
	}
	if result.OldPath != "/test/pending.bin" || result.NewPath != "/test/pending.bin" {
		t.Fatalf("unexpected rename result: %+v", result)
	}

	read, err := mut.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile after same-path rename: %v", err)
	}
	if !bytes.Equal(read.Data, payload) {
		t.Fatal("pending file data mismatch after same-path rename recovery")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after same-path rename: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after same-path rename: %+v", summary)
	}
}

func TestRenameFileSamePathRequiresExistingFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.RenameFile("/test/missing.bin", "/test/missing.bin")
	if err == nil {
		t.Fatal("expected same-path rename of missing file to fail")
	}
}
