package journal

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestScrubForcesRecoveryBeforeScanningMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := makePayload(4096, 0x61)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "scrub-recovery-guard",
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

	scrubber := NewCoordinator(metaPath, journalPath)
	result, err := scrubber.Scrub(false)
	if err != nil {
		t.Fatalf("Scrub should force recovery before scanning: %v", err)
	}
	if result.ExtentsChecked == 0 {
		t.Fatal("expected scrub to inspect recovered extents")
	}

	read, err := scrubber.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile pending after scrub-triggered recovery: %v", err)
	}
	if !bytes.Equal(read.Data, payload) {
		t.Fatal("pending file data mismatch after scrub-triggered recovery")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after scrub: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after scrub-triggered recovery: %+v", summary)
	}
}

func TestRebuildForcesRecoveryBeforeSelectingExtents(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := makePayload(4096, 0x72)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "rebuild-recovery-guard",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        payload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if len(res.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}
	targetDiskID := res.Extents[0].DataDiskID

	rebuilder := NewCoordinator(metaPath, journalPath)
	result, err := rebuilder.RebuildDataDisk(targetDiskID)
	if err != nil {
		t.Fatalf("RebuildDataDisk should force recovery before selecting extents: %v", err)
	}
	if result.ExtentsScanned == 0 {
		t.Fatal("expected rebuild to scan recovered extents")
	}

	read, err := rebuilder.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile pending after rebuild-triggered recovery: %v", err)
	}
	if !bytes.Equal(read.Data, payload) {
		t.Fatal("pending file data mismatch after rebuild-triggered recovery")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after rebuild: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after rebuild-triggered recovery: %+v", summary)
	}
}
