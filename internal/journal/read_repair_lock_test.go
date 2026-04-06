package journal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestReadRepairBlocksOnExclusiveOperationLock(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord1 := NewCoordinator(metaPath, journalPath)
	coord2 := NewCoordinator(metaPath, journalPath)

	writeResult, err := coord1.WriteFile(context.Background(), WriteRequest{
		PoolName:       "read-repair-lock",
		LogicalPath:    "/test/repair.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected extents")
	}

	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt extent: %v", err)
	}

	lock, err := coord1.acquireExclusiveOperationLock()
	if err != nil {
		t.Fatalf("acquireExclusiveOperationLock: %v", err)
	}
	defer lock.release()

	done := make(chan error, 1)
	go func() {
		_, err := coord2.ReadFile("/test/repair.bin")
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("read repair completed while lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
		// expected: blocked on pool lock
	}

	if err := lock.release(); err != nil {
		t.Fatalf("release lock: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read repair after unlock failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read repair did not complete after lock release")
	}
}

func TestReadRepairAndConcurrentWriteRemainConsistent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	writeResult, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "read-repair-consistency",
		LogicalPath:    "/test/repair.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("initial WriteFile: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected extents")
	}

	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt extent: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/repair.bin")
		readDone <- err
	}()

	writeDone := make(chan error, 1)
	go func() {
		_, err := NewCoordinator(metaPath, journalPath).WriteFile(context.Background(), WriteRequest{
			PoolName:       "read-repair-consistency",
			LogicalPath:    "/test/new.bin",
			AllowSynthetic: true,
			SizeBytes:      4096,
		})
		writeDone <- err
	}()

	if err := <-readDone; err != nil {
		t.Fatalf("read repair failed: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("concurrent write failed: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed: %v", err)
	}
	if len(state.Files) != 2 {
		t.Fatalf("expected 2 files after read-repair + write, got %d", len(state.Files))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated: %v", violations[0])
	}
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal replay summary failed: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal should be clean after read-repair + write: %+v", summary)
	}
}
