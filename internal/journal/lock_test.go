package journal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestExclusiveOperationLockBlocksConcurrentWrite(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")

	coord1 := NewCoordinator(metaPath, journalPath)
	coord2 := NewCoordinator(metaPath, journalPath)

	lock, err := coord1.acquireExclusiveOperationLock()
	if err != nil {
		t.Fatalf("acquireExclusiveOperationLock: %v", err)
	}
	defer lock.release()

	done := make(chan error, 1)
	go func() {
		_, err := coord2.WriteFile(context.Background(), WriteRequest{
			PoolName:       "lock-test",
			LogicalPath:    "/test/blocked-write.bin",
			AllowSynthetic: true,
			SizeBytes:      4096,
		})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("write completed while lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
		// expected: blocked on metadata lock
	}

	if err := lock.release(); err != nil {
		t.Fatalf("release lock: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after unlock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not complete after lock release")
	}
}

func TestExclusiveOperationLockBlocksConcurrentRecovery(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")

	writer := NewCoordinator(metaPath, journalPath)
	_, err := writer.WriteFile(context.Background(), WriteRequest{
		PoolName:       "lock-recovery",
		LogicalPath:    "/test/recover.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateParityWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	coord1 := NewCoordinator(metaPath, journalPath)
	coord2 := NewCoordinator(metaPath, journalPath)

	lock, err := coord1.acquireExclusiveOperationLock()
	if err != nil {
		t.Fatalf("acquireExclusiveOperationLock: %v", err)
	}
	defer lock.release()

	done := make(chan error, 1)
	go func() {
		_, err := coord2.RecoverWithState(metadata.PrototypeState("lock-recovery"))
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("recovery completed while lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
		// expected
	}

	if err := lock.release(); err != nil {
		t.Fatalf("release lock: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("recovery after unlock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not complete after lock release")
	}
}
