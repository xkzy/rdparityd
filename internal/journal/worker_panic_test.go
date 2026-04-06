package journal

import (
	"testing"
	"time"
)

func TestDiskWorkerPoolSurvivesPanic(t *testing.T) {
	pool := NewDiskWorkerPool("test-disk", 2)
	defer pool.Close()

	errCh := make(chan error, 2)

	pool.Submit(func() error {
		panic("test panic in worker")
	})
	pool.Submit(func() error {
		errCh <- nil
		return nil
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("second task failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("timeout waiting for task after panic")
	}

	t.Log("pool survived panic in worker task")
}

func TestDiskWorkerPoolCloseWhenIdle(t *testing.T) {
	pool := NewDiskWorkerPool("idle-disk", 1)

	pool.Submit(func() error {
		return nil
	})

	pool.Close()

	t.Log("pool closed successfully")
}
