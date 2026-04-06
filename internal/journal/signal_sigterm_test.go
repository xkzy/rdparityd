package journal

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestSignalHandling(t *testing.T) {
	t.Skip("Signal injection requires running daemon process")
}

func TestGracefulShutdownOnSIGTERM(t *testing.T) {
	t.Skip("Signal injection requires running daemon process")
}

func TestScrubCancelledByContext(t *testing.T) {
	t.Skip("Context cancellation during scrub tested via ScrubContext")
}

func TestRebuildCancelledByContext(t *testing.T) {
	t.Skip("Context cancellation during rebuild tested via RebuildDataDiskContext")
}

func TestSignalInterruptDuringReplay(t *testing.T) {
	t.Skip("Signal injection requires running daemon process")
}

func TestSignalTrap(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		time.Sleep(100 * time.Millisecond)
		syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	}()

	select {
	case sig := <-sigCh:
		t.Logf("received signal %v", sig)
	case <-time.After(5 * time.Second):
		t.Error("timeout waiting for signal")
	}
}
