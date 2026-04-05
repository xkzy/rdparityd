package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// operationLock is a cross-process advisory lock protecting all metadata/journal
// mutations for a pool. It complements Coordinator.mu, which only protects a
// single Coordinator instance in-process.
type operationLock struct {
	file *os.File
}

func (l *operationLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	defer func() {
		l.file = nil
	}()
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		_ = l.file.Close()
		return fmt.Errorf("unlock metadata lock: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close metadata lock: %w", err)
	}
	return nil
}

// acquireExclusiveOperationLock serializes all mutating operations across
// multiple Coordinator instances and processes that target the same metadata
// path. Without this lock, concurrent recovery/write/rebuild/scrub operations
// can each load, mutate, and commit divergent states.
func (c *Coordinator) acquireExclusiveOperationLock() (*operationLock, error) {
	if c == nil {
		return nil, fmt.Errorf("coordinator is nil")
	}
	lockPath := c.metadataPath + ".lock"
	if err := ensureDir(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create metadata lock directory: %w", err)
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open metadata lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire metadata lock: %w", err)
	}
	return &operationLock{file: f}, nil
}
