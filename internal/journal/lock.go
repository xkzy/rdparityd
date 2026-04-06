/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
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
//
// Uses non-blocking flock with retry to avoid indefinite blocking on stale locks.
func (c *Coordinator) acquireExclusiveOperationLock() (*operationLock, error) {
	if c == nil {
		return nil, fmt.Errorf("coordinator is nil")
	}
	lockPath := c.metadataPath + ".lock"
	if err := ensureDir(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create metadata lock directory: %w", err)
	}

	const (
		maxRetries   = 100
		retryDelay   = 100 * time.Millisecond
		totalTimeout = 10 * time.Second
	)

	var f *os.File
	var err error

	deadline := time.Now().Add(totalTimeout)

	for attempt := 0; attempt < maxRetries; attempt++ {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("acquire metadata lock: timeout after %v (stale lock)", totalTimeout)
		}

		f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open metadata lock file: %w", err)
		}

		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &operationLock{file: f}, nil
		}

		_ = f.Close()

		if !isBlockError(err) {
			return nil, fmt.Errorf("acquire metadata lock: %w", err)
		}

		time.Sleep(retryDelay)
	}

	return nil, fmt.Errorf("acquire metadata lock: gave up after %d attempts (stale lock)", maxRetries)
}

func isBlockError(err error) bool {
	return err == syscall.EWOULDBLOCK || err == syscall.EAGAIN
}
