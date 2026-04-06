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
