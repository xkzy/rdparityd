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
