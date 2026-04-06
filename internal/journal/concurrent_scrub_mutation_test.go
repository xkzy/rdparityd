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
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentScrubAndWrite(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubDoesNotBlockWrites(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubSeesConsistentState(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestMutateAndScrubRace(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubProgressIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	metadataPath := filepath.Join(tmpDir, "metadata.bin")
	journalPath := filepath.Join(tmpDir, "journal.bin")

	c1 := NewCoordinator(metadataPath, journalPath)
	c2 := NewCoordinator(metadataPath, journalPath)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		coord := c1
		coord.Scrub(false)
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		defer wg.Done()
		coord := c2
		coord.Scrub(false)
	}()

	wg.Wait()
	t.Log("both scrubs completed without deadlock")
}
