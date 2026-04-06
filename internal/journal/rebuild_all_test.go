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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRebuildAllDataDisksNoExtents(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	result, err := coord.RebuildAllDataDisks()
	if err != nil {
		t.Fatalf("RebuildAllDataDisks returned error: %v", err)
	}
	if len(result.Results) != 0 {
		t.Fatalf("expected 0 disk results, got %d", len(result.Results))
	}
	if result.ExtentsRebuilt != 0 {
		t.Fatalf("expected 0 extents rebuilt, got %d", result.ExtentsRebuilt)
	}
}

func TestRebuildAllDataDisksRebuildsFromParity(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("rebuild-all-data-"), 65536)
	writeResult, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/rebuild-all/test.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}

	// Remove the first extent file to simulate data disk failure.
	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}

	result, err := coord.RebuildAllDataDisks()
	if err != nil {
		t.Fatalf("RebuildAllDataDisks returned error: %v", err)
	}
	if result.ExtentsRebuilt < 1 {
		t.Fatalf("expected at least 1 extent rebuilt, got %d", result.ExtentsRebuilt)
	}

	// Verify data integrity after rebuild.
	readResult, err := coord.ReadFile("/rebuild-all/test.bin")
	if err != nil {
		t.Fatalf("ReadFile after rebuild returned error: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after RebuildAllDataDisks")
	}
}

func TestRebuildAllDataDisksHealthyPoolIsNoOp(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	payload := bytes.Repeat([]byte("healthy-"), 65536)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/healthy/test.bin",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coord.RebuildAllDataDisks()
	if err != nil {
		t.Fatalf("RebuildAllDataDisks returned error: %v", err)
	}
	// All extents are healthy — none should need rebuilding.
	if result.ExtentsRebuilt != 0 {
		t.Fatalf("expected 0 extents rebuilt for healthy pool, got %d", result.ExtentsRebuilt)
	}
	if !result.Healthy {
		t.Fatal("expected healthy result for healthy pool")
	}
}
