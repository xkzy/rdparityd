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
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestScrubInterruptionPersistsProgressAndResumes(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	writeResult, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "scrub-resume",
		LogicalPath:    "/test/large.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 4096, // 3 extents on default geometry
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if len(writeResult.Extents) < 2 {
		t.Fatalf("expected multiple extents, got %d", len(writeResult.Extents))
	}

	// Corrupt one early extent to trigger repair crash while later extents can
	// still be checkpointed as completed.
	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt extent: %v", err)
	}

	first, err := coord.scrubWithRepairFailAfter(true, StatePrepared)
	if err != nil {
		t.Fatalf("scrubWithRepairFailAfter: %v", err)
	}
	if first.FailedCount == 0 {
		t.Fatal("expected interrupted scrub to report a failed repair")
	}

	progressPath := scrubProgressPath(metaPath)
	if _, err := os.Stat(progressPath); err != nil {
		t.Fatalf("expected scrub progress file after interruption: %v", err)
	}
	progress, err := loadScrubProgress(metaPath)
	if err != nil {
		t.Fatalf("loadScrubProgress: %v", err)
	}
	if len(progress.CompletedExtents) == 0 {
		t.Fatal("expected at least one completed extent in scrub progress")
	}

	resumed, err := NewCoordinator(metaPath, journalPath).Scrub(true)
	if err != nil {
		t.Fatalf("resumed Scrub(true): %v", err)
	}
	if !resumed.Resumed {
		t.Fatal("expected resumed scrub to report Resumed=true")
	}
	if resumed.ExtentsSkipped == 0 && resumed.ParityGroupsSkipped == 0 {
		t.Fatal("expected resumed scrub to skip previously checkpointed work")
	}
	if !resumed.Healthy {
		t.Fatalf("expected resumed scrub to finish healthy, got %+v", resumed)
	}
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Fatalf("expected scrub progress file to be removed after successful resume, err=%v", err)
	}
}

func TestScrubProgressModeMismatchStartsFresh(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "scrub-mode",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	progress := ScrubProgress{
		Repair:           true,
		CompletedExtents: []string{"extent-000001"},
	}
	if err := saveScrubProgress(metaPath, progress); err != nil {
		t.Fatalf("saveScrubProgress: %v", err)
	}

	// An audit-only scrub must not incorrectly reuse repair-mode progress.
	result, err := NewCoordinator(metaPath, journalPath).Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false): %v", err)
	}
	if result.Resumed {
		t.Fatal("expected audit-only scrub to ignore repair-mode progress and start fresh")
	}
}
