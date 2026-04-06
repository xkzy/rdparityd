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

// TestWriteDoesNotPoisonParityFromCorruptedCommittedExtent verifies that a new
// write joining an existing parity group does not recompute parity from a
// corrupted committed member. The write path must repair/validate existing
// members first, otherwise it can silently destroy recoverability.
func TestWriteDoesNotPoisonParityFromCorruptedCommittedExtent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	resA, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "poison-guard",
		LogicalPath:    "/test/a.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile A: %v", err)
	}
	if len(resA.Extents) != 1 {
		t.Fatalf("expected one extent for A, got %d", len(resA.Extents))
	}
	groupID := resA.Extents[0].ParityGroupID

	// Corrupt A after commit.
	aPath := filepath.Join(dir, resA.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("read A extent: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(aPath, data, 0o600); err != nil {
		t.Fatalf("corrupt A extent: %v", err)
	}

	// B should land in the same parity group and must not poison parity.
	resB, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "poison-guard",
		LogicalPath:    "/test/b.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile B: %v", err)
	}
	if len(resB.Extents) != 1 {
		t.Fatalf("expected one extent for B, got %d", len(resB.Extents))
	}
	if resB.Extents[0].ParityGroupID != groupID {
		t.Fatalf("expected B to join parity group %s, got %s", groupID, resB.Extents[0].ParityGroupID)
	}

	// A must still be reconstructable / readable after B's parity rewrite.
	readA, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/a.bin")
	if err != nil {
		t.Fatalf("ReadFile A after B write: %v", err)
	}
	if !readA.Verified {
		t.Fatal("A read not verified after B write")
	}
	// Stronger outcome is allowed: the write path may have repaired A before
	// recomputing parity, leaving nothing for the read path to heal.

	// Final scrub must report a healthy pool and no replay debt.
	scrub, err := NewCoordinator(metaPath, journalPath).Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false): %v", err)
	}
	if !scrub.Healthy {
		t.Fatalf("expected healthy pool after guarded parity rewrite, got %+v", scrub)
	}
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay: %+v", summary)
	}
}
