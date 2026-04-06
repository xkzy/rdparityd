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

package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreSaveLoadRoundTrip(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "metadata.json"))
	state := PrototypeState("demo")

	allocator := NewAllocator(&state)
	_, extents, err := allocator.AllocateFile("/shares/media/movie.mkv", 2*state.Pool.ExtentSizeBytes+123)
	if err != nil {
		t.Fatalf("AllocateFile returned error: %v", err)
	}
	if len(extents) != 3 {
		t.Fatalf("expected 3 extents, got %d", len(extents))
	}

	saved, err := store.Save(state)
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if saved.StateChecksum == "" {
		t.Fatal("expected state checksum to be set")
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if loaded.Pool.Name != "demo" {
		t.Fatalf("unexpected pool name: %q", loaded.Pool.Name)
	}
	if len(loaded.Files) != 1 || len(loaded.Extents) != 3 {
		t.Fatalf("unexpected loaded state sizes: files=%d extents=%d", len(loaded.Files), len(loaded.Extents))
	}
}

func TestStoreLoadRejectsTamperedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.bin")
	store := NewStore(path)
	state := PrototypeState("demo")
	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	// The payload starts at offset 72 (after the 72-byte binary header).
	// Flip a byte in the payload so the BLAKE3 hash no longer matches.
	if len(contents) <= 72 {
		t.Fatalf("snapshot file too short to tamper (%d bytes)", len(contents))
	}
	contents[72] ^= 0xFF
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err = store.Load()
	if err == nil {
		t.Fatal("expected checksum validation error")
	}
}

func TestStoreSaveLoadRoundTripOnFreshNestedPath(t *testing.T) {
	root := t.TempDir()
	store := NewStore(filepath.Join(root, "state", "snapshots", "metadata.bin"))
	state := PrototypeState("nested")

	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if loaded.Pool.Name != "nested" {
		t.Fatalf("unexpected pool name: %q", loaded.Pool.Name)
	}
}

func TestAllocatorUsesOnlineDataDisksAndSplitsByExtentSize(t *testing.T) {
	state := PrototypeState("demo")
	allocator := NewAllocator(&state)

	file, extents, err := allocator.AllocateFile("/shares/docs/archive.tar", 2*state.Pool.ExtentSizeBytes+1)
	if err != nil {
		t.Fatalf("AllocateFile returned error: %v", err)
	}
	if file.SizeBytes != 2*state.Pool.ExtentSizeBytes+1 {
		t.Fatalf("unexpected file size: %d", file.SizeBytes)
	}
	if len(extents) != 3 {
		t.Fatalf("expected 3 extents, got %d", len(extents))
	}
	if extents[0].DataDiskID == extents[1].DataDiskID {
		t.Fatalf("expected allocator to spread early extents across data disks, got %q twice", extents[0].DataDiskID)
	}
	if extents[2].LogicalOffset != 2*state.Pool.ExtentSizeBytes {
		t.Fatalf("unexpected final logical offset: %d", extents[2].LogicalOffset)
	}
	if extents[2].Length != 1 {
		t.Fatalf("expected tail extent length of 1, got %d", extents[2].Length)
	}
}

func TestLoadOrCreateReturnsSavedStateWhenFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.bin")
	store := NewStore(path)
	state := PrototypeState("loadorcreate")
	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	loaded, err := store.LoadOrCreate(PrototypeState("other"))
	if err != nil {
		t.Fatalf("LoadOrCreate returned error: %v", err)
	}
	if loaded.Pool.Name != "loadorcreate" {
		t.Fatalf("expected pool name %q, got %q", "loadorcreate", loaded.Pool.Name)
	}
}

func TestLoadOrCreateSavesAndReturnsDefaultWhenMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh", "metadata.bin")
	store := NewStore(path)
	defaultState := PrototypeState("freshpool")

	loaded, err := store.LoadOrCreate(defaultState)
	if err != nil {
		t.Fatalf("LoadOrCreate returned error: %v", err)
	}
	if loaded.Pool.Name != "freshpool" {
		t.Fatalf("expected pool name %q, got %q", "freshpool", loaded.Pool.Name)
	}

	// File must now exist.
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("expected metadata file to be created: %v", statErr)
	}
}

func TestStoreSaveLoadRoundTripWithParityGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.bin")
	store := NewStore(path)
	state := PrototypeState("pgtest")

	state.ParityGroups = []ParityGroup{
		{
			ParityGroupID:   "pg-001",
			ParityDiskID:    "disk-parity",
			MemberExtentIDs: []string{"ext-001", "ext-002"},
			ParityChecksum:  "abcdef1234567890",
			Generation:      7,
		},
		{
			ParityGroupID:   "pg-002",
			ParityDiskID:    "disk-parity",
			MemberExtentIDs: []string{"ext-003"},
			ParityChecksum:  "fedcba9876543210",
			Generation:      3,
		},
	}

	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(loaded.ParityGroups) != 2 {
		t.Fatalf("expected 2 parity groups, got %d", len(loaded.ParityGroups))
	}
	pg := loaded.ParityGroups[0]
	if pg.ParityGroupID != "pg-001" {
		t.Fatalf("unexpected parity group id: %q", pg.ParityGroupID)
	}
	if pg.ParityDiskID != "disk-parity" {
		t.Fatalf("unexpected parity disk id: %q", pg.ParityDiskID)
	}
	if len(pg.MemberExtentIDs) != 2 || pg.MemberExtentIDs[0] != "ext-001" || pg.MemberExtentIDs[1] != "ext-002" {
		t.Fatalf("unexpected member extent ids: %v", pg.MemberExtentIDs)
	}
	if pg.ParityChecksum != "abcdef1234567890" {
		t.Fatalf("unexpected parity checksum: %q", pg.ParityChecksum)
	}
	if pg.Generation != 7 {
		t.Fatalf("unexpected generation: %d", pg.Generation)
	}
}

func TestStoreSaveLoadRoundTripWithScrubHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.bin")
	store := NewStore(path)
	state := PrototypeState("scrubtest")

	now := PrototypeState("x").Pool.CreatedAt
	state.ScrubHistory = []ScrubRun{
		{
			RunID:                "scrub-111",
			StartedAt:            now,
			CompletedAt:          now,
			Repair:               true,
			Healthy:              false,
			FilesChecked:         5,
			ExtentsChecked:       10,
			ParityGroupsChecked:  3,
			HealedCount:          2,
			FailedCount:          1,
			IssueCount:           3,
			HealedExtentIDs:      []string{"ext-a", "ext-b"},
			HealedParityGroupIDs: []string{"pg-x"},
		},
	}

	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(loaded.ScrubHistory) != 1 {
		t.Fatalf("expected 1 scrub run, got %d", len(loaded.ScrubHistory))
	}
	sr := loaded.ScrubHistory[0]
	if sr.RunID != "scrub-111" {
		t.Fatalf("unexpected run id: %q", sr.RunID)
	}
	if !sr.Repair {
		t.Fatal("expected Repair=true")
	}
	if sr.Healthy {
		t.Fatal("expected Healthy=false")
	}
	if sr.FilesChecked != 5 {
		t.Fatalf("unexpected FilesChecked: %d", sr.FilesChecked)
	}
	if sr.ExtentsChecked != 10 {
		t.Fatalf("unexpected ExtentsChecked: %d", sr.ExtentsChecked)
	}
	if sr.ParityGroupsChecked != 3 {
		t.Fatalf("unexpected ParityGroupsChecked: %d", sr.ParityGroupsChecked)
	}
	if sr.HealedCount != 2 {
		t.Fatalf("unexpected HealedCount: %d", sr.HealedCount)
	}
	if sr.FailedCount != 1 {
		t.Fatalf("unexpected FailedCount: %d", sr.FailedCount)
	}
	if sr.IssueCount != 3 {
		t.Fatalf("unexpected IssueCount: %d", sr.IssueCount)
	}
	if len(sr.HealedExtentIDs) != 2 || sr.HealedExtentIDs[0] != "ext-a" {
		t.Fatalf("unexpected HealedExtentIDs: %v", sr.HealedExtentIDs)
	}
	if len(sr.HealedParityGroupIDs) != 1 || sr.HealedParityGroupIDs[0] != "pg-x" {
		t.Fatalf("unexpected HealedParityGroupIDs: %v", sr.HealedParityGroupIDs)
	}
}

func TestChecksumStateIsDeterministic(t *testing.T) {
	state := PrototypeState("checksumtest")

	h1 := checksumState(state)
	h2 := checksumState(state)

	if h1 != h2 {
		t.Fatal("checksumState should be deterministic")
	}
	if h1 == "" {
		t.Fatal("checksumState should not return empty string")
	}
	if len(h1) != 64 {
		t.Fatalf("expected 64-char hex digest, got %d chars", len(h1))
	}

	// Changing the state should change the checksum.
	state.Pool.Name = "different"
	h3 := checksumState(state)
	if h1 == h3 {
		t.Fatal("checksumState should differ for different state")
	}
}
