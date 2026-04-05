package journal

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestCoordinatorRebuildDataDiskRestoresMissingExtents(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/rebuild.bin",
		SizeBytes:   (2 << 20) + 99,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	targetDiskID := writeResult.Extents[0].DataDiskID
	deleted := make([]string, 0)
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != targetDiskID {
			continue
		}
		extentPath := filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)
		if err := os.Remove(extentPath); err != nil {
			t.Fatalf("Remove returned error: %v", err)
		}
		deleted = append(deleted, extent.ExtentID)
	}
	if len(deleted) == 0 {
		t.Fatal("expected at least one extent on the target disk")
	}

	result, err := coordinator.RebuildDataDisk(targetDiskID)
	if err != nil {
		t.Fatalf("RebuildDataDisk returned error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy rebuild result, got %#v", result)
	}
	if result.ExtentsRebuilt != len(deleted) {
		t.Fatalf("expected %d rebuilt extents, got %d", len(deleted), result.ExtentsRebuilt)
	}
	for _, extentID := range deleted {
		if !slices.Contains(result.RebuiltExtentIDs, extentID) {
			t.Fatalf("expected rebuilt extent %q in %#v", extentID, result.RebuiltExtentIDs)
		}
	}
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != targetDiskID {
			continue
		}
		extentPath := filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)
		if _, err := os.Stat(extentPath); err != nil {
			t.Fatalf("expected rebuilt extent file %s to exist: %v", extent.PhysicalLocator.RelativePath, err)
		}
	}
}

func TestCoordinatorRebuildAllDataDisksRestoresMultipleMissingExtents(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/rebuild-all.bin",
		SizeBytes:   (3 << 20) + 99,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	selected := make([]metadata.Extent, 0, 2)
	usedDisks := make(map[string]bool)
	usedGroups := make(map[string]bool)
	for _, extent := range writeResult.Extents {
		if usedDisks[extent.DataDiskID] || usedGroups[extent.ParityGroupID] {
			continue
		}
		selected = append(selected, extent)
		usedDisks[extent.DataDiskID] = true
		usedGroups[extent.ParityGroupID] = true
	}
	if len(selected) < 2 {
		t.Fatalf("expected a recoverable multi-disk selection, got %#v", writeResult.Extents)
	}

	deletedByDisk := make(map[string]int)
	for _, extent := range selected {
		extentPath := filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)
		if err := os.Remove(extentPath); err != nil {
			t.Fatalf("Remove returned error: %v", err)
		}
		deletedByDisk[extent.DataDiskID]++
	}

	result, err := coordinator.RebuildAllDataDisks()
	if err != nil {
		t.Fatalf("RebuildAllDataDisks returned error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy rebuild-all result, got %#v", result)
	}
	if result.DisksRebuilt != len(deletedByDisk) {
		t.Fatalf("expected %d disks rebuilt, got %d", len(deletedByDisk), result.DisksRebuilt)
	}
	if result.ExtentsRebuilt != len(selected) {
		t.Fatalf("expected %d rebuilt extents, got %d", len(selected), result.ExtentsRebuilt)
	}
	if len(result.Results) != len(deletedByDisk) {
		t.Fatalf("expected %d per-disk results, got %d", len(deletedByDisk), len(result.Results))
	}
	for _, diskResult := range result.Results {
		if !diskResult.Healthy {
			t.Fatalf("expected healthy per-disk result, got %#v", diskResult)
		}
		if expected, ok := deletedByDisk[diskResult.DiskID]; ok && diskResult.ExtentsRebuilt != expected {
			t.Fatalf("expected %d rebuilt extents for %s, got %d", expected, diskResult.DiskID, diskResult.ExtentsRebuilt)
		}
	}
}

// TestRebuildProgressPersistenceAndResume verifies that rebuild progress is
// correctly saved, loaded, and used to skip already-rebuilt extents on the
// next run (resume semantics).
func TestRebuildProgressPersistenceAndResume(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/rebuild-resume.bin",
		SizeBytes:   (2 << 20) + 1,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID

	// Save a progress file claiming one extent is already done.
	fakeCompleted := []string{writeResult.Extents[0].ExtentID}
	progress := RebuildProgress{
		DiskID:           diskID,
		CompletedExtents: fakeCompleted,
	}
	if err := saveRebuildProgress(metadataPath, progress); err != nil {
		t.Fatalf("saveRebuildProgress: %v", err)
	}

	// Load it back and verify round-trip fidelity.
	loaded, err := loadRebuildProgress(metadataPath, diskID)
	if err != nil {
		t.Fatalf("loadRebuildProgress: %v", err)
	}
	if len(loaded.CompletedExtents) != 1 || loaded.CompletedExtents[0] != fakeCompleted[0] {
		t.Fatalf("loaded progress mismatch: %#v", loaded)
	}

	// Delete an extent file so rebuild has real work to do for remaining extents.
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			continue
		}
		// Only delete extents NOT in fakeCompleted so the skipping logic has
		// extents to skip.
		alreadyDone := false
		for _, id := range fakeCompleted {
			if id == extent.ExtentID {
				alreadyDone = true
				break
			}
		}
		if alreadyDone {
			continue
		}
		path := filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)
		_ = os.Remove(path)
	}

	// Rebuild — it should resume, skip the pre-completed extent, and fix the rest.
	result, err := coordinator.RebuildDataDisk(diskID)
	if err != nil {
		t.Fatalf("RebuildDataDisk (resume): %v", err)
	}
	if !result.Resumed {
		t.Error("expected Resumed=true when progress file exists")
	}
	if result.ExtentsSkipped == 0 {
		t.Error("expected at least one skipped extent on resume")
	}

	// After a fully successful rebuild the progress file should be gone.
	if _, statErr := os.Stat(progressPath(metadataPath, diskID)); !os.IsNotExist(statErr) {
		t.Error("expected progress file to be removed after successful rebuild")
	}
}

// TestRebuildProgressChecksumRejection verifies that a tampered progress file
// is detected and the rebuild starts from scratch.
func TestRebuildProgressChecksumRejection(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	diskID := "disk-01"
	path := progressPath(metadataPath, diskID)

	// Write a valid progress file first.
	prog := RebuildProgress{
		DiskID:           diskID,
		CompletedExtents: []string{"extent-abc"},
	}
	if err := saveRebuildProgress(metadataPath, prog); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Corrupt one byte of the payload.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) > rebuildProgressHdr {
		data[rebuildProgressHdr] ^= 0xFF
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	// Load should return an error.
	if _, err := loadRebuildProgress(metadataPath, diskID); err == nil {
		t.Fatal("expected checksum error for corrupted progress file")
	}
}
