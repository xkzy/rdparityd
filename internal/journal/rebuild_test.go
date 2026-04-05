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
