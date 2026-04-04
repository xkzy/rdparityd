package journal

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
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
