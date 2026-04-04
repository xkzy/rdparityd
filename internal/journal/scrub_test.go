package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCoordinatorScrubReportsHealthyPool(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	_, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/scrub-healthy.bin",
		SizeBytes:   8192,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coordinator.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub returned error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy scrub result, got %#v", result)
	}
	if result.FilesChecked != 1 {
		t.Fatalf("expected 1 file checked, got %d", result.FilesChecked)
	}
	if result.ExtentsChecked != 1 {
		t.Fatalf("expected 1 extent checked, got %d", result.ExtentsChecked)
	}
	if result.HealedCount != 0 {
		t.Fatalf("expected no healed extents, got %d", result.HealedCount)
	}
	if result.FailedCount != 0 {
		t.Fatalf("expected no failed extents, got %d", result.FailedCount)
	}
}

func TestCoordinatorScrubRepairsCorruptedExtent(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/scrub-heal.bin",
		SizeBytes:   8192,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	extentPath := filepath.Join(filepath.Dir(metadataPath), writeResult.Extents[0].PhysicalLocator.RelativePath)
	original, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	corrupted := append([]byte(nil), original...)
	corrupted[0] ^= 0xAA
	if err := os.WriteFile(extentPath, corrupted, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coordinator.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub returned error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy scrub result after repair, got %#v", result)
	}
	if result.HealedCount != 1 {
		t.Fatalf("expected 1 healed extent, got %d", result.HealedCount)
	}
	if !slices.Contains(result.HealedExtentIDs, writeResult.Extents[0].ExtentID) {
		t.Fatalf("expected healed extent %q in %#v", writeResult.Extents[0].ExtentID, result.HealedExtentIDs)
	}

	repaired, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !bytes.Equal(repaired, original) {
		t.Fatal("expected scrub to restore the original extent bytes")
	}
}
