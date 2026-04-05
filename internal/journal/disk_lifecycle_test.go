package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// TestAddDiskPersistsNewDisk verifies that AddDisk appends a disk to metadata
// and that the new disk is visible after reloading.
func TestAddDiskPersistsNewDisk(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	// Bootstrap metadata.
	if _, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/seed.bin",
		SizeBytes:   1024,
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := c.AddDisk("disk-new", metadata.DiskRoleData, "/mnt/new", 2<<40); err != nil {
		t.Fatalf("AddDisk: %v", err)
	}

	// Reload to verify persistence.
	c2 := NewCoordinator(metadataPath, journalPath)
	state, err := c2.metadata.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	found := false
	for _, d := range state.Disks {
		if d.DiskID == "disk-new" {
			found = true
			if d.Role != metadata.DiskRoleData {
				t.Errorf("expected role=%s got %s", metadata.DiskRoleData, d.Role)
			}
			if d.HealthStatus != "online" {
				t.Errorf("expected online status, got %q", d.HealthStatus)
			}
		}
	}
	if !found {
		t.Fatal("disk-new not found after AddDisk")
	}
}

// TestAddDiskRejectsDuplicate verifies that adding a disk with an existing
// DiskID is rejected.
func TestAddDiskRejectsDuplicate(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	if _, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/seed.bin",
		SizeBytes:   512,
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// "disk-01" already exists in PrototypeState.
	if err := c.AddDisk("disk-01", metadata.DiskRoleData, "/mnt/x", 1<<30); err == nil {
		t.Fatal("expected error when adding duplicate disk-01, got nil")
	}
}

// TestReplaceDiskUpdatesExtentOwnership verifies that all extents previously
// on the old disk are reassigned to the new disk.
func TestReplaceDiskUpdatesExtentOwnership(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	writeResult, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/replace-test.bin",
		SizeBytes:   (2 << 20) + 1,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	oldDiskID := writeResult.Extents[0].DataDiskID
	newDiskID := "disk-replacement"

	if err := c.ReplaceDisk(oldDiskID, newDiskID); err != nil {
		t.Fatalf("ReplaceDisk: %v", err)
	}

	// Reload and verify.
	state, err := c.metadata.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Old disk should no longer exist.
	for _, d := range state.Disks {
		if d.DiskID == oldDiskID {
			t.Fatalf("old disk %q still present after replace", oldDiskID)
		}
	}

	// New disk should exist.
	found := false
	for _, d := range state.Disks {
		if d.DiskID == newDiskID {
			found = true
		}
	}
	if !found {
		t.Fatalf("replacement disk %q not found", newDiskID)
	}

	// All extents that were on oldDiskID should now reference newDiskID.
	for _, extent := range state.Extents {
		if extent.DataDiskID == oldDiskID {
			t.Fatalf("extent %q still references replaced disk %q", extent.ExtentID, oldDiskID)
		}
	}

	// M2 invariant must pass: no extent references a non-existent disk.
	if vs := CheckStateInvariants(state); len(vs) > 0 {
		t.Fatalf("invariant violations after ReplaceDisk: %v", vs)
	}
}

// TestReplaceDiskRejectsUnknownOldDisk verifies that replacing a non-existent
// disk is rejected.
func TestReplaceDiskRejectsUnknownOldDisk(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	if _, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/seed.bin",
		SizeBytes:   512,
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := c.ReplaceDisk("disk-nonexistent", "disk-new"); err == nil {
		t.Fatal("expected error when replacing nonexistent disk, got nil")
	}
}

// TestFailDiskMarksHealthStatus verifies that FailDisk sets health_status to
// "failed" and persists the change.
func TestFailDiskMarksHealthStatus(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	if _, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/seed.bin",
		SizeBytes:   512,
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := c.FailDisk("disk-01"); err != nil {
		t.Fatalf("FailDisk: %v", err)
	}

	// Reload and verify.
	c2 := NewCoordinator(metadataPath, journalPath)
	state, err := c2.metadata.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	found := false
	for _, d := range state.Disks {
		if d.DiskID == "disk-01" {
			found = true
			if d.HealthStatus != "failed" {
				t.Errorf("expected health_status=failed, got %q", d.HealthStatus)
			}
		}
	}
	if !found {
		t.Fatal("disk-01 not found after FailDisk")
	}
}

// TestFailDiskRejectsUnknownDisk verifies that failing a non-existent disk
// returns an error.
func TestFailDiskRejectsUnknownDisk(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	if _, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/seed.bin",
		SizeBytes:   512,
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := c.FailDisk("disk-nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent disk, got nil")
	}
}

// TestFailThenReplaceAndRebuild exercises the full disk failure lifecycle:
// fail → replace → rebuild from parity.
func TestFailThenReplaceAndRebuild(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	c := NewCoordinator(metadataPath, journalPath)

	writeResult, err := c.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/lifecycle.bin",
		SizeBytes:   (2 << 20) + 7,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Choose a disk that has extents.
	failedDisk := writeResult.Extents[0].DataDiskID
	newDisk := "disk-replacement-lc"

	// Step 1: fail disk.
	if err := c.FailDisk(failedDisk); err != nil {
		t.Fatalf("FailDisk: %v", err)
	}

	// Step 2: replace disk (updates extent ownership).
	if err := c.ReplaceDisk(failedDisk, newDisk); err != nil {
		t.Fatalf("ReplaceDisk: %v", err)
	}

	// Step 3: delete extent files to simulate the physical disk being gone.
	rootDir := filepath.Dir(metadataPath)
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != failedDisk {
			continue
		}
		path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		// Remove to simulate disk loss; ignore not-found errors.
		_ = os.Remove(path)
	}

	// Step 4: rebuild from parity using the new disk ID.
	rebuildResult, err := c.RebuildDataDisk(newDisk)
	if err != nil {
		t.Fatalf("RebuildDataDisk: %v", err)
	}
	if !rebuildResult.Healthy {
		t.Fatalf("rebuild not healthy: %#v", rebuildResult)
	}

	// Step 5: invariants must all pass after the full lifecycle.
	state, err := c.metadata.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if vs := CheckIntegrityInvariants(rootDir, state); len(vs) > 0 {
		t.Fatalf("invariant violations after full lifecycle: %v", vs)
	}
}
