package journal

import (
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestProtectionStatus_OneDisk(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	result, err := coord.WriteFile(WriteRequest{
		PoolName:       "test",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	status, err := coord.ProtectionStatus()
	if err != nil {
		t.Fatalf("ProtectionStatus failed: %v", err)
	}

	if status.DiskCount != 4 {
		t.Errorf("expected 4 disks (prototype state), got %d", status.DiskCount)
	}
	if status.DataDiskCount != 2 {
		t.Errorf("expected 2 data disks, got %d", status.DataDiskCount)
	}
	if status.ParityDiskCount != 1 {
		t.Errorf("expected 1 parity disk, got %d", status.ParityDiskCount)
	}
	if status.ExtentCounts[metadata.ExtentChecksumOnly] != len(result.Extents) {
		t.Errorf("expected %d checksum-only extents, got %d",
			len(result.Extents), status.ExtentCounts[metadata.ExtentChecksumOnly])
	}
}

func TestProtectionState(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	state, err := coord.ProtectionState()
	if err != nil {
		t.Fatalf("ProtectionState failed: %v", err)
	}

	if state != metadata.ProtectionParity {
		t.Errorf("expected parity (prototype has parity disk), got %s", state)
	}
}

func TestExtentProtectionClass(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	result, err := coord.WriteFile(WriteRequest{
		PoolName:       "test",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	class, err := coord.ExtentProtectionClass(result.Extents[0].ExtentID)
	if err != nil {
		t.Fatalf("ExtentProtectionClass failed: %v", err)
	}

	if class != metadata.ExtentChecksumOnly {
		t.Errorf("expected checksum_only, got %s", class)
	}
}

func TestExtentProtectionClass_NotFound(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.ExtentProtectionClass("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent extent")
	}
}
