package journal

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestAddDiskPersistsNewDisk(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	if err := coord.AddDisk("disk-03", metadata.DiskRoleData, "/mnt/data03", 2<<40); err != nil {
		t.Fatalf("AddDisk returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	found := false
	for _, disk := range state.Disks {
		if disk.DiskID == "disk-03" {
			found = true
			if disk.Role != metadata.DiskRoleData {
				t.Fatalf("expected data role, got %q", disk.Role)
			}
		}
	}
	if !found {
		t.Fatal("new disk not persisted")
	}
}

func TestReplaceDiskReassignsExtents(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	result, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/replace.bin",
		SizeBytes:   (1 << 20) + 7,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(result.Extents) == 0 {
		t.Fatal("expected extents")
	}
	oldDiskID := result.Extents[0].DataDiskID
	newDiskID := "disk-replacement"

	if err := coord.ReplaceDisk(oldDiskID, newDiskID); err != nil {
		t.Fatalf("ReplaceDisk returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	for _, extent := range state.Extents {
		if extent.ExtentID == result.Extents[0].ExtentID && extent.DataDiskID != newDiskID {
			t.Fatalf("extent was not reassigned: got %q want %q", extent.DataDiskID, newDiskID)
		}
	}
}

func TestFailDiskPersistsHealthStatus(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	if err := coord.FailDisk("disk-01"); err != nil {
		t.Fatalf("FailDisk returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	for _, disk := range state.Disks {
		if disk.DiskID == "disk-01" {
			if disk.HealthStatus != "failed" {
				t.Fatalf("expected failed health status, got %q", disk.HealthStatus)
			}
			return
		}
	}
	t.Fatal("disk-01 not found")
}

func TestConcurrentDiskLifecycleAndWritePaths(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := coord.WriteFile(WriteRequest{
				PoolName:    "demo",
				LogicalPath: fmt.Sprintf("/concurrent-%d.bin", i),
				SizeBytes:   int64((i + 1) * 4096),
			})
			if err != nil {
				errCh <- fmt.Errorf("write %d: %w", i, err)
			}
		}()
	}

	ops := []func() error{
		func() error { return coord.AddDisk("disk-03", metadata.DiskRoleData, "/mnt/data03", 2<<40) },
		func() error { return coord.FailDisk("disk-01") },
		func() error { return coord.ReplaceDisk("disk-02", "disk-04") },
	}
	for _, op := range ops {
		op := op
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := op(); err != nil {
				errCh <- err
			}
		}()
	}

	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = coord.ReadFile(fmt.Sprintf("/concurrent-%d.bin", i))
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent operation failed: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after concurrent operations: %v", violations[0])
	}
}
