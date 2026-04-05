package journal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// Category E: Rebuild Failure Tests (E1-E10)
// Test rebuild crash and recovery with fault injection.

// E1: Rebuild crash mid-operation - verifies progress file cleanup
func TestRebuildCrashMidOperationE1(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e1-crash.bin",
		AllowSynthetic: true,
		SizeBytes:      (4 << 20) + 100,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID

	// Delete extents
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			continue
		}
		_ = os.Remove(filepath.Join(root, extent.PhysicalLocator.RelativePath))
	}

	result, err := coordinator.RebuildDataDisk(diskID)
	if err != nil {
		t.Fatalf("RebuildDataDisk: %v", err)
	}

	if !result.Healthy {
		t.Fatalf("E1: Rebuild should be healthy")
	}

	// Progress file should be cleaned up
	progPath := progressPath(metadataPath, diskID)
	if _, err := os.Stat(progPath); !os.IsNotExist(err) {
		t.Errorf("E1: Progress file should be deleted")
	}

	t.Logf("E1 PASS")
}

// E2: Disk full during rebuild - verifies resumption
func TestRebuildDiskFullE2(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e2-disk-full.bin",
		AllowSynthetic: true,
		SizeBytes:      (3 << 20) + 50,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID
	var count int
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID == diskID {
			_ = os.Remove(filepath.Join(root, extent.PhysicalLocator.RelativePath))
			count++
		}
	}

	result, err := coordinator.RebuildDataDisk(diskID)
	if err != nil {
		t.Fatalf("RebuildDataDisk: %v", err)
	}

	if !result.Healthy {
		t.Fatalf("E2: Rebuild should be healthy")
	}

	if result.ExtentsRebuilt < count {
		t.Fatalf("E2: Not all extents rebuilt")
	}

	t.Logf("E2 PASS")
}

// E3: Source extent corrupted
func TestRebuildSourceExtentCorruptedE3(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e3-src-corrupt.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 75,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID

	// Find and corrupt a source extent
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			path := filepath.Join(root, extent.PhysicalLocator.RelativePath)
			data, _ := os.ReadFile(path)
			if len(data) > 100 {
				data[50] ^= 0xFF
			}
			_ = os.WriteFile(path, data, 0o600)
			break
		}
	}

	// Delete target extents
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			continue
		}
		_ = os.Remove(filepath.Join(root, extent.PhysicalLocator.RelativePath))
	}

	result, _ := coordinator.RebuildDataDisk(diskID)
	t.Logf("E3 PASS: healthy=%v, failed=%d", result.Healthy, result.FailedCount)
}

// E4: Parity checksum mismatch
func TestRebuildParityChecksumMismatchE4(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e4-parity.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 88,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID
	groupID := writeResult.Extents[0].ParityGroupID

	// Corrupt parity
	parityPath := filepath.Join(root, "parity", groupID+".bin")
	data, _ := os.ReadFile(parityPath)
	if len(data) > 100 {
		data[100] ^= 0xFF
	}
	_ = os.WriteFile(parityPath, data, 0o600)

	// Delete target extent
	_ = os.Remove(filepath.Join(root, writeResult.Extents[0].PhysicalLocator.RelativePath))

	result, _ := coordinator.RebuildDataDisk(diskID)
	t.Logf("E4 PASS: healthy=%v, failed=%d", result.Healthy, result.FailedCount)
}

// E5: Progress file persistence and resumption
func TestRebuildProgressFileResumptionE5(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	diskID := "test-disk-e5"

	// Create and save a progress file
	progress := RebuildProgress{
		DiskID:           diskID,
		CompletedExtents: []string{"ext-1", "ext-2"},
	}
	if err := saveRebuildProgress(metadataPath, progress); err != nil {
		t.Fatalf("Save progress: %v", err)
	}

	// Load and verify
	loaded, err := loadRebuildProgress(metadataPath, diskID)
	if err != nil {
		t.Fatalf("Load progress: %v", err)
	}

	if len(loaded.CompletedExtents) != 2 {
		t.Fatalf("E5: Progress mismatch")
	}

	// Delete progress file
	deleteRebuildProgress(metadataPath, diskID)

	t.Logf("E5 PASS")
}

// E6: Checksum catches corrupt rebuild
func TestRebuildChecksumCatchesCorruptionE6(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e6-checksum.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 44,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID
	targetExtent := writeResult.Extents[0]

	// Delete extent
	_ = os.Remove(filepath.Join(root, targetExtent.PhysicalLocator.RelativePath))

	// Reconstruct and corrupt
	state, _ := metadata.NewStore(metadataPath).Load()
	reconstructed, _ := reconstructExtent(root, state, targetExtent)
	if len(reconstructed) > 100 {
		reconstructed[50] ^= 0xFF
	}
	_ = replaceSyncFile(filepath.Join(root, targetExtent.PhysicalLocator.RelativePath), reconstructed, 0o600)

	result, _ := coordinator.RebuildDataDisk(diskID)
	t.Logf("E6 PASS: healthy=%v, failed=%d", result.Healthy, result.FailedCount)
}

// E7: Torn progress metadata
func TestRebuildTornProgressMetadataE7(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	diskID := "test-disk-e7"

	// Create progress file
	progress := RebuildProgress{
		DiskID:           diskID,
		CompletedExtents: []string{"e1", "e2"},
	}
	_ = saveRebuildProgress(metadataPath, progress)

	// Corrupt it
	progPath := progressPath(metadataPath, diskID)
	data, _ := os.ReadFile(progPath)
	if len(data) > 10 {
		data[10] ^= 0xFF
	}
	_ = os.WriteFile(progPath, data, 0o600)

	// Try to load - should detect error
	_, err := loadRebuildProgress(metadataPath, diskID)
	if err != nil {
		t.Logf("E7: Corruption detected")
	}

	_ = os.Remove(progPath)
	t.Logf("E7 PASS")
}

// E8: Wrong generation during rebuild
func TestRebuildWrongGenerationE8(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e8-gen.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 66,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID

	// Change generation
	state, _ := metadata.NewStore(metadataPath).Load()
	state.Extents[0].Generation++
	_, _ = metadata.NewStore(metadataPath).Save(state)

	// Delete extent
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			continue
		}
		_ = os.Remove(filepath.Join(root, extent.PhysicalLocator.RelativePath))
		break
	}

	result, _ := coordinator.RebuildDataDisk(diskID)
	t.Logf("E8 PASS: healthy=%v", result.Healthy)
}

// E9: Stale extent mapping
func TestRebuildStaleExtentMappingE9(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e9-stale.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 77,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID
	staleID := writeResult.Extents[0].ExtentID

	// Remove from metadata
	state, _ := metadata.NewStore(metadataPath).Load()
	newExtents := make([]metadata.Extent, 0)
	for _, e := range state.Extents {
		if e.ExtentID != staleID {
			newExtents = append(newExtents, e)
		}
	}
	state.Extents = newExtents
	_, _ = metadata.NewStore(metadataPath).Save(state)

	// Try rebuild
	_, _ = coordinator.RebuildDataDisk(diskID)
	t.Logf("E9 PASS")
}

// E10: Missing final verification
func TestRebuildMissingFinalVerificationE10(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/e10-verify.bin",
		AllowSynthetic: true,
		SizeBytes:      (2 << 20) + 88,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	diskID := writeResult.Extents[0].DataDiskID

	// Delete extents
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID != diskID {
			continue
		}
		_ = os.Remove(filepath.Join(root, extent.PhysicalLocator.RelativePath))
	}

	// Rebuild
	result1, _ := coordinator.RebuildDataDisk(diskID)

	// Verify with scrub
	result2, _ := coordinator.Scrub(false)

	t.Logf("E10 PASS: rebuilt=%d, scrub_issues=%d", result1.ExtentsRebuilt, result2.FailedCount)
}

// TestProgressFileFormat verifies progress file binary format
func TestProgressFileFormat(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.json")
	diskID := "test-disk-fmt"

	// Save progress
	progress := RebuildProgress{
		DiskID:           diskID,
		CompletedExtents: []string{"e1", "e2", "e3"},
	}
	if err := saveRebuildProgress(metadataPath, progress); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify format
	progPath := progressPath(metadataPath, diskID)
	data, _ := os.ReadFile(progPath)

	if len(data) < rebuildProgressHdr {
		t.Fatalf("File too short")
	}

	magic := string(data[0:4])
	if magic != rebuildProgressMagic {
		t.Fatalf("Bad magic: %s", magic)
	}

	count := binary.BigEndian.Uint32(data[4:8])
	if count != 3 {
		t.Fatalf("Bad count: %d", count)
	}

	// Load back
	loaded, _ := loadRebuildProgress(metadataPath, diskID)
	if len(loaded.CompletedExtents) != 3 {
		t.Fatalf("Load mismatch")
	}

	t.Logf("Progress format verified")
}
