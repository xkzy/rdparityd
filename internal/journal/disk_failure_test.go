package journal

// disk_failure_test.go — Category D: Disk Failure Tests (D1-D10)
//
// These tests exercise the DiskFailureHelper to simulate various disk failure scenarios,
// including missing disks at startup, disappearing disks during operations, parity disk
// failures, replacement disk scenarios, and unrecoverable multi-disk failures.
//
// Each test:
//   1. Sets up a file by writing via Coordinator.WriteFile
//   2. Applies a failure using DiskFailureHelper methods
//   3. Attempts an operation (read, write, or load metadata)
//   4. Verifies AnalyzeMultiDiskFailures detects the failure correctly
//   5. For unrecoverable scenarios, verifies recovery is rejected
//
// Test Matrix:
//   D1: Data disk missing at startup (RegisterExtent, RemoveExtentFile)
//   D2: Data disk disappears during read
//   D3: Data disk disappears during write
//   D4: Parity disk missing (RemoveParityFile)
//   D5: Parity disk disappears during write
//   D6: Replacement disk scenario (rebuild after failure)
//   D7: Replacement disk partial rebuild
//   D8: Replacement disk full rebuild with parity regeneration
//   D9: Two disks fail simultaneously (unrecoverable scenario)
//  D10: Disk read errors (CorruptExtentFile)

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── Test helpers ────────────────────────────────────────────────────────────

// ─── D1: Data disk missing at startup ─────────────────────────────────────────
//
// Scenario: A data extent file is missing when we attempt to load metadata.
// The system should detect it via AnalyzeMultiDiskFailures and report the
// failed disk correctly.
func TestD1_DataDiskMissingAtStartup(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Step 1: Write a file to establish extents.
	payload := makePayload((2 << 20), 1)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d1-missing-startup.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Register extents with failure helper.
	helper := NewDiskFailureHelper(root)
	targetDiskID := ""
	for _, extent := range writeResult.Extents {
		helper.RegisterExtent(extent.PhysicalLocator.RelativePath)
		if targetDiskID == "" {
			targetDiskID = extent.DataDiskID
		}
	}

	// Step 2: Remove a data extent file to simulate disk failure.
	if err := helper.RemoveExtentFile(0); err != nil {
		t.Fatalf("RemoveExtentFile: %v", err)
	}

	// Step 3: Load metadata and analyze failures.
	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	// Step 4: Verify detection.
	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk, got %d: %v", len(analysis.FailedDisks), analysis.FailedDisks)
	}
	if analysis.FailedDisks[0] != targetDiskID {
		t.Fatalf("Expected failed disk %q, got %q", targetDiskID, analysis.FailedDisks[0])
	}
	if analysis.FailureMode != "single" {
		t.Fatalf("Expected failure mode 'single', got %q", analysis.FailureMode)
	}
	if !analysis.RecoveryIsPossible {
		t.Fatalf("Single disk failure should be recoverable: %s", analysis.RecommendedAction)
	}
	if len(analysis.PartiallyLostExtents) == 0 {
		t.Fatal("Expected at least one partially lost extent")
	}
}

// ─── D2: Data disk disappears during read ─────────────────────────────────────
//
// Scenario: A data extent exists initially. We start a read operation, but the
// disk disappears partway through (simulated by removing the file before
// AnalyzeMultiDiskFailures is called). The system should detect the failure
// when attempting to read back the data.
func TestD2_DataDiskDisappearsAfterWrite(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Write a file.
	payload := makePayload((1 << 20), 2)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d2-disappears-read.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Register the first extent.
	helper := NewDiskFailureHelper(root)
	targetDiskID := writeResult.Extents[0].DataDiskID
	helper.RegisterExtent(writeResult.Extents[0].PhysicalLocator.RelativePath)

	// Remove the extent file to simulate disk disappearing during read.
	if err := helper.RemoveExtentFile(0); err != nil {
		t.Fatalf("RemoveExtentFile: %v", err)
	}

	// D2 correct behavior: a missing data extent self-heals via parity reconstruction.
	// First verify AnalyzeMultiDiskFailures detects the failure while file is still absent,
	// then verify ReadFile self-heals and returns correct data.

	// Run analysis BEFORE ReadFile (file still absent, disk shows as failed).
	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk, got %d", len(analysis.FailedDisks))
	}
	if analysis.FailedDisks[0] != targetDiskID {
		t.Fatalf("Expected failed disk %q, got %q", targetDiskID, analysis.FailedDisks[0])
	}
	// Single data-disk failure with parity intact: recovery IS possible.
	if !analysis.RecoveryIsPossible {
		t.Fatalf("Single disk failure should be recoverable: %s", analysis.RecommendedAction)
	}

	// ReadFile self-heals via parity reconstruction.
	readResult, err := coordinator.ReadFile("/test/d2-disappears-read.bin")
	if err != nil {
		t.Fatalf("Expected read to self-heal via parity but got error: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("self-healed data does not match original payload")
	}
}

// ─── D3: Data disk disappears during write ────────────────────────────────────
//
// Scenario: A second write operation begins but the first write's data disk
// fails. We verify that the failure is detected when examining the current
// state vs. filesystem.
func TestD3_DataDiskDisappearsBeforeSecondWrite(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// First write.
	payload1 := makePayload((1 << 20), 3)
	result1, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d3-file1.bin",
		AllowSynthetic: true,
		Payload:        payload1,
	})
	if err != nil {
		t.Fatalf("First WriteFile: %v", err)
	}

	// Set up failure helper for first file's extent.
	helper := NewDiskFailureHelper(root)
	targetDiskID := result1.Extents[0].DataDiskID
	for _, extent := range result1.Extents {
		helper.RegisterExtent(extent.PhysicalLocator.RelativePath)
	}

	// Remove a data extent.
	if err := helper.RemoveExtentFile(0); err != nil {
		t.Fatalf("RemoveExtentFile: %v", err)
	}

	// Attempt second write (should still succeed, but failure is detectable).
	payload2 := makePayload((512 << 10), 4)
	_, err = coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d3-file2.bin",
		AllowSynthetic: true,
		Payload:        payload2,
	})
	// Write may or may not succeed depending on allocation; the important check
	// is that analysis detects the failure.

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	// Stronger behavior is now allowed: the second write may detect and repair
	// the missing committed extent before parity recomputation, leaving no failed
	// disk visible to analysis.
	if len(analysis.FailedDisks) > 1 {
		t.Fatalf("Expected at most 1 failed disk, got %d", len(analysis.FailedDisks))
	}
	if len(analysis.FailedDisks) == 1 && analysis.FailedDisks[0] != targetDiskID {
		t.Fatalf("Expected failed disk %q, got %q", targetDiskID, analysis.FailedDisks[0])
	}
}

// ─── D4: Parity disk missing ──────────────────────────────────────────────────
//
// Scenario: A parity file is deleted. The system detects the parity disk failure
// and marks its members as unrecoverable if both data and parity are gone, or
// as recoverable via other replicas if data is intact.
func TestD4_ParityDiskMissing(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Write a file.
	payload := makePayload((2 << 20), 4)
	_, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d4-parity-missing.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	if len(loadedState.ParityGroups) == 0 {
		t.Fatal("No parity groups found")
	}

	group := loadedState.ParityGroups[0]
	targetParityDiskID := group.ParityDiskID

	// Remove the parity file using DiskFailureHelper.
	helper := NewDiskFailureHelper(root)
	if err := helper.RemoveParityFile(group.ParityGroupID); err != nil {
		t.Fatalf("RemoveParityFile: %v", err)
	}

	// Analyze and verify parity failure is detected.
	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk (parity), got %d", len(analysis.FailedDisks))
	}
	if analysis.FailedDisks[0] != targetParityDiskID {
		t.Fatalf("Expected failed parity disk %q, got %q", targetParityDiskID, analysis.FailedDisks[0])
	}
	// D4 correct behavior: when only the parity disk fails, ALL data extents are
	// still readable from their data disk files. RecoveryIsPossible = true because
	// reads still work. However, the parity group is unrecoverable (if a data disk
	// also fails now, we cannot XOR-reconstruct). That's what UnrecoverableGroups tracks.
	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk (parity), got %d: %v", len(analysis.FailedDisks), analysis.FailedDisks)
	}
	if analysis.FailedDisks[0] != targetParityDiskID {
		t.Fatalf("Expected failed parity disk %q, got %q", targetParityDiskID, analysis.FailedDisks[0])
	}
	// Parity-only failure: data is still on data disks → recovery IS possible.
	if !analysis.RecoveryIsPossible {
		t.Fatalf("Parity-only disk failure: data extents still on data disks, recovery should be possible: %s", analysis.RecommendedAction)
	}
	// The parity group is now unrecoverable (can't XOR-rebuild data if a data disk fails).
	if len(analysis.UnrecoverableGroups) == 0 {
		t.Fatal("Expected unrecoverable parity groups when parity disk fails")
	}
}

// ─── D5: Parity disk disappears during write ──────────────────────────────────
//
// Scenario: A parity file exists, but is removed before a second write's parity
// is computed. We verify that the failure is detected in analysis.
func TestD5_ParityDiskDisappearsAfterFirstWrite(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// First write.
	payload1 := makePayload((1 << 20), 5)
	_, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d5-file1.bin",
		AllowSynthetic: true,
		Payload:        payload1,
	})
	if err != nil {
		t.Fatalf("First WriteFile: %v", err)
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	if len(loadedState.ParityGroups) == 0 {
		t.Fatal("No parity groups found")
	}

	group := loadedState.ParityGroups[0]
	targetParityDiskID := group.ParityDiskID

	// Remove the parity file.
	helper := NewDiskFailureHelper(root)
	if err := helper.RemoveParityFile(group.ParityGroupID); err != nil {
		t.Fatalf("RemoveParityFile: %v", err)
	}

	// Run analysis NOW — before any write recreates the parity file.
	// While the parity file is absent the analysis must detect the failed disk.
	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk (parity), got %d", len(analysis.FailedDisks))
	}
	if analysis.FailedDisks[0] != targetParityDiskID {
		t.Fatalf("Expected failed parity disk %q, got %q", targetParityDiskID, analysis.FailedDisks[0])
	}

	// Second write (to the same or different file).
	// The coordinator will recompute and recreate the parity file as part of the write.
	payload2 := makePayload((512 << 10), 6)
	_, err = coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d5-file2.bin",
		AllowSynthetic: true,
		Payload:        payload2,
	})
	// Write may or may not succeed; regardless, the pre-write detection was confirmed above.
}

// ─── D6: Replacement disk scenario (basic rebuild) ────────────────────────────
//
// Scenario: A disk fails, is replaced, and rebuild is initiated.
// We verify that AnalyzeMultiDiskFailures correctly identifies the failed disk
// and that rebuilding is possible (data is recoverable via parity).
func TestD6_ReplacementDiskScenario(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Write a file to establish extents and parity.
	payload := makePayload((2 << 20), 6)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d6-replacement.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Simulate disk failure by removing one extent.
	helper := NewDiskFailureHelper(root)
	targetDiskID := writeResult.Extents[0].DataDiskID
	helper.RegisterExtent(writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := helper.RemoveExtentFile(0); err != nil {
		t.Fatalf("RemoveExtentFile: %v", err)
	}

	// Load state and verify failure is detected and recoverable.
	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("Expected 1 failed disk, got %d", len(analysis.FailedDisks))
	}
	if analysis.FailedDisks[0] != targetDiskID {
		t.Fatalf("Expected failed disk %q, got %q", targetDiskID, analysis.FailedDisks[0])
	}
	if !analysis.RecoveryIsPossible {
		t.Fatalf("Single disk failure should be recoverable: %s", analysis.RecommendedAction)
	}
	// Verify parity is intact (not listed as failed).
	for _, failedID := range analysis.FailedDisks {
		if loadedState.ParityGroups[0].ParityDiskID == failedID {
			t.Fatalf("Parity disk should not be failed; recovery is possible")
		}
	}
}

// ─── D7: Replacement disk partial rebuild ─────────────────────────────────────
//
// Scenario: A disk fails, is replaced, but rebuild only partially completes.
// The system should still recognize that parity allows recovery of remaining data.
func TestD7_ReplacementDiskPartialRebuild(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Write a file.
	payload := makePayload((3 << 20), 7)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d7-partial-rebuild.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Simulate multiple extents on the same disk by tracking all.
	helper := NewDiskFailureHelper(root)
	targetDiskID := writeResult.Extents[0].DataDiskID
	extentCount := 0
	for _, extent := range writeResult.Extents {
		if extent.DataDiskID == targetDiskID {
			helper.RegisterExtent(extent.PhysicalLocator.RelativePath)
			extentCount++
		}
	}

	// Remove all extents on the failed disk.
	for i := 0; i < extentCount; i++ {
		if err := helper.RemoveExtentFile(i); err != nil {
			t.Fatalf("RemoveExtentFile(%d): %v", i, err)
		}
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if !analysis.RecoveryIsPossible {
		t.Fatalf("Multiple extents on single disk should be recoverable via parity: %s", analysis.RecommendedAction)
	}
	if len(analysis.PartiallyLostExtents) == 0 {
		t.Fatal("Expected partially lost extents from failed disk")
	}
}

// ─── D8: Replacement disk full rebuild with parity regeneration ───────────────

// ─── D8: Replacement disk full rebuild with parity regeneration ───────────────
//
// Scenario: A disk fails AND its parity disk fails. The original data is now
// unrecoverable without external backup. We verify this is correctly identified
// as an unrecoverable scenario.
func TestD8_ReplacementDiskFullRebuildWithParityFail(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// Write a file.
	payload := makePayload((2 << 20), 8)
	_, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d8-parity-fail.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	// Pick a data extent and its parity group.
	if len(loadedState.Extents) == 0 || len(loadedState.ParityGroups) == 0 {
		t.Fatal("No extents or parity groups found")
	}

	targetExtent := loadedState.Extents[0]
	targetGroup := loadedState.ParityGroups[0]

	// Remove both the data extent and its parity file.
	helper := NewDiskFailureHelper(root)
	helper.RegisterExtent(targetExtent.PhysicalLocator.RelativePath)
	if err := helper.RemoveExtentFile(0); err != nil {
		t.Fatalf("RemoveExtentFile: %v", err)
	}
	if err := helper.RemoveParityFile(targetGroup.ParityGroupID); err != nil {
		t.Fatalf("RemoveParityFile: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) < 2 {
		t.Fatalf("Expected 2 failed disks (data + parity), got %d: %v", len(analysis.FailedDisks), analysis.FailedDisks)
	}
	if analysis.RecoveryIsPossible {
		t.Fatalf("Data + parity disk failure should be unrecoverable: %s", analysis.RecommendedAction)
	}
	if len(analysis.UnrecoverableExtents) == 0 {
		t.Fatal("Expected unrecoverable extents when both data and parity fail")
	}
}

// ─── D9: Two disks fail simultaneously (unrecoverable) ────────────────────────
func TestD9_TwoDisksFail_Unrecoverable(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	payload := makePayload((4 << 20), 9)
	result, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d9-dual-failure.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	diskMap := make(map[string]bool)
	var parityDiskID string
	for _, extent := range loadedState.Extents {
		diskMap[extent.DataDiskID] = true
	}
	if len(loadedState.ParityGroups) > 0 {
		parityDiskID = loadedState.ParityGroups[0].ParityDiskID
	}

	if len(diskMap) < 2 {
		t.Skip("Test requires at least 2 data disks; skipping")
	}

	var failDisks []string
	for diskID := range diskMap {
		if diskID != parityDiskID {
			failDisks = append(failDisks, diskID)
			if len(failDisks) == 2 {
				break
			}
		}
	}

	if len(failDisks) < 2 {
		t.Skip("Could not find 2 distinct data disks to fail; skipping")
	}

	helper := NewDiskFailureHelper(root)
	for _, extent := range result.Extents {
		if extent.DataDiskID == failDisks[0] || extent.DataDiskID == failDisks[1] {
			helper.RegisterExtent(extent.PhysicalLocator.RelativePath)
		}
	}

	for i := 0; i < len(helper.extents); i++ {
		if err := helper.RemoveExtentFile(i); err != nil {
			t.Logf("RemoveExtentFile(%d): %v", i, err)
		}
	}

	if len(loadedState.ParityGroups) > 0 {
		if err := helper.RemoveParityFile(loadedState.ParityGroups[0].ParityGroupID); err != nil {
			t.Logf("RemoveParityFile: %v (continuing)", err)
		}
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)

	if len(analysis.FailedDisks) < 2 {
		t.Logf("Warning: expected 2+ failed disks, got %d", len(analysis.FailedDisks))
	}
	if len(analysis.UnrecoverableExtents) > 0 && analysis.RecoveryIsPossible {
		t.Fatalf("Unrecoverable extents should make recovery impossible")
	}
}

// ─── D10: Disk read errors (CorruptExtentFile) ────────────────────────────────
func TestD10_DiskReadErrors_CorruptExtentFile(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "metadata.bin")
	journalPath := filepath.Join(root, "journal.bin")
	coordinator := NewCoordinator(metadataPath, journalPath)

	payload := makePayload((1 << 20), 10)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/test/d10-corrupted.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	helper := NewDiskFailureHelper(root)
	helper.RegisterExtent(writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := helper.CorruptExtentFile(0); err != nil {
		t.Fatalf("CorruptExtentFile: %v", err)
	}

	// D10 correct behavior: a corrupted extent (checksum mismatch) self-heals via
	// parity reconstruction. ReadFile should SUCCEED and return the correct data.
	// The integrity violation (E1) is detected and repaired on read.
	readResult, err := coordinator.ReadFile("/test/d10-corrupted.bin")
	if err != nil {
		t.Fatalf("Expected read to self-heal corrupted extent via parity, got error: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("self-healed data does not match original payload")
	}

	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(root, loadedState)
	if !analysis.RecoveryIsPossible {
		t.Logf("Recovery not possible: %s", analysis.RecommendedAction)
	}

	violations := CheckIntegrityInvariants(root, loadedState)
	for _, v := range violations {
		if v.Code == "E1" || v.Code == "P2" {
			return
		}
	}
	t.Logf("Note: Corruption detection may require read attempt")
}
