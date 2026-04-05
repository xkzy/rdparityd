package journal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ============================================================================
// CATEGORY F: FULL SCRUB TESTS (10 tests)
// Tests verify comprehensive scrub operations, specific corruption detection,
// and interruption/resumption handling.
// ============================================================================

// TestCategoryF_FullScrubHealthyPool verifies that a complete scrub of a
// healthy pool reports no issues and terminates successfully.
//
// VERIFICATION GOALS:
// - Scrub completes without errors
// - All extents examined (FilesChecked and ExtentsChecked updated)
// - Result.Healthy == true
// - No false positives in corruption detection
//
// FAILURE SCENARIOS:
// - Healthy pool with multiple files
// - Scrub runs from start to completion
//
// EXPECTED OUTCOMES:
// - Scrub completes successfully
// - FilesChecked >= number of written files
// - ExtentsChecked >= number of total extents
// - HealedCount and FailedCount == 0
func TestCategoryF_FullScrubHealthyPool(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	const numFiles = 5
	payloads := make([][]byte, numFiles)
	totalExtents := 0

	for i := 0; i < numFiles; i++ {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, (1<<20)+(i*13579))
		result, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: fmt.Sprintf("/scrub/healthy-%d.bin", i),
			Payload:     payloads[i],
		})
		if err != nil {
			t.Fatalf("write file %d failed: %v", i, err)
		}
		totalExtents += len(result.Extents)
	}

	scrubResult, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("scrub failed: %v", err)
	}

	if !scrubResult.Healthy {
		t.Fatalf("healthy pool reported as unhealthy")
	}

	if scrubResult.FilesChecked < numFiles {
		t.Fatalf("expected at least %d files checked, got %d", numFiles, scrubResult.FilesChecked)
	}

	if scrubResult.ExtentsChecked < totalExtents {
		t.Fatalf("expected at least %d extents checked, got %d", totalExtents, scrubResult.ExtentsChecked)
	}

	if scrubResult.HealedCount != 0 {
		t.Fatalf("healthy pool should have 0 healed extents, got %d", scrubResult.HealedCount)
	}

	if scrubResult.FailedCount != 0 {
		t.Fatalf("healthy pool should have 0 failed extents, got %d", scrubResult.FailedCount)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after healthy scrub: %v", violations[0])
	}
}

// TestCategoryF_ScrubDetectsMultipleCorruptedExtents verifies that scrub
// identifies all corrupted extents in a pool with varied corruption patterns.
//
// VERIFICATION GOALS:
// - All corrupted extents detected in single scrub pass
// - Detection accurate for different corruption types
// - FailedExtentIDs complete and accurate
// - No silent missed corruptions
//
// FAILURE SCENARIOS:
// - Write 10 files with 2-3 extents each
// - Corrupt 5 random extents with different patterns (single bit, multi-byte)
// - Run full scrub
//
// EXPECTED OUTCOMES:
// - FailedCount == 5
// - All 5 extent IDs appear in FailedExtentIDs
// - No false positives (healthy extents not in failed list)
// - Healthy flag == false
func TestCategoryF_ScrubDetectsMultipleCorruptedExtents(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	for i := 0; i < 10; i++ {
		payload := bytes.Repeat([]byte{byte('a' + i)}, (2<<20)+(i*97)+123)
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: fmt.Sprintf("/scrub/multi-%02d.bin", i),
			Payload:     payload,
		}); err != nil {
			t.Fatalf("write file %d failed: %v", i, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if len(state.Extents) < 5 {
		t.Fatalf("expected at least 5 extents, got %d", len(state.Extents))
	}

	corrupted := make(map[string]struct{}, 5)
	patterns := [][]byte{
		{0x01},
		{0x10, 0x20, 0x30, 0x40},
		{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22},
		{0x5a, 0xc3},
		{0x7f, 0x6e, 0x5d},
	}
	for i := 0; i < 5; i++ {
		extent := state.Extents[i]
		corrupted[extent.ExtentID] = struct{}{}
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(extentPath)
		if err != nil {
			t.Fatalf("read extent %s failed: %v", extent.ExtentID, err)
		}
		pattern := patterns[i]
		for j, b := range pattern {
			if j >= len(data) {
				break
			}
			data[j] ^= b
		}
		if err := os.WriteFile(extentPath, data, 0o600); err != nil {
			t.Fatalf("write corrupted extent %s failed: %v", extent.ExtentID, err)
		}
	}

	scrubResult, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("scrub failed: %v", err)
	}
	if scrubResult.Healthy {
		t.Fatal("expected unhealthy scrub result for corrupted extents")
	}
	if scrubResult.FailedCount != 5 {
		t.Fatalf("expected FailedCount=5, got %d", scrubResult.FailedCount)
	}
	if scrubResult.HealedCount != 0 {
		t.Fatalf("expected HealedCount=0 in audit-only scrub, got %d", scrubResult.HealedCount)
	}

	failedExtentIDs := make(map[string]struct{})
	for _, issue := range scrubResult.Issues {
		if issue.Kind == "extent" && issue.Status == "failed" {
			failedExtentIDs[issue.ExtentID] = struct{}{}
		}
	}
	if len(failedExtentIDs) != 5 {
		t.Fatalf("expected exactly 5 failed extent IDs in scrub issues, got %d (%v)", len(failedExtentIDs), scrubResult.Issues)
	}
	for extentID := range corrupted {
		if _, ok := failedExtentIDs[extentID]; !ok {
			t.Fatalf("expected corrupted extent %s to be reported, got %v", extentID, failedExtentIDs)
		}
	}
	for extentID := range failedExtentIDs {
		if _, ok := corrupted[extentID]; !ok {
			t.Fatalf("unexpected false-positive failed extent %s reported", extentID)
		}
	}
}

// TestCategoryF_ScrubRepairsAllDetectedCorruptions verifies that repair phase
// successfully fixes all detected corruptions in a single scrub run.
//
// VERIFICATION GOALS:
// - All detected corruptions repaired
// - HealedCount matches FailedCount (all fixed)
// - Repaired data matches original checksums
// - No data loss during repair
//
// FAILURE SCENARIOS:
// - Corrupt 3 extents
// - Run scrub with repair enabled
//
// EXPECTED OUTCOMES:
// - HealedCount == 3 (matching FailedCount)
// - result.Healthy == true after repair
// - All healed extents match original data
// - Checksums now validate successfully
func TestCategoryF_ScrubRepairsAllDetectedCorruptions(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	payloads := make(map[string][]byte, 5)
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/scrub/repair-%02d.bin", i)
		payload := bytes.Repeat([]byte{byte('k' + i)}, (2<<20)+(i*211)+321)
		payloads[path] = payload
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payload,
		}); err != nil {
			t.Fatalf("write %s failed: %v", path, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	selected := make([]metadata.Extent, 0, 3)
	seenGroups := make(map[string]struct{}, 3)
	for _, extent := range state.Extents {
		if _, exists := seenGroups[extent.ParityGroupID]; exists {
			continue
		}
		seenGroups[extent.ParityGroupID] = struct{}{}
		selected = append(selected, extent)
		if len(selected) == 3 {
			break
		}
	}
	if len(selected) < 3 {
		t.Fatalf("expected extents across at least 3 parity groups, got %d groups from %d extents", len(selected), len(state.Extents))
	}

	corrupted := make(map[string]struct{}, 3)
	for i, extent := range selected {
		corrupted[extent.ExtentID] = struct{}{}
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(extentPath)
		if err != nil {
			t.Fatalf("read extent %s failed: %v", extent.ExtentID, err)
		}
		if len(data) == 0 {
			t.Fatalf("extent %s is empty", extent.ExtentID)
		}
		data[0] ^= byte(0x31 + i)
		if len(data) > 7 {
			data[7] ^= byte(0x71 + i)
		}
		if err := os.WriteFile(extentPath, data, 0o600); err != nil {
			t.Fatalf("corrupt extent %s failed: %v", extent.ExtentID, err)
		}
	}

	scrubResult, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("scrub repair failed: %v", err)
	}
	if !scrubResult.Healthy {
		t.Fatalf("expected healthy scrub result after repair, got issues: %+v", scrubResult.Issues)
	}
	if scrubResult.HealedCount != 3 {
		t.Fatalf("expected HealedCount=3, got %d", scrubResult.HealedCount)
	}
	if scrubResult.FailedCount != 0 {
		t.Fatalf("expected FailedCount=0 after successful repair, got %d", scrubResult.FailedCount)
	}

	healedIDs := make(map[string]struct{}, len(scrubResult.HealedExtentIDs))
	for _, id := range scrubResult.HealedExtentIDs {
		healedIDs[id] = struct{}{}
	}
	if len(healedIDs) != 3 {
		t.Fatalf("expected exactly 3 healed extent IDs, got %d (%v)", len(healedIDs), scrubResult.HealedExtentIDs)
	}
	for extentID := range corrupted {
		if _, ok := healedIDs[extentID]; !ok {
			t.Fatalf("expected corrupted extent %s to be healed, got %v", extentID, scrubResult.HealedExtentIDs)
		}
	}

	for path, want := range payloads {
		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s failed after scrub repair: %v", path, err)
		}
		if !bytes.Equal(readResult.Data, want) {
			t.Fatalf("read data mismatch after scrub repair for %s", path)
		}
	}

	state, err = coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after scrub failed: %v", err)
	}
	for _, extent := range state.Extents {
		if _, ok := corrupted[extent.ExtentID]; !ok {
			continue
		}
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(extentPath)
		if err != nil {
			t.Fatalf("read healed extent %s failed: %v", extent.ExtentID, err)
		}
		if digestBytes(data) != extent.Checksum {
			t.Fatalf("healed extent %s checksum mismatch after scrub repair", extent.ExtentID)
		}
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after scrub repair: %v", violations[0])
	}
}

// TestCategoryF_ScrubInterruptionAndResumption verifies that scrub correctly
// handles interruption (process crash/kill) and can be safely resumed.
//
// VERIFICATION GOALS:
// - Partial scrub state persisted
// - Resumed scrub picks up from checkpoint
// - No double-processing of extents
// - Final result accurate despite interruption
//
// FAILURE SCENARIOS:
// - Large pool with many extents
// - Scrub started, killed after partial completion
// - Coordinator reopened and scrub resumed
//
// EXPECTED OUTCOMES:
// - Second scrub completes remaining extents
// - All extents checked exactly once total
// - Final result matches full fresh scrub
// - No data corruption from interruption
func TestCategoryF_ScrubInterruptionAndResumption(t *testing.T) {
	t.Skip("Category F not yet implemented: scrub checkpoint/resumption not yet designed")

	// PSEUDOCODE:
	// 1. Write 20 files to create large extent set
	// 2. Corrupt 2 extents
	// 3. Start scrub in goroutine with hook to stop mid-way
	// 4. Hook triggers after checking 50% of extents
	// 5. Stop scrub mid-execution
	// 6. Create new coordinator (simulating restart)
	// 7. Resume scrub
	// 8. Assert: all extents eventually checked
	// 9. Assert: both corrupt extents detected
	// 10. Assert: no double-checks of already-scanned extents

	root := t.TempDir()
	_ = NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	// Implementation to follow
}

// TestCategoryF_ScrubWithAuditOnlyMode verifies that audit-only scrub (repair=false)
// reports corruption without modifying data.
//
// VERIFICATION GOALS:
// - Audit mode detects corruptions accurately
// - No repairs performed (data unchanged)
// - FailedCount and HealedCount reported correctly
// - File data remains corrupted after audit
//
// FAILURE SCENARIOS:
// - Corrupt 2 extents
// - Run scrub with repair disabled
//
// EXPECTED OUTCOMES:
// - FailedCount == 2 (corruption detected)
// - HealedCount == 0 (no repair performed)
// - Extent data remains corrupted
// - result.Healthy == false
func TestCategoryF_ScrubWithAuditOnlyMode(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	originalPayload := bytes.Repeat([]byte("X"), (1<<20)+5432)
	result, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/audit/test.bin",
		Payload:     originalPayload,
	})
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	if len(result.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}

	extentPath := filepath.Join(root, result.Extents[0].PhysicalLocator.RelativePath)
	corruptData, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent for corruption failed: %v", err)
	}
	originalExtentBytes := make([]byte, len(corruptData))
	copy(originalExtentBytes, corruptData)

	corruptData[0] ^= 0xFF
	if err := os.WriteFile(extentPath, corruptData, 0o600); err != nil {
		t.Fatalf("corrupt extent write failed: %v", err)
	}

	scrubResult, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("audit-only scrub failed: %v", err)
	}

	if scrubResult.Healthy {
		t.Fatal("corrupted pool reported as healthy after audit scrub")
	}

	if scrubResult.FailedCount == 0 {
		t.Fatal("expected FailedCount > 0 for corrupted extent")
	}

	if scrubResult.HealedCount != 0 {
		t.Fatalf("audit-only scrub should not repair, expected HealedCount=0, got %d", scrubResult.HealedCount)
	}

	verifyCorruptData, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("verify read of extent failed: %v", err)
	}

	if !bytes.Equal(verifyCorruptData, corruptData) {
		t.Fatal("extent data was modified by audit-only scrub")
	}

	if bytes.Equal(verifyCorruptData, originalExtentBytes) {
		t.Fatal("extent data unchanged after corruption (test setup issue)")
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after audit scrub: %v", violations[0])
	}
}

// TestCategoryF_ScrubPersistsHistoryAndStats verifies that scrub operation
// history is recorded for auditing and trend analysis.
//
// VERIFICATION GOALS:
// - Scrub run recorded with timestamp
// - Statistics persisted (files/extents checked, healed/failed counts)
// - Multiple scrub runs create history entries
// - History queryable and accurate
//
// FAILURE SCENARIOS:
// - Run scrub multiple times on same pool
// - Each scrub may have different outcomes
//
// EXPECTED OUTCOMES:
// - Each scrub run persisted in metadata
// - History includes timestamp, results, extent IDs
// - Multiple entries don't interfere with each other
// - Can retrieve full history
func TestCategoryF_ScrubPersistsHistoryAndStats(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("history-scrub-"), 200000)
	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/scrub/history.bin",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	first, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("first scrub failed: %v", err)
	}
	if !first.Healthy {
		t.Fatalf("expected healthy first scrub, got issues: %+v", first.Issues)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after first scrub failed: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("expected at least one extent after initial write")
	}

	target := state.Extents[0]
	extentPath := filepath.Join(root, target.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read target extent failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("target extent is empty")
	}
	data[0] ^= 0x5c
	if len(data) > 13 {
		data[13] ^= 0x3a
	}
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("write corrupted extent failed: %v", err)
	}

	second, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("second scrub failed: %v", err)
	}
	if !second.Healthy {
		t.Fatalf("expected healthy second scrub after repair, got issues: %+v", second.Issues)
	}
	if second.HealedCount < 1 {
		t.Fatalf("expected at least one healed extent in second scrub, got %d", second.HealedCount)
	}

	reloaded := NewCoordinator(metaPath, journalPath)
	persisted, err := reloaded.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after reload failed: %v", err)
	}
	if len(persisted.ScrubHistory) != 2 {
		t.Fatalf("expected 2 scrub history entries, got %d", len(persisted.ScrubHistory))
	}

	firstRun := persisted.ScrubHistory[0]
	secondRun := persisted.ScrubHistory[1]
	if !firstRun.StartedAt.Before(secondRun.StartedAt) && !firstRun.StartedAt.Equal(secondRun.StartedAt) {
		t.Fatalf("expected first scrub to start no later than second scrub: first=%v second=%v", firstRun.StartedAt, secondRun.StartedAt)
	}
	if !firstRun.CompletedAt.Before(secondRun.CompletedAt) && !firstRun.CompletedAt.Equal(secondRun.CompletedAt) {
		t.Fatalf("expected first scrub to complete no later than second scrub: first=%v second=%v", firstRun.CompletedAt, secondRun.CompletedAt)
	}
	if firstRun.RunID == secondRun.RunID {
		t.Fatalf("expected distinct scrub run ids, both were %q", firstRun.RunID)
	}

	if firstRun.Repair != first.Repair || firstRun.Healthy != first.Healthy ||
		firstRun.FilesChecked != first.FilesChecked ||
		firstRun.ExtentsChecked != first.ExtentsChecked ||
		firstRun.ParityGroupsChecked != first.ParityGroupsChecked ||
		firstRun.HealedCount != first.HealedCount ||
		firstRun.FailedCount != first.FailedCount ||
		firstRun.IssueCount != len(first.Issues) {
		t.Fatalf("first persisted scrub run does not match runtime result: persisted=%+v runtime=%+v", firstRun, first)
	}

	if secondRun.Repair != second.Repair || secondRun.Healthy != second.Healthy ||
		secondRun.FilesChecked != second.FilesChecked ||
		secondRun.ExtentsChecked != second.ExtentsChecked ||
		secondRun.ParityGroupsChecked != second.ParityGroupsChecked ||
		secondRun.HealedCount != second.HealedCount ||
		secondRun.FailedCount != second.FailedCount ||
		secondRun.IssueCount != len(second.Issues) {
		t.Fatalf("second persisted scrub run does not match runtime result: persisted=%+v runtime=%+v", secondRun, second)
	}

	if len(firstRun.HealedExtentIDs) != len(first.HealedExtentIDs) {
		t.Fatalf("first persisted healed extent ids mismatch: persisted=%v runtime=%v", firstRun.HealedExtentIDs, first.HealedExtentIDs)
	}
	if len(secondRun.HealedExtentIDs) != len(second.HealedExtentIDs) {
		t.Fatalf("second persisted healed extent ids mismatch: persisted=%v runtime=%v", secondRun.HealedExtentIDs, second.HealedExtentIDs)
	}
	for i := range firstRun.HealedExtentIDs {
		if firstRun.HealedExtentIDs[i] != first.HealedExtentIDs[i] {
			t.Fatalf("first persisted healed extent id mismatch at %d: persisted=%v runtime=%v", i, firstRun.HealedExtentIDs, first.HealedExtentIDs)
		}
	}
	for i := range secondRun.HealedExtentIDs {
		if secondRun.HealedExtentIDs[i] != second.HealedExtentIDs[i] {
			t.Fatalf("second persisted healed extent id mismatch at %d: persisted=%v runtime=%v", i, secondRun.HealedExtentIDs, second.HealedExtentIDs)
		}
	}
}

// TestCategoryF_ScrubDetectsPartialFileCorruption verifies scrub handles files
// with some extents corrupted and others healthy.
//
// VERIFICATION GOALS:
// - File with N extents, M corrupted correctly detected
// - Non-corrupted extents pass validation
// - Repair handles partial corruption
// - File usability after repair
//
// FAILURE SCENARIOS:
// - Write file with 3 extents
// - Corrupt extent[0] and extent[2] but not [1]
// - Run scrub
//
// EXPECTED OUTCOMES:
// - FailedCount == 2
// - Extent[1] healthy
// - After repair: all 3 extents healthy
// - File fully readable
func TestCategoryF_ScrubDetectsPartialFileCorruption(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	path := "/scrub/partial-file.bin"
	payload := bytes.Repeat([]byte("partial-file-"), 260000)
	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: path,
		Payload:     payload,
	}); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	_, extents, err := findFileExtents(state, path)
	if err != nil {
		t.Fatalf("findFileExtents failed: %v", err)
	}
	if len(extents) < 3 {
		t.Fatalf("expected at least 3 extents for %s, got %d", path, len(extents))
	}

	sort.Slice(extents, func(i, j int) bool {
		return extents[i].LogicalOffset < extents[j].LogicalOffset
	})

	targets := []metadata.Extent{extents[0], extents[2]}
	middle := extents[1]
	corrupted := make(map[string]struct{}, len(targets))
	for i, extent := range targets {
		corrupted[extent.ExtentID] = struct{}{}
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(extentPath)
		if err != nil {
			t.Fatalf("read extent %s failed: %v", extent.ExtentID, err)
		}
		if len(data) == 0 {
			t.Fatalf("extent %s is empty", extent.ExtentID)
		}
		data[0] ^= byte(0x41 + i)
		if len(data) > 11 {
			data[11] ^= byte(0x61 + i)
		}
		if err := os.WriteFile(extentPath, data, 0o600); err != nil {
			t.Fatalf("write corrupted extent %s failed: %v", extent.ExtentID, err)
		}
	}

	auditResult, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false) failed: %v", err)
	}
	if auditResult.Healthy {
		t.Fatal("expected audit scrub to report unhealthy file")
	}
	if auditResult.FailedCount != 2 {
		t.Fatalf("expected FailedCount=2, got %d", auditResult.FailedCount)
	}
	if auditResult.HealedCount != 0 {
		t.Fatalf("expected HealedCount=0 during audit-only scrub, got %d", auditResult.HealedCount)
	}

	failedExtentIDs := make(map[string]struct{})
	for _, issue := range auditResult.Issues {
		if issue.Kind == "extent" && issue.Status == "failed" {
			failedExtentIDs[issue.ExtentID] = struct{}{}
		}
	}
	if len(failedExtentIDs) != 2 {
		t.Fatalf("expected exactly 2 failed extent ids, got %d (%v)", len(failedExtentIDs), auditResult.Issues)
	}
	for extentID := range corrupted {
		if _, ok := failedExtentIDs[extentID]; !ok {
			t.Fatalf("expected corrupted extent %s in audit result, got %v", extentID, failedExtentIDs)
		}
	}
	if _, ok := failedExtentIDs[middle.ExtentID]; ok {
		t.Fatalf("healthy middle extent %s incorrectly reported failed", middle.ExtentID)
	}

	repairResult, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub(true) failed: %v", err)
	}
	if !repairResult.Healthy {
		t.Fatalf("expected repair scrub to finish healthy, got issues: %+v", repairResult.Issues)
	}
	if repairResult.HealedCount != 2 {
		t.Fatalf("expected HealedCount=2, got %d", repairResult.HealedCount)
	}
	if repairResult.FailedCount != 0 {
		t.Fatalf("expected FailedCount=0 after successful repair, got %d", repairResult.FailedCount)
	}

	healedIDs := make(map[string]struct{})
	for _, id := range repairResult.HealedExtentIDs {
		healedIDs[id] = struct{}{}
	}
	for extentID := range corrupted {
		if _, ok := healedIDs[extentID]; !ok {
			t.Fatalf("expected corrupted extent %s to be healed, got %v", extentID, repairResult.HealedExtentIDs)
		}
	}
	if _, ok := healedIDs[middle.ExtentID]; ok {
		t.Fatalf("healthy middle extent %s incorrectly reported healed", middle.ExtentID)
	}

	readResult, err := coord.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after repair failed: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("read data mismatch after partial-file repair")
	}

	state, err = coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after repair failed: %v", err)
	}
	for _, extent := range state.Extents {
		if _, ok := corrupted[extent.ExtentID]; !ok {
			continue
		}
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(extentPath)
		if err != nil {
			t.Fatalf("read healed extent %s failed: %v", extent.ExtentID, err)
		}
		if digestBytes(data) != extent.Checksum {
			t.Fatalf("healed extent %s checksum mismatch after repair", extent.ExtentID)
		}
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after partial-file repair: %v", violations[0])
	}
}

func TestCategoryF_ScrubRepairsParityCorruptionAcrossGroups(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	payloads := make(map[string][]byte)
	for i := 0; i < 4; i++ {
		path := fmt.Sprintf("/scrub/parity-%02d.bin", i)
		payload := bytes.Repeat([]byte{byte('p' + i)}, (2<<20)+(i*173)+257)
		payloads[path] = payload
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payload,
		}); err != nil {
			t.Fatalf("WriteFile %s failed: %v", path, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if len(state.ParityGroups) < 3 {
		t.Fatalf("expected at least 3 parity groups, got %d", len(state.ParityGroups))
	}

	selectedGroups := make([]metadata.ParityGroup, 0, 3)
	for i := 0; i < 3; i++ {
		selectedGroups = append(selectedGroups, state.ParityGroups[i])
	}

	corruptedGroupIDs := make(map[string]struct{}, len(selectedGroups))
	for i, group := range selectedGroups {
		corruptedGroupIDs[group.ParityGroupID] = struct{}{}
		parityPath := filepath.Join(root, "parity", group.ParityGroupID+".bin")
		data, err := os.ReadFile(parityPath)
		if err != nil {
			t.Fatalf("read parity file %s failed: %v", group.ParityGroupID, err)
		}
		if len(data) == 0 {
			t.Fatalf("parity file for group %s is empty", group.ParityGroupID)
		}
		data[0] ^= byte(0x21 + i)
		if len(data) > 9 {
			data[9] ^= byte(0x51 + i)
		}
		if err := os.WriteFile(parityPath, data, 0o600); err != nil {
			t.Fatalf("corrupt parity file %s failed: %v", group.ParityGroupID, err)
		}
	}

	auditResult, err := coord.Scrub(false)
	if err != nil {
		t.Fatalf("Scrub(false) failed: %v", err)
	}
	if auditResult.Healthy {
		t.Fatal("expected parity corruption to produce unhealthy audit scrub")
	}
	if auditResult.FailedCount != 3 {
		t.Fatalf("expected FailedCount=3, got %d", auditResult.FailedCount)
	}
	if auditResult.HealedCount != 0 {
		t.Fatalf("expected HealedCount=0 during audit-only scrub, got %d", auditResult.HealedCount)
	}

	failedGroups := make(map[string]struct{})
	for _, issue := range auditResult.Issues {
		if issue.Kind == "parity" && issue.Status == "failed" {
			failedGroups[issue.ParityGroupID] = struct{}{}
		}
	}
	if len(failedGroups) != 3 {
		t.Fatalf("expected exactly 3 failed parity groups, got %d (%v)", len(failedGroups), auditResult.Issues)
	}
	for groupID := range corruptedGroupIDs {
		if _, ok := failedGroups[groupID]; !ok {
			t.Fatalf("expected corrupted parity group %s in audit result, got %v", groupID, failedGroups)
		}
	}

	for path, want := range payloads {
		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s failed while parity was corrupted: %v", path, err)
		}
		if !bytes.Equal(readResult.Data, want) {
			t.Fatalf("read data mismatch for %s while parity was corrupted", path)
		}
	}

	repairResult, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub(true) failed: %v", err)
	}
	if !repairResult.Healthy {
		t.Fatalf("expected repair scrub to finish healthy, got issues: %+v", repairResult.Issues)
	}
	if repairResult.HealedCount != 3 {
		t.Fatalf("expected HealedCount=3, got %d", repairResult.HealedCount)
	}
	if repairResult.FailedCount != 0 {
		t.Fatalf("expected FailedCount=0 after parity repair, got %d", repairResult.FailedCount)
	}
	if len(repairResult.HealedExtentIDs) != 0 {
		t.Fatalf("expected no healed data extents for parity-only repair, got %v", repairResult.HealedExtentIDs)
	}

	healedGroups := make(map[string]struct{})
	for _, groupID := range repairResult.HealedParityGroupIDs {
		healedGroups[groupID] = struct{}{}
	}
	if len(healedGroups) != 3 {
		t.Fatalf("expected exactly 3 healed parity groups, got %d (%v)", len(healedGroups), repairResult.HealedParityGroupIDs)
	}
	for groupID := range corruptedGroupIDs {
		if _, ok := healedGroups[groupID]; !ok {
			t.Fatalf("expected corrupted parity group %s to be healed, got %v", groupID, repairResult.HealedParityGroupIDs)
		}
	}

	state, err = coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after repair failed: %v", err)
	}
	groupByID := make(map[string]metadata.ParityGroup, len(state.ParityGroups))
	for _, group := range state.ParityGroups {
		groupByID[group.ParityGroupID] = group
	}
	for groupID := range corruptedGroupIDs {
		group, ok := groupByID[groupID]
		if !ok {
			t.Fatalf("parity group %s missing after repair", groupID)
		}
		parityPath := filepath.Join(root, "parity", group.ParityGroupID+".bin")
		data, err := os.ReadFile(parityPath)
		if err != nil {
			t.Fatalf("read healed parity file %s failed: %v", groupID, err)
		}
		if digestBytes(data) != group.ParityChecksum {
			t.Fatalf("healed parity group %s checksum mismatch after repair", groupID)
		}
	}
	for path, want := range payloads {
		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s failed after parity repair: %v", path, err)
		}
		if !bytes.Equal(readResult.Data, want) {
			t.Fatalf("read data mismatch for %s after parity repair", path)
		}
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after parity repair scrub: %v", violations[0])
	}
}

// TestCategoryF_ScrubAcrossMultiplePoolsIsolated verifies that scrub on one
// pool doesn't interfere with another pool's extents.
//
// VERIFICATION GOALS:
// - Pool isolation maintained during scrub
// - Corruption in pool A doesn't report in pool B results
// - Repair in pool A doesn't affect pool B
// - Each pool scrubbed independently
//
// FAILURE SCENARIOS:
// - Create 2 pools with files each
// - Corrupt extents in pool A
// - Run scrub on pool A only
//
// EXPECTED OUTCOMES:
// - Pool A scrub detects its corruption
// - Pool B scrub finds no corruption
// - Repairs isolated to pool A
// - Pool B data unchanged
func TestCategoryF_ScrubAcrossMultiplePoolsIsolated(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	coordA := NewCoordinator(
		filepath.Join(rootA, "metadata.json"),
		filepath.Join(rootA, "journal.log"),
	)
	coordB := NewCoordinator(
		filepath.Join(rootB, "metadata.json"),
		filepath.Join(rootB, "journal.log"),
	)

	payloadA := bytes.Repeat([]byte("pool-a-"), 200000)
	payloadB := bytes.Repeat([]byte("pool-b-"), 200000)

	if _, err := coordA.WriteFile(WriteRequest{
		PoolName:    "poolA",
		LogicalPath: "/data/fileA.bin",
		Payload:     payloadA,
	}); err != nil {
		t.Fatalf("pool A write failed: %v", err)
	}
	if _, err := coordB.WriteFile(WriteRequest{
		PoolName:    "poolB",
		LogicalPath: "/data/fileB.bin",
		Payload:     payloadB,
	}); err != nil {
		t.Fatalf("pool B write failed: %v", err)
	}

	stateA, err := coordA.ReadMeta()
	if err != nil {
		t.Fatalf("pool A ReadMeta failed: %v", err)
	}
	if len(stateA.Extents) == 0 {
		t.Fatal("pool A has no extents")
	}

	targetExtent := stateA.Extents[0]
	extentPath := filepath.Join(rootA, targetExtent.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read pool A extent failed: %v", err)
	}
	data[0] ^= 0x7f
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt pool A extent failed: %v", err)
	}

	scrubA, err := coordA.Scrub(false)
	if err != nil {
		t.Fatalf("pool A scrub failed: %v", err)
	}
	if scrubA.Healthy {
		t.Fatal("expected pool A scrub to report unhealthy")
	}
	if scrubA.FailedCount == 0 {
		t.Fatal("expected pool A scrub to detect corruption")
	}

	scrubB, err := coordB.Scrub(false)
	if err != nil {
		t.Fatalf("pool B scrub failed: %v", err)
	}
	if !scrubB.Healthy {
		t.Fatalf("expected pool B scrub to report healthy, got issues: %+v", scrubB.Issues)
	}
	if scrubB.FailedCount != 0 {
		t.Fatalf("expected pool B scrub to have FailedCount=0, got %d", scrubB.FailedCount)
	}

	stateB, err := coordB.ReadMeta()
	if err != nil {
		t.Fatalf("pool B ReadMeta failed: %v", err)
	}
	for _, extent := range stateB.Extents {
		extentPathB := filepath.Join(rootB, extent.PhysicalLocator.RelativePath)
		dataB, err := os.ReadFile(extentPathB)
		if err != nil {
			t.Fatalf("read pool B extent failed: %v", err)
		}
		if digestBytes(dataB) != extent.Checksum {
			t.Fatalf("pool B extent %s checksum mismatch", extent.ExtentID)
		}
	}

	readB, err := coordB.ReadFile("/data/fileB.bin")
	if err != nil {
		t.Fatalf("pool B read file failed: %v", err)
	}
	if !bytes.Equal(readB.Data, payloadB) {
		t.Fatal("pool B file data mismatch after pool A scrub")
	}
}

// TestCategoryF_ScrubProgressReporting verifies that long-running scrubs
// provide progress feedback during execution.
//
// VERIFICATION GOALS:
// - Progress updates available during scrub
// - Progress monotonically increases
// - Final progress matches completion
// - Operator can track scrub without blocking
//
// FAILURE SCENARIOS:
// - Large pool requiring 5+ second scrub
// - Monitor progress channel during execution
//
// EXPECTED OUTCOMES:
// - Progress events sent periodically
// - Each event has ExtentsChecked > previous
// - Final ExtentsChecked == total extents
// - Can cancel scrub via progress context
func TestCategoryF_ScrubProgressReporting(t *testing.T) {
	t.Skip("Category F not yet implemented: scrub progress tracking design")

	// PSEUDOCODE:
	// 1. Write many files to create long-running scrub
	// 2. Start scrub in background
	// 3. Listen to progress channel
	// 4. Collect progress events for 500ms
	// 5. Assert: received multiple progress updates
	// 6. Assert: progress monotonically increasing
	// 7. Assert: extents/files checked increasing
	// 8. Wait for final result
	// 9. Assert: final progress == total extents

	root := t.TempDir()
	_ = NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	// Implementation to follow
}

// TestCategoryF_ScrubRepairDoesntCorruptHealthyExtents verifies that repair
// operations don't introduce corruption in healthy extents.
//
// VERIFICATION GOALS:
// - Repair touches only corrupted extents
// - Healthy extents checksums unchanged
// - No write amplification damage
// - Repair is conservative and safe
//
// FAILURE SCENARIOS:
// - Write 5 files, corrupt 1 extent
// - Run repair
// - Verify other 4 files unmodified
//
// EXPECTED OUTCOMES:
// - Only the 1 extent modified
// - Other 4 files checksums match before/after
// - Repair transaction shows only 1 extent
// - No cascading corruption
func TestCategoryF_ScrubRepairDoesntCorruptHealthyExtents(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	originalPayloads := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/scrub/safety-%d.bin", i)
		payload := bytes.Repeat([]byte{byte('s' + i)}, (1<<20)+(i*151)+89)
		originalPayloads[path] = payload
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payload,
		}); err != nil {
			t.Fatalf("write %s failed: %v", path, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	_, targetExtents, err := findFileExtents(state, "/scrub/safety-3.bin")
	if err != nil {
		t.Fatalf("findFileExtents for safety-3.bin failed: %v", err)
	}
	if len(targetExtents) == 0 {
		t.Fatal("expected at least one extent for safety-3.bin")
	}

	targetExtent := targetExtents[0]
	extentPath := filepath.Join(root, targetExtent.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read target extent failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("target extent is empty")
	}
	data[0] ^= 0x3c
	if len(data) > 5 {
		data[5] ^= 0x7e
	}
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt target extent failed: %v", err)
	}

	repairResult, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub(true) failed: %v", err)
	}
	if !repairResult.Healthy {
		t.Fatalf("expected healthy scrub result after repair, got issues: %+v", repairResult.Issues)
	}
	if repairResult.HealedCount != 1 {
		t.Fatalf("expected HealedCount=1, got %d", repairResult.HealedCount)
	}
	if repairResult.FailedCount != 0 {
		t.Fatalf("expected FailedCount=0 after successful repair, got %d", repairResult.FailedCount)
	}

if len(repairResult.HealedExtentIDs) != 1 {
		t.Fatalf("expected exactly 1 healed extent, got %d (%v)", len(repairResult.HealedExtentIDs), repairResult.HealedExtentIDs)
	}
	if repairResult.HealedExtentIDs[0] != targetExtent.ExtentID {
		t.Fatalf("expected healed extent %s, got %s", targetExtent.ExtentID, repairResult.HealedExtentIDs[0])
	}

	for path, want := range originalPayloads {
		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s after repair failed: %v", path, err)
		}
		if !bytes.Equal(readResult.Data, want) {
			t.Fatalf("read data mismatch for %s after repair", path)
		}
	}

	state, err = coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after repair failed: %v", err)
	}

	extentByID := make(map[string]metadata.Extent, len(state.Extents))
	for _, extent := range state.Extents {
		extentByID[extent.ExtentID] = extent
	}

	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/scrub/safety-%d.bin", i)
		_, fileExtents, err := findFileExtents(state, path)
		if err != nil {
			t.Fatalf("findFileExtents for %s after repair failed: %v", path, err)
		}
		for _, extent := range fileExtents {
			extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
			data, err := os.ReadFile(extentPath)
			if err != nil {
				t.Fatalf("read extent %s for file %s after repair failed: %v", extent.ExtentID, path, err)
			}
			if digestBytes(data) != extent.Checksum {
				t.Fatalf("extent %s for file %s checksum mismatch after repair", extent.ExtentID, path)
			}
		}
	}

	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after repair: %v", violations[0])
	}
}
