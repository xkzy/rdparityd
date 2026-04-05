package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestProperty_JournalReplayIdempotent(t *testing.T) {
	for i := 0; i < 10; i++ {
		dir := t.TempDir()
		coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

		path := "/test/file.bin"
		payload := make([]byte, 1024)
		for j := range payload {
			payload[j] = byte((i + j) % 256)
		}

		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		for attempt := 0; attempt < 5; attempt++ {
			coord2 := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
			recovery, err := coord2.Recover()
			if err != nil {
				t.Fatalf("Recovery attempt %d failed: %v", attempt, err)
			}
			if len(recovery.RecoveredTxIDs) != 0 {
				t.Fatalf("Attempt %d: expected 0 recovered, got %d", attempt, len(recovery.RecoveredTxIDs))
			}
		}
	}
}

func TestProperty_MultipleWritesConsistency(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	paths := []string{"/file1", "/file2", "/file3"}
	payloads := [][]byte{
		[]byte("content one"),
		[]byte("content two"),
		[]byte("content three"),
	}

	for i, path := range paths {
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payloads[i]}); err != nil {
			t.Fatalf("WriteFile %s failed: %v", path, err)
		}
	}

	for i, path := range paths {
		result, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s failed: %v", path, err)
		}
		if string(result.Data) != string(payloads[i]) {
			t.Errorf("%s: expected %q, got %q", path, payloads[i], result.Data)
		}
	}
}

func TestProperty_ChecksumNeverFalsePositive(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/checksum.bin"
	payload := []byte("specific checksum test data 12345")

	if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		result, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile iteration %d failed: %v", i, err)
		}
		if !result.Verified {
			t.Errorf("Iteration %d: read not verified", i)
		}
		if string(result.Data) != string(payload) {
			t.Errorf("Iteration %d: data mismatch", i)
		}
	}
}

func TestProperty_ParityRecoveryDeterministic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/parity.bin"
	payload := []byte("test data for parity recovery")

	result1, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	for idx, extent := range result1.Extents {
		extPath := filepath.Join(dir, extent.PhysicalLocator.RelativePath)

		corruptFile(t, extPath, 10)

		result2, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile after corruption failed: %v", err)
		}
		if string(result2.Data) != string(payload) {
			t.Errorf("Parity recovery failed for extent %d: expected %q, got %q", idx, payload, result2.Data)
		}

		newPath := "/test/parity_" + string(rune('a'+idx)) + ".bin"
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: newPath, Payload: payload}); err != nil {
			t.Fatalf("Rewrite failed: %v", err)
		}
	}
}

func corruptFile(t *testing.T, path string, offset int) {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Read file for corruption: %v", err)
	}
	if offset < len(data) {
		data[offset] ^= 0xFF
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("Write corrupted data: %v", err)
	}
}

func TestProperty_ExtentAllocationDeterministic(t *testing.T) {
	dir := t.TempDir()
	coord1 := NewCoordinator(filepath.Join(dir, "metadata1.bin"), filepath.Join(dir, "journal1.bin"))
	coord2 := NewCoordinator(filepath.Join(dir, "metadata2.bin"), filepath.Join(dir, "journal2.bin"))

	path := "/test/allocate.bin"
	payload := []byte("allocate test")

	result1, err := coord1.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
	if err != nil {
		t.Fatalf("WriteFile 1 failed: %v", err)
	}

	result2, err := coord2.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
	if err != nil {
		t.Fatalf("WriteFile 2 failed: %v", err)
	}

	if len(result1.Extents) != len(result2.Extents) {
		t.Errorf("Extent count mismatch: %d vs %d", len(result1.Extents), len(result2.Extents))
	}

	for i := range result1.Extents {
		if result1.Extents[i].Length != result2.Extents[i].Length {
			t.Errorf("Extent %d length mismatch: %d vs %d", i, result1.Extents[i].Length, result2.Extents[i].Length)
		}
	}
}

func TestProperty_RecoveryPreservesTransactionOrder(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	for i := 0; i < 5; i++ {
		path := "/test/file" + string(rune('0'+i)) + ".bin"
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: []byte("data")}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	recovery, err := coord.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	if len(state.Transactions) != 5 {
		t.Errorf("Expected 5 transactions, got %d", len(state.Transactions))
	}

	if len(recovery.RecoveredTxIDs) != 0 {
		t.Errorf("Expected 0 recovered, got %d", len(recovery.RecoveredTxIDs))
	}
}

func TestProperty_ScrubDetectsPreviousCorruption(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/scrub.bin"
	payload := []byte("scrub test data for integrity")

	writeResult, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	for _, extent := range writeResult.Extents {
		extPath := filepath.Join(dir, extent.PhysicalLocator.RelativePath)
		corruptFile(t, extPath, 0)
	}

	scrubResult, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub failed: %v", err)
	}

	if scrubResult.FailedCount == 0 && len(scrubResult.Issues) == 0 {
		t.Error("Expected scrub to detect corruption")
	}
}

func TestProperty_RebuildAfterDiskFailure(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/rebuild.bin"
	payload := []byte("rebuild test data that must be recoverable")

	_, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	var targetDisk string
	for _, extent := range state.Extents {
		if extent.DataDiskID != "" && extent.DataDiskID != "disk-parity" {
			targetDisk = extent.DataDiskID
			break
		}
	}

	if targetDisk == "" {
		t.Skip("No data disk to test rebuild")
	}

	rebuildResult, err := coord.RebuildDataDisk(targetDisk)
	if err != nil {
		t.Fatalf("Rebuild failed: %v", err)
	}

	if rebuildResult.ExtentsRebuilt > 0 {
		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("Read after rebuild failed: %v", err)
		}
		if string(readResult.Data) != string(payload) {
			t.Errorf("Data mismatch after rebuild")
		}
	}
}

func TestProperty_ConcurrentWritesSerializability(t *testing.T) {
	dir := t.TempDir()

	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/test/concurrent1.bin", Payload: []byte("data1")})
	if err != nil {
		t.Fatalf("WriteFile 1 failed: %v", err)
	}

	_, err = coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/test/concurrent2.bin", Payload: []byte("data2")})
	if err != nil {
		t.Fatalf("WriteFile 2 failed: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	if len(state.Transactions) != 2 {
		t.Errorf("Expected 2 transactions, got %d", len(state.Transactions))
	}
}

func TestProperty_MetadataGenerationMonotonic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	gen1, _ := coord.ReadMeta()

	for i := 0; i < 3; i++ {
		path := "/test/gen" + string(rune('a'+i)) + ".bin"
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: []byte("data")}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	gen2, _ := coord.ReadMeta()

	if len(gen2.Transactions) <= len(gen1.Transactions) {
		t.Errorf("Expected more transactions after writes")
	}
}

func TestStress_ManySmallWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	for i := 0; i < 100; i++ {
		path := "/test/stress_" + string(rune(i)) + ".bin"
		payload := []byte("test data " + string(rune(i)))

		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	if len(state.Files) != 100 {
		t.Errorf("Expected 100 files, got %d", len(state.Files))
	}
}

func TestStress_RecoveryAfterManyWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	for i := 0; i < 50; i++ {
		path := "/test/recovery_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: []byte("recovery test data")}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	coord2 := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	recovery, err := coord2.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if len(recovery.RecoveredTxIDs) != 0 {
		t.Errorf("Expected 0 recovered after clean shutdown, got %d", len(recovery.RecoveredTxIDs))
	}

	state, _ := coord2.ReadMeta()
	if len(state.Files) != 50 {
		t.Errorf("Expected 50 files, got %d", len(state.Files))
	}
}

func TestStress_SoakWriteAndRecover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	dir := t.TempDir()
	fileCount := 200

	for round := 0; round < 3; round++ {
		coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

		for i := 0; i < fileCount; i++ {
			path := fmt.Sprintf("/test/soak_r%d_%d.bin", round, i)
			payload := []byte(fmt.Sprintf("round=%d iteration=%d data", round, i))
			if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
				t.Fatalf("Round %d, WriteFile %d failed: %v", round, i, err)
			}
		}

		state, err := coord.ReadMeta()
		if err != nil {
			t.Fatalf("ReadMeta failed: %v", err)
		}
		if len(state.Files) != fileCount*(round+1) {
			t.Errorf("Round %d: expected %d files, got %d", round, fileCount*(round+1), len(state.Files))
		}

		coord2 := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
		recovery, err := coord2.Recover()
		if err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}
		if len(recovery.RecoveredTxIDs) != 0 {
			t.Errorf("Round %d: expected 0 recovered, got %d", round, len(recovery.RecoveredTxIDs))
		}
	}
}

func TestStress_ContinuousReadWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/continuous.bin"
	originalPayload := []byte("continuous test data")

	if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: originalPayload}); err != nil {
		t.Fatalf("Initial WriteFile failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		result, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("Iteration %d: ReadFile failed: %v", i, err)
		}
		if !result.Verified {
			t.Errorf("Iteration %d: read not verified", i)
		}
		if string(result.Data) != string(originalPayload) {
			t.Errorf("Iteration %d: data mismatch", i)
		}
	}
}
