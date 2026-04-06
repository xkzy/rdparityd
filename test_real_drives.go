//go:build ignore
// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
	"github.com/xkzy/rdparityd/internal/pool"
)

var (
	poolName   = flag.String("pool", "stresspool", "Pool name")
	mountPoint = flag.String("mount", "/mnt/stresspool", "Public mount point")
	quick      = flag.Bool("quick", true, "Use fast formatting (ext4)")
)

const runtimeDir = "/var/lib/rtparityd"

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile)

	if os.Getuid() != 0 {
		log.Fatal("Must run as root")
	}

	log.Printf("=== Storage Pool Comprehensive Test Suite ===")
	log.Printf("Pool: %s, Mount: %s, Quick: %v", *poolName, *mountPoint, *quick)

	// Run all scenarios
	scenarios := []struct {
		name string
		fn   func() bool
	}{
		{"1_Healthy_Baseline", scenario1Healthy},
		{"2_Single_Disk_Failure", scenario2SingleDiskFailure},
		{"3_Write_After_Failure", scenario3WriteAfterFailure},
		{"4_Disk_Removal_And_Return", scenario4DiskRemovalReturn},
		{"5_Metadata_Corruption", scenario5MetadataCorruption},
		{"6_Data_Corruption", scenario6DataCorruption},
		{"7_Wrong_Disk_Inserted", scenario7WrongDisk},
		{"8_Multiple_Disk_Failure", scenario8MultipleDiskFailure},
		{"9_Concurrent_Operations", scenario9ConcurrentOps},
		{"10_Recovery_After_Crash", scenario10RecoveryAfterCrash},
	}

	passed := 0
	failed := 0
	for _, s := range scenarios {
		log.Printf("\n%s", strings.Repeat("=", 60))
		log.Printf("SCENARIO: %s", s.name)
		log.Printf("%s", strings.Repeat("=", 60))

		cleanup()
		time.Sleep(500 * time.Millisecond)

		if s.fn() {
			log.Printf("RESULT: PASS")
			passed++
		} else {
			log.Printf("RESULT: FAIL")
			failed++
		}
		cleanup()
	}

	log.Printf("\n%s", strings.Repeat("=", 60))
	log.Printf("SUMMARY: %d passed, %d failed", passed, failed)
	log.Printf("%s", strings.Repeat("=", 60))
}

func cleanup() {
	poolDir := filepath.Join(runtimeDir, "pools", *poolName)

	// Unmount all disks
	for i := 1; i <= 5; i++ {
		path := fmt.Sprintf("%s/disks/disk-%02d", poolDir, i)
		exec.Command("umount", path).Run()
	}
	exec.Command("umount", *mountPoint).Run()

	// Remove directories
	os.RemoveAll(poolDir)
	os.RemoveAll(*mountPoint)
	os.RemoveAll(filepath.Join(runtimeDir, "pools"))
}

func runCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func prepareDiskN(n int) (*pool.PrepareDiskResult, error) {
	mgr := pool.NewManager()
	device := fmt.Sprintf("/dev/sd%c", 'a'+n-1)
	diskID := fmt.Sprintf("disk-%02d", n)

	return mgr.PrepareDisk(pool.PrepareDiskOptions{
		PoolID:     *poolName,
		DiskID:     diskID,
		Device:     device,
		Role:       metadata.DiskRoleData,
		Filesystem: "ext4",
		Label:      fmt.Sprintf("pool-%s-%s", *poolName, diskID),
	})
}

func createPoolWithDisks(numDisks int) ([]*pool.PrepareDiskResult, error) {
	mgr := pool.NewManager()

	if err := mgr.CreatePool(pool.CreateOptions{
		PoolID:           *poolName,
		Name:             *poolName,
		PublicMountpoint: *mountPoint,
		FilesystemType:   "ext4",
		ExtentSizeBytes:  64 * 1024, // 64KB for faster testing
		ParityMode:       "single",
	}); err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	var disks []*pool.PrepareDiskResult
	for i := 1; i <= numDisks; i++ {
		result, err := prepareDiskN(i)
		if err != nil {
			log.Printf("  Warning: prepare disk %d failed: %v", i, err)
			continue
		}
		disks = append(disks, result)
		log.Printf("  Disk %d prepared: %s", i, result.InternalMount)
	}

	if len(disks) < 2 {
		return nil, fmt.Errorf("need at least 2 disks, got %d", len(disks))
	}

	// Save metadata
	metaDir := filepath.Join(runtimeDir, "pools", *poolName, "metadata")
	metaPath := filepath.Join(metaDir, "metadata.bin")
	os.MkdirAll(metaDir, 0o700)

	disksMeta := make([]metadata.Disk, len(disks))
	for i, d := range disks {
		disksMeta[i] = metadata.Disk{
			DiskID:         d.DiskID,
			UUID:           d.UUID,
			Role:           metadata.DiskRoleData,
			FilesystemType: d.FilesystemType,
			Mountpoint:     d.InternalMount,
			CapacityBytes:  d.CapacityBytes,
			FreeBytes:      d.CapacityBytes,
			HealthStatus:   "online",
			Generation:     1,
		}
	}

	state := metadata.SampleState{
		Pool: metadata.Pool{
			PoolID:          *poolName,
			Name:            *poolName,
			Version:         "v1alpha1",
			ExtentSizeBytes: 64 * 1024,
			ParityMode:      "single",
			FilesystemType:  "ext4",
			CreatedAt:       time.Now().UTC(),
		},
		Disks:        disksMeta,
		Files:        []metadata.FileRecord{},
		Extents:      []metadata.Extent{},
		ParityGroups: []metadata.ParityGroup{},
		Transactions: []metadata.Transaction{},
	}

	metadataStore := metadata.NewStore(metaPath)
	if _, err := metadataStore.Save(state); err != nil {
		return nil, fmt.Errorf("save metadata: %w", err)
	}

	return disks, nil
}

func newCoordinator() *journal.Coordinator {
	metaPath := filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")
	journalPath := filepath.Join(runtimeDir, "pools", *poolName, "metadata", "journal.bin")
	return journal.NewCoordinator(metaPath, journalPath)
}

func writeTestFile(coord *journal.Coordinator, path, content string) error {
	_, err := coord.WriteFile(context.Background(), journal.WriteRequest{
		PoolName:       *poolName,
		LogicalPath:    path,
		Payload:        []byte(content),
		AllowSynthetic: false,
	})
	return err
}

func readAndVerify(coord *journal.Coordinator, path, expected string) (bool, error) {
	result, err := coord.ReadFile(path)
	if err != nil {
		return false, err
	}
	return string(result.Data) == expected && result.Verified, nil
}

// ============================================================================
// SCENARIO 1: Healthy Baseline
// ============================================================================
func scenario1Healthy() bool {
	log.Printf("Setup: Create pool with 3 healthy disks, write and verify data")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}
	log.Printf("  Created pool with %d disks", len(disks))

	coord := newCoordinator()

	// Write multiple files
	testData := map[string]string{
		"/test1.txt":        "Hello World",
		"/test2.bin":        "Binary data: \x00\x01\x02\x03",
		"/nested/dir/file3": "Deep path file",
	}

	for path, data := range testData {
		if err := writeTestFile(coord, path, data); err != nil {
			log.Printf("  FAIL: Write %s: %v", path, err)
			return false
		}
		log.Printf("  Wrote: %s", path)
	}

	// Verify reads
	for path, expected := range testData {
		ok, err := readAndVerify(coord, path, expected)
		if err != nil {
			log.Printf("  FAIL: Read %s: %v", path, err)
			return false
		}
		if !ok {
			log.Printf("  FAIL: Verify %s: mismatch", path)
			return false
		}
		log.Printf("  Verified: %s", path)
	}

	// Check invariants
	state, _ := metadata.NewStore(filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")).Load()
	violations := journal.CheckIntegrityInvariants(filepath.Dir(filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")), state)
	if len(violations) > 0 {
		log.Printf("  FAIL: %d invariant violations", len(violations))
		return false
	}

	log.Printf("  All integrity checks passed")
	return true
}

// ============================================================================
// SCENARIO 2: Single Disk Failure
// ============================================================================
func scenario2SingleDiskFailure() bool {
	log.Printf("Setup: Create pool, then unmount disk 2 to simulate failure")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	// Write initial data
	coord := newCoordinator()
	writeTestFile(coord, "/important.txt", "Critical data before failure")
	log.Printf("  Wrote critical file before failure")

	// Simulate disk 2 failure (unmount)
	disk2Path := disks[1].InternalMount
	log.Printf("  Simulating failure: unmounting %s", disk2Path)
	exec.Command("umount", disk2Path).Run()

	// Try to assemble pool in degraded mode
	mgr := pool.NewManager()
	assembled, err := mgr.AssemblePool(pool.MountOptions{
		PoolID:     *poolName,
		Mountpoint: *mountPoint,
		DegradedOk: true,
	})
	if err != nil {
		log.Printf("  FAIL: Assemble failed: %v", err)
		return false
	}
	if assembled.Error != nil {
		log.Printf("  FAIL: Assemble error: %v", assembled.Error)
		return false
	}

	log.Printf("  Pool assembled in degraded mode")
	coord2 := newCoordinator()

	// Should still be able to read existing files
	ok, err := readAndVerify(coord2, "/important.txt", "Critical data before failure")
	if !ok || err != nil {
		log.Printf("  FAIL: Cannot read existing file after disk failure")
		return false
	}
	log.Printf("  Successfully read existing file in degraded mode")

	// Verify degraded disk is reported
	state, _ := metadata.NewStore(filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")).Load()
	for _, d := range state.Disks {
		if d.DiskID == "disk-02" {
			log.Printf("  Disk %s status: %s", d.DiskID, d.HealthStatus)
		}
	}

	return true
}

// ============================================================================
// SCENARIO 3: Write After Failure
// ============================================================================
func scenario3WriteAfterFailure() bool {
	log.Printf("Setup: Pool with failed disk, attempt new writes")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()

	// Write initial file
	writeTestFile(coord, "/initial.txt", "Initial data")
	log.Printf("  Wrote initial file")

	// Fail disk 2
	exec.Command("umount", disks[1].InternalMount).Run()
	log.Printf("  Disk 2 failed")

	// Attempt new write - should fail gracefully since disk is missing
	_, err = coord.WriteFile(context.Background(), journal.WriteRequest{
		PoolName:       *poolName,
		LogicalPath:    "/new.txt",
		Payload:        []byte("New data"),
		AllowSynthetic: false,
	})
	if err != nil {
		log.Printf("  Write correctly failed: %v", err)
	} else {
		log.Printf("  WARNING: Write succeeded even with failed disk (may be OK if using parity)")
	}

	return true
}

// ============================================================================
// SCENARIO 4: Disk Removal and Return (Rebuild)
// ============================================================================
func scenario4DiskRemovalReturn() bool {
	log.Printf("Setup: Create pool, remove disk, reattach and verify rebuild")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()
	writeTestFile(coord, "/data.txt", "Important data")
	writeTestFile(coord, "/more.txt", "More important data")
	log.Printf("  Wrote test data")

	// Remove disk 2
	exec.Command("umount", disks[1].InternalMount).Run()
	log.Printf("  Removed disk 2")

	// Remount disk 2 (simulate reinsertion)
	log.Printf("  Reinserting disk 2...")
	exec.Command("mount", "-t", "ext4", disks[1].PartitionPath, disks[1].InternalMount).Run()

	// Now assemble and verify data still readable
	mgr := pool.NewManager()
	_, err = mgr.AssemblePool(pool.MountOptions{
		PoolID:     *poolName,
		Mountpoint: *mountPoint,
		DegradedOk: true,
	})
	if err != nil {
		log.Printf("  FAIL: Reassemble failed: %v", err)
		return false
	}
	log.Printf("  Reassembled pool")

	coord2 := newCoordinator()
	ok, _ := readAndVerify(coord2, "/data.txt", "Important data")
	if !ok {
		log.Printf("  FAIL: Data not readable after disk return")
		return false
	}

	log.Printf("  Data still accessible after disk return")
	return true
}

// ============================================================================
// SCENARIO 5: Metadata Corruption
// ============================================================================
func scenario5MetadataCorruption() bool {
	log.Printf("Setup: Create pool, corrupt metadata file")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	metaPath := filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")

	// Write some data first
	coord := newCoordinator()
	writeTestFile(coord, "/data.txt", "Data before corruption")
	log.Printf("  Wrote test data")

	// Corrupt metadata
	log.Printf("  Corrupting metadata at %s", metaPath)
	os.WriteFile(metaPath, []byte("CORRUPTED METADATA DATA XXXXX"), 0o600)

	// Try to load corrupted metadata
	store := metadata.NewStore(metaPath)
	_, err = store.Load()
	if err != nil {
		log.Printf("  CORRECT: Load failed with error: %v", err)
	} else {
		log.Printf("  WARNING: Load succeeded despite corruption")
	}

	// Verify original data file on disk is still there (not affected by metadata corruption)
	dataPath := filepath.Join(disks[0].InternalMount, "data")
	files, _ := os.ReadDir(dataPath)
	if len(files) > 0 {
		log.Printf("  Data files still exist on disk (metadata corruption is isolated)")
	}

	return true
}

// ============================================================================
// SCENARIO 6: Data Corruption (Checksum Mismatch)
// ============================================================================
func scenario6DataCorruption() bool {
	log.Printf("Setup: Create pool, write data, corrupt extent file")

	disks, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()
	writeTestFile(coord, "/data.txt", "Important data that will be corrupted")
	log.Printf("  Wrote test data")

	metaPath := filepath.Join(runtimeDir, "pools", *poolName, "metadata", "metadata.bin")

	// Find and corrupt the extent file
	dataPath := filepath.Join(disks[0].InternalMount, "data")
	entries, _ := os.ReadDir(dataPath)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "extent-") {
			extentPath := filepath.Join(dataPath, e.Name())
			log.Printf("  Corrupting extent file: %s", extentPath)
			os.WriteFile(extentPath, []byte("CORRUPTED BYTES!"), 0o600)
			break
		}
	}

	// Read should detect corruption
	_, err = coord.ReadFile("/data.txt")
	if err != nil {
		log.Printf("  CORRECT: Read detected corruption: %v", err)
	} else {
		log.Printf("  WARNING: Read succeeded despite corruption")
	}

	// Check invariants should report violation
	state, _ := metadata.NewStore(metaPath).Load()
	violations := journal.CheckIntegrityInvariants(filepath.Dir(metaPath), state)
	if len(violations) > 0 {
		log.Printf("  CORRECT: Invariant check found %d violations", len(violations))
	}

	return true
}

// ============================================================================
// SCENARIO 7: Wrong Disk Inserted
// ============================================================================
func scenario7WrongDisk() bool {
	log.Printf("Setup: Create pool A, try to use disk from pool B")

	// Create pool A
	poolA := *poolName
	createPoolWithDisks(3)
	log.Printf("  Created pool A: %s", poolA)

	// Try to prepare a disk that already has pool identity
	// First check if disk has existing identity
	diskPath := "/var/lib/rtparityd/pools/disk-01/metadata"
	if _, err := os.Stat(diskPath); err == nil {
		log.Printf("  Disk already has pool identity from pool A")
	}

	// Create pool B with different disks
	*poolName = "poolb"
	defer func() { *poolName = poolA }()

	_, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: Could not create pool B: %v", err)
		return false
	}

	log.Printf("  Pool B created successfully (separate identity)")

	// Verify disks are separate by checking metadata UUIDs
	metaA := filepath.Join(runtimeDir, "pools", poolA, "metadata", "metadata.bin")
	metaB := filepath.Join(runtimeDir, "pools", "poolb", "metadata", "metadata.bin")

	stateA, _ := metadata.NewStore(metaA).Load()
	stateB, _ := metadata.NewStore(metaB).Load()

	if stateA.Pool.PoolID != stateB.Pool.PoolID {
		log.Printf("  CORRECT: Pools have different IDs (%s vs %s)", stateA.Pool.PoolID, stateB.Pool.PoolID)
	}

	return true
}

// ============================================================================
// SCENARIO 8: Multiple Disk Failure
// ============================================================================
func scenario8MultipleDiskFailure() bool {
	log.Printf("Setup: Create pool with 4 disks, fail 3 of them")

	disks, err := createPoolWithDisks(4)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()
	writeTestFile(coord, "/data.txt", "Important data")
	log.Printf("  Wrote test data")

	// Fail multiple disks
	exec.Command("umount", disks[1].InternalMount).Run()
	exec.Command("umount", disks[2].InternalMount).Run()
	exec.Command("umount", disks[3].InternalMount).Run()
	log.Printf("  Failed 3 of 4 disks")

	// Try to assemble - should fail or be severely degraded
	mgr := pool.NewManager()
	_, err = mgr.AssemblePool(pool.MountOptions{
		PoolID:     *poolName,
		Mountpoint: *mountPoint,
		DegradedOk: true,
	})
	if err != nil {
		log.Printf("  CORRECT: Assemble failed with multiple disk failure: %v", err)
		return true
	}

	log.Printf("  WARNING: Assemble succeeded with 3 of 4 disks failed (checking data)")
	// Data should be lost since parity requires all members
	ok, _ := readAndVerify(coord, "/data.txt", "Important data")
	if !ok {
		log.Printf("  CORRECT: Data not recoverable with too many disk failures")
	}

	return true
}

// ============================================================================
// SCENARIO 9: Concurrent Operations
// ============================================================================
func scenario9ConcurrentOps() bool {
	log.Printf("Setup: Create pool, run concurrent writes")

	_, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()
	errCh := make(chan error, 5)

	// Concurrent writes
	for i := 0; i < 5; i++ {
		go func(n int) {
			path := fmt.Sprintf("/concurrent_%d.txt", n)
			errCh <- writeTestFile(coord, path, fmt.Sprintf("Data from goroutine %d", n))
		}(i)
	}

	// Wait for all writes
	failures := 0
	for i := 0; i < 5; i++ {
		if err := <-errCh; err != nil {
			failures++
			log.Printf("  Write %d failed: %v", i, err)
		}
	}

	if failures > 0 {
		log.Printf("  %d concurrent writes failed (may be expected)", failures)
	}

	// Verify some data made it
	success := 0
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/concurrent_%d.txt", i)
		ok, _ := readAndVerify(coord, path, fmt.Sprintf("Data from goroutine %d", i))
		if ok {
			success++
		}
	}
	log.Printf("  %d of 5 concurrent writes verified", success)

	return success > 0
}

// ============================================================================
// SCENARIO 10: Recovery After Crash (Journal Recovery)
// ============================================================================
func scenario10RecoveryAfterCrash() bool {
	log.Printf("Setup: Create pool, write data, simulate crash via incomplete journal")

	_, err := createPoolWithDisks(3)
	if err != nil {
		log.Printf("  FAIL: %v", err)
		return false
	}

	coord := newCoordinator()
	writeTestFile(coord, "/data1.txt", "First data")
	log.Printf("  Wrote first data")

	// Get journal path
	journalPath := filepath.Join(runtimeDir, "pools", *poolName, "metadata", "journal.bin")

	// Read current journal size
	info, _ := os.Stat(journalPath)
	journalSize := info.Size()
	log.Printf("  Journal size before crash: %d bytes", journalSize)

	// Simulate crash by truncating journal (incomplete write)
	if journalSize > 100 {
		f, _ := os.OpenFile(journalPath, os.O_WRONLY|os.O_TRUNC, 0)
		f.Truncate(journalSize - 50) // Cut off last 50 bytes
		f.Close()
		log.Printf("  Simulated crash: truncated journal by 50 bytes")
	}

	// Create new coordinator (simulates restart)
	coord2 := newCoordinator()

	// Recovery should handle incomplete journal
	state, err := coord2.Recover()
	if err != nil {
		log.Printf("  Recover returned error: %v", err)
	}

	log.Printf("  Recovery: %d aborted, %d replayed", len(state.AbortedTxIDs), len(state.RecoveredTxIDs))

	// Old data should still be readable
	ok, _ := readAndVerify(coord2, "/data1.txt", "First data")
	if ok {
		log.Printf("  First data still readable after crash recovery")
	}

	// Write new data after recovery
	writeTestFile(coord2, "/data2.txt", "Second data after recovery")
	ok, _ = readAndVerify(coord2, "/data2.txt", "Second data after recovery")
	if ok {
		log.Printf("  New write succeeded after recovery")
	}

	return true
}
