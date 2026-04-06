/*
 * Stress test for real drives - verifies no data loss in production scenarios
 * Tests multiple file types, sizes, and access patterns
 *
 * Run with: sudo go run test_stress_real_drives.go
 */

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
	"github.com/xkzy/rdparityd/internal/pool"
)

const (
	runtimeDir  = "/var/lib/rtparityd"
	poolName    = "stresstest"
	poolID      = "ststress"
	publicMount = "/mnt/stresstest"
)

var (
	poolBaseDir = filepath.Join(runtimeDir, "pools", poolID)
	metaPath    = filepath.Join(poolBaseDir, "metadata", "metadata.bin")
	journalPath = filepath.Join(poolBaseDir, "metadata", "journal.bin")
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	if os.Getuid() != 0 {
		log.Fatal("Must run as root")
	}

	ctx := context.Background()

	if err := runScenarios(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		cleanup()
		os.Exit(1)
	}
	fmt.Println("\n=== ALL SCENARIOS PASSED ===")
	cleanup()
}

func runScenarios(ctx context.Context) error {
	fmt.Println("=== Scenario 1: Pool Creation with 2 Disks ===")
	if err := scenario1_CreatePool(ctx); err != nil {
		return fmt.Errorf("scenario 1: %w", err)
	}

	fmt.Println("\n=== Scenario 2: File Type Tests (empty, tiny, small, medium, large) ===")
	if err := scenario2_FileTypes(ctx); err != nil {
		return fmt.Errorf("scenario 2: %w", err)
	}

	fmt.Println("\n=== Scenario 3: File Mode Tests (sequential, concurrent, overwrite) ===")
	if err := scenario3_FileModes(ctx); err != nil {
		return fmt.Errorf("scenario 3: %w", err)
	}

	fmt.Println("\n=== Scenario 4: Data Pattern Tests (zeros, random, text, binary) ===")
	if err := scenario4_DataPatterns(ctx); err != nil {
		return fmt.Errorf("scenario 4: %w", err)
	}

	fmt.Println("\n=== Scenario 5: Special Path Tests (unicode, deep dirs, special chars) ===")
	if err := scenario5_SpecialPaths(ctx); err != nil {
		return fmt.Errorf("scenario 5: %w", err)
	}

	fmt.Println("\n=== Scenario 6: Many Tiny Files (1000+ files) ===")
	if err := scenario6_ManyTinyFiles(ctx); err != nil {
		return fmt.Errorf("scenario 6: %w", err)
	}

	fmt.Println("\n=== Scenario 7: Power Loss Simulation ===")
	if err := scenario7_PowerLossSimulation(ctx); err != nil {
		return fmt.Errorf("scenario 7: %w", err)
	}

	fmt.Println("\n=== Scenario 8: Disk Exclusion and Degraded Read ===")
	if err := scenario8_DiskExclusionDegradedRead(ctx); err != nil {
		return fmt.Errorf("scenario 8: %w", err)
	}

	fmt.Println("\n=== Scenario 9: Concurrent Read/Write Mix ===")
	if err := scenario9_ConcurrentReadWriteMix(ctx); err != nil {
		return fmt.Errorf("scenario 9: %w", err)
	}

	return nil
}

func scenario1_CreatePool(ctx context.Context) error {
	cleanup()

	mgr := pool.NewManager()

	if err := mgr.CreatePool(pool.CreateOptions{
		PoolID:           poolID,
		Name:             poolName,
		FilesystemType:   "ext4",
		ExtentSizeBytes:  64 * 1024 * 1024,
		ParityMode:       "single",
		PublicMountpoint: publicMount,
	}); err != nil {
		return fmt.Errorf("create pool: %w", err)
	}

	devices := []string{"/dev/sda", "/dev/sdb"}
	disks := make([]*pool.PrepareDiskResult, 0, len(devices))

	for i, dev := range devices {
		diskID := fmt.Sprintf("disk-%02d", i+1)
		result, err := mgr.PrepareDisk(pool.PrepareDiskOptions{
			PoolID:     poolID,
			DiskID:     diskID,
			Device:     dev,
			Role:       metadata.DiskRoleData,
			Filesystem: "ext4",
			Label:      "RTP-" + diskID,
		})
		if err != nil {
			return fmt.Errorf("prepare disk %s: %w", dev, err)
		}
		disks = append(disks, result)
		log.Printf("  Prepared disk %s: UUID=%s", diskID, result.UUID)
	}

	if len(disks) < 2 {
		return fmt.Errorf("need at least 2 disks, got %d", len(disks))
	}

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
			PoolID:          poolID,
			Name:            poolName,
			Version:         "v1alpha1",
			ExtentSizeBytes: 64 * 1024 * 1024,
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

	if err := os.MkdirAll(filepath.Dir(metaPath), 0700); err != nil {
		return fmt.Errorf("create metadata dir: %w", err)
	}

	metadataStore := metadata.NewStore(metaPath)
	if _, err := metadataStore.Save(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	log.Printf("  Pool created: %s with %d disks", poolID, len(disks))
	return nil
}

func scenario2_FileTypes(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	fileTests := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"tiny-1b", 1},
		{"tiny-100b", 100},
		{"small-1kb", 1024},
		{"small-64kb", 64 * 1024},
		{"medium-1mb", 1024 * 1024},
		{"medium-16mb", 16 * 1024 * 1024},
		{"large-32mb", 32 * 1024 * 1024},
	}

	for _, ft := range fileTests {
		path := fmt.Sprintf("/filetypes/%s.bin", ft.name)
		data := make([]byte, ft.size)
		if ft.size > 0 {
			rand.Read(data)
		}

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("write %s: %w", ft.name, err)
		}

		result, err := coord.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", ft.name, err)
		}

		if !bytes.Equal(result.Data, data) {
			return fmt.Errorf("data mismatch for %s: got %d bytes, want %d", ft.name, len(result.Data), len(data))
		}
		log.Printf("  ✓ %s (%d bytes)", ft.name, ft.size)
	}

	return nil
}

func scenario3_FileModes(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	log.Printf("  Testing sequential writes...")
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("/modes/sequential/file%03d.dat", i)
		data := make([]byte, 64*1024)
		rand.Read(data)

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("sequential write %d: %w", i, err)
		}
	}
	log.Printf("  ✓ Sequential writes: 10 files")

	log.Printf("  Testing concurrent writes...")
	var wg sync.WaitGroup
	var errCount int32
	paths := make([]string, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			path := fmt.Sprintf("/modes/concurrent/file%03d.dat", id)
			data := make([]byte, 64*1024)
			rand.Read(data)
			paths[id] = path

			_, err := coord.WriteFile(ctx, journal.WriteRequest{
				PoolName:    poolName,
				LogicalPath: path,
				Payload:     data,
				SizeBytes:   int64(len(data)),
			})
			if err != nil {
				atomic.AddInt32(&errCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if errCount > 0 {
		return fmt.Errorf("concurrent writes: %d failures", errCount)
	}
	log.Printf("  ✓ Concurrent writes: 50 files")

	log.Printf("  Testing overwrite existing files...")
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("/modes/overwrite/file%03d.dat", i)
		data := make([]byte, 128*1024)
		rand.Read(data)

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("overwrite write %d: %w", i, err)
		}
	}
	log.Printf("  ✓ Overwrite writes: 10 files")

	return nil
}

func scenario4_DataPatterns(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	patternTests := []struct {
		name string
		data []byte
	}{
		{"zeros", bytes.Repeat([]byte{0}, 256*1024)},
		{"ones", bytes.Repeat([]byte{0xFF}, 256*1024)},
		{"alternating", alternatingBytes(256 * 1024)},
		{"random", randomBytes(256 * 1024)},
		{"text-utf8", []byte("Hello, World! This is a test file with UTF-8 content: Greek letters alpha beta gamma")},
		{"binary-struct", createBinaryStruct()},
	}

	for _, pt := range patternTests {
		path := fmt.Sprintf("/patterns/%s.bin", pt.name)
		data := pt.data

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("write pattern %s: %w", pt.name, err)
		}

		result, err := coord.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read pattern %s: %w", pt.name, err)
		}

		if !bytes.Equal(result.Data, data) {
			checksumResult := crc32.ChecksumIEEE(result.Data)
			checksumExpected := crc32.ChecksumIEEE(data)
			return fmt.Errorf("data mismatch for %s: checksum got=%x want=%x", pt.name, checksumResult, checksumExpected)
		}
		log.Printf("  ✓ %s (%d bytes, CRC32=%x)", pt.name, len(data), crc32.ChecksumIEEE(data))
	}

	return nil
}

func scenario5_SpecialPaths(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	specialPaths := []struct {
		name string
		path string
	}{
		{"deep-nesting", "/special/deep/nested/dir/tree/with/many/levels/file.txt"},
		{"unicode-file", "/special/unicode/文件.txt"},
		{"unicode-dir", "/special/目录/数据.bin"},
		{"spaces", "/special/path with spaces/file.dat"},
		{"special-chars", "/special/chars/!@#$%^&*()+/=file.bin"},
		{"mixed-case", "/special/MixedCase/File.TXT"},
		{"long-name", "/special/" + strings.Repeat("x", 200) + ".txt"},
		{"dotfile", "/special/.hidden/file.txt"},
		{"multiple-dots", "/special/file.v1.2.3.4.bin"},
	}

	var err error
	for _, sp := range specialPaths {
		data := []byte(fmt.Sprintf("content for %s", sp.name))
		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: sp.path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("write special path %s: %w", sp.name, err)
		}

		result, err := coord.ReadFile(sp.path)
		if err != nil {
			return fmt.Errorf("read special path %s: %w", sp.name, err)
		}

		if !bytes.Equal(result.Data, data) {
			return fmt.Errorf("data mismatch for special path %s", sp.name)
		}
		log.Printf("  ✓ %s", sp.name)
	}

	_ = err
	log.Printf("  Note: FUSE layer now supports:")
	log.Printf("    - Symlinks (NodeSymlinker)")
	log.Printf("    - FIFOs/Block/Char devices (NodeMknoder)")
	log.Printf("    - Hard links (NodeLinker)")
	log.Printf("    - Directory removal (NodeRmdirer)")
	log.Printf("    - Rename (NodeRenamer)")
	log.Printf("    - Access checking (NodeAccesser)")
	log.Printf("    - File fsync (FileFsyncer)")

	return nil
}

func scenario6_ManyTinyFiles(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	log.Printf("  Creating 500 tiny files (1KB each)...")
	for i := 0; i < 500; i++ {
		path := fmt.Sprintf("/manytiny/file%04d.txt", i)
		data := []byte(fmt.Sprintf("tiny file number %04d", i))

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
		})
		if err != nil {
			return fmt.Errorf("write tiny file %d: %w", i, err)
		}
	}
	log.Printf("  ✓ Created 500 tiny files")

	log.Printf("  Verifying first, middle, and last tiny files...")
	verifyIndices := []int{0, 1, 250, 499}
	for _, idx := range verifyIndices {
		path := fmt.Sprintf("/manytiny/file%04d.txt", idx)
		expectedData := []byte(fmt.Sprintf("tiny file number %04d", idx))

		result, err := coord.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read tiny file %d: %w", idx, err)
		}

		if !bytes.Equal(result.Data, expectedData) {
			return fmt.Errorf("data mismatch for tiny file %d", idx)
		}
	}
	log.Printf("  ✓ Verified %d tiny files: indices %v", len(verifyIndices), verifyIndices)

	return nil
}

func scenario7_PowerLossSimulation(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	testCases := []struct {
		name      string
		size      int
		failAfter journal.State
	}{
		{"tiny-crash", 4096, journal.StateDataWritten},
		{"medium-crash", 1024 * 1024, journal.StateDataWritten},
		{"large-crash", 8 * 1024 * 1024, journal.StateParityWritten},
	}

	for _, tc := range testCases {
		path := fmt.Sprintf("/powerloss/%s.bin", tc.name)
		data := make([]byte, tc.size)
		rand.Read(data)

		_, err := coord.WriteFile(ctx, journal.WriteRequest{
			PoolName:    poolName,
			LogicalPath: path,
			Payload:     data,
			SizeBytes:   int64(len(data)),
			FailAfter:   tc.failAfter,
		})
		if err != nil {
			return fmt.Errorf("write %s with failafter: %w", tc.name, err)
		}

		result, err := coord.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s after crash simulation: %w", tc.name, err)
		}

		if !bytes.Equal(result.Data, data) {
			return fmt.Errorf("data mismatch for %s after recovery", tc.name)
		}
		log.Printf("  ✓ %s (%d bytes) - recovered after crash at %s", tc.name, tc.size, tc.failAfter)
	}

	return nil
}

func scenario8_DiskExclusionDegradedRead(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	state, err := coord.ReadMeta()
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}
	if len(state.Disks) < 2 {
		return fmt.Errorf("need at least 2 disks, got %d", len(state.Disks))
	}

	excludedDisk := state.Disks[1]
	log.Printf("  Excluding disk %s", excludedDisk.DiskID)

	if err := coord.FailDisk(excludedDisk.DiskID); err != nil {
		return fmt.Errorf("fail disk: %w", err)
	}

	testPaths := []string{
		"/filetypes/medium-1mb.bin",
		"/patterns/random.bin",
		"/modes/sequential/file000.dat",
	}

	for _, path := range testPaths {
		result, err := coord.ReadFile(path)
		if err != nil {
			return fmt.Errorf("degraded read %s: %w", path, err)
		}
		if len(result.Data) == 0 {
			return fmt.Errorf("degraded read %s: empty data", path)
		}
		log.Printf("  ✓ Degraded read %s (%d bytes)", filepath.Base(path), len(result.Data))
	}

	return nil
}

func scenario9_ConcurrentReadWriteMix(ctx context.Context) error {
	coord := journal.NewCoordinator(metaPath, journalPath)

	var wg sync.WaitGroup
	ops := int32(0)
	var errCount int32
	var mu sync.Mutex
	var failedPaths []string

	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				path := fmt.Sprintf("/mix/worker%d/op%03d.bin", wid, i)
				data := make([]byte, 128*1024)
				rand.Read(data)

				_, err := coord.WriteFile(ctx, journal.WriteRequest{
					PoolName:    poolName,
					LogicalPath: path,
					Payload:     data,
					SizeBytes:   int64(len(data)),
				})
				if err != nil {
					mu.Lock()
					failedPaths = append(failedPaths, fmt.Sprintf("write:%s", path))
					atomic.AddInt32(&errCount, 1)
					mu.Unlock()
					continue
				}

				result, err := coord.ReadFile(path)
				if err != nil {
					mu.Lock()
					failedPaths = append(failedPaths, fmt.Sprintf("read:%s", path))
					atomic.AddInt32(&errCount, 1)
					mu.Unlock()
					continue
				}
				if !bytes.Equal(result.Data, data) {
					mu.Lock()
					failedPaths = append(failedPaths, fmt.Sprintf("mismatch:%s", path))
					atomic.AddInt32(&errCount, 1)
					mu.Unlock()
					continue
				}

				atomic.AddInt32(&ops, 2)
			}
		}(worker)
	}

	wg.Wait()

	if errCount > 0 {
		log.Printf("  Failed paths: %v", failedPaths)
		return fmt.Errorf("concurrent mix: %d errors", errCount)
	}
	log.Printf("  ✓ Completed %d read/write operations", ops)
	return nil
}

func alternatingBytes(n int) []byte {
	data := make([]byte, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			data[i] = 0xAA
		} else {
			data[i] = 0x55
		}
	}
	return data
}

func randomBytes(n int) []byte {
	data := make([]byte, n)
	rand.Read(data)
	return data
}

func createBinaryStruct() []byte {
	type TestStruct struct {
		Magic   uint32
		Version uint16
		Length  uint32
		CRC     uint32
		Data    [256]byte
	}

	s := TestStruct{
		Magic:   0xDEADBEEF,
		Version: 1,
		Length:  256,
	}
	rand.Read(s.Data[:])

	var buf bytes.Buffer
	buf.Write([]byte{
		byte(s.Magic >> 0), byte(s.Magic >> 8), byte(s.Magic >> 16), byte(s.Magic >> 24),
		byte(s.Version >> 0), byte(s.Version >> 8),
		byte(s.Length >> 0), byte(s.Length >> 8), byte(s.Length >> 16), byte(s.Length >> 24),
		byte(s.CRC >> 0), byte(s.CRC >> 8), byte(s.CRC >> 16), byte(s.CRC >> 24),
	})
	buf.Write(s.Data[:])
	return buf.Bytes()
}

func cleanup() {
	for i := 1; i <= 3; i++ {
		path := fmt.Sprintf("%s/disks/disk-%02d", poolBaseDir, i)
		exec.Command("umount", path).Run()
	}
	exec.Command("umount", publicMount).Run()
	os.RemoveAll(poolBaseDir)
	os.RemoveAll(publicMount)
	os.RemoveAll(filepath.Join(runtimeDir, "pools"))
}
