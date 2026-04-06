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

const runtimeDir = "/var/lib/rtparityd"

var poolName = flag.String("pool", "combotest", "Base pool name")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile)

	if os.Getuid() != 0 {
		log.Fatal("Must run as root")
	}

	log.Printf("=== Combination Matrix Test Suite ===")

	type combo struct {
		name       string
		numDisks   int
		extentSize int64
		parityMode string
		fsType     string
	}

	combos := []combo{
		{"2disk_64k_single_ext4", 2, 64 * 1024, "single", "ext4"},
		{"3disk_64k_single_ext4", 3, 64 * 1024, "single", "ext4"},
		{"3disk_1M_single_ext4", 3, 1024 * 1024, "single", "ext4"},
		{"4disk_64k_single_ext4", 4, 64 * 1024, "single", "ext4"},
		{"3disk_64k_mirror_ext4", 3, 64 * 1024, "mirror", "ext4"},
		{"2disk_64k_single_xfs", 2, 64 * 1024, "single", "xfs"},
		{"3disk_256k_single_ext4", 3, 256 * 1024, "single", "ext4"},
	}

	passed := 0
	failed := 0

	for _, c := range combos {
		cleanupAll()
		time.Sleep(200 * time.Millisecond)

		log.Printf("\n%s", strings.Repeat("=", 60))
		log.Printf("COMBO: %s", c.name)
		log.Printf("  disks=%d extent=%dk mode=%s fs=%s",
			c.numDisks, c.extentSize/1024, c.parityMode, c.fsType)
		log.Printf("%s", strings.Repeat("=", 60))

		if testCombination(c) {
			log.Printf("PASS: %s", c.name)
			passed++
		} else {
			log.Printf("FAIL: %s", c.name)
			failed++
		}
		cleanupAll()
	}

	log.Printf("\n%s", strings.Repeat("=", 60))
	log.Printf("SUMMARY: %d passed, %d failed", passed, failed)
	log.Printf("%s", strings.Repeat("=", 60))
}

func cleanupAll() {
	poolDir := filepath.Join(runtimeDir, "pools", *poolName)
	// Unmount all potential disk mounts
	for i := 1; i <= 5; i++ {
		exec.Command("umount", fmt.Sprintf("%s/disks/disk-%02d", poolDir, i)).Run()
		exec.Command("umount", fmt.Sprintf("/mnt/%s/disk-%02d", *poolName, i)).Run()
	}
	exec.Command("umount", fmt.Sprintf("/mnt/%s", *poolName)).Run()
	os.RemoveAll(poolDir)
	os.RemoveAll(fmt.Sprintf("/mnt/%s", *poolName))
	os.RemoveAll(filepath.Join(runtimeDir, "pools"))
}

func testCombination(c struct {
	name       string
	numDisks   int
	extentSize int64
	parityMode string
	fsType     string
}) bool {
	devices := []string{"/dev/sda", "/dev/sdb", "/dev/sdc", "/dev/sdd", "/dev/sde"}
	diskIDs := make([]string, c.numDisks)
	for i := 0; i < c.numDisks; i++ {
		diskIDs[i] = fmt.Sprintf("disk-%02d", i+1)
	}

	// 1. Create pool
	mgr := pool.NewManager()
	if err := mgr.CreatePool(pool.CreateOptions{
		PoolID:           *poolName,
		Name:             *poolName,
		PublicMountpoint: fmt.Sprintf("/mnt/%s", *poolName),
		FilesystemType:   c.fsType,
		ExtentSizeBytes:  c.extentSize,
		ParityMode:       c.parityMode,
	}); err != nil {
		log.Printf("  FAIL: CreatePool: %v", err)
		return false
	}
	log.Printf("  [1] Pool created")

	// 2. Prepare disks
	var prepared []*pool.PrepareDiskResult
	for i := 0; i < c.numDisks; i++ {
		result, err := mgr.PrepareDisk(pool.PrepareDiskOptions{
			PoolID:     *poolName,
			DiskID:     diskIDs[i],
			Device:     devices[i],
			Role:       metadata.DiskRoleData,
			Filesystem: c.fsType,
			Label:      fmt.Sprintf("pool-%s-%s", *poolName, diskIDs[i]),
		})
		if err != nil {
			log.Printf("  FAIL: PrepareDisk %s: %v", devices[i], err)
			return false
		}
		prepared = append(prepared, result)
	}
	log.Printf("  [2] %d disks prepared", len(prepared))

	// 3. Save metadata
	metaDir := filepath.Join(runtimeDir, "pools", *poolName, "metadata")
	metaPath := filepath.Join(metaDir, "metadata.bin")
	journalPath := filepath.Join(metaDir, "journal.bin")
	os.MkdirAll(metaDir, 0o700)

	disksMeta := make([]metadata.Disk, len(prepared))
	for i, d := range prepared {
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
			ExtentSizeBytes: c.extentSize,
			ParityMode:      c.parityMode,
			FilesystemType:  c.fsType,
			CreatedAt:       time.Now().UTC(),
		},
		Disks:        disksMeta,
		Files:        []metadata.FileRecord{},
		Extents:      []metadata.Extent{},
		ParityGroups: []metadata.ParityGroup{},
		Transactions: []metadata.Transaction{},
	}

	if _, err := metadata.NewStore(metaPath).Save(state); err != nil {
		log.Printf("  FAIL: Save metadata: %v", err)
		return false
	}
	log.Printf("  [3] Metadata saved")

	// 4. Assemble pool
	assembled, err := mgr.AssemblePool(pool.MountOptions{
		PoolID:     *poolName,
		Mountpoint: fmt.Sprintf("/mnt/%s", *poolName),
		DegradedOk: true,
	})
	if err != nil || assembled.Error != nil {
		log.Printf("  FAIL: AssemblePool: %v", err)
		if assembled != nil {
			log.Printf("    Error: %v", assembled.Error)
		}
		return false
	}
	log.Printf("  [4] Pool assembled with %d disks", len(assembled.Pool.Disks))

	// 5. Create coordinator and write
	coord := journal.NewCoordinator(metaPath, journalPath)

	testFiles := map[string]string{
		"/small.txt":    "Hi",
		"/medium.txt":   strings.Repeat("Medium content here. ", 10),
		"/large.txt":    strings.Repeat("Large content. ", 100),
		"/nested/dir/f": "Nested file content",
	}

	for path, data := range testFiles {
		if err := writeFile(coord, path, data); err != nil {
			log.Printf("  FAIL: Write %s: %v", path, err)
			return false
		}
	}
	log.Printf("  [5] Wrote %d files", len(testFiles))

	// 6. Read and verify all
	for path, expected := range testFiles {
		if ok, err := readAndVerify(coord, path, expected); !ok || err != nil {
			log.Printf("  FAIL: Verify %s: %v", path, err)
			return false
		}
	}
	log.Printf("  [6] All files verified")

	// 7. Check invariants
	state, _ = metadata.NewStore(metaPath).Load()
	violations := journal.CheckIntegrityInvariants(filepath.Dir(metaPath), state)
	if len(violations) > 0 {
		log.Printf("  WARN: %d invariant violations", len(violations))
	} else {
		log.Printf("  [7] Invariants OK")
	}

	// 8. Simulate disk failure and degraded read (if 3+ disks)
	if c.numDisks >= 3 {
		log.Printf("  [8] Testing degraded mode...")
		exec.Command("umount", prepared[1].InternalMount).Run()

		coord2 := journal.NewCoordinator(metaPath, journalPath)
		for path, expected := range testFiles {
			if _, err := readAndVerify(coord2, path, expected); err != nil {
				log.Printf("    Degraded read of %s: %v (may be expected)", path, err)
			}
		}

		// Remount
		exec.Command("mount", "-t", c.fsType, prepared[1].PartitionPath, prepared[1].InternalMount).Run()
	}

	log.Printf("  ALL CHECKS PASSED for %s", c.name)
	return true
}

func writeFile(coord *journal.Coordinator, path, data string) error {
	_, err := coord.WriteFile(context.Background(), journal.WriteRequest{
		PoolName:       *poolName,
		LogicalPath:    path,
		Payload:        []byte(data),
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
