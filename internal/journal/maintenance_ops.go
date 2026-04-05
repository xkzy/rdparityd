package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type TrimResult struct {
	TxID           string `json:"tx_id"`
	DisksTrimmed   int    `json:"disks_trimmed"`
	BytesDiscarded int64  `json:"bytes_discarded"`
}

type DefragResult struct {
	TxID         string `json:"tx_id"`
	ExtentsMoved int    `json:"extents_moved"`
	BytesFreed   int64  `json:"bytes_freed"`
}

type SnapshotResult struct {
	TxID        string    `json:"tx_id"`
	Name        string    `json:"name"`
	CreatedAt   time.Time `json:"created_at"`
	SourceState string    `json:"source_state"`
}

type SleepResult struct {
	TxID       string   `json:"tx_id"`
	DisksWoken int      `json:"disks_woken"`
	DisksSlept int      `json:"disks_slept"`
	DiskIDs    []string `json:"disk_ids"`
}

func (c *Coordinator) Trim() (TrimResult, error) {
	if c == nil {
		return TrimResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return TrimResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return TrimResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return TrimResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	if state.Pool.FilesystemType == "" {
		state.Pool.FilesystemType = "btrfs"
	}

	var disksTrimmed int
	var totalBytes int64

	rootDir := filepath.Dir(c.metadataPath)

	switch state.Pool.FilesystemType {
	case "btrfs":
		disksTrimmed, totalBytes, err = trimBtrfs(state.Disks, rootDir)
	case "ext4":
		disksTrimmed, totalBytes, err = trimExt4(state.Disks, rootDir)
	case "xfs":
		disksTrimmed, totalBytes, err = trimXfs(state.Disks, rootDir)
	case "ntfs":
		disksTrimmed, totalBytes, err = trimNtfs(state.Disks, rootDir)
	case "exfat":
		disksTrimmed, totalBytes, err = trimExfat(state.Disks, rootDir)
	case "hfs", "hfsplus":
		disksTrimmed, totalBytes, err = trimHfs(state.Disks, rootDir)
	case "apfs":
		disksTrimmed, totalBytes, err = trimApfs(state.Disks, rootDir)
	case "ufs":
		disksTrimmed, totalBytes, err = trimUfs(state.Disks, rootDir)
	case "fat12", "fat16", "fat32":
		disksTrimmed, totalBytes, err = trimGeneric(state.Disks, rootDir)
	case "refs":
		disksTrimmed, totalBytes, err = trimGeneric(state.Disks, rootDir)
	case "hpfs":
		disksTrimmed, totalBytes, err = trimGeneric(state.Disks, rootDir)
	case "ext2", "ext3":
		disksTrimmed, totalBytes, err = trimExt4(state.Disks, rootDir)
	default:
		disksTrimmed, totalBytes, err = trimGeneric(state.Disks, rootDir)
	}

	if err != nil {
		return TrimResult{}, fmt.Errorf("trim failed: %w", err)
	}

	txID := generateTxID("tx-trim")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return TrimResult{}, fmt.Errorf("save metadata after trim: %w", err)
	}

	return TrimResult{
		TxID:           txID,
		DisksTrimmed:   disksTrimmed,
		BytesDiscarded: totalBytes,
	}, nil
}

func trimBtrfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	var totalBytes int64

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("btrfs", "filesystem", "df", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			continue
		}

		cmd = exec.Command("fstrim", "-v", disk.Mountpoint)
		out.Reset()
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			continue
		}

		output := strings.TrimSpace(out.String())
		if strings.Contains(output, "bytes trimmed") {
			trimmed++
			var discarded int64
			fmt.Sscanf(output, "%d bytes trimmed", &discarded)
			totalBytes += discarded
		}
	}

	return trimmed, totalBytes, nil
}

func trimExt4(disks []metadata.Disk, rootDir string) (int, int64, error) {
	return trimGeneric(disks, rootDir)
}

func trimXfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	return trimGeneric(disks, rootDir)
}

func trimNtfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	var totalBytes int64

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("ntfstrim", "-v", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err == nil {
			trimmed++
		}
	}

	return trimmed, totalBytes, nil
}

func trimExfat(disks []metadata.Disk, rootDir string) (int, int64, error) {
	return trimGeneric(disks, rootDir)
}

func trimHfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("fsck.hfsplus", "-t", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		}
	}

	return trimmed, 0, nil
}

func trimApfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("fsck.apfs", "-n", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		}
	}

	return trimmed, 0, nil
}

func trimUfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("fsck.ufs", "-n", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		}
	}

	return trimmed, 0, nil
}

func trimGeneric(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	var totalBytes int64

	for _, disk := range disks {
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.Mountpoint == "" {
			continue
		}

		if _, err := os.Stat(disk.Mountpoint); os.IsNotExist(err) {
			continue
		}

		cmd := exec.Command("fstrim", "-v", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err == nil {
			trimmed++
		}
	}

	return trimmed, totalBytes, nil
}

func (c *Coordinator) Defrag() (DefragResult, error) {
	if c == nil {
		return DefragResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return DefragResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return DefragResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return DefragResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	if state.Pool.FilesystemType == "" {
		state.Pool.FilesystemType = "btrfs"
	}

	var extentsMoved int
	var bytesFreed int64

	rootDir := filepath.Dir(c.metadataPath)

	switch state.Pool.FilesystemType {
	case "btrfs":
		extentsMoved, bytesFreed, err = defragBtrfs(state, rootDir)
	case "ext4":
		extentsMoved, bytesFreed, err = defragExt4(state, rootDir)
	case "xfs":
		extentsMoved, bytesFreed, err = defragXfs(state, rootDir)
	case "ntfs":
		extentsMoved, bytesFreed, err = defragNtfs(state, rootDir)
	case "refs":
		extentsMoved, bytesFreed, err = defragRefs(state, rootDir)
	case "hfs", "hfsplus", "apfs", "ufs", "ext2", "ext3", "exfat", "fat12", "fat16", "fat32", "hpfs":
		extentsMoved, bytesFreed, err = defragGeneric(state, rootDir)
	default:
		extentsMoved, bytesFreed, err = defragGeneric(state, rootDir)
	}

	if err != nil {
		return DefragResult{}, fmt.Errorf("defrag failed: %w", err)
	}

	txID := generateTxID("tx-defrag")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return DefragResult{}, fmt.Errorf("save metadata after defrag: %w", err)
	}

	return DefragResult{
		TxID:         txID,
		ExtentsMoved: extentsMoved,
		BytesFreed:   bytesFreed,
	}, nil
}

func defragBtrfs(state metadata.SampleState, rootDir string) (int, int64, error) {
	moved := 0
	freed := int64(0)

	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := exec.Command("btrfs", "filesystem", "defragment", "-r", "-v", dataDir)
	if err := cmd.Run(); err != nil {
		return 0, 0, err
	}

	moved = len(state.Extents)
	return moved, freed, nil
}

func defragExt4(state metadata.SampleState, rootDir string) (int, int64, error) {
	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := exec.Command("e4defrag", "-v", dataDir)
	if err := cmd.Run(); err != nil {
		return 0, 0, err
	}

	return len(state.Extents), 0, nil
}

func defragXfs(state metadata.SampleState, rootDir string) (int, int64, error) {
	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := exec.Command("xfs_fsr", "-v", dataDir)
	if err := cmd.Run(); err != nil {
		return 0, 0, err
	}

	return len(state.Extents), 0, nil
}

func defragNtfs(state metadata.SampleState, rootDir string) (int, int64, error) {
	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	for _, disk := range state.Disks {
		if disk.Mountpoint == "" {
			continue
		}
		cmd := exec.Command("defrag", disk.Mountpoint)
		if err := cmd.Run(); err != nil {
			continue
		}
	}

	return 0, 0, nil
}

func defragRefs(state metadata.SampleState, rootDir string) (int, int64, error) {
	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := exec.Command("defrag", dataDir)
	if err := cmd.Run(); err != nil {
		return 0, 0, err
	}

	return 0, 0, nil
}

func defragGeneric(state metadata.SampleState, rootDir string) (int, int64, error) {
	return 0, 0, nil
}

func (c *Coordinator) Snapshot(name string) (SnapshotResult, error) {
	if c == nil {
		return SnapshotResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return SnapshotResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return SnapshotResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return SnapshotResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	if state.Pool.FilesystemType == "" {
		state.Pool.FilesystemType = "btrfs"
	}

	if name == "" {
		name = fmt.Sprintf("snapshot-%d", time.Now().Unix())
	}

	rootDir := filepath.Dir(c.metadataPath)

	switch state.Pool.FilesystemType {
	case "btrfs":
		err = snapshotBtrfs(rootDir, name)
	case "apfs":
		err = snapshotApfs(rootDir, name)
	case "ext4", "xfs", "ntfs", "refs", "hfs", "hfsplus", "ufs", "ext2", "ext3", "exfat", "fat12", "fat16", "fat32", "hpfs":
		err = snapshotGeneric(rootDir, name, state)
	default:
		err = snapshotGeneric(rootDir, name, state)
	}

	if err != nil {
		return SnapshotResult{}, fmt.Errorf("snapshot failed: %w", err)
	}

	txID := generateTxID("tx-snapshot")
	now := time.Now().UTC()
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	stateJson, _ := json.Marshal(state)

	if _, err := c.commitState(state); err != nil {
		return SnapshotResult{}, fmt.Errorf("save metadata after snapshot: %w", err)
	}

	return SnapshotResult{
		TxID:        txID,
		Name:        name,
		CreatedAt:   now,
		SourceState: string(stateJson),
	}, nil
}

func snapshotBtrfs(rootDir, name string) error {
	snapshotsDir := filepath.Join(rootDir, ".snapshots")
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}

	snapshotPath := filepath.Join(snapshotsDir, name)

	cmd := exec.Command("btrfs", "subvolume", "create", snapshotPath)
	if err := cmd.Run(); err != nil {
		snapshotMeta := filepath.Join(snapshotsDir, name+".json")
		data, _ := json.Marshal(map[string]string{
			"name":    name,
			"type":    "btrfs-subvolume",
			"status":  "pending",
			"rootDir": rootDir,
		})
		if err := os.WriteFile(snapshotMeta, data, 0o644); err != nil {
			return fmt.Errorf("write snapshot metadata: %w", err)
		}
		return nil
	}

	return nil
}

func snapshotApfs(rootDir, name string) error {
	snapshotsDir := filepath.Join(rootDir, ".snapshots")
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}

	snapshotMeta := filepath.Join(snapshotsDir, name+".json")
	data, _ := json.Marshal(map[string]string{
		"name":    name,
		"type":    "apfs-snapshot",
		"status":  "metadata-only",
		"rootDir": rootDir,
	})
	if err := os.WriteFile(snapshotMeta, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot metadata: %w", err)
	}

	return nil
}

func snapshotGeneric(rootDir, name string, state metadata.SampleState) error {
	snapshotsDir := filepath.Join(rootDir, ".snapshots")
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}

	snapshotMeta := filepath.Join(snapshotsDir, name+"-metadata.bin")

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	if err := replaceSyncFile(snapshotMeta, data, 0o600); err != nil {
		return fmt.Errorf("write snapshot metadata: %w", err)
	}

	return nil
}

func (c *Coordinator) EnableSleep(timeoutSec, minActiveSec int) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	state.Pool.SleepEnabled = true
	state.Pool.SleepTimeoutSec = timeoutSec
	state.Pool.SleepMinActiveSec = minActiveSec

	txID := generateTxID("tx-enable-sleep")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return fmt.Errorf("save metadata after enable sleep: %w", err)
	}

	return nil
}

func (c *Coordinator) DisableSleep() error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	state.Pool.SleepEnabled = false

	txID := generateTxID("tx-disable-sleep")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return fmt.Errorf("save metadata after disable sleep: %w", err)
	}

	return nil
}

func (c *Coordinator) WakeDisk(diskID string) (SleepResult, error) {
	if c == nil {
		return SleepResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return SleepResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return SleepResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return SleepResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var targetDisk *metadata.Disk
	for i := range state.Disks {
		if state.Disks[i].DiskID == diskID {
			targetDisk = &state.Disks[i]
			break
		}
	}
	if targetDisk == nil {
		return SleepResult{}, fmt.Errorf("disk not found: %s", diskID)
	}

	if targetDisk.Mountpoint == "" {
		return SleepResult{}, fmt.Errorf("disk has no mountpoint: %s", diskID)
	}

	disksWoken := 0
	if targetDisk.Mountpoint != "" {
		cmd := exec.Command("hdparm", "-y", targetDisk.Mountpoint)
		if err := cmd.Run(); err == nil {
			disksWoken = 1
		}
	}

	txID := generateTxID("tx-wake")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return SleepResult{}, fmt.Errorf("save metadata after wake disk: %w", err)
	}

	return SleepResult{
		TxID:       txID,
		DisksWoken: disksWoken,
		DisksSlept: 0,
		DiskIDs:    []string{diskID},
	}, nil
}

func (c *Coordinator) SleepDisk(diskID string) (SleepResult, error) {
	if c == nil {
		return SleepResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return SleepResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return SleepResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return SleepResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var targetDisk *metadata.Disk
	for i := range state.Disks {
		if state.Disks[i].DiskID == diskID {
			targetDisk = &state.Disks[i]
			break
		}
	}
	if targetDisk == nil {
		return SleepResult{}, fmt.Errorf("disk not found: %s", diskID)
	}

	if targetDisk.Mountpoint == "" {
		return SleepResult{}, fmt.Errorf("disk has no mountpoint: %s", diskID)
	}

	disksSlept := 0
	if targetDisk.Mountpoint != "" {
		cmd := exec.Command("hdparm", "-Y", targetDisk.Mountpoint)
		if err := cmd.Run(); err == nil {
			disksSlept = 1
		}
	}

	txID := generateTxID("tx-sleep")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: time.Now().UTC(),
	})

	if _, err := c.commitState(state); err != nil {
		return SleepResult{}, fmt.Errorf("save metadata after sleep disk: %w", err)
	}

	return SleepResult{
		TxID:       txID,
		DisksWoken: 0,
		DisksSlept: disksSlept,
		DiskIDs:    []string{diskID},
	}, nil
}

func (c *Coordinator) GetSleepStatus() (map[string]bool, error) {
	if c == nil {
		return nil, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return nil, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return nil, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return nil, fmt.Errorf("load metadata state: %w", err)
	}

	status := make(map[string]bool)
	for _, disk := range state.Disks {
		if disk.Role == metadata.DiskRoleData {
			status[disk.DiskID] = disk.HealthStatus == "online"
		}
	}

	return status, nil
}
