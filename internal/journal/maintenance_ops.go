/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package journal

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

var execCommand = exec.Command

var ErrUnsupportedAdminOperation = fmt.Errorf("admin operation is not supported in the current production feature set")

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
	return TrimResult{}, fmt.Errorf("%w: trim requires filesystem-specific block-device and mount orchestration that rtparityd does not model", ErrUnsupportedAdminOperation)
}

func trimBtrfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	var totalBytes int64
	failures := make([]string, 0)

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

		cmd := execCommand("btrfs", "filesystem", "df", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			failures = append(failures, fmt.Sprintf("%s: btrfs filesystem df: %v", disk.Mountpoint, err))
			continue
		}

		cmd = execCommand("fstrim", "-v", disk.Mountpoint)
		out.Reset()
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			failures = append(failures, fmt.Sprintf("%s: fstrim: %v", disk.Mountpoint, err))
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

	if len(failures) > 0 {
		return trimmed, totalBytes, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
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
	failures := make([]string, 0)

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

		cmd := execCommand("ntfstrim", "-v", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err == nil {
			trimmed++
		} else {
			failures = append(failures, fmt.Sprintf("%s: ntfstrim: %v", disk.Mountpoint, err))
		}
	}

	if len(failures) > 0 {
		return trimmed, totalBytes, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
	}
	return trimmed, totalBytes, nil
}

func trimExfat(disks []metadata.Disk, rootDir string) (int, int64, error) {
	return trimGeneric(disks, rootDir)
}

func trimHfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	failures := make([]string, 0)

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

		cmd := execCommand("fsck.hfsplus", "-t", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		} else {
			failures = append(failures, fmt.Sprintf("%s: fsck.hfsplus: %v", disk.Mountpoint, err))
		}
	}

	if len(failures) > 0 {
		return trimmed, 0, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
	}
	return trimmed, 0, nil
}

func trimApfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	failures := make([]string, 0)

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

		cmd := execCommand("fsck.apfs", "-n", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		} else {
			failures = append(failures, fmt.Sprintf("%s: fsck.apfs: %v", disk.Mountpoint, err))
		}
	}

	if len(failures) > 0 {
		return trimmed, 0, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
	}
	return trimmed, 0, nil
}

func trimUfs(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	failures := make([]string, 0)

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

		cmd := execCommand("fsck.ufs", "-n", disk.Mountpoint)
		if err := cmd.Run(); err == nil {
			trimmed++
		} else {
			failures = append(failures, fmt.Sprintf("%s: fsck.ufs: %v", disk.Mountpoint, err))
		}
	}

	if len(failures) > 0 {
		return trimmed, 0, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
	}
	return trimmed, 0, nil
}

func trimGeneric(disks []metadata.Disk, rootDir string) (int, int64, error) {
	trimmed := 0
	var totalBytes int64
	failures := make([]string, 0)

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

		cmd := execCommand("fstrim", "-v", disk.Mountpoint)
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err == nil {
			trimmed++
		} else {
			failures = append(failures, fmt.Sprintf("%s: fstrim: %v", disk.Mountpoint, err))
		}
	}

	if len(failures) > 0 {
		return trimmed, totalBytes, fmt.Errorf("trim encountered failures: %s", strings.Join(failures, "; "))
	}
	return trimmed, totalBytes, nil
}

func (c *Coordinator) Defrag() (DefragResult, error) {
	return DefragResult{}, fmt.Errorf("%w: defrag is filesystem-specific and not coherently modeled against extent placement or allocator state", ErrUnsupportedAdminOperation)
}

func defragBtrfs(state metadata.SampleState, rootDir string) (int, int64, error) {
	moved := 0
	freed := int64(0)

	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := execCommand("btrfs", "filesystem", "defragment", "-r", "-v", dataDir)
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

	cmd := execCommand("e4defrag", "-v", dataDir)
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

	cmd := execCommand("xfs_fsr", "-v", dataDir)
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

	failures := make([]string, 0)
	successes := 0
	for _, disk := range state.Disks {
		if disk.Mountpoint == "" {
			continue
		}
		cmd := execCommand("defrag", disk.Mountpoint)
		if err := cmd.Run(); err != nil {
			failures = append(failures, fmt.Sprintf("%s: defrag: %v", disk.Mountpoint, err))
			continue
		}
		successes++
	}

	if len(failures) > 0 {
		return successes, 0, fmt.Errorf("defrag encountered failures: %s", strings.Join(failures, "; "))
	}
	return successes, 0, nil
}

func defragRefs(state metadata.SampleState, rootDir string) (int, int64, error) {
	dataDir := filepath.Join(rootDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	cmd := execCommand("defrag", dataDir)
	if err := cmd.Run(); err != nil {
		return 0, 0, err
	}

	return 0, 0, nil
}

func defragGeneric(state metadata.SampleState, rootDir string) (int, int64, error) {
	return 0, 0, nil
}

func (c *Coordinator) Snapshot(name string) (SnapshotResult, error) {
	return SnapshotResult{}, fmt.Errorf("%w: snapshot semantics are not integrated with data-disk filesystems or the journal recovery model", ErrUnsupportedAdminOperation)
}

func snapshotBtrfs(snapshotsDir, rootDir, name string) error {
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}

	snapshotPath := filepath.Join(snapshotsDir, name)

	cmd := execCommand("btrfs", "subvolume", "create", snapshotPath)
	if err := cmd.Run(); err != nil {
		if err := writeSnapshotDescriptor(filepath.Join(snapshotsDir, name+".meta"), map[string]string{
			"name":     name,
			"type":     "btrfs-subvolume",
			"status":   "pending",
			"root_dir": rootDir,
		}); err != nil {
			return fmt.Errorf("write snapshot metadata: %w", err)
		}
		return nil
	}

	return nil
}

func snapshotApfs(snapshotsDir, rootDir, name string) error {
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}

	if err := writeSnapshotDescriptor(filepath.Join(snapshotsDir, name+".meta"), map[string]string{
		"name":     name,
		"type":     "apfs-snapshot",
		"status":   "metadata-only",
		"root_dir": rootDir,
	}); err != nil {
		return fmt.Errorf("write snapshot metadata: %w", err)
	}

	return nil
}

func snapshotGeneric(snapshotsDir string) error {
	if err := ensureDir(snapshotsDir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}
	return nil
}

func writeSnapshotDescriptor(path string, fields map[string]string) error {
	lines := make([]string, 0, len(fields))
	for _, key := range []string{"name", "type", "status", "root_dir"} {
		if value, ok := fields[key]; ok {
			lines = append(lines, key+"="+value)
		}
	}
	return replaceSyncFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600)
}

func resolveDiskControlPath(disk metadata.Disk) (string, error) {
	if disk.Mountpoint == "" {
		return "", fmt.Errorf("disk has no control path: %s", disk.DiskID)
	}
	if !filepath.IsAbs(disk.Mountpoint) {
		return "", fmt.Errorf("disk control path must be absolute, got %s for %s", disk.Mountpoint, disk.DiskID)
	}
	cleaned := filepath.Clean(disk.Mountpoint)
	if cleaned != disk.Mountpoint {
		return "", fmt.Errorf("disk control path must be canonical, got %s for %s", disk.Mountpoint, disk.DiskID)
	}
	for _, part := range strings.Split(disk.Mountpoint, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("disk control path contains .. component, rejecting %s for %s", disk.Mountpoint, disk.DiskID)
		}
	}
	info, err := os.Stat(disk.Mountpoint)
	if err != nil {
		return "", fmt.Errorf("stat disk control path %s: %w", disk.Mountpoint, err)
	}
	if info.Mode()&os.ModeDevice == 0 {
		return "", fmt.Errorf(
			"disk power control requires a block device path, got non-device mountpoint %s for %s",
			disk.Mountpoint, disk.DiskID)
	}
	return disk.Mountpoint, nil
}

func queryDiskAwake(controlPath string) (bool, error) {
	cmd := execCommand("hdparm", "-C", controlPath)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("query disk power state: %w", err)
	}
	status := strings.ToLower(out.String())
	switch {
	case strings.Contains(status, "active/idle"), strings.Contains(status, "active"), strings.Contains(status, "idle"):
		return true, nil
	case strings.Contains(status, "standby"), strings.Contains(status, "sleep"):
		return false, nil
	default:
		return false, fmt.Errorf("unrecognized disk power state output: %q", strings.TrimSpace(out.String()))
	}
}

func (c *Coordinator) EnableSleep(timeoutSec, minActiveSec int) error {
	return fmt.Errorf("%w: drive sleep management requires explicit block-device modeling and verified OS integration", ErrUnsupportedAdminOperation)
}

func (c *Coordinator) DisableSleep() error {
	return fmt.Errorf("%w: drive sleep management requires explicit block-device modeling and verified OS integration", ErrUnsupportedAdminOperation)
}

func (c *Coordinator) WakeDisk(diskID string) (SleepResult, error) {
	return SleepResult{}, fmt.Errorf("%w: drive wake requires explicit block-device modeling and verified OS integration", ErrUnsupportedAdminOperation)
}

func (c *Coordinator) SleepDisk(diskID string) (SleepResult, error) {
	return SleepResult{}, fmt.Errorf("%w: drive sleep requires explicit block-device modeling and verified OS integration", ErrUnsupportedAdminOperation)
}

func (c *Coordinator) GetSleepStatus() (map[string]bool, error) {
	return nil, fmt.Errorf("%w: drive sleep status requires explicit block-device modeling and verified OS integration", ErrUnsupportedAdminOperation)
}
