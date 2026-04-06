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

package pool

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	DiskIdentityFile = ".rtparityd-disk-identity"
	PoolIdentityFile = ".rtparityd-pool-identity"
	LayoutVersion    = 1
)

type DiskIdentity struct {
	PoolID         string            `json:"pool_id"`
	PoolUUID       string            `json:"pool_uuid"`
	DiskID         string            `json:"disk_id"`
	DiskUUID       string            `json:"disk_uuid"`
	Role           metadata.DiskRole `json:"role"`
	FilesystemType string            `json:"filesystem_type"`
	LayoutVersion  int               `json:"layout_version"`
	CreatedAt      string            `json:"created_at"`
}

type PoolIdentity struct {
	PoolID            string `json:"pool_id"`
	PoolUUID          string `json:"pool_uuid"`
	Name              string `json:"name"`
	ExpectedDiskCount int    `json:"expected_disk_count"`
	FilesystemType    string `json:"filesystem_type"`
	LayoutVersion     int    `json:"layout_version"`
	CreatedAt         string `json:"created_at"`
}

func WriteDiskIdentity(mountpoint, poolID, poolUUID, diskID, diskUUID string, role metadata.DiskRole, fsType string) error {
	identity := DiskIdentity{
		PoolID:         poolID,
		PoolUUID:       poolUUID,
		DiskID:         diskID,
		DiskUUID:       diskUUID,
		Role:           role,
		FilesystemType: fsType,
		LayoutVersion:  LayoutVersion,
		CreatedAt:      "2025-01-01T00:00:00Z",
	}

	data, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("marshal disk identity: %w", err)
	}

	path := filepath.Join(mountpoint, DiskIdentityFile)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write disk identity: %w", err)
	}
	return nil
}

func ReadDiskIdentity(mountpoint string) (*DiskIdentity, error) {
	path := filepath.Join(mountpoint, DiskIdentityFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read disk identity: %w", err)
	}

	var identity DiskIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return nil, fmt.Errorf("unmarshal disk identity: %w", err)
	}

	if identity.LayoutVersion != LayoutVersion {
		return nil, fmt.Errorf("incompatible layout version: got %d, want %d", identity.LayoutVersion, LayoutVersion)
	}

	return &identity, nil
}

func WritePoolIdentity(mountpoint string, poolID, poolUUID, name string, expectedDiskCount int, fsType string) error {
	identity := PoolIdentity{
		PoolID:            poolID,
		PoolUUID:          poolUUID,
		Name:              name,
		ExpectedDiskCount: expectedDiskCount,
		FilesystemType:    fsType,
		LayoutVersion:     LayoutVersion,
		CreatedAt:         "2025-01-01T00:00:00Z",
	}

	data, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("marshal pool identity: %w", err)
	}

	path := filepath.Join(mountpoint, PoolIdentityFile)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write pool identity: %w", err)
	}
	return nil
}

func ReadPoolIdentity(mountpoint string) (*PoolIdentity, error) {
	path := filepath.Join(mountpoint, PoolIdentityFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pool identity: %w", err)
	}

	var identity PoolIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return nil, fmt.Errorf("unmarshal pool identity: %w", err)
	}

	if identity.LayoutVersion != LayoutVersion {
		return nil, fmt.Errorf("incompatible layout version: got %d, want %d", identity.LayoutVersion, LayoutVersion)
	}

	return &identity, nil
}

type DiscoveredDisk struct {
	DevicePath string
	Mountpoint string
	Identity   *DiskIdentity
}

type DiscoveryResult struct {
	PoolIdentity    *PoolIdentity
	FoundDisks      []DiscoveredDisk
	MissingDiskIDs  []string
	Duplicates      []string
	ValidationError error
	IsDegraded      bool
}

func DiscoverPoolFromDisk(devicePath string) (*DiscoveryResult, error) {
	result := &DiscoveryResult{}

	mounted, err := isMounted(devicePath)
	if err != nil {
		return nil, fmt.Errorf("check if device mounted: %w", err)
	}

	var mountpoint string
	if mounted {
		mountpoint, err = getMountPoint(devicePath)
		if err != nil {
			return nil, fmt.Errorf("get mount point: %w", err)
		}
	} else {
		tmpMount := filepath.Join(os.TempDir(), "rtparityd-tmp-mount")
		if err := os.MkdirAll(tmpMount, 0o755); err != nil {
			return nil, fmt.Errorf("create temp mount point: %w", err)
		}
		defer os.Rename(tmpMount, tmpMount)

		fsType, err := detectFilesystem(devicePath)
		if err != nil {
			return nil, fmt.Errorf("detect filesystem: %w", err)
		}

		if err := mountTemp(devicePath, tmpMount, fsType); err != nil {
			return nil, fmt.Errorf("temp mount: %w", err)
		}
		mountpoint = tmpMount
		defer unmount(tmpMount)
	}

	poolIdentity, err := ReadPoolIdentity(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("read pool identity from %s: %w", mountpoint, err)
	}
	result.PoolIdentity = poolIdentity

	discovered, err := scanForPoolDisks(poolIdentity.PoolUUID)
	if err != nil {
		result.ValidationError = fmt.Errorf("scan for disks: %w", err)
		return result, nil
	}
	result.FoundDisks = discovered

	diskIDSet := make(map[string]bool)
	for _, d := range discovered {
		if diskIDSet[d.Identity.DiskID] {
			result.Duplicates = append(result.Duplicates, d.Identity.DiskID)
		}
		diskIDSet[d.Identity.DiskID] = true
	}

	allDiskIDs := make(map[string]bool)
	for _, d := range discovered {
		allDiskIDs[d.Identity.DiskID] = true
	}

	metadataPath := filepath.Join(mountpoint, "metadata", "metadata.bin")
	if _, err := os.Stat(metadataPath); err == nil {
		state, err := loadMetadataFromMount(mountpoint)
		if err == nil {
			for _, disk := range state.Disks {
				if !allDiskIDs[disk.DiskID] {
					result.MissingDiskIDs = append(result.MissingDiskIDs, disk.DiskID)
				}
			}
		}
	}

	result.IsDegraded = len(result.MissingDiskIDs) > 0

	return result, nil
}

func scanForPoolDisks(poolUUID string) ([]DiscoveredDisk, error) {
	var disks []DiscoveredDisk

	blockDevices, err := filepath.Glob("/dev/sd*")
	if err != nil {
		return nil, fmt.Errorf("glob block devices: %w", err)
	}

	loopDevices, err := filepath.Glob("/dev/loop*")
	if err != nil {
		return nil, fmt.Errorf("glob loop devices: %w", err)
	}

	allDevices := append(blockDevices, loopDevices...)

	for _, device := range allDevices {
		fi, err := os.Stat(device)
		if err != nil {
			continue
		}
		if fi.Mode()&os.ModeDevice == 0 {
			continue
		}

		mounted, err := isMounted(device)
		if err != nil {
			continue
		}
		if !mounted {
			continue
		}

		mountpoint, err := getMountPoint(device)
		if err != nil || mountpoint == "" {
			continue
		}

		identity, err := ReadDiskIdentity(mountpoint)
		if err != nil {
			continue
		}

		if identity.PoolUUID == poolUUID {
			disks = append(disks, DiscoveredDisk{
				DevicePath: device,
				Mountpoint: mountpoint,
				Identity:   identity,
			})
		}
	}

	return disks, nil
}

func loadMetadataFromMount(mountpoint string) (*metadata.SampleState, error) {
	metadataPath := filepath.Join(mountpoint, "metadata", "metadata.bin")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return nil, err
	}

	store := metadata.NewStore(metadataPath)
	state, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}
	return &state, nil
}

func isMounted(device string) (bool, error) {
	output, err := execCommand("findmnt", "-n", "-o", "TARGET", device)
	if err != nil {
		if strings.Contains(err.Error(), "exit status 1") {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(output) != "", nil
}

func getMountPoint(device string) (string, error) {
	output, err := execCommand("findmnt", "-n", "-o", "TARGET", device)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(output), nil
}

func detectFilesystem(device string) (string, error) {
	output, err := execCommand("blkid", "-s", "TYPE", "-o", "value", device)
	if err != nil {
		return "ext4", nil
	}
	return strings.TrimSpace(output), nil
}

func mountTemp(device, target, fsType string) error {
	if fsType == "" {
		fsType = "ext4"
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	return mount(fsType, device, target)
}

func execCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return string(exitErr.Stderr), fmt.Errorf("exit status %d", exitErr.ExitCode())
		}
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}
