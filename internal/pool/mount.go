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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type MountStatus string

const (
	MountStatusOnline   MountStatus = "online"
	MountStatusDegraded MountStatus = "degraded"
	MountStatusReadOnly MountStatus = "read_only"
	MountStatusFailed   MountStatus = "failed"
)

type AssembledPool struct {
	PoolID       string
	Name         string
	PoolUUID     string
	Mountpoint   string
	Disks        []AssembledDisk
	Status       MountStatus
	MissingDisks []string
}

type AssembledDisk struct {
	DiskID       string
	DiskUUID     string
	DevicePath   string
	InternalPath string
	Role         metadata.DiskRole
	Filesystem   string
	IsPresent    bool
}

type MountOptions struct {
	PoolID       string
	FromDisk     string
	Mountpoint   string
	AutoDiscover bool
	DegradedOk   bool
	ReadOnly     bool
}

type MountResult struct {
	Pool        *AssembledPool
	PublicMount string
	Error       error
}

func (m *Manager) AssemblePool(opts MountOptions) (*MountResult, error) {
	if opts.PoolID == "" && opts.FromDisk == "" {
		return nil, fmt.Errorf("either pool-id or from-disk is required")
	}

	if opts.AutoDiscover && opts.FromDisk != "" {
		return nil, fmt.Errorf("cannot use both auto-discover and from-disk")
	}

	if opts.Mountpoint == "" {
		opts.Mountpoint = filepath.Join("/mnt", opts.PoolID)
	}

	if opts.FromDisk != "" {
		return m.assembleFromDisk(opts)
	}

	return m.assembleFromPoolID(opts)
}

func (m *Manager) assembleFromDisk(opts MountOptions) (*MountResult, error) {
	discovery, err := DiscoverPoolFromDisk(opts.FromDisk)
	if err != nil {
		return nil, fmt.Errorf("discover pool from disk: %w", err)
	}

	if discovery.ValidationError != nil {
		if !opts.DegradedOk {
			return nil, fmt.Errorf("validation error (use --degraded to ignore): %w", discovery.ValidationError)
		}
	}

	poolID := discovery.PoolIdentity.PoolID

	assembled := &AssembledPool{
		PoolID:       poolID,
		Name:         discovery.PoolIdentity.Name,
		PoolUUID:     discovery.PoolIdentity.PoolUUID,
		Mountpoint:   opts.Mountpoint,
		Status:       MountStatusOnline,
		MissingDisks: discovery.MissingDiskIDs,
		Disks:        make([]AssembledDisk, 0),
	}

	for _, disk := range discovery.FoundDisks {
		assembled.Disks = append(assembled.Disks, AssembledDisk{
			DiskID:       disk.Identity.DiskID,
			DiskUUID:     disk.Identity.DiskUUID,
			DevicePath:   disk.DevicePath,
			InternalPath: disk.Mountpoint,
			Role:         disk.Identity.Role,
			Filesystem:   disk.Identity.FilesystemType,
			IsPresent:    true,
		})
	}

	for _, missingID := range discovery.MissingDiskIDs {
		assembled.MissingDisks = append(assembled.MissingDisks, missingID)
	}

	if len(assembled.MissingDisks) > 0 {
		if len(assembled.Disks) == 0 {
			assembled.Status = MountStatusFailed
			return nil, fmt.Errorf("no disks found for pool %s", poolID)
		}
		if opts.DegradedOk {
			assembled.Status = MountStatusDegraded
		} else {
			assembled.Status = MountStatusDegraded
		}
	}

	if assembled.Status == MountStatusDegraded {
		parityOk := m.checkParityIntegrity(assembled)
		if !parityOk && !opts.DegradedOk {
			return nil, fmt.Errorf("parity integrity check failed (use --degraded to ignore)")
		}
	}

	if err := m.createPoolMountPoints(assembled); err != nil {
		return nil, fmt.Errorf("create pool mount points: %w", err)
	}

	return &MountResult{
		Pool:        assembled,
		PublicMount: opts.Mountpoint,
	}, nil
}

func (m *Manager) assembleFromPoolID(opts MountOptions) (*MountResult, error) {
	poolDir := m.PoolDir(opts.PoolID)

	poolIdentity, err := ReadPoolIdentity(poolDir)
	if err != nil {
		return nil, fmt.Errorf("read pool identity: %w", err)
	}

	assembled := &AssembledPool{
		PoolID:       opts.PoolID,
		Name:         poolIdentity.Name,
		PoolUUID:     poolIdentity.PoolUUID,
		Mountpoint:   opts.Mountpoint,
		Status:       MountStatusOnline,
		MissingDisks: make([]string, 0),
		Disks:        make([]AssembledDisk, 0),
	}

	if opts.AutoDiscover {
		discovery, err := m.discoverPoolDisks(opts.PoolID)
		if err != nil {
			if !opts.DegradedOk {
				return nil, fmt.Errorf("auto-discover failed: %w", err)
			}
		} else {
			for _, disk := range discovery.FoundDisks {
				assembled.Disks = append(assembled.Disks, AssembledDisk{
					DiskID:       disk.Identity.DiskID,
					DiskUUID:     disk.Identity.DiskUUID,
					DevicePath:   disk.DevicePath,
					InternalPath: disk.Mountpoint,
					Role:         disk.Identity.Role,
					Filesystem:   disk.Identity.FilesystemType,
					IsPresent:    true,
				})
			}
			assembled.MissingDisks = discovery.MissingDiskIDs
		}
	}

	metadataPath := BuildPoolMetadataPath(opts.PoolID)
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("metadata not found for pool %s at %s", opts.PoolID, metadataPath)
	}

	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	if len(assembled.Disks) == 0 {
		for _, disk := range state.Disks {
			assembled.Disks = append(assembled.Disks, AssembledDisk{
				DiskID:       disk.DiskID,
				DiskUUID:     disk.UUID,
				InternalPath: disk.Mountpoint,
				Role:         disk.Role,
				Filesystem:   disk.FilesystemType,
				IsPresent:    false,
			})
		}
	}

	if len(assembled.MissingDisks) > 0 {
		if opts.DegradedOk {
			assembled.Status = MountStatusDegraded
		} else {
			assembled.Status = MountStatusDegraded
		}
	}

	if err := m.createPoolMountPoints(assembled); err != nil {
		return nil, fmt.Errorf("create pool mount points: %w", err)
	}

	return &MountResult{
		Pool:        assembled,
		PublicMount: opts.Mountpoint,
	}, nil
}

func (m *Manager) discoverPoolDisks(poolID string) (*DiscoveryResult, error) {
	poolDir := m.PoolDir(poolID)
	poolIdentity, err := ReadPoolIdentity(poolDir)
	if err != nil {
		return nil, fmt.Errorf("read pool identity: %w", err)
	}

	discovery := &DiscoveryResult{
		PoolIdentity:   poolIdentity,
		FoundDisks:     make([]DiscoveredDisk, 0),
		MissingDiskIDs: make([]string, 0),
	}

	discovered, err := scanForPoolDisks(poolIdentity.PoolUUID)
	if err != nil {
		return nil, fmt.Errorf("scan for pool disks: %w", err)
	}
	discovery.FoundDisks = discovered

	diskIDSet := make(map[string]bool)
	for _, d := range discovered {
		diskIDSet[d.Identity.DiskID] = true
	}

	metadataPath := BuildPoolMetadataPath(poolID)
	state, err := metadata.NewStore(metadataPath).Load()
	if err == nil {
		for _, disk := range state.Disks {
			if !diskIDSet[disk.DiskID] {
				discovery.MissingDiskIDs = append(discovery.MissingDiskIDs, disk.DiskID)
			}
		}
	}

	discovery.IsDegraded = len(discovery.MissingDiskIDs) > 0

	return discovery, nil
}

func (m *Manager) checkParityIntegrity(assembled *AssembledPool) bool {
	presentDataDisks := 0
	for _, disk := range assembled.Disks {
		if disk.IsPresent && disk.Role == metadata.DiskRoleData {
			presentDataDisks++
		}
	}

	totalDataDisks := presentDataDisks + len(assembled.MissingDisks)

	if totalDataDisks == 0 {
		return false
	}

	if presentDataDisks >= totalDataDisks-1 {
		return true
	}

	return false
}

func (m *Manager) createPoolMountPoints(assembled *AssembledPool) error {
	if err := os.MkdirAll(assembled.Mountpoint, 0o755); err != nil {
		return fmt.Errorf("create public mount point: %w", err)
	}

	poolDir := m.PoolDir(assembled.PoolID)
	if err := bindMount(poolDir, assembled.Mountpoint); err != nil {
		return fmt.Errorf("bind mount pool dir to public mount: %w", err)
	}

	return nil
}

func (m *Manager) ValidatePool(poolID string) (*PoolValidationResult, error) {
	result := &PoolValidationResult{
		PoolID: poolID,
	}

	poolDir := m.PoolDir(poolID)
	poolIdentity, err := ReadPoolIdentity(poolDir)
	if err != nil {
		result.Error = fmt.Errorf("read pool identity: %w", err)
		return result, nil
	}
	result.PoolIdentity = poolIdentity

	discovery, err := m.discoverPoolDisks(poolID)
	if err != nil {
		result.Valid = false
		result.Error = err
		return result, nil
	}

	result.FoundDiskCount = len(discovery.FoundDisks)
	result.ExpectedDiskCount = len(discovery.PoolIdentity.PoolUUID)

	for _, missing := range discovery.MissingDiskIDs {
		result.MissingDiskIDs = append(result.MissingDiskIDs, missing)
	}

	for _, dup := range discovery.Duplicates {
		result.DuplicateDiskIDs = append(result.DuplicateDiskIDs, dup)
	}

	if len(result.MissingDiskIDs) > 0 {
		result.Valid = false
		result.Status = "degraded"
	} else if len(result.DuplicateDiskIDs) > 0 {
		result.Valid = false
		result.Status = "invalid"
	} else {
		result.Valid = true
		result.Status = "healthy"
	}

	return result, nil
}

type PoolValidationResult struct {
	PoolID            string
	Valid             bool
	Status            string
	Error             error
	PoolIdentity      *PoolIdentity
	FoundDiskCount    int
	ExpectedDiskCount int
	MissingDiskIDs    []string
	DuplicateDiskIDs  []string
}

type RemountOptions struct {
	PoolID     string
	Mountpoint string
	DegradedOk bool
}

type RemountResult struct {
	Pool        *AssembledPool
	PublicMount string
	WasDegraded bool
	Error       error
}

func (m *Manager) RemountPool(opts RemountOptions) (*RemountResult, error) {
	if opts.PoolID == "" {
		return nil, fmt.Errorf("pool id is required")
	}
	if opts.Mountpoint == "" {
		opts.Mountpoint = filepath.Join("/mnt", opts.PoolID)
	}

	poolDir := m.PoolDir(opts.PoolID)

	poolIdentity, err := ReadPoolIdentity(poolDir)
	if err != nil {
		return nil, fmt.Errorf("read pool identity: %w", err)
	}

	result := &RemountResult{
		WasDegraded: false,
	}

	assembled := &AssembledPool{
		PoolID:       opts.PoolID,
		Name:         poolIdentity.Name,
		PoolUUID:     poolIdentity.PoolUUID,
		Mountpoint:   opts.Mountpoint,
		Status:       MountStatusOnline,
		MissingDisks: make([]string, 0),
		Disks:        make([]AssembledDisk, 0),
	}

	metadataPath := BuildPoolMetadataPath(opts.PoolID)
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	for _, disk := range state.Disks {
		internalPath := m.DiskDir(opts.PoolID, disk.DiskID)

		mounted, err := isMounted(internalPath)
		if err != nil {
			return nil, fmt.Errorf("check if disk %s mounted: %w", disk.DiskID, err)
		}

		if !mounted {
			devicePath, err := m.findDeviceByUUID(disk.UUID)
			if err != nil {
				if opts.DegradedOk {
					assembled.MissingDisks = append(assembled.MissingDisks, disk.DiskID)
					result.WasDegraded = true
					continue
				}
				return nil, fmt.Errorf("find device for disk %s: %w (use --degraded to allow missing disk)", disk.DiskID, err)
			}

			if err := m.remountDisk(devicePath, internalPath, disk.FilesystemType); err != nil {
				if opts.DegradedOk {
					assembled.MissingDisks = append(assembled.MissingDisks, disk.DiskID)
					result.WasDegraded = true
					continue
				}
				return nil, fmt.Errorf("remount disk %s: %w", disk.DiskID, err)
			}
		}

		assembled.Disks = append(assembled.Disks, AssembledDisk{
			DiskID:       disk.DiskID,
			DiskUUID:     disk.UUID,
			InternalPath: internalPath,
			Role:         disk.Role,
			Filesystem:   disk.FilesystemType,
			IsPresent:    true,
		})
	}

	if len(assembled.MissingDisks) > 0 {
		if result.WasDegraded || opts.DegradedOk {
			assembled.Status = MountStatusDegraded
		} else {
			return nil, fmt.Errorf("pool is degraded (use --degraded to allow)")
		}
	}

	if err := os.MkdirAll(opts.Mountpoint, 0o755); err != nil {
		return nil, fmt.Errorf("create mount point: %w", err)
	}

	alreadyMounted, err := isMounted(opts.Mountpoint)
	if err != nil {
		return nil, fmt.Errorf("check if mountpoint mounted: %w", err)
	}

	if !alreadyMounted {
		if err := bindMount(poolDir, opts.Mountpoint); err != nil {
			return nil, fmt.Errorf("bind mount pool: %w", err)
		}
	}

	result.Pool = assembled
	result.PublicMount = opts.Mountpoint

	return result, nil
}

func (m *Manager) findDeviceByUUID(diskUUID string) (string, error) {
	output, err := execCommand("blkid", "-U", diskUUID)
	if err != nil {
		return "", fmt.Errorf("blkid -U: %w", err)
	}
	path := strings.TrimSpace(output)
	if path == "" {
		return "", fmt.Errorf("no device found for uuid %s", diskUUID)
	}
	return path, nil
}

func (m *Manager) remountDisk(devicePath, internalPath, fsType string) error {
	if err := os.MkdirAll(internalPath, 0o700); err != nil {
		return fmt.Errorf("create internal mount point: %w", err)
	}

	mounted, err := isMounted(devicePath)
	if err != nil {
		return fmt.Errorf("check mounted: %w", err)
	}

	if mounted {
		currentMount, err := getMountPoint(devicePath)
		if err != nil {
			return fmt.Errorf("get current mount: %w", err)
		}
		if currentMount == internalPath {
			return nil
		}
		return fmt.Errorf("device %s is mounted at %s, not at %s", devicePath, currentMount, internalPath)
	}

	return mount(fsType, devicePath, internalPath)
}
