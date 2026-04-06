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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/xkzy/rdparityd/internal/diskprovision"
	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	RuntimeDir    = "/var/lib/rtparityd"
	RunDir        = "/var/run/rtparityd"
	PoolDirName   = "pools"
	DiskDirName   = "disks"
	DataDirName   = "data"
	ParityDirName = "parity"
	MetaDirName   = "metadata"
)

func generateUUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

type Manager struct {
	poolsDir      string
	healthMonitor *HealthMonitor
}

func NewManager() *Manager {
	return &Manager{
		poolsDir: filepath.Join(RuntimeDir, PoolDirName),
	}
}

func (m *Manager) PoolDir(poolID string) string {
	return filepath.Join(m.poolsDir, poolID)
}

func (m *Manager) DiskDir(poolID, diskID string) string {
	return filepath.Join(m.PoolDir(poolID), DiskDirName, diskID)
}

func (m *Manager) DataDir(poolID string) string {
	return filepath.Join(m.PoolDir(poolID), DataDirName)
}

func (m *Manager) ParityDir(poolID string) string {
	return filepath.Join(m.PoolDir(poolID), ParityDirName)
}

func (m *Manager) MetaDir(poolID string) string {
	return filepath.Join(m.PoolDir(poolID), MetaDirName)
}

func (m *Manager) HealthMonitor(poolID string) *HealthMonitor {
	if m.healthMonitor == nil {
		m.healthMonitor = NewHealthMonitor(poolID)
	}
	return m.healthMonitor
}

func (m *Manager) GetDiskHealth(diskID string) (*DiskHealth, bool) {
	if m.healthMonitor == nil {
		return nil, false
	}
	return m.healthMonitor.GetDiskHealth(diskID)
}

func (m *Manager) GetAllDiskHealth() map[string]*DiskHealth {
	if m.healthMonitor == nil {
		return nil
	}
	return m.healthMonitor.GetAllDiskHealth()
}

func (m *Manager) ExcludeDisk(diskID string) error {
	if m.healthMonitor == nil {
		return fmt.Errorf("health monitor not initialized")
	}
	return m.healthMonitor.ExcludeDisk(diskID)
}

func (m *Manager) RegisterDisk(diskID, diskUUID string) {
	if m.healthMonitor == nil {
		return
	}
	m.healthMonitor.RegisterDisk(diskID, diskUUID)
}

type CreateOptions struct {
	PoolID           string
	PoolUUID         string
	Name             string
	FilesystemType   string
	ExtentSizeBytes  int64
	ParityMode       string
	PublicMountpoint string
}

func (m *Manager) CreatePool(opts CreateOptions) error {
	if opts.PoolID == "" {
		return fmt.Errorf("pool id is required")
	}
	if opts.Name == "" {
		return fmt.Errorf("pool name is required")
	}
	if opts.PublicMountpoint == "" {
		opts.PublicMountpoint = filepath.Join("/mnt", opts.Name)
	}
	if opts.FilesystemType == "" {
		opts.FilesystemType = "ext4"
	}
	if opts.ExtentSizeBytes == 0 {
		opts.ExtentSizeBytes = metadata.DefaultExtentSize
	}
	if opts.ParityMode == "" {
		opts.ParityMode = "single"
	}

	if opts.PoolUUID == "" {
		var err error
		opts.PoolUUID, err = generateUUID()
		if err != nil {
			return fmt.Errorf("generate pool uuid: %w", err)
		}
	}

	poolDir := m.PoolDir(opts.PoolID)
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return fmt.Errorf("create pool dir %s: %w", poolDir, err)
	}

	if err := os.MkdirAll(m.DataDir(opts.PoolID), 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(m.ParityDir(opts.PoolID), 0o700); err != nil {
		return fmt.Errorf("create parity dir: %w", err)
	}
	if err := os.MkdirAll(m.MetaDir(opts.PoolID), 0o700); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(opts.PublicMountpoint), 0o755); err != nil {
		return fmt.Errorf("create public mountpoint parent: %w", err)
	}

	if err := WritePoolIdentity(poolDir, opts.PoolID, opts.PoolUUID, opts.Name, 0, opts.FilesystemType); err != nil {
		return fmt.Errorf("write pool identity: %w", err)
	}

	return nil
}

type PrepareDiskOptions struct {
	PoolID     string
	PoolUUID   string
	DiskID     string
	Device     string
	Role       metadata.DiskRole
	Filesystem string
	Label      string
}

type PrepareDiskResult struct {
	PoolID         string
	PoolUUID       string
	DiskID         string
	Device         string
	PartitionPath  string
	FilesystemType string
	InternalMount  string
	CapacityBytes  int64
	UUID           string
}

func (m *Manager) PrepareDisk(opts PrepareDiskOptions) (*PrepareDiskResult, error) {
	if opts.PoolID == "" {
		return nil, fmt.Errorf("pool id is required")
	}
	if opts.DiskID == "" {
		return nil, fmt.Errorf("disk id is required")
	}
	if opts.Device == "" {
		return nil, fmt.Errorf("device is required")
	}
	if opts.Role == "" {
		opts.Role = metadata.DiskRoleData
	}
	if opts.Filesystem == "" {
		opts.Filesystem = "ext4"
	}

	internalMount := m.DiskDir(opts.PoolID, opts.DiskID)

	if err := diskprovision.ValidateDevicePath(opts.Device); err != nil {
		return nil, fmt.Errorf("validate device: %w", err)
	}

	mounted, err := isDeviceMounted(opts.Device)
	if err != nil {
		return nil, fmt.Errorf("check mounted status: %w", err)
	}
	if mounted {
		return nil, fmt.Errorf("device %s is already mounted", opts.Device)
	}

	result, err := diskprovision.Provision(diskprovision.ProvisionOptions{
		Device:     opts.Device,
		MountPoint: internalMount,
		FSType:     opts.Filesystem,
		Label:      opts.Label,
	})
	if err != nil {
		return nil, fmt.Errorf("provision disk: %w", err)
	}

	if err := os.MkdirAll(internalMount, 0o700); err != nil {
		return nil, fmt.Errorf("create internal mount point: %w", err)
	}

	if err := mount(opts.Filesystem, result.PartitionPath, internalMount); err != nil {
		return nil, fmt.Errorf("mount disk: %w", err)
	}

	dataSubdir := filepath.Join(internalMount, DataDirName)
	paritySubdir := filepath.Join(internalMount, ParityDirName)
	metaSubdir := filepath.Join(internalMount, MetaDirName)
	if err := os.MkdirAll(dataSubdir, 0o700); err != nil {
		_ = unmount(internalMount)
		return nil, fmt.Errorf("create data subdir: %w", err)
	}
	if err := os.MkdirAll(paritySubdir, 0o700); err != nil {
		_ = unmount(internalMount)
		return nil, fmt.Errorf("create parity subdir: %w", err)
	}
	if err := os.MkdirAll(metaSubdir, 0o700); err != nil {
		_ = unmount(internalMount)
		return nil, fmt.Errorf("create meta subdir: %w", err)
	}

	if opts.PoolUUID == "" {
		poolIdentity, err := ReadPoolIdentity(m.PoolDir(opts.PoolID))
		if err != nil {
			_ = unmount(internalMount)
			return nil, fmt.Errorf("read pool identity: %w", err)
		}
		opts.PoolUUID = poolIdentity.PoolUUID
	}

	diskUUID, err := generateUUID()
	if err != nil {
		_ = unmount(internalMount)
		return nil, fmt.Errorf("generate disk uuid: %w", err)
	}

	if err := WriteDiskIdentity(internalMount, opts.PoolID, opts.PoolUUID, opts.DiskID, diskUUID, opts.Role, opts.Filesystem); err != nil {
		_ = unmount(internalMount)
		return nil, fmt.Errorf("write disk identity: %w", err)
	}

	return &PrepareDiskResult{
		PoolID:         opts.PoolID,
		PoolUUID:       opts.PoolUUID,
		DiskID:         opts.DiskID,
		Device:         result.Device,
		PartitionPath:  result.PartitionPath,
		FilesystemType: result.FSType,
		InternalMount:  internalMount,
		CapacityBytes:  result.SizeBytes,
		UUID:           diskUUID,
	}, nil
}

func (m *Manager) AddDiskToPool(poolID string, result *PrepareDiskResult, role metadata.DiskRole) error {
	if poolID == "" || result == nil {
		return fmt.Errorf("pool id and prepare result are required")
	}
	return nil
}

func (m *Manager) MountPublicPool(poolID, publicMountpoint string) error {
	poolDir := m.PoolDir(poolID)
	if _, err := os.Stat(poolDir); os.IsNotExist(err) {
		return fmt.Errorf("pool %s does not exist", poolID)
	}

	if err := os.MkdirAll(publicMountpoint, 0o755); err != nil {
		return fmt.Errorf("create public mountpoint: %w", err)
	}

	metaDir := m.MetaDir(poolID)
	metaFiles, err := filepath.Glob(filepath.Join(metaDir, "*.bin"))
	if err != nil || len(metaFiles) == 0 {
		return fmt.Errorf("no metadata found for pool %s", poolID)
	}

	return bindMount(poolDir, publicMountpoint)
}

func (m *Manager) UnmountPublicPool(poolID, publicMountpoint string) error {
	return unmount(publicMountpoint)
}

func mount(fsType, source, target string) error {
	cmd := exec.Command("mount", "-t", fsType, source, target)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}
	return nil
}

func unmount(path string) error {
	cmd := exec.Command("umount", path)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("unmount failed: %w", err)
	}
	return nil
}

func bindMount(source, target string) error {
	cmd := exec.Command("mount", "--bind", source, target)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bind mount failed: %w", err)
	}
	return nil
}

func GetDiskMountpoint(state *metadata.SampleState, diskID string) string {
	for _, disk := range state.Disks {
		if disk.DiskID == diskID {
			return disk.Mountpoint
		}
	}
	return ""
}

func BuildPoolMetadataPath(poolID string) string {
	return filepath.Join(RuntimeDir, PoolDirName, poolID, MetaDirName, "metadata.bin")
}

func BuildPoolJournalPath(poolID string) string {
	return filepath.Join(RuntimeDir, PoolDirName, poolID, MetaDirName, "journal.bin")
}

func IsValidPoolName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, c := range name {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz0123456789-_ ", c) {
			return false
		}
	}
	return true
}

func isDeviceMounted(device string) (bool, error) {
	output, err := exec.Command("findmnt", "-n", "-o", "TARGET", device).Output()
	if err != nil {
		if strings.Contains(err.Error(), "exit status 1") {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(string(output)) != "", nil
}
