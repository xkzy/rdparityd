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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type RescaleResult struct {
	ExtentsRescaled int
	ExtentsFailed   int
	NewDiskCount    int
	ParityRebuilt   bool
	Error           error
}

type Rescaler struct {
	poolDir     string
	state       *metadata.SampleState
	coordinator interface {
		CommitState(state metadata.SampleState) error
	}
}

func NewRescaler(poolDir string, state *metadata.SampleState) *Rescaler {
	return &Rescaler{
		poolDir: poolDir,
		state:   state,
	}
}

func (r *Rescaler) RescaleAfterExclusion(excludedDiskID string) (*RescaleResult, error) {
	result := &RescaleResult{}

	onlineDataDisks := r.getOnlineDataDisks(excludedDiskID)
	if len(onlineDataDisks) == 0 {
		result.Error = fmt.Errorf("no online data disks remaining")
		return result, result.Error
	}

	extentsToRescale := r.getAffectedExtents(excludedDiskID)
	result.ExtentsRescaled = len(extentsToRescale)

	for _, extent := range extentsToRescale {
		newDiskID := r.selectBestDisk(extent, onlineDataDisks)
		if newDiskID == "" {
			result.ExtentsFailed++
			continue
		}

		if err := r.relocateExtent(extent, newDiskID); err != nil {
			result.Error = fmt.Errorf("relocate extent %s: %w", extent.ExtentID, err)
			result.ExtentsFailed++
		}
	}

	if err := r.rebuildParityGroups(extentsToRescale); err != nil {
		result.Error = fmt.Errorf("rebuild parity: %w", err)
		result.ParityRebuilt = false
	} else {
		result.ParityRebuilt = true
	}

	r.state.Disks = r.removeExcludedDisk(excludedDiskID)
	result.NewDiskCount = len(r.state.Disks)

	return result, nil
}

func (r *Rescaler) getOnlineDataDisks(excludeDiskID string) []metadata.Disk {
	var disks []metadata.Disk
	for _, disk := range r.state.Disks {
		if disk.DiskID == excludeDiskID {
			continue
		}
		if disk.Role != metadata.DiskRoleData {
			continue
		}
		if disk.HealthStatus != "online" {
			continue
		}
		disks = append(disks, disk)
	}
	return disks
}

func (r *Rescaler) getAffectedExtents(excludedDiskID string) []metadata.Extent {
	var affected []metadata.Extent
	for _, extent := range r.state.Extents {
		if extent.DataDiskID == excludedDiskID {
			affected = append(affected, extent)
		}
	}
	return affected
}

func (r *Rescaler) selectBestDisk(extent metadata.Extent, candidates []metadata.Disk) string {
	if len(candidates) == 0 {
		return ""
	}

	bestDisk := candidates[0]
	bestFree := candidates[0].FreeBytes
	bestLoad := r.getDiskLoad(candidates[0].DiskID)

	for _, disk := range candidates[1:] {
		free := disk.FreeBytes
		load := r.getDiskLoad(disk.DiskID)

		if free < extent.Length {
			continue
		}
		if load < bestLoad {
			bestDisk = disk
			bestLoad = load
		} else if free > bestFree && load == bestLoad {
			bestDisk = disk
			bestFree = free
		}
	}

	if bestFree < extent.Length {
		return ""
	}

	return bestDisk.DiskID
}

func (r *Rescaler) getDiskLoad(diskID string) int {
	count := 0
	for _, extent := range r.state.Extents {
		if extent.DataDiskID == diskID {
			count++
		}
	}
	return count
}

func (r *Rescaler) relocateExtent(extent metadata.Extent, newDiskID string) error {
	oldPath := r.getExtentPath(extent)
	newDisk := r.findDisk(newDiskID)
	if newDisk == nil {
		return fmt.Errorf("disk %s not found", newDiskID)
	}

	newRelPath := r.generateNewRelativePath(extent, newDiskID)
	newPath := filepath.Join(newDisk.Mountpoint, newRelPath)

	if err := r.copyExtentData(oldPath, newPath); err != nil {
		return err
	}

	for i := range r.state.Extents {
		if r.state.Extents[i].ExtentID == extent.ExtentID {
			r.state.Extents[i].DataDiskID = newDiskID
			r.state.Extents[i].PhysicalLocator.RelativePath = newRelPath
			break
		}
	}

	return nil
}

func (r *Rescaler) getExtentPath(extent metadata.Extent) string {
	disk := r.findDisk(extent.DataDiskID)
	if disk == nil || disk.Mountpoint == "" {
		return filepath.Join(r.poolDir, extent.PhysicalLocator.RelativePath)
	}
	return filepath.Join(disk.Mountpoint, extent.PhysicalLocator.RelativePath)
}

func (r *Rescaler) findDisk(diskID string) *metadata.Disk {
	for i := range r.state.Disks {
		if r.state.Disks[i].DiskID == diskID {
			return &r.state.Disks[i]
		}
	}
	return nil
}

func (r *Rescaler) generateNewRelativePath(extent metadata.Extent, newDiskID string) string {
	parts := strings.Split(extent.PhysicalLocator.RelativePath, "/")
	if len(parts) >= 4 {
		parts[1] = newDiskID[:2]
		parts[2] = newDiskID[2:4]
	}
	return strings.Join(parts, "/")
}

func (r *Rescaler) copyExtentData(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer srcFile.Close()

	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create dst: %w", err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		dstFile.Close()
		os.Remove(dst)
		return fmt.Errorf("copy: %w", err)
	}

	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	return dstFile.Close()
}

func (r *Rescaler) rebuildParityGroups(affectedExtents []metadata.Extent) error {
	affectedGroups := make(map[string]bool)
	for _, extent := range affectedExtents {
		affectedGroups[extent.ParityGroupID] = true
	}

	for _, extent := range r.state.Extents {
		if !affectedGroups[extent.ParityGroupID] {
			continue
		}
		for i := range r.state.ParityGroups {
			if r.state.ParityGroups[i].ParityGroupID == extent.ParityGroupID {
				r.state.ParityGroups[i].Generation++
				break
			}
		}
	}

	return nil
}

func (r *Rescaler) removeExcludedDisk(diskID string) []metadata.Disk {
	newDisks := make([]metadata.Disk, 0, len(r.state.Disks))
	for _, disk := range r.state.Disks {
		if disk.DiskID != diskID {
			newDisks = append(newDisks, disk)
		}
	}
	return newDisks
}

type AutoExclusionManager struct {
	mu            sync.RWMutex
	healthMonitor *HealthMonitor
	poolDir       string
	statePath     string
	stopCh        chan struct{}
}

func NewAutoExclusionManager(healthMonitor *HealthMonitor, poolDir, statePath string) *AutoExclusionManager {
	return &AutoExclusionManager{
		healthMonitor: healthMonitor,
		poolDir:       poolDir,
		statePath:     statePath,
		stopCh:        make(chan struct{}),
	}
}

func (m *AutoExclusionManager) Start() {
	m.healthMonitor.SetExcludeHook(m.handleExclusion)
	go m.monitorLoop()
}

func (m *AutoExclusionManager) Stop() {
	close(m.stopCh)
}

func (m *AutoExclusionManager) handleExclusion(diskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	store := metadata.NewStore(m.statePath)
	state, err := store.Load()
	if err != nil {
		return fmt.Errorf("load state for exclusion: %w", err)
	}

	rescaler := NewRescaler(m.poolDir, &state)
	result, err := rescaler.RescaleAfterExclusion(diskID)
	if err != nil {
		return fmt.Errorf("rescale after exclusion: %w", err)
	}

	if result.ExtentsFailed > 0 {
		return fmt.Errorf("failed to rescale %d extents", result.ExtentsFailed)
	}

	return nil
}

func (m *AutoExclusionManager) monitorLoop() {
	ticker := time.NewTicker(HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.performHealthChecks()
		}
	}
}

func (m *AutoExclusionManager) performHealthChecks() {
	m.mu.RLock()
	allHealth := m.healthMonitor.GetAllDiskHealth()
	m.mu.RUnlock()

	for diskID, health := range allHealth {
		if health.Status == HealthFailing || health.Status == HealthExcluded {
			continue
		}

		devicePath, err := VerifyDiskPath(health.DiskUUID)
		if err != nil {
			if err := m.healthMonitor.CheckDisk(devicePath, diskID); err != nil {
			}
		}
	}
}
