package journal

// disk_lifecycle.go — Phase 7: safe disk add, replace, and failure handling.
//
// Invariants preserved by every operation:
//   - M2: every extent's data_disk_id must reference an existing disk.
//   - Metadata is always durably persisted (fsync'd) before returning.
//   - No data loss occurs; disk failure triggers parity-based reconstruction.
//
// Durability model:
//   Metadata is updated and saved via c.metadata.Save (which fsyncs the file
//   and its parent directory) before the operation returns.

import (
	"fmt"
	"strings"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func validateDiskIdentity(uuid string) error {
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return fmt.Errorf("disk uuid is required")
	}
	return nil
}

func ensureUUIDUnique(state metadata.SampleState, uuid string, exceptDiskID string) error {
	for _, d := range state.Disks {
		if d.DiskID == exceptDiskID {
			continue
		}
		if strings.TrimSpace(d.UUID) == uuid {
			return fmt.Errorf("disk uuid %q already exists on disk %q", uuid, d.DiskID)
		}
	}
	return nil
}

// AddDisk adds a new disk to the pool and persists the updated metadata.
// The disk must not already exist (duplicate DiskID is rejected), and its
// hardware identity UUID must be explicitly provided and unique.
// role must be one of DiskRoleData, DiskRoleParity, or DiskRoleMetadata.
func (c *Coordinator) AddDisk(diskID, uuid string, role metadata.DiskRole, mountpoint string, capacityBytes int64) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	diskID = strings.TrimSpace(diskID)
	if diskID == "" {
		return fmt.Errorf("disk id is required")
	}
	if err := validateDiskIdentity(uuid); err != nil {
		return err
	}
	if mountpoint == "" {
		return fmt.Errorf("mountpoint is required")
	}
	if capacityBytes <= 0 {
		return fmt.Errorf("capacity must be positive")
	}
	switch role {
	case metadata.DiskRoleData, metadata.DiskRoleParity, metadata.DiskRoleMetadata:
	default:
		return fmt.Errorf("invalid disk role %q", role)
	}

	state, err := c.loadState(metadata.PrototypeState("demo"))
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	for _, d := range state.Disks {
		if d.DiskID == diskID {
			return fmt.Errorf("disk %q already exists", diskID)
		}
	}
	if err := ensureUUIDUnique(state, uuid, ""); err != nil {
		return err
	}

	state.Disks = append(state.Disks, metadata.Disk{
		DiskID:        diskID,
		UUID:          uuid,
		Role:          role,
		Mountpoint:    mountpoint,
		CapacityBytes: capacityBytes,
		FreeBytes:     capacityBytes,
		HealthStatus:  "online",
		Generation:    int64(len(state.Transactions) + 1),
	})

	if _, err := c.commitState(state); err != nil {
		return fmt.Errorf("persist metadata after add disk: %w", err)
	}
	return nil
}

// ReplaceDisk replaces a failed or removed disk with a new one.
// All extents that were on oldDiskID are re-assigned to newDiskID.
// The parity reconstruction needed to restore data on newDiskID must be
// performed separately via RebuildDataDisk(newDiskID).
//
// Invariants: newDiskID must not already exist; oldDiskID must exist;
// newUUID must be non-empty, unique, and must differ from the old disk UUID.
func (c *Coordinator) ReplaceDisk(oldDiskID, newDiskID, newUUID string) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	oldDiskID = strings.TrimSpace(oldDiskID)
	newDiskID = strings.TrimSpace(newDiskID)
	if oldDiskID == "" || newDiskID == "" {
		return fmt.Errorf("disk ids must be non-empty")
	}
	if oldDiskID == newDiskID {
		return fmt.Errorf("old and new disk id must differ")
	}
	if err := validateDiskIdentity(newUUID); err != nil {
		return err
	}

	state, err := c.loadState(metadata.PrototypeState("demo"))
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	oldIdx := -1
	for i, d := range state.Disks {
		if d.DiskID == newDiskID {
			return fmt.Errorf("disk %q already exists", newDiskID)
		}
		if d.DiskID == oldDiskID {
			oldIdx = i
		}
	}
	if oldIdx < 0 {
		return fmt.Errorf("disk %q not found", oldDiskID)
	}
	if err := ensureUUIDUnique(state, newUUID, oldDiskID); err != nil {
		return err
	}

	// Inherit metadata from old disk, update to new identity.
	old := state.Disks[oldIdx]
	if strings.TrimSpace(old.UUID) == "" {
		return fmt.Errorf("disk %q has no recorded uuid; cannot safely replace without durable identity", oldDiskID)
	}
	if old.UUID == newUUID {
		return fmt.Errorf("replacement disk uuid must differ from old disk uuid %q", old.UUID)
	}
	newDisk := metadata.Disk{
		DiskID:         newDiskID,
		UUID:           newUUID,
		Role:           old.Role,
		FilesystemType: old.FilesystemType,
		Mountpoint:     old.Mountpoint,
		CapacityBytes:  old.CapacityBytes,
		FreeBytes:      old.FreeBytes,
		HealthStatus:   "online",
		Generation:     old.Generation + 1,
	}

	// Replace disk in the disks list.
	state.Disks[oldIdx] = newDisk

	// Re-assign all extents to the new disk ID so M2 is preserved.
	for i := range state.Extents {
		if state.Extents[i].DataDiskID == oldDiskID {
			state.Extents[i].DataDiskID = newDiskID
		}
	}

	if violations := CheckStateInvariants(state); len(violations) > 0 {
		return fmt.Errorf("invariant violation after replace disk: %v", violations[0])
	}

	if _, err := c.commitState(state); err != nil {
		return fmt.Errorf("persist metadata after replace disk: %w", err)
	}
	return nil
}

// FailDisk marks a disk as failed and updates its health status in metadata.
// Data extents that were on the disk remain in metadata so that
// RebuildDataDisk can reconstruct them from parity.
//
// A failed disk is not removed from the disk list; it is left with
// HealthStatus="failed" so that invariant checks can detect the degraded state.
func (c *Coordinator) FailDisk(diskID string) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	diskID = strings.TrimSpace(diskID)
	if diskID == "" {
		return fmt.Errorf("disk id is required")
	}

	state, err := c.loadState(metadata.PrototypeState("demo"))
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	found := false
	for i := range state.Disks {
		if state.Disks[i].DiskID == diskID {
			state.Disks[i].HealthStatus = "failed"
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("disk %q not found", diskID)
	}

	if _, err := c.commitState(state); err != nil {
		return fmt.Errorf("persist metadata after fail disk: %w", err)
	}
	return nil
}
