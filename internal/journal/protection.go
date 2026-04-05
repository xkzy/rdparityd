package journal

import (
	"fmt"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type ProtectionStatus struct {
	State           metadata.PoolProtectionState           `json:"state"`
	DiskCount       int                                    `json:"disk_count"`
	DataDiskCount   int                                    `json:"data_disk_count"`
	MirrorDiskCount int                                    `json:"mirror_disk_count"`
	ParityDiskCount int                                    `json:"parity_disk_count"`
	ExtentCounts    map[metadata.ExtentProtectionClass]int `json:"extent_counts"`
	CorruptExtents  int                                    `json:"corrupt_extents"`
	Unrecoverable   int                                    `json:"unrecoverable"`
}

func (c *Coordinator) ProtectionStatus() (ProtectionStatus, error) {
	if err := c.ensureRecoveredLocked(""); err != nil {
		return ProtectionStatus{}, err
	}

	state, err := c.loadState(metadata.PrototypeState(""))
	if err != nil {
		return ProtectionStatus{}, fmt.Errorf("load state: %w", err)
	}

	status := ProtectionStatus{
		ExtentCounts: make(map[metadata.ExtentProtectionClass]int),
	}

	for _, disk := range state.Disks {
		status.DiskCount++
		switch disk.Role {
		case metadata.DiskRoleData:
			status.DataDiskCount++
		case metadata.DiskRoleMirror:
			status.MirrorDiskCount++
		case metadata.DiskRoleParity:
			status.ParityDiskCount++
		}
	}

	for _, ext := range state.Extents {
		class := ext.ProtectionClass
		if class == "" {
			class = metadata.ExtentChecksumOnly
		}
		status.ExtentCounts[class]++

		switch ext.CorruptionStatus {
		case metadata.ExtentCorrupt:
			status.CorruptExtents++
		case metadata.ExtentQuarantined:
			status.Unrecoverable++
		}
	}

	if status.DiskCount == 1 {
		status.State = metadata.ProtectionIntegrityOnly
	} else if status.ParityDiskCount > 0 {
		status.State = metadata.ProtectionParity
	} else if status.MirrorDiskCount > 0 {
		status.State = metadata.ProtectionMirrored
	} else {
		status.State = metadata.ProtectionIntegrityOnly
	}

	return status, nil
}

func (c *Coordinator) ProtectionState() (metadata.PoolProtectionState, error) {
	status, err := c.ProtectionStatus()
	if err != nil {
		return "", err
	}
	return status.State, nil
}

func (c *Coordinator) ExtentProtectionClass(extentID string) (metadata.ExtentProtectionClass, error) {
	state, err := c.loadState(metadata.PrototypeState(""))
	if err != nil {
		return "", fmt.Errorf("load state: %w", err)
	}

	for _, ext := range state.Extents {
		if ext.ExtentID == extentID {
			if ext.ProtectionClass == "" {
				return metadata.ExtentChecksumOnly, nil
			}
			return ext.ProtectionClass, nil
		}
	}
	return "", fmt.Errorf("extent not found: %s", extentID)
}
