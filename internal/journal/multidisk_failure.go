package journal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// MultiDiskFailureAnalysis describes the severity and scope of multiple
// simultaneous disk failures in the pool.
type MultiDiskFailureAnalysis struct {
	FailedDisks           []string                   `json:"failed_disks"`
	UnrecoverableExtents  []string                   `json:"unrecoverable_extents"`
	UnrecoverableGroups   []string                   `json:"unrecoverable_parity_groups"`
	PartiallyLostExtents  []string                   `json:"partially_lost_extents"`
	RecoveryIsPossible    bool                       `json:"recovery_is_possible"`
	FailureMode           string                     `json:"failure_mode"` // "single", "dual", "cascade"
	RecommendedAction     string                     `json:"recommended_action"`
	AffectedDataBytes     int64                      `json:"affected_data_bytes"`
	RecoverableDataBytes  int64                      `json:"recoverable_data_bytes"`
	UnrecoverableDataBytes int64                     `json:"unrecoverable_data_bytes"`
}

// AnalyzeMultiDiskFailures examines the metadata state against the filesystem
// to detect if multiple disks have failed and determines recoverability.
//
// It detects:
//   - Disks with missing extent/parity files (failed disks)
//   - Parity groups missing members (data loss risk)
//   - Extents unrecoverable due to missing all copies and parity
func AnalyzeMultiDiskFailures(rootDir string, state metadata.SampleState) *MultiDiskFailureAnalysis {
	analysis := &MultiDiskFailureAnalysis{
		FailedDisks:            []string{},
		UnrecoverableExtents:   []string{},
		UnrecoverableGroups:    []string{},
		PartiallyLostExtents:   []string{},
		RecoveryIsPossible:     true,
		FailureMode:            "single",
		AffectedDataBytes:      0,
		RecoverableDataBytes:   0,
		UnrecoverableDataBytes: 0,
	}

	// Track health per disk ID. Start optimistic.
	diskHealth := make(map[string]bool)
	for _, disk := range state.Disks {
		diskHealth[disk.DiskID] = true
	}

	// Probe every extent file; mark its data disk failed if the file is absent.
	for _, extent := range state.Extents {
		path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		if _, err := os.Stat(path); err != nil {
			diskHealth[extent.DataDiskID] = false
			analysis.AffectedDataBytes += extent.Length
		} else {
			analysis.RecoverableDataBytes += extent.Length
		}
	}

	// Probe every parity file; mark its parity disk failed if the file is absent.
	for _, group := range state.ParityGroups {
		parityPath := filepath.Join(rootDir, "parity", group.ParityGroupID+".bin")
		if _, err := os.Stat(parityPath); err != nil {
			diskHealth[group.ParityDiskID] = false
		}
	}

	for diskID, healthy := range diskHealth {
		if !healthy {
			analysis.FailedDisks = append(analysis.FailedDisks, diskID)
		}
	}

	if len(analysis.FailedDisks) == 0 {
		analysis.RecoveryIsPossible = true
		analysis.RecommendedAction = "No failed disks detected; pool is healthy."
		return analysis
	}

	if len(analysis.FailedDisks) > 1 {
		analysis.FailureMode = "dual"
	}

	failedDiskSet := make(map[string]bool)
	for _, diskID := range analysis.FailedDisks {
		failedDiskSet[diskID] = true
	}

	// A parity group is unrecoverable if its parity disk failed (cannot XOR-reconstruct
	// any member extent). If only data disks failed the parity disk is intact and
	// reconstruction is possible.
	groupRecoveryMap := make(map[string]bool)
	for _, group := range state.ParityGroups {
		if failedDiskSet[group.ParityDiskID] {
			analysis.UnrecoverableGroups = append(analysis.UnrecoverableGroups, group.ParityGroupID)
			groupRecoveryMap[group.ParityGroupID] = false
		} else {
			groupRecoveryMap[group.ParityGroupID] = true
		}
	}

	for _, extent := range state.Extents {
		if !failedDiskSet[extent.DataDiskID] {
			continue
		}
		if !groupRecoveryMap[extent.ParityGroupID] {
			analysis.UnrecoverableExtents = append(analysis.UnrecoverableExtents, extent.ExtentID)
			analysis.UnrecoverableDataBytes += extent.Length
		} else {
			analysis.PartiallyLostExtents = append(analysis.PartiallyLostExtents, extent.ExtentID)
		}
	}

	if len(analysis.UnrecoverableExtents) > 0 {
		analysis.RecoveryIsPossible = false
		analysis.RecommendedAction = fmt.Sprintf(
			"UNRECOVERABLE DATA LOSS: %d extents on failed disks cannot be reconstructed. "+
				"Multiple disks in the same parity group have failed. "+
				"Restore from backup or rebuild with additional parity.",
			len(analysis.UnrecoverableExtents),
		)
		return analysis
	}

	if len(analysis.UnrecoverableGroups) > 0 {
		analysis.RecoveryIsPossible = true
		analysis.RecommendedAction = fmt.Sprintf(
			"Parity groups %v are unrecoverable (parity disk failed). "+
				"Rebuild all data disks that reference these groups. "+
				"Regenerate parity after rebuild.",
			analysis.UnrecoverableGroups,
		)
		return analysis
	}

	if len(analysis.FailedDisks) == 1 {
		analysis.FailureMode = "single"
		analysis.RecoveryIsPossible = true
		analysis.RecommendedAction = fmt.Sprintf(
			"Single disk failure detected (%s). "+
				"Run rebuild on the failed disk to recover all %d extents.",
			analysis.FailedDisks[0],
			len(analysis.PartiallyLostExtents),
		)
		return analysis
	}

	analysis.RecommendedAction = fmt.Sprintf(
		"Multiple disk failures detected: %v. "+
			"All %d failed extents can be recovered via parity. "+
			"Run rebuild for each failed disk in sequence.",
		analysis.FailedDisks,
		len(analysis.PartiallyLostExtents),
	)
	return analysis
}

// ValidateRecoverabilityInvariants checks that the recovered state has no
// unrecoverable data. Returns a violation if recovery would result in silent
// data loss.
func ValidateRecoverabilityInvariants(rootDir string, state metadata.SampleState) []InvariantViolation {
	analysis := AnalyzeMultiDiskFailures(rootDir, state)
	var violations []InvariantViolation

	if len(analysis.UnrecoverableExtents) > 0 {
		violations = append(violations, InvariantViolation{
			Code:    "M4",
			Kind:    "metadata",
			Entity:  "multi-disk-failure",
			Message: fmt.Sprintf("Unrecoverable data loss: %d extents in failed parity groups", len(analysis.UnrecoverableExtents)),
		})
	}

	if len(analysis.UnrecoverableGroups) > 0 {
		violations = append(violations, InvariantViolation{
			Code:    "P5",
			Kind:    "parity",
			Entity:  "multi-disk-failure",
			Message: fmt.Sprintf("Parity groups %v have failed parity disks; cannot recover their members", analysis.UnrecoverableGroups),
		})
	}

	return violations
}
