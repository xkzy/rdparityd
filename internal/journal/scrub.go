package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type ScrubIssue struct {
	Kind          string `json:"kind"`
	FilePath      string `json:"file_path,omitempty"`
	ExtentID      string `json:"extent_id,omitempty"`
	ParityGroupID string `json:"parity_group_id,omitempty"`
	Status        string `json:"status"`
	Detail        string `json:"detail,omitempty"`
}

type ScrubResult struct {
	StartedAt            time.Time    `json:"started_at"`
	CompletedAt          time.Time    `json:"completed_at"`
	Repair               bool         `json:"repair"`
	Healthy              bool         `json:"healthy"`
	FilesChecked         int          `json:"files_checked"`
	ExtentsChecked       int          `json:"extents_checked"`
	ParityGroupsChecked  int          `json:"parity_groups_checked"`
	HealedCount          int          `json:"healed_count"`
	FailedCount          int          `json:"failed_count"`
	HealedExtentIDs      []string     `json:"healed_extent_ids,omitempty"`
	HealedParityGroupIDs []string     `json:"healed_parity_group_ids,omitempty"`
	Issues               []ScrubIssue `json:"issues,omitempty"`
}

const maxScrubHistory = 32

func (c *Coordinator) Scrub(repair bool) (ScrubResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scrubWithRepairFailAfter(repair, "")
}

func (c *Coordinator) scrubWithRepairFailAfter(repair bool, failAfter State) (ScrubResult, error) {
	if c == nil {
		return ScrubResult{}, fmt.Errorf("coordinator is nil")
	}

	result := ScrubResult{
		StartedAt: time.Now().UTC(),
		Repair:    repair,
		Healthy:   true,
	}

	state, err := c.loadState(metadata.PrototypeState("demo"))
	if err != nil {
		return result, fmt.Errorf("load metadata state: %w", err)
	}

	rootDir := filepath.Dir(c.metadataPath)
	result.FilesChecked = len(state.Files)
	result.ExtentsChecked = len(state.Extents)
	result.ParityGroupsChecked = len(state.ParityGroups)

	fileByID := make(map[string]metadata.FileRecord, len(state.Files))
	for _, file := range state.Files {
		fileByID[file.FileID] = file
	}

	extents := append([]metadata.Extent(nil), state.Extents...)
	sort.Slice(extents, func(i, j int) bool {
		leftPath := fileByID[extents[i].FileID].Path
		rightPath := fileByID[extents[j].FileID].Path
		if leftPath == rightPath {
			return extents[i].LogicalOffset < extents[j].LogicalOffset
		}
		return leftPath < rightPath
	})

	for _, extent := range extents {
		_, repaired, err := verifyExtent(c.metadataPath, c.journal, state, extent, repair, failAfter)
		if err != nil {
			result.FailedCount++
			result.Healthy = false
			result.Issues = append(result.Issues, ScrubIssue{
				Kind:          "extent",
				FilePath:      fileByID[extent.FileID].Path,
				ExtentID:      extent.ExtentID,
				ParityGroupID: extent.ParityGroupID,
				Status:        "failed",
				Detail:        err.Error(),
			})
			continue
		}
		if repaired {
			result.HealedCount++
			result.HealedExtentIDs = append(result.HealedExtentIDs, extent.ExtentID)
			result.Issues = append(result.Issues, ScrubIssue{
				Kind:          "extent",
				FilePath:      fileByID[extent.FileID].Path,
				ExtentID:      extent.ExtentID,
				ParityGroupID: extent.ParityGroupID,
				Status:        "healed",
				Detail:        "restored from parity",
			})
		}
	}

	groups := append([]metadata.ParityGroup(nil), state.ParityGroups...)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].ParityGroupID < groups[j].ParityGroupID
	})

	groupsToRepair := make(map[string]struct{})
	for _, group := range groups {
		if err := verifyParityGroup(rootDir, group); err != nil {
			if repair {
				groupsToRepair[group.ParityGroupID] = struct{}{}
				continue
			}
			result.FailedCount++
			result.Healthy = false
			result.Issues = append(result.Issues, ScrubIssue{
				Kind:          "parity",
				ParityGroupID: group.ParityGroupID,
				Status:        "failed",
				Detail:        err.Error(),
			})
		}
	}

	if repair && len(groupsToRepair) > 0 {
		groupIDs := make([]string, 0, len(groupsToRepair))
		for groupID := range groupsToRepair {
			groupIDs = append(groupIDs, groupID)
		}
		sort.Strings(groupIDs)
		for _, groupID := range groupIDs {
			groupOnly := map[string]struct{}{groupID: struct{}{}}
			repairExtents := extentsForParityGroups(state, groupOnly)
			repaired, err := runParityRepair(c.metadataPath, c.journal, &state, groupID, repairExtents, failAfter)
			if err != nil {
				result.FailedCount++
				result.Healthy = false
				result.Issues = append(result.Issues, ScrubIssue{
					Kind:          "parity",
					ParityGroupID: groupID,
					Status:        "failed",
					Detail:        err.Error(),
				})
				continue
			}
			if repaired {
				result.HealedCount++
				result.HealedParityGroupIDs = append(result.HealedParityGroupIDs, groupID)
				result.Issues = append(result.Issues, ScrubIssue{
					Kind:          "parity",
					ParityGroupID: groupID,
					Status:        "healed",
					Detail:        "regenerated from member extents",
				})
			}
		}
	}

	result.CompletedAt = time.Now().UTC()
	appendScrubHistory(&state, result)
	if _, err := c.commitState(state); err != nil {
		return result, fmt.Errorf("save metadata snapshot after scrub: %w", err)
	}
	return result, nil
}

func appendScrubHistory(state *metadata.SampleState, result ScrubResult) {
	if state == nil {
		return
	}

	run := metadata.ScrubRun{
		RunID:                fmt.Sprintf("scrub-%d", result.StartedAt.UnixNano()),
		StartedAt:            result.StartedAt,
		CompletedAt:          result.CompletedAt,
		Repair:               result.Repair,
		Healthy:              result.Healthy,
		FilesChecked:         result.FilesChecked,
		ExtentsChecked:       result.ExtentsChecked,
		ParityGroupsChecked:  result.ParityGroupsChecked,
		HealedCount:          result.HealedCount,
		FailedCount:          result.FailedCount,
		IssueCount:           len(result.Issues),
		HealedExtentIDs:      append([]string(nil), result.HealedExtentIDs...),
		HealedParityGroupIDs: append([]string(nil), result.HealedParityGroupIDs...),
	}
	state.ScrubHistory = append(state.ScrubHistory, run)
	if len(state.ScrubHistory) > maxScrubHistory {
		state.ScrubHistory = append([]metadata.ScrubRun(nil), state.ScrubHistory[len(state.ScrubHistory)-maxScrubHistory:]...)
	}
}

func verifyParityGroup(rootDir string, group metadata.ParityGroup) error {
	if group.ParityGroupID == "" {
		return fmt.Errorf("missing parity group id")
	}
	if group.ParityChecksum == "" {
		return fmt.Errorf("missing parity checksum for group %s", group.ParityGroupID)
	}

	parityPath := filepath.Join(rootDir, "parity", group.ParityGroupID+".bin")
	data, err := os.ReadFile(parityPath)
	if err != nil {
		return fmt.Errorf("read parity file: %w", err)
	}
	if digestBytes(data) != group.ParityChecksum {
		return fmt.Errorf("parity checksum mismatch for group %s", group.ParityGroupID)
	}
	return nil
}

func extentsForParityGroups(state metadata.SampleState, groupIDs map[string]struct{}) []metadata.Extent {
	if len(groupIDs) == 0 {
		return nil
	}

	extents := make([]metadata.Extent, 0)
	for _, extent := range state.Extents {
		if _, ok := groupIDs[extent.ParityGroupID]; ok {
			extents = append(extents, extent)
		}
	}
	return extents
}
