package journal

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rtparityd/rtparityd/internal/metadata"
)

type RebuildIssue struct {
	ExtentID      string `json:"extent_id,omitempty"`
	ParityGroupID string `json:"parity_group_id,omitempty"`
	Status        string `json:"status"`
	Detail        string `json:"detail,omitempty"`
}

type RebuildResult struct {
	StartedAt        time.Time      `json:"started_at"`
	CompletedAt      time.Time      `json:"completed_at"`
	DiskID           string         `json:"disk_id"`
	Healthy          bool           `json:"healthy"`
	ExtentsScanned   int            `json:"extents_scanned"`
	ExtentsRebuilt   int            `json:"extents_rebuilt"`
	FailedCount      int            `json:"failed_count"`
	RebuiltExtentIDs []string       `json:"rebuilt_extent_ids,omitempty"`
	Issues           []RebuildIssue `json:"issues,omitempty"`
}

func (c *Coordinator) RebuildDataDisk(diskID string) (RebuildResult, error) {
	result := RebuildResult{
		StartedAt: time.Now().UTC(),
		DiskID:    strings.TrimSpace(diskID),
		Healthy:   true,
	}
	if c == nil {
		return result, fmt.Errorf("coordinator is nil")
	}
	if result.DiskID == "" {
		return result, fmt.Errorf("disk id is required")
	}

	state, err := c.metadata.Load()
	if err != nil {
		return result, fmt.Errorf("load metadata state: %w", err)
	}

	rootDir := filepath.Dir(c.metadataPath)
	extents := make([]metadata.Extent, 0)
	for _, extent := range state.Extents {
		if extent.DataDiskID == result.DiskID {
			extents = append(extents, extent)
		}
	}
	if len(extents) == 0 {
		return result, fmt.Errorf("no extents mapped to disk %s", result.DiskID)
	}

	sort.Slice(extents, func(i, j int) bool {
		if extents[i].ParityGroupID == extents[j].ParityGroupID {
			return extents[i].LogicalOffset < extents[j].LogicalOffset
		}
		return extents[i].ParityGroupID < extents[j].ParityGroupID
	})

	result.ExtentsScanned = len(extents)
	for _, extent := range extents {
		_, repaired, err := verifyExtent(rootDir, state, extent, true)
		if err != nil {
			result.FailedCount++
			result.Healthy = false
			result.Issues = append(result.Issues, RebuildIssue{
				ExtentID:      extent.ExtentID,
				ParityGroupID: extent.ParityGroupID,
				Status:        "failed",
				Detail:        err.Error(),
			})
			continue
		}
		if repaired {
			result.ExtentsRebuilt++
			result.RebuiltExtentIDs = append(result.RebuiltExtentIDs, extent.ExtentID)
			result.Issues = append(result.Issues, RebuildIssue{
				ExtentID:      extent.ExtentID,
				ParityGroupID: extent.ParityGroupID,
				Status:        "rebuilt",
				Detail:        "restored from parity",
			})
		}
	}

	result.CompletedAt = time.Now().UTC()
	return result, nil
}
