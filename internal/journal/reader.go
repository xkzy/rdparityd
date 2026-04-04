package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/rtparityd/rtparityd/internal/metadata"
)

type ReadResult struct {
	File            metadata.FileRecord `json:"file"`
	BytesRead       int64               `json:"bytes_read"`
	Verified        bool                `json:"verified"`
	HealedExtentIDs []string            `json:"healed_extent_ids,omitempty"`
	ContentChecksum string              `json:"content_checksum"`
	Data            []byte              `json:"-"`
}

func (c *Coordinator) ReadFile(logicalPath string) (ReadResult, error) {
	if c == nil {
		return ReadResult{}, fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return ReadResult{}, fmt.Errorf("logical path is required")
	}

	state, err := c.metadata.Load()
	if err != nil {
		return ReadResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	file, extents, err := findFileExtents(state, logicalPath)
	if err != nil {
		return ReadResult{}, err
	}

	rootDir := filepath.Dir(c.metadataPath)
	content := make([]byte, 0, file.SizeBytes)
	healed := make([]string, 0)
	for _, extent := range extents {
		data, repaired, err := readVerifiedExtent(rootDir, state, extent)
		if err != nil {
			return ReadResult{}, fmt.Errorf("read extent %s: %w", extent.ExtentID, err)
		}
		content = append(content, data...)
		if repaired {
			healed = append(healed, extent.ExtentID)
		}
	}

	return ReadResult{
		File:            file,
		BytesRead:       int64(len(content)),
		Verified:        true,
		HealedExtentIDs: healed,
		ContentChecksum: digestBytes(content),
		Data:            content,
	}, nil
}

func findFileExtents(state metadata.SampleState, logicalPath string) (metadata.FileRecord, []metadata.Extent, error) {
	for _, file := range state.Files {
		if file.Path != logicalPath {
			continue
		}

		extents := make([]metadata.Extent, 0)
		for _, extent := range state.Extents {
			if extent.FileID == file.FileID {
				extents = append(extents, extent)
			}
		}
		sort.Slice(extents, func(i, j int) bool {
			return extents[i].LogicalOffset < extents[j].LogicalOffset
		})
		return file, extents, nil
	}

	return metadata.FileRecord{}, nil, fmt.Errorf("file not found: %s", logicalPath)
}

func readVerifiedExtent(rootDir string, state metadata.SampleState, extent metadata.Extent) ([]byte, bool, error) {
	path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(path)
	if err == nil {
		normalized := normalizeExtentLength(data, extent.Length)
		if digestBytes(normalized) == extent.Checksum {
			return normalized, false, nil
		}
	}

	rebuilt, err := reconstructExtent(rootDir, state, extent)
	if err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, false, fmt.Errorf("create extent directory: %w", err)
	}
	if err := os.WriteFile(path, rebuilt, 0o600); err != nil {
		return nil, false, fmt.Errorf("rewrite healed extent: %w", err)
	}
	return rebuilt, true, nil
}

func reconstructExtent(rootDir string, state metadata.SampleState, target metadata.Extent) ([]byte, error) {
	parityPath := filepath.Join(rootDir, "parity", target.ParityGroupID+".bin")
	parityData, err := os.ReadFile(parityPath)
	if err != nil {
		return nil, fmt.Errorf("read parity file: %w", err)
	}

	for _, group := range state.ParityGroups {
		if group.ParityGroupID == target.ParityGroupID && group.ParityChecksum != "" {
			if digestBytes(parityData) != group.ParityChecksum {
				return nil, fmt.Errorf("parity checksum mismatch for group %s", group.ParityGroupID)
			}
			break
		}
	}

	rebuilt := append([]byte(nil), parityData...)
	for _, extent := range state.Extents {
		if extent.ParityGroupID != target.ParityGroupID || extent.ExtentID == target.ExtentID {
			continue
		}
		memberPath := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		memberData, err := os.ReadFile(memberPath)
		if err != nil {
			return nil, fmt.Errorf("read peer extent for reconstruction: %w", err)
		}
		xorInto(rebuilt, normalizeExtentLength(memberData, int64(len(rebuilt))))
	}

	normalized := normalizeExtentLength(rebuilt, target.Length)
	if digestBytes(normalized) != target.Checksum {
		return nil, fmt.Errorf("reconstructed data checksum mismatch for extent %s", target.ExtentID)
	}
	return normalized, nil
}

func normalizeExtentLength(data []byte, length int64) []byte {
	if length < 0 {
		length = 0
	}
	if int64(len(data)) == length {
		return append([]byte(nil), data...)
	}
	if int64(len(data)) > length {
		return append([]byte(nil), data[:length]...)
	}
	out := make([]byte, length)
	copy(out, data)
	return out
}
