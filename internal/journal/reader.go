package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ReadMeta returns the current metadata state. It returns an empty SampleState
// (not an error) when no metadata file exists yet. This is used by the FUSE
// layer to enumerate files and directories without needing to go through the
// full coordinator write path.
func (c *Coordinator) ReadMeta() (metadata.SampleState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cachedStateSet {
		return *c.cachedState, nil
	}
	state, err := c.metadata.Load()
	if err == nil {
		return state, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return metadata.SampleState{}, nil
	}
	return metadata.SampleState{}, fmt.Errorf("read metadata: %w", err)
}

// PoolName returns the pool name from the current metadata state, or "demo" if
// no metadata has been written yet.
func (c *Coordinator) PoolName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cachedStateSet {
		if c.cachedState.Pool.Name != "" {
			return c.cachedState.Pool.Name
		}
	}
	state, err := c.metadata.Load()
	if err != nil || state.Pool.Name == "" {
		return "demo"
	}
	return state.Pool.Name
}

// RootDir returns the directory that hosts extent and parity files — the same
// directory that contains the metadata snapshot.
func (c *Coordinator) RootDir() string {
	return filepath.Dir(c.metadataPath)
}

type ReadResult struct {
	File            metadata.FileRecord `json:"file"`
	BytesRead       int64               `json:"bytes_read"`
	Verified        bool                `json:"verified"`
	HealedExtentIDs []string            `json:"healed_extent_ids,omitempty"`
	ContentChecksum string              `json:"content_checksum"`
	Data            []byte              `json:"-"`
}

func (c *Coordinator) ReadFile(logicalPath string) (ReadResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readFileWithRepairFailAfter(logicalPath, "")
}

func (c *Coordinator) readFileWithRepairFailAfter(logicalPath string, failAfter State) (ReadResult, error) {
	if c == nil {
		return ReadResult{}, fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return ReadResult{}, fmt.Errorf("logical path is required")
	}

	var state metadata.SampleState
	var err error
	if c.cachedStateSet {
		state = *c.cachedState
	} else {
		state, err = c.metadata.Load()
		if err != nil {
			return ReadResult{}, fmt.Errorf("load metadata state: %w", err)
		}
		// Don't cache here: a read should not prime the cache with a
		// potentially-incomplete state. Only commits populate the cache.
	}

	file, extents, err := findFileExtents(state, logicalPath)
	if err != nil {
		return ReadResult{}, err
	}

	content := make([]byte, 0, file.SizeBytes)
	healed := make([]string, 0)
	for _, extent := range extents {
		data, repaired, err := c.readVerifiedExtent(state, extent, failAfter)
		if err != nil {
			return ReadResult{}, fmt.Errorf("read extent %s: %w", extent.ExtentID, err)
		}
		content = append(content, data...)
		if repaired {
			healed = append(healed, extent.ExtentID)
		}
	}

	// I13 (Read Correctness): the assembled content must be exactly as long as
	// the committed file size. A mismatch indicates a bug in the read path or
	// a metadata/extent length inconsistency that was not caught by I5.
	if int64(len(content)) != file.SizeBytes {
		return ReadResult{}, fmt.Errorf(
			"I13: read length mismatch for %s: committed size=%d got=%d",
			logicalPath, file.SizeBytes, len(content))
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

// findFileExtents locates the FileRecord and all associated extents for the
// given logical path, returning extents sorted by LogicalOffset.
//
// Complexity: O(F) file scan + O(E) extent scan where F = number of files and
// E = number of extents. For pools with many files, a future version will
// maintain in-memory indexes (map[path]FileRecord, map[fileID][]Extent) built
// once at state load time and invalidated on commit. The on-disk format does
// not need to change to add those indexes.
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

func (c *Coordinator) readVerifiedExtent(state metadata.SampleState, extent metadata.Extent, failAfter State) ([]byte, bool, error) {
	return verifyExtent(c.metadataPath, c.journal, state, extent, true, failAfter)
}

func verifyExtent(metadataPath string, journal *Store, state metadata.SampleState, extent metadata.Extent, repair bool, failAfter State) ([]byte, bool, error) {
	rootDir := filepath.Dir(metadataPath)
	path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(path)
	if err == nil {
		if int64(len(data)) == extent.Length && digestBytes(data) == extent.Checksum {
			return append([]byte(nil), data...), false, nil
		}
		if !repair {
			if int64(len(data)) != extent.Length {
				return nil, false, fmt.Errorf("extent length mismatch for %s: committed=%d disk=%d", extent.ExtentID, extent.Length, len(data))
			}
			return nil, false, fmt.Errorf("extent checksum mismatch for %s", extent.ExtentID)
		}
	} else if !repair {
		return nil, false, fmt.Errorf("read extent file: %w", err)
	}

	if !repair {
		return nil, false, fmt.Errorf("extent %s is missing or corrupt", extent.ExtentID)
	}
	if journal == nil {
		return nil, false, fmt.Errorf("journal store is nil")
	}
	return runExtentRepair(metadataPath, journal, state, extent, failAfter)
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
		if int64(len(memberData)) != extent.Length {
			return nil, fmt.Errorf("peer extent length mismatch for %s: committed=%d disk=%d", extent.ExtentID, extent.Length, len(memberData))
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
