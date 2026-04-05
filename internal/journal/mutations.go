package journal

// mutations.go — file mutation operations for rtparityd.
//
// v1 supports: WriteFile (new files only), RenameFile (path change only).
// v2 will add: OverwriteFile, TruncateFile, GrowFile.
//
// Operations not supported in v1 return ErrNotSupported so that callers get
// a clear, actionable error instead of silent wrong behavior.

import (
	"errors"
	"fmt"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ErrNotSupported is returned by mutation operations that are not yet
// implemented. It is a sentinel that callers can test with errors.Is.
var ErrNotSupported = errors.New("operation not supported in v1")

// RenameResult carries the outcome of a rename operation.
type RenameResult struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

// RenameFile changes the logical path of a file without touching any extent
// data or parity data. It is the only metadata-only mutation supported in v1.
//
// Durability: the updated metadata snapshot is saved atomically via
// replaceSyncFile before the function returns.
//
// Constraints:
//   - oldPath must refer to an existing committed file.
//   - newPath must not already exist in the pool.
//   - Extent data, parity blocks, and physical files are not moved.
func (c *Coordinator) RenameFile(oldPath, newPath string) (RenameResult, error) {
	if c == nil {
		return RenameResult{}, fmt.Errorf("coordinator is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if oldPath == "" {
		return RenameResult{}, fmt.Errorf("old path is required")
	}
	if newPath == "" {
		return RenameResult{}, fmt.Errorf("new path is required")
	}
	if oldPath == newPath {
		return RenameResult{OldPath: oldPath, NewPath: newPath}, nil
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return RenameResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	fileIdx := -1
	for i, file := range state.Files {
		if file.Path == oldPath {
			fileIdx = i
		}
		if file.Path == newPath {
			return RenameResult{}, fmt.Errorf("rename %q -> %q: destination already exists", oldPath, newPath)
		}
	}
	if fileIdx == -1 {
		return RenameResult{}, fmt.Errorf("rename %q: file not found", oldPath)
	}

	state.Files[fileIdx].Path = newPath
	state.Files[fileIdx].MTime = time.Now().UTC()

	if _, err := c.commitState(state); err != nil {
		return RenameResult{}, fmt.Errorf("save metadata after rename: %w", err)
	}

	return RenameResult{OldPath: oldPath, NewPath: newPath}, nil
}

// OverwriteFile updates a byte range within an existing file. Not supported
// in v1. Returns ErrNotSupported.
//
// v2 design: identify overlapping extents, for each: read-modify-write the
// extent, recompute checksum, update parity for the group, commit as one
// journal transaction.
func (c *Coordinator) OverwriteFile(logicalPath string, offset int64, data []byte) error {
	return fmt.Errorf("OverwriteFile %q at offset %d: %w", logicalPath, offset, ErrNotSupported)
}

// TruncateFile reduces the size of an existing file. Not supported in v1.
// Returns ErrNotSupported.
//
// v2 design: identify trailing extents beyond the new size, remove them from
// their parity groups, recompute affected parity blocks, free extent files,
// update metadata in one journal transaction.
func (c *Coordinator) TruncateFile(logicalPath string, newSize int64) error {
	return fmt.Errorf("TruncateFile %q to %d bytes: %w", logicalPath, newSize, ErrNotSupported)
}

// GrowFile extends an existing file with new data appended at the end. Not
// supported in v1. Returns ErrNotSupported.
//
// v2 design: compute new extents from current EOF, allocate on data disks,
// write extent data, update parity for new extents, commit as one journal
// transaction. The last existing extent may also need to be padded if it was
// a partial extent.
func (c *Coordinator) GrowFile(logicalPath string, appendData []byte) error {
	return fmt.Errorf("GrowFile %q: %w", logicalPath, ErrNotSupported)
}
