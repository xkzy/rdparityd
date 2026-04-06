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

package journal

// mutations.go — file mutation operations for rtparityd.
//
// v1 supports: WriteFile (new files only), RenameFile (path change only).
// v2 adds: OverwriteFile, TruncateFile, GrowFile.
//
// All mutation operations use the same durability model as WriteFile:
// - journal append → fsync → data write → fsync → parity write → fsync →
//   metadata write → fsync → commit record → fsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

var (
	ErrNotSupported  = errors.New("operation not supported in v1")
	ErrEmptyPayload  = errors.New("payload is required")
	ErrInvalidOffset = errors.New("offset must be non-negative")
	ErrInvalidSize   = errors.New("size must be non-negative")
	ErrFileNotFound  = errors.New("file not found")
	ErrNegativeSize  = errors.New("new size cannot be negative")
)

type OverwriteResult struct {
	TxID    string            `json:"tx_id"`
	Extents []metadata.Extent `json:"extents"`
	Healed  bool              `json:"healed"`
}

type TruncateResult struct {
	TxID           string `json:"tx_id"`
	RemovedExtents int    `json:"removed_extents"`
	FreedBytes     int64  `json:"freed_bytes"`
}

type GrowResult struct {
	TxID    string            `json:"tx_id"`
	Extents []metadata.Extent `json:"extents"`
}

type RenameResult struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

type DeleteResult struct {
	TxID           string `json:"tx_id"`
	RemovedExtents int    `json:"removed_extents"`
	FreedBytes     int64  `json:"freed_bytes"`
}

// commonMutationSetup performs the standard validation and locking sequence
// for mutation operations. Returns the lock, state, and error.
func (c *Coordinator) commonMutationSetup() (*operationLock, metadata.SampleState, error) {
	if c == nil {
		return nil, metadata.SampleState{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return nil, metadata.SampleState{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return nil, metadata.SampleState{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return nil, metadata.SampleState{}, fmt.Errorf("load metadata state: %w", err)
	}
	return lock, state, nil
}

// findFileByPath locates a file in state by its logical path and returns a pointer
// to it along with its index. Returns nil if not found.
func findFileByPath(state *metadata.SampleState, logicalPath string) (*metadata.FileRecord, int) {
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			return &state.Files[i], i
		}
	}
	return nil, -1
}

// findExtentsForFile returns all extents belonging to a given file.
func findExtentsForFile(state *metadata.SampleState, fileID string) []metadata.Extent {
	var result []metadata.Extent
	for _, ext := range state.Extents {
		if ext.FileID == fileID {
			result = append(result, ext)
		}
	}
	return result
}

// gatherAffectedExtentIDs extracts extent IDs from a slice of extents.
func gatherAffectedExtentIDs(extents []metadata.Extent) []string {
	ids := make([]string, len(extents))
	for i, e := range extents {
		ids[i] = e.ExtentID
	}
	return ids
}

// removeExtentsFromState removes specified extents from state and updates parity groups.
// Returns the total bytes freed.
func removeExtentsFromState(state *metadata.SampleState, toRemove []metadata.Extent) int64 {
	idSet := make(map[string]bool)
	var freedBytes int64
	for _, ext := range toRemove {
		idSet[ext.ExtentID] = true
		freedBytes += ext.Length
	}

	// Remove from Extents
	var remaining []metadata.Extent
	for _, ext := range state.Extents {
		if !idSet[ext.ExtentID] {
			remaining = append(remaining, ext)
		}
	}
	state.Extents = remaining

	// Update parity groups
	for i := range state.ParityGroups {
		var newMembers []string
		for _, memberID := range state.ParityGroups[i].MemberExtentIDs {
			if !idSet[memberID] {
				newMembers = append(newMembers, memberID)
			}
		}
		state.ParityGroups[i].MemberExtentIDs = newMembers
	}

	// Update disk free bytes
	for i := range state.Disks {
		for _, ext := range toRemove {
			if ext.DataDiskID == state.Disks[i].DiskID {
				state.Disks[i].FreeBytes += ext.Length
			}
		}
	}

	return freedBytes
}

// RenameFile changes the logical path of a file without touching any extent
// data or parity data.
//
// Durability: the updated metadata snapshot is saved atomically via
// replaceSyncFile before the function returns.
func (c *Coordinator) RenameFile(oldPath, newPath string) (RenameResult, error) {
	if oldPath == "" || newPath == "" {
		return RenameResult{}, fmt.Errorf("old and new path are required")
	}

	lock, state, err := c.commonMutationSetup()
	if err != nil {
		return RenameResult{}, err
	}
	defer lock.release()

	file, fileIdx := findFileByPath(&state, oldPath)
	if file == nil {
		return RenameResult{}, fmt.Errorf("rename %q: file not found", oldPath)
	}

	// Check no-op first (same path)
	if oldPath == newPath {
		return RenameResult{OldPath: oldPath, NewPath: newPath}, nil
	}

	// Check destination doesn't exist
	for _, f := range state.Files {
		if f.Path == newPath {
			return RenameResult{}, fmt.Errorf("rename %q -> %q: destination already exists", oldPath, newPath)
		}
	}

	state.Files[fileIdx].Path = newPath
	state.Files[fileIdx].MTime = time.Now().UTC()

	if _, err := c.commitState(state); err != nil {
		return RenameResult{}, fmt.Errorf("save metadata after rename: %w", err)
	}

	return RenameResult{OldPath: oldPath, NewPath: newPath}, nil
}

// OverwriteFile updates a byte range within an existing file.
//
// It identifies all extents that overlap with the given offset range,
// reads the extent data, modifies it with the new data, writes the modified
// extent, recomputes checksums, and updates parity.
func (c *Coordinator) OverwriteFile(logicalPath string, offset int64, data []byte) (OverwriteResult, error) {
	if c == nil {
		return OverwriteResult{}, fmt.Errorf("coordinator is nil")
	}
	if data == nil || len(data) == 0 {
		return OverwriteResult{}, ErrEmptyPayload
	}
	if offset < 0 {
		return OverwriteResult{}, ErrInvalidOffset
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return OverwriteResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return OverwriteResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return OverwriteResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var file *metadata.FileRecord
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			file = &state.Files[i]
			break
		}
	}
	if file == nil {
		return OverwriteResult{}, fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	var extents []metadata.Extent
	for _, ext := range state.Extents {
		if ext.FileID == file.FileID {
			extents = append(extents, ext)
		}
	}
	if len(extents) == 0 {
		return OverwriteResult{}, fmt.Errorf("file %s has no extents", logicalPath)
	}

	endOffset := offset + int64(len(data))
	if endOffset > file.SizeBytes {
		return OverwriteResult{}, fmt.Errorf("write extends beyond file size: offset %d + len %d > size %d", offset, len(data), file.SizeBytes)
	}

	modifiedExtents := findOverlappingExtents(extents, offset, endOffset)
	if len(modifiedExtents) == 0 {
		return OverwriteResult{}, fmt.Errorf("no extents overlap with offset %d", offset)
	}

	txID := generateTxID("tx-overwrite")
	fileCopy := *file
	oldGen := int64(len(state.Transactions))
	newGen := oldGen + 1
	baseRecord := Record{
		TxID:              txID,
		PoolName:          state.Pool.Name,
		Timestamp:         time.Now().UTC(),
		State:             StatePrepared,
		File:              &fileCopy,
		Extents:           modifiedExtents,
		OldGeneration:     oldGen,
		NewGeneration:     newGen,
		AffectedExtentIDs: extentIDs(modifiedExtents),
	}

	if _, err := c.journal.Append(baseRecord); err != nil {
		return OverwriteResult{}, fmt.Errorf("append prepared record: %w", err)
	}

	rootDir := filepath.Dir(c.metadataPath)

	for _, ext := range modifiedExtents {
		extentStart := ext.LogicalOffset
		extentEnd := ext.LogicalOffset + ext.Length

		overlapStart := maxInt(offset, extentStart)
		overlapEnd := minInt(endOffset, extentEnd)
		overlapLen := overlapEnd - overlapStart

		dataOffsetInExtent := overlapStart - extentStart
		dataOffsetInPayload := overlapStart - offset

		extentPath := filepath.Join(rootDir, ext.PhysicalLocator.RelativePath)
		extentData, err := os.ReadFile(extentPath)
		if err != nil {
			return OverwriteResult{}, fmt.Errorf("read extent %s: %w", ext.ExtentID, err)
		}

		copy(extentData[dataOffsetInExtent:dataOffsetInExtent+overlapLen], data[dataOffsetInPayload:dataOffsetInPayload+overlapLen])

		checksum := digestBytes(extentData)
		for i := range state.Extents {
			if state.Extents[i].ExtentID == ext.ExtentID {
				state.Extents[i].Checksum = checksum
				state.Extents[i].ChecksumAlg = ChecksumAlgorithm
				break
			}
		}
		for i := range baseRecord.Extents {
			if baseRecord.Extents[i].ExtentID == ext.ExtentID {
				baseRecord.Extents[i].Checksum = checksum
				baseRecord.Extents[i].ChecksumAlg = ChecksumAlgorithm
				break
			}
		}

		if err := replaceSyncFile(extentPath, extentData, 0o600); err != nil {
			return OverwriteResult{}, fmt.Errorf("write extent %s: %w", ext.ExtentID, err)
		}

		ext.Checksum = checksum
	}

	if _, err := c.journal.Append(withState(baseRecord, StateDataWritten)); err != nil {
		return OverwriteResult{}, fmt.Errorf("append data-written record: %w", err)
	}

	mergeParityGroups(&state, modifiedExtents)
	if err := writeParityFiles(rootDir, &state, modifiedExtents, 0); err != nil {
		return OverwriteResult{}, fmt.Errorf("write parity files: %w", err)
	}

	if _, err := c.journal.Append(withState(baseRecord, StateParityWritten)); err != nil {
		return OverwriteResult{}, fmt.Errorf("append parity-written record: %w", err)
	}

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              txID,
		State:             string(StateCommitted),
		StartedAt:         baseRecord.Timestamp,
		AffectedExtentIDs: extentIDs(modifiedExtents),
		OldGeneration:     oldGen,
		NewGeneration:     newGen,
	})

	file.MTime = time.Now().UTC()

	// Use split commit: saveStateSnapshot + journal.Append + publishCommittedState
	// to satisfy invariant I1 (metadata durable before committed record).
	if _, err := c.saveStateSnapshot(state); err != nil {
		return OverwriteResult{}, fmt.Errorf("save metadata: %w", err)
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return OverwriteResult{}, fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return OverwriteResult{
		TxID:    txID,
		Extents: modifiedExtents,
		Healed:  false,
	}, nil
}

func findOverlappingExtents(extents []metadata.Extent, start, end int64) []metadata.Extent {
	var result []metadata.Extent
	for _, ext := range extents {
		extStart := ext.LogicalOffset
		extEnd := ext.LogicalOffset + ext.Length
		if !(end <= extStart || start >= extEnd) {
			result = append(result, ext)
		}
	}
	return result
}

func extentIDs(extents []metadata.Extent) []string {
	ids := make([]string, len(extents))
	for i, ext := range extents {
		ids[i] = ext.ExtentID
	}
	return ids
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// TruncateFile reduces the size of an existing file.
func (c *Coordinator) TruncateFile(logicalPath string, newSize int64) (TruncateResult, error) {
	if c == nil {
		return TruncateResult{}, fmt.Errorf("coordinator is nil")
	}
	if newSize < 0 {
		return TruncateResult{}, ErrNegativeSize
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return TruncateResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return TruncateResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return TruncateResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var file *metadata.FileRecord
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			file = &state.Files[i]
			break
		}
	}
	if file == nil {
		return TruncateResult{}, fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	if newSize >= file.SizeBytes {
		return TruncateResult{TxID: "", RemovedExtents: 0, FreedBytes: 0}, nil
	}

	var fileExtents []metadata.Extent
	for _, ext := range state.Extents {
		if ext.FileID == file.FileID {
			fileExtents = append(fileExtents, ext)
		}
	}

	var toRemove []metadata.Extent
	var freedBytes int64
	for _, ext := range fileExtents {
		if ext.LogicalOffset+ext.Length > newSize {
			toRemove = append(toRemove, ext)
			freedBytes += ext.Length
		}
	}

	if len(toRemove) == 0 {
		return TruncateResult{TxID: "", RemovedExtents: 0, FreedBytes: 0}, nil
	}

	txID := generateTxID("tx-truncate")
	now := time.Now().UTC()

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              txID,
		State:             string(StateCommitted),
		StartedAt:         now,
		AffectedExtentIDs: extentIDs(toRemove),
	})

	file.SizeBytes = newSize
	file.MTime = now

	extentIDSet := make(map[string]bool)
	for _, ext := range toRemove {
		extentIDSet[ext.ExtentID] = true
	}

	var remainingExtents []metadata.Extent
	for _, ext := range state.Extents {
		if !extentIDSet[ext.ExtentID] {
			remainingExtents = append(remainingExtents, ext)
		}
	}
	state.Extents = remainingExtents

	for i, group := range state.ParityGroups {
		var newMembers []string
		for _, memberID := range group.MemberExtentIDs {
			if !extentIDSet[memberID] {
				newMembers = append(newMembers, memberID)
			}
		}
		state.ParityGroups[i].MemberExtentIDs = newMembers
	}

	for i, disk := range state.Disks {
		for _, ext := range toRemove {
			if ext.DataDiskID == disk.DiskID {
				state.Disks[i].FreeBytes += ext.Length
			}
		}
	}

	if _, err := c.saveStateSnapshot(state); err != nil {
		return TruncateResult{}, fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return TruncateResult{}, fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	rootDir := filepath.Dir(c.metadataPath)
	for _, ext := range toRemove {
		extentPath := filepath.Join(rootDir, ext.PhysicalLocator.RelativePath)
		if err := os.Remove(extentPath); err != nil && !os.IsNotExist(err) {
			return TruncateResult{}, fmt.Errorf("remove extent file: %w", err)
		}
	}

	return TruncateResult{
		TxID:           txID,
		RemovedExtents: len(toRemove),
		FreedBytes:     freedBytes,
	}, nil
}

// SaveFileData persists file data, either creating a new file or overwriting an existing one.
// This is used by the FUSE Flush operation to persist buffered write data.
func (c *Coordinator) SaveFileData(logicalPath string, data []byte) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	// First, delete the existing file if it exists
	c.mu.Lock()
	state, err := c.loadState(metadata.PrototypeState("demo"))
	c.mu.Unlock()
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	var fileExists bool
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			fileExists = true
			break
		}
	}

	if fileExists {
		// Delete existing file - this handles its own locking
		_, err := c.DeleteFile(logicalPath)
		if err != nil {
			return fmt.Errorf("delete existing file: %w", err)
		}
	}

	// Now use WriteFile to create the new file - it handles locking internally
	_, err = c.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    logicalPath,
		Payload:        data,
		AllowSynthetic: false,
	})
	return err
}

// DeleteFile removes a file and all its extents from the pool.
func (c *Coordinator) DeleteFile(logicalPath string) (DeleteResult, error) {
	if c == nil {
		return DeleteResult{}, fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return DeleteResult{}, fmt.Errorf("logical path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return DeleteResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return DeleteResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return DeleteResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var fileIndex int = -1
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			fileIndex = i
			break
		}
	}
	if fileIndex < 0 {
		return DeleteResult{}, fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	file := state.Files[fileIndex]
	var fileExtents []metadata.Extent
	for _, ext := range state.Extents {
		if ext.FileID == file.FileID {
			fileExtents = append(fileExtents, ext)
		}
	}

	if len(fileExtents) == 0 {
		txID := generateTxID("tx-delete")
		state.Files = append(state.Files[:fileIndex], state.Files[fileIndex+1:]...)
		if _, err := c.saveStateSnapshot(state); err != nil {
			return DeleteResult{}, fmt.Errorf("save metadata: %w", err)
		}
		baseRecord := Record{
			TxID:      txID,
			State:     StatePrepared,
			Timestamp: time.Now().UTC(),
		}
		if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
			return DeleteResult{}, fmt.Errorf("append committed record: %w", err)
		}
		c.publishCommittedState(state)
		return DeleteResult{TxID: txID, RemovedExtents: 0, FreedBytes: 0}, nil
	}

	txID := generateTxID("tx-delete")
	now := time.Now().UTC()

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              txID,
		State:             string(StateCommitted),
		StartedAt:         now,
		AffectedExtentIDs: extentIDs(fileExtents),
	})

	extentIDSet := make(map[string]bool)
	for _, ext := range fileExtents {
		extentIDSet[ext.ExtentID] = true
	}

	var remainingExtents []metadata.Extent
	for _, ext := range state.Extents {
		if !extentIDSet[ext.ExtentID] {
			remainingExtents = append(remainingExtents, ext)
		}
	}
	state.Extents = remainingExtents

	for i, group := range state.ParityGroups {
		var newMembers []string
		for _, memberID := range group.MemberExtentIDs {
			if !extentIDSet[memberID] {
				newMembers = append(newMembers, memberID)
			}
		}
		state.ParityGroups[i].MemberExtentIDs = newMembers
	}

	var freedBytes int64
	for i, disk := range state.Disks {
		for _, ext := range fileExtents {
			if ext.DataDiskID == disk.DiskID {
				state.Disks[i].FreeBytes += ext.Length
				freedBytes += ext.Length
			}
		}
	}

	state.Files = append(state.Files[:fileIndex], state.Files[fileIndex+1:]...)

	rootDir := filepath.Dir(c.metadataPath)
	for _, ext := range fileExtents {
		extentPath := filepath.Join(rootDir, ext.PhysicalLocator.RelativePath)
		if err := os.Remove(extentPath); err != nil && !os.IsNotExist(err) {
			return DeleteResult{}, fmt.Errorf("remove extent file: %w", err)
		}
	}

	if _, err := c.saveStateSnapshot(state); err != nil {
		return DeleteResult{}, fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return DeleteResult{}, fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return DeleteResult{
		TxID:           txID,
		RemovedExtents: len(fileExtents),
		FreedBytes:     freedBytes,
	}, nil
}

// GrowFile extends an existing file with new data appended at the end.
func (c *Coordinator) GrowFile(logicalPath string, appendData []byte) (GrowResult, error) {
	if c == nil {
		return GrowResult{}, fmt.Errorf("coordinator is nil")
	}
	if appendData == nil || len(appendData) == 0 {
		return GrowResult{}, ErrEmptyPayload
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return GrowResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return GrowResult{}, err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return GrowResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	var file *metadata.FileRecord
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			file = &state.Files[i]
			break
		}
	}
	if file == nil {
		return GrowResult{}, fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	oldSize := file.SizeBytes

	// Allocate extents manually for existing file
	extentSize := state.Pool.ExtentSizeBytes
	if extentSize <= 0 {
		extentSize = metadata.DefaultExtentSize
	}

	newExtents := allocateExtentsForExistingFile(file.FileID, oldSize, int64(len(appendData)), extentSize, &state)
	if len(newExtents) == 0 {
		return GrowResult{}, fmt.Errorf("no extents needed")
	}

	txID := generateTxID("tx-grow")
	now := time.Now().UTC()

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              txID,
		State:             string(StateCommitted),
		StartedAt:         now,
		AffectedExtentIDs: extentIDs(newExtents),
	})

	file.SizeBytes = oldSize + int64(len(appendData))
	file.MTime = now

	mergeParityGroups(&state, newExtents)

	rootDir := filepath.Dir(c.metadataPath)
	for i := range newExtents {
		data := clampedPayloadSlice(appendData, newExtents[i].LogicalOffset-oldSize, newExtents[i].Length)
		newExtents[i].Checksum = digestBytes(data)
		newExtents[i].ChecksumAlg = ChecksumAlgorithm
	}
	for i := range state.Extents {
		for j := range newExtents {
			if state.Extents[i].ExtentID == newExtents[j].ExtentID {
				state.Extents[i].Checksum = newExtents[j].Checksum
				state.Extents[i].ChecksumAlg = newExtents[j].ChecksumAlg
			}
		}
	}
	for _, ext := range newExtents {
		data := clampedPayloadSlice(appendData, ext.LogicalOffset-oldSize, ext.Length)
		path := filepath.Join(rootDir, ext.PhysicalLocator.RelativePath)
		if err := replaceSyncFile(path, data, 0o600); err != nil {
			return GrowResult{}, fmt.Errorf("write extent %s: %w", ext.ExtentID, err)
		}
	}

	if err := writeParityFiles(rootDir, &state, newExtents, 0); err != nil {
		return GrowResult{}, fmt.Errorf("write parity: %w", err)
	}

	if _, err := c.saveStateSnapshot(state); err != nil {
		return GrowResult{}, fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return GrowResult{}, fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return GrowResult{
		TxID:    txID,
		Extents: newExtents,
	}, nil
}

func allocateExtentsForExistingFile(fileID string, startOffset, appendSize int64, extentSize int64, state *metadata.SampleState) []metadata.Extent {
	if appendSize <= 0 {
		return nil
	}

	var extents []metadata.Extent
	remaining := appendSize
	offset := startOffset

	for remaining > 0 {
		allocLen := extentSize
		if remaining < allocLen {
			allocLen = remaining
		}

		extentNumber := len(state.Extents) + len(extents) + 1
		groupWidth := int64(3)
		parityGroupID := fmt.Sprintf("pg-%06d", ((int64(extentNumber)-1)/groupWidth)+1)

		dataDiskIdx := extentNumber % len(state.Disks)
		if dataDiskIdx < 0 {
			dataDiskIdx = -dataDiskIdx
		}

		extent := metadata.Extent{
			ExtentID:      fmt.Sprintf("extent-%06d", extentNumber),
			FileID:        fileID,
			LogicalOffset: offset,
			Length:        allocLen,
			DataDiskID:    state.Disks[dataDiskIdx].DiskID,
			PhysicalLocator: metadata.Locator{
				RelativePath: fmt.Sprintf("data/%02x/%02x/extent-%06d.bin", (extentNumber/256)%256, extentNumber%256, extentNumber),
				OffsetBytes:  0,
				LengthBytes:  allocLen,
			},
			Checksum:      "",
			ChecksumAlg:   ChecksumAlgorithm,
			Generation:    1,
			ParityGroupID: parityGroupID,
			State:         metadata.ExtentStateAllocated,
		}

		extents = append(extents, extent)
		state.Disks[dataDiskIdx].FreeBytes -= allocLen

		offset += allocLen
		remaining -= allocLen
	}

	state.Extents = append(state.Extents, extents...)
	return extents
}

func generateTxID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// CreateDirectory creates a new directory at the specified path.
func (c *Coordinator) CreateDirectory(logicalPath string, mode uint32) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	for _, f := range state.Files {
		if f.Path == logicalPath {
			return fmt.Errorf("file already exists: %s", logicalPath)
		}
	}

	now := time.Now().UTC()
	fileID := fmt.Sprintf("dir-%06d", len(state.Files)+1)
	file := metadata.FileRecord{
		FileID:    fileID,
		Path:      logicalPath,
		SizeBytes: 0,
		MTime:     now,
		CTime:     now,
		Policy:    "default",
		State:     metadata.FileStateAllocated,
		FileType:  metadata.FileTypeDirectory,
		Mode:      mode & 0o7777,
	}

	state.Files = append(state.Files, file)

	txID := generateTxID("tx-mkdir")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// DeleteDirectory removes an empty directory.
func (c *Coordinator) DeleteDirectory(logicalPath string) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	var fileIndex int = -1
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			fileIndex = i
			break
		}
	}
	if fileIndex < 0 {
		return fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	file := state.Files[fileIndex]
	if file.FileType != metadata.FileTypeDirectory {
		return fmt.Errorf("not a directory: %s", logicalPath)
	}

	dirPrefix := logicalPath + "/"
	for _, f := range state.Files {
		if strings.HasPrefix(f.Path, dirPrefix) {
			return fmt.Errorf("directory not empty: %s", logicalPath)
		}
	}

	now := time.Now().UTC()
	txID := generateTxID("tx-rmdir")
	state.Files = append(state.Files[:fileIndex], state.Files[fileIndex+1:]...)

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// CreateSymlink creates a symbolic link at logicalPath pointing to target.
func (c *Coordinator) CreateSymlink(logicalPath string, target string, mode uint32) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}
	if target == "" {
		return fmt.Errorf("target is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	for _, f := range state.Files {
		if f.Path == logicalPath {
			return fmt.Errorf("file already exists: %s", logicalPath)
		}
	}

	now := time.Now().UTC()
	fileID := fmt.Sprintf("sym-%06d", len(state.Files)+1)
	file := metadata.FileRecord{
		FileID:     fileID,
		Path:       logicalPath,
		SizeBytes:  int64(len(target)),
		MTime:      now,
		CTime:      now,
		Policy:     "default",
		State:      metadata.FileStateAllocated,
		FileType:   metadata.FileTypeSymlink,
		Mode:       mode & 0o7777,
		LinkTarget: target,
	}

	state.Files = append(state.Files, file)

	txID := generateTxID("tx-symlink")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// CreateHardLink creates a hard link at newPath pointing to existing file at oldPath.
func (c *Coordinator) CreateHardLink(oldPath string, newPath string) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if oldPath == "" || newPath == "" {
		return fmt.Errorf("old path and new path are required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	var sourceFile *metadata.FileRecord
	for i := range state.Files {
		if state.Files[i].Path == oldPath {
			sourceFile = &state.Files[i]
			break
		}
	}
	if sourceFile == nil {
		return fmt.Errorf("%w: %s", ErrFileNotFound, oldPath)
	}

	if sourceFile.FileType == metadata.FileTypeDirectory {
		return fmt.Errorf("cannot hard link directories")
	}

	for _, f := range state.Files {
		if f.Path == newPath {
			return fmt.Errorf("file already exists: %s", newPath)
		}
	}

	now := time.Now().UTC()
	link := *sourceFile
	link.Path = newPath
	link.CTime = now
	link.MTime = now

	state.Files = append(state.Files, link)

	txID := generateTxID("tx-link")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// SetFileMode updates the mode of an existing file.
func (c *Coordinator) SetFileMode(logicalPath string, mode uint32) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	var fileIndex int = -1
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			fileIndex = i
			break
		}
	}
	if fileIndex < 0 {
		return fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	now := time.Now().UTC()
	state.Files[fileIndex].Mode = mode & 0o7777
	state.Files[fileIndex].MTime = now

	txID := generateTxID("tx-chmod")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// SetFileTimes updates the access and modification times of an existing file.
func (c *Coordinator) SetFileTimes(logicalPath string, atime time.Time, mtime time.Time) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	var fileIndex int = -1
	for i := range state.Files {
		if state.Files[i].Path == logicalPath {
			fileIndex = i
			break
		}
	}
	if fileIndex < 0 {
		return fmt.Errorf("%w: %s", ErrFileNotFound, logicalPath)
	}

	now := time.Now().UTC()
	state.Files[fileIndex].MTime = mtime
	state.Files[fileIndex].CTime = now

	txID := generateTxID("tx-utimens")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}

// CreateSpecialFile creates a special file (FIFO, socket, block device, char device).
func (c *Coordinator) CreateSpecialFile(logicalPath string, mode uint32, devMajor uint32, devMinor uint32) error {
	if c == nil {
		return fmt.Errorf("coordinator is nil")
	}
	if logicalPath == "" {
		return fmt.Errorf("path is required")
	}

	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureRecoveredLocked(""); err != nil {
		return err
	}

	state, err := c.loadState(metadata.SampleState{})
	if err != nil {
		return fmt.Errorf("load metadata state: %w", err)
	}

	for _, f := range state.Files {
		if f.Path == logicalPath {
			return fmt.Errorf("file already exists: %s", logicalPath)
		}
	}

	fileType := metadata.FileTypeFromMode(mode)
	now := time.Now().UTC()
	fileID := fmt.Sprintf("dev-%06d", len(state.Files)+1)
	file := metadata.FileRecord{
		FileID:    fileID,
		Path:      logicalPath,
		SizeBytes: 0,
		MTime:     now,
		CTime:     now,
		Policy:    "default",
		State:     metadata.FileStateAllocated,
		FileType:  fileType,
		Mode:      mode & 0o7777,
		DevMajor:  devMajor,
		DevMinor:  devMinor,
	}

	switch fileType {
	case metadata.FileTypeFIFO:
		file.Mode = mode | syscall.S_IFIFO
	case metadata.FileTypeSocket:
		file.Mode = mode | syscall.S_IFSOCK
	case metadata.FileTypeBlockDevice:
		file.Mode = mode | syscall.S_IFBLK
	case metadata.FileTypeCharDevice:
		file.Mode = mode | syscall.S_IFCHR
	default:
		return fmt.Errorf("not a special file type: %v", fileType)
	}

	state.Files = append(state.Files, file)

	txID := generateTxID("tx-mknod")
	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:      txID,
		State:     string(StateCommitted),
		StartedAt: now,
	})

	if _, err := c.saveStateSnapshot(state); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	baseRecord := Record{
		TxID:      txID,
		State:     StatePrepared,
		Timestamp: now,
	}
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return fmt.Errorf("append committed record: %w", err)
	}
	c.publishCommittedState(state)

	return nil
}
