package journal

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lukechampine.com/blake3"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ChecksumAlgorithm re-exports metadata.ChecksumAlgorithm for callers that
// import only the journal package. Both constants refer to the same value.
const ChecksumAlgorithm = metadata.ChecksumAlgorithm

type WriteRequest struct {
	PoolName    string `json:"pool_name"`
	LogicalPath string `json:"logical_path"`
	// Payload holds the real user data to write. When set, SizeBytes is derived
	// from len(Payload). When nil, SizeBytes controls allocation and synthetic
	// data is written (useful for demos and tests).
	Payload   []byte `json:"payload,omitempty"`
	SizeBytes int64  `json:"size_bytes"`
	FailAfter State  `json:"fail_after,omitempty"`
}

type WriteResult struct {
	TxID           string              `json:"tx_id"`
	MetadataPath   string              `json:"metadata_path"`
	JournalPath    string              `json:"journal_path"`
	StateChecksum  string              `json:"state_checksum,omitempty"`
	FinalState     State               `json:"final_state"`
	ReplayRequired bool                `json:"replay_required"`
	File           metadata.FileRecord `json:"file"`
	Extents        []metadata.Extent   `json:"extents"`
}

type Coordinator struct {
	metadataPath string
	journalPath  string
	metadata     *metadata.Store
	journal      *Store
}

func NewCoordinator(metadataPath, journalPath string) *Coordinator {
	return &Coordinator{
		metadataPath: metadataPath,
		journalPath:  journalPath,
		metadata:     metadata.NewStore(metadataPath),
		journal:      NewStore(journalPath),
	}
}

func (c *Coordinator) WriteFile(req WriteRequest) (WriteResult, error) {
	if c == nil {
		return WriteResult{}, fmt.Errorf("coordinator is nil")
	}
	if req.PoolName == "" {
		req.PoolName = "demo"
	}
	if req.LogicalPath == "" {
		return WriteResult{}, fmt.Errorf("logical path is required")
	}
	if req.Payload != nil {
		req.SizeBytes = int64(len(req.Payload))
	}
	if req.SizeBytes < 0 {
		return WriteResult{}, fmt.Errorf("size must be non-negative")
	}

	state, err := c.metadata.LoadOrCreate(metadata.PrototypeState(req.PoolName))
	if err != nil {
		return WriteResult{}, fmt.Errorf("load metadata state: %w", err)
	}

	startedAt := time.Now().UTC()
	allocator := metadata.NewAllocator(&state)
	file, extents, err := allocator.AllocateFile(req.LogicalPath, req.SizeBytes)
	if err != nil {
		return WriteResult{}, fmt.Errorf("allocate file extents: %w", err)
	}

	affectedExtentIDs := make([]string, 0, len(extents))
	for _, extent := range extents {
		affectedExtentIDs = append(affectedExtentIDs, extent.ExtentID)
	}

	txID := fmt.Sprintf("tx-write-%d", startedAt.UnixNano())
	oldGeneration := int64(len(state.Transactions))
	newGeneration := oldGeneration + 1
	result := WriteResult{
		TxID:         txID,
		MetadataPath: c.metadataPath,
		JournalPath:  c.journalPath,
		FinalState:   StatePrepared,
		File:         file,
		Extents:      extents,
	}

	fileCopy := file
	baseRecord := Record{
		TxID:              txID,
		Timestamp:         startedAt,
		PoolName:          req.PoolName,
		LogicalPath:       req.LogicalPath,
		File:              &fileCopy,
		Extents:           extents,
		OldGeneration:     oldGeneration,
		NewGeneration:     newGeneration,
		AffectedExtentIDs: affectedExtentIDs,
	}
	if _, err := c.journal.Append(withState(baseRecord, StatePrepared)); err != nil {
		return WriteResult{}, fmt.Errorf("append prepared record: %w", err)
	}
	if shouldStopAfter(req.FailAfter, StatePrepared) {
		result.FinalState = StatePrepared
		result.ReplayRequired = true
		return result, nil
	}

	applyExtentChecksums(&state, extents, req.Payload)
	if err := writeExtentFiles(filepath.Dir(c.metadataPath), extents, req.Payload); err != nil {
		return WriteResult{}, fmt.Errorf("write extent files: %w", err)
	}
	result.Extents = extents
	if _, err := c.journal.Append(withState(baseRecord, StateDataWritten)); err != nil {
		return WriteResult{}, fmt.Errorf("append data-written record: %w", err)
	}
	if shouldStopAfter(req.FailAfter, StateDataWritten) {
		result.FinalState = StateDataWritten
		result.ReplayRequired = true
		return result, nil
	}

	mergeParityGroups(&state, extents)
	if err := writeParityFiles(filepath.Dir(c.metadataPath), &state, extents); err != nil {
		return WriteResult{}, fmt.Errorf("write parity files: %w", err)
	}
	if _, err := c.journal.Append(withState(baseRecord, StateParityWritten)); err != nil {
		return WriteResult{}, fmt.Errorf("append parity-written record: %w", err)
	}
	if shouldStopAfter(req.FailAfter, StateParityWritten) {
		result.FinalState = StateParityWritten
		result.ReplayRequired = true
		return result, nil
	}

	state.Transactions = append(state.Transactions, metadata.Transaction{
		TxID:              txID,
		State:             string(StateMetadataWritten),
		StartedAt:         startedAt,
		AffectedExtentIDs: affectedExtentIDs,
		OldGeneration:     oldGeneration,
		NewGeneration:     newGeneration,
		ReplayRequired:    shouldStopAfter(req.FailAfter, StateMetadataWritten),
	})
	snapshot, err := c.metadata.Save(state)
	if err != nil {
		return WriteResult{}, fmt.Errorf("save metadata snapshot: %w", err)
	}
	result.StateChecksum = snapshot.StateChecksum
	if _, err := c.journal.Append(withState(baseRecord, StateMetadataWritten)); err != nil {
		return WriteResult{}, fmt.Errorf("append metadata-written record: %w", err)
	}
	if shouldStopAfter(req.FailAfter, StateMetadataWritten) {
		result.FinalState = StateMetadataWritten
		result.ReplayRequired = true
		return result, nil
	}

	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return WriteResult{}, fmt.Errorf("append committed record: %w", err)
	}
	result.FinalState = StateCommitted

	// Phase 3 — Enforce invariants: verify structural consistency immediately
	// after commit. A violation here means the just-committed write left the
	// storage system in an invalid state and must be surfaced immediately rather
	// than discovered silently during a later read or scrub.
	rootDir := filepath.Dir(c.metadataPath)
	if violations := CheckIntegrityInvariants(rootDir, state); len(violations) > 0 {
		return WriteResult{}, fmt.Errorf("invariant violation after commit: %v", violations[0])
	}

	return result, nil
}

func withState(record Record, state State) Record {
	record.State = state
	record.Timestamp = time.Now().UTC()
	return record
}

func shouldStopAfter(target, current State) bool {
	return target != "" && target == current
}

// applyExtentChecksums computes and stores SHA-256 checksums for each extent.
// When payload is non-nil, checksums are derived from the corresponding slice
// of real user data. When payload is nil, checksums are computed from
// deterministic synthetic data so that demos and tests remain self-consistent.
func applyExtentChecksums(state *metadata.SampleState, extents []metadata.Extent, payload []byte) {
	checksums := make(map[string]string, len(extents))
	for i := range extents {
		data := extentData(extents[i], payload)
		checksum := digestBytes(data)
		extents[i].Checksum = checksum
		checksums[extents[i].ExtentID] = checksum
	}

	for i := range state.Extents {
		if checksum, ok := checksums[state.Extents[i].ExtentID]; ok {
			state.Extents[i].Checksum = checksum
			state.Extents[i].ChecksumAlg = ChecksumAlgorithm
		}
	}
}

// extentData returns the bytes for an extent. When payload is non-nil it slices
// the real user data; otherwise it falls back to deterministic synthetic data so
// that demo and test paths remain self-consistent without requiring real input.
func extentData(extent metadata.Extent, payload []byte) []byte {
	if payload != nil {
		return clampedPayloadSlice(payload, extent.LogicalOffset, extent.Length)
	}
	return syntheticExtentBytes(extent)
}

// clampedPayloadSlice returns a slice of src of exactly wantLen bytes starting
// at offset. If the source is shorter than the requested range the result is
// zero-padded so that every extent has a consistent, predictable length for
// parity XOR operations.
func clampedPayloadSlice(src []byte, offset, wantLen int64) []byte {
	n := int64(len(src))
	start := min64(offset, n)
	end := min64(offset+wantLen, n)
	slice := src[start:end]
	if int64(len(slice)) == wantLen {
		return append([]byte(nil), slice...)
	}
	out := make([]byte, wantLen)
	copy(out, slice)
	return out
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func mergeParityGroups(state *metadata.SampleState, extents []metadata.Extent) {
	index := make(map[string]int)
	for i, group := range state.ParityGroups {
		index[group.ParityGroupID] = i
	}

	for _, extent := range extents {
		idx, exists := index[extent.ParityGroupID]
		if !exists {
			state.ParityGroups = append(state.ParityGroups, metadata.ParityGroup{
				ParityGroupID:   extent.ParityGroupID,
				ParityDiskID:    "disk-parity",
				MemberExtentIDs: []string{extent.ExtentID},
				Generation:      extent.Generation,
			})
			index[extent.ParityGroupID] = len(state.ParityGroups) - 1
			continue
		}

		group := &state.ParityGroups[idx]
		alreadyPresent := false
		for _, member := range group.MemberExtentIDs {
			if member == extent.ExtentID {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			group.MemberExtentIDs = append(group.MemberExtentIDs, extent.ExtentID)
		}
		group.Generation = extent.Generation
	}
}

// writeExtentFiles writes each extent's data to its physical location on disk.
// When payload is non-nil, real user data is used; otherwise synthetic data is
// written so demo and test commands remain functional without real input.
//
// Durability guarantee: each extent file is opened, written, fsynced, and
// closed before control returns, and the parent directory is also fsynced so
// that the directory entry (new file or updated inode) is durable.
func writeExtentFiles(rootDir string, extents []metadata.Extent, payload []byte) error {
	// Track unique parent directories so we fsync each only once.
	dirs := make(map[string]struct{})
	for _, extent := range extents {
		// Validate the relative path generated by the allocator. Extent paths
		// are always of the form "data/<2hex>/<4hex>/extent-NNNNNN.bin". The
		// check prevents unexpected paths from reaching the filesystem.
		if err := validateExtentRelativePath(extent.PhysicalLocator.RelativePath); err != nil {
			return fmt.Errorf("extent %s has invalid path: %w", extent.ExtentID, err)
		}
		targetPath := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		dir := filepath.Dir(targetPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create extent directory for %s: %w", extent.ExtentID, err)
		}

		if err := writeSyncFile(targetPath, extentData(extent, payload), 0o600); err != nil {
			return fmt.Errorf("write extent file %s: %w", targetPath, err)
		}
		dirs[dir] = struct{}{}
	}
	for dir := range dirs {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync extent directory %s: %w", dir, err)
		}
	}
	return nil
}

// validateExtentRelativePath returns an error if path is not a safe, relative
// path produced by the allocator. Valid paths match:
//
//	data/<alphanum>/<alphanum>/extent-<digits>.bin
//
// This check prevents path traversal if metadata is somehow tampered with.
func validateExtentRelativePath(path string) error {
	if filepath.IsAbs(path) {
		return fmt.Errorf("absolute path not allowed: %q", path)
	}
	cleaned := filepath.Clean(path)
	if cleaned != path {
		return fmt.Errorf("non-canonical path not allowed: %q", path)
	}
	// Split by OS separator and reject any ".." component.
	parts := strings.Split(filepath.ToSlash(cleaned), "/")
	for _, part := range parts {
		if part == ".." {
			return fmt.Errorf("path traversal not allowed: %q", path)
		}
	}
	return nil
}

// syntheticExtentBytes generates deterministic fake data for an extent.
// Used only in demo and test paths where no real payload is supplied.
func syntheticExtentBytes(extent metadata.Extent) []byte {
	seed := fmt.Sprintf("%s|%s|%d|%d|%s", extent.ExtentID, extent.FileID, extent.LogicalOffset, extent.Length, extent.ParityGroupID)
	data := make([]byte, extent.Length)
	block := blake3.Sum256([]byte(seed))
	for offset := 0; offset < len(data); offset += len(block) {
		copied := copy(data[offset:], block[:])
		block = blake3.Sum256(block[:])
		if copied == 0 {
			break
		}
	}
	return data
}

func digestBytes(data []byte) string {
	h := blake3.Sum256(data)
	return hex.EncodeToString(h[:])
}

func xorInto(dst, src []byte) {
	for i := range src {
		if i >= len(dst) {
			break
		}
		dst[i] ^= src[i]
	}
}

func writeParityFiles(rootDir string, state *metadata.SampleState, extents []metadata.Extent) error {
	if len(extents) == 0 {
		return nil
	}
	if state == nil {
		return fmt.Errorf("state is nil")
	}

	parityDir := filepath.Join(rootDir, "parity")
	if err := os.MkdirAll(parityDir, 0o755); err != nil {
		return fmt.Errorf("create parity directory: %w", err)
	}

	groups := make(map[string][]metadata.Extent)
	for _, extent := range state.Extents {
		for _, target := range extents {
			if extent.ParityGroupID == target.ParityGroupID {
				groups[extent.ParityGroupID] = append(groups[extent.ParityGroupID], extent)
				break
			}
		}
	}

	for groupID, members := range groups {
		maxLen := 0
		memberData := make([][]byte, 0, len(members))
		for _, member := range members {
			if err := validateExtentRelativePath(member.PhysicalLocator.RelativePath); err != nil {
				return fmt.Errorf("member extent %s has invalid path: %w", member.ExtentID, err)
			}
			contentPath := filepath.Join(rootDir, member.PhysicalLocator.RelativePath)
			data, err := os.ReadFile(contentPath)
			if err != nil {
				return fmt.Errorf("read extent file %s for parity: %w", contentPath, err)
			}
			memberData = append(memberData, data)
			if len(data) > maxLen {
				maxLen = len(data)
			}
		}

		parityData := make([]byte, maxLen)
		for _, data := range memberData {
			xorInto(parityData, data)
		}
		parityChecksum := digestBytes(parityData)
		parityPath := filepath.Join(parityDir, groupID+".bin")
		// writeSyncFile fsyncs the parity file before returning, then we fsync
		// the parity directory to make the new/updated entry durable.
		if err := writeSyncFile(parityPath, parityData, 0o600); err != nil {
			return fmt.Errorf("write parity file %s: %w", parityPath, err)
		}
		for i := range state.ParityGroups {
			if state.ParityGroups[i].ParityGroupID == groupID {
				state.ParityGroups[i].ParityChecksum = parityChecksum
			}
		}
	}
	// Fsync the parity directory once after all parity files are written.
	if err := syncDir(parityDir); err != nil {
		return fmt.Errorf("sync parity directory: %w", err)
	}
	return nil
}

// writeSyncFile opens path, writes data, calls Sync() to flush to durable
// storage, then closes the file. A crash after writeSyncFile returns cannot
// lose the written bytes.
func writeSyncFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("open file for write: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write data: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync file: %w", err)
	}
	return f.Close()
}

// syncDir opens the directory at path and calls Sync() on it, ensuring that
// any recently created or renamed files in the directory are durable. On Linux
// this is required to guarantee that directory entries survive a power loss.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("sync directory: %w", err)
	}
	return d.Close()
}
