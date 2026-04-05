package journal

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	// Payload holds the real user data to write. In production paths, writes
	// must provide real payload bytes. Synthetic extent generation is permitted
	// only when AllowSynthetic is explicitly set for demos/tests.
	Payload []byte `json:"payload,omitempty"`
	// AllowSynthetic explicitly opts into writing deterministic synthetic bytes
	// when Payload is nil. This exists only for tests, demos, and simulation
	// tooling. Production callers must provide Payload.
	AllowSynthetic bool  `json:"allow_synthetic,omitempty"`
	SizeBytes      int64 `json:"size_bytes"`
	FailAfter      State `json:"fail_after,omitempty"`
	// CompressionAlg specifies the compression algorithm to use for extent data.
	// If empty, no compression is applied. Supported values: "zstd", "lz4", "snappy".
	CompressionAlg string `json:"compression_alg,omitempty"`

	// Test-only fault injection. Zero values disable the fault.
	// extentWriteLimit: crash WriteFile after writing this many extent files
	// (before appending StateDataWritten). Remaining extents are not written.
	// parityWriteLimit: crash WriteFile after writing this many parity group
	// files (before appending StateParityWritten).
	extentWriteLimit int
	parityWriteLimit int
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
	mu           sync.Mutex
	metadataPath string
	journalPath  string
	metadata     *metadata.Store
	journal      *Store
	// writeScheduler is used for parallel extent writes. Initialized lazily.
	// Protected by schedulerMu, separate from mu to avoid deadlocks.
	writeScheduler *DiskScheduler
	schedulerMu    sync.Mutex
	// ioScheduler handles background I/O scheduling for reads/writes.
	// Initialized lazily.
	ioScheduler *IOScheduler
	readCache   *ReadAheadCache
	// cachedState holds the last successfully committed metadata state.
	// It is populated on first use and kept up-to-date by every mutating
	// operation (WriteFile, Recover, Scrub, Rebuild). Under the mu lock,
	// reads use it directly without hitting the disk.
	cachedState    *metadata.SampleState
	cachedStateSet bool
}

func NewCoordinator(metadataPath, journalPath string) *Coordinator {
	return &Coordinator{
		metadataPath: metadataPath,
		journalPath:  journalPath,
		metadata:     metadata.NewStore(metadataPath),
		journal:      NewStore(journalPath),
	}
}

func (c *Coordinator) getWriteScheduler() *DiskScheduler {
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	if c.writeScheduler == nil {
		c.writeScheduler = NewDiskScheduler(4, 2)
	}
	return c.writeScheduler
}

func (c *Coordinator) getIOScheduler() *IOScheduler {
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	if c.ioScheduler == nil {
		c.ioScheduler = NewIOScheduler(4)
		c.ioScheduler.Start()
	}
	return c.ioScheduler
}

func (c *Coordinator) getReadCache() *ReadAheadCache {
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	if c.readCache == nil {
		c.readCache = NewReadAheadCache(16*1024*1024, 4*1024*1024)
	}
	return c.readCache
}

// loadState returns the current metadata state. It uses the in-memory cache
// when available, falling back to disk only on first use or after invalidation.
// Must be called with c.mu held.
func (c *Coordinator) loadState(defaultState metadata.SampleState) (metadata.SampleState, error) {
	if c.cachedStateSet {
		return *c.cachedState, nil
	}
	state, err := c.metadata.LoadOrCreate(defaultState)
	if err != nil {
		return metadata.SampleState{}, err
	}
	c.cachedState = &state
	c.cachedStateSet = true
	return state, nil
}

// recoveryDefaultStateLocked chooses a safe prototype state to use as the
// fallback input to recoverWithStateLocked when replay is required before a
// mutating or read operation proceeds. Must be called with c.mu held.
func (c *Coordinator) recoveryDefaultStateLocked(fallbackPoolName string) metadata.SampleState {
	if c != nil && c.cachedStateSet && c.cachedState != nil && c.cachedState.Pool.Name != "" {
		return metadata.PrototypeState(c.cachedState.Pool.Name)
	}
	if c != nil && c.metadata != nil {
		if state, err := c.metadata.Load(); err == nil && state.Pool.Name != "" {
			return metadata.PrototypeState(state.Pool.Name)
		}
	}
	if fallbackPoolName != "" {
		return metadata.PrototypeState(fallbackPoolName)
	}
	return metadata.PrototypeState("demo")
}

// ensureRecoveredLocked forces journal replay before the caller performs any
// mutation based on the currently loaded metadata snapshot. Without this
// guard, metadata-only mutators like RenameFile/AddDisk/FailDisk could commit
// fresh metadata derived from a stale snapshot while replayable transactions
// still exist in the journal.
func (c *Coordinator) ensureRecoveredLocked(fallbackPoolName string) error {
	summary, err := c.journal.Replay()
	if err != nil {
		return fmt.Errorf("pre-operation journal replay check: %w", err)
	}
	if summary.RequiresReplay {
		if _, err := c.recoverWithStateLocked(c.recoveryDefaultStateLocked(fallbackPoolName)); err != nil {
			return fmt.Errorf("pre-operation recovery: %w", err)
		}
	}
	return nil
}

// saveStateSnapshot persists state to disk without updating the in-memory cache.
// This allows metadata to be durable BEFORE the committed journal record is appended,
// which is required for invariant I1.
//
// Must be called with c.mu held.
func (c *Coordinator) saveStateSnapshot(state metadata.SampleState) (metadata.SnapshotEnvelope, error) {
	if vs := checkPreCommitInvariants(state); len(vs) > 0 {
		return metadata.SnapshotEnvelope{}, fmt.Errorf(
			"I5: refusing to save state with %d invariant violation(s): %v",
			len(vs), vs[0],
		)
	}
	newGen := int64(len(state.Transactions))
	if c.cachedStateSet {
		cachedGen := int64(len(c.cachedState.Transactions))
		if newGen < cachedGen {
			return metadata.SnapshotEnvelope{}, fmt.Errorf(
				"I11: refusing non-monotonic generation: cached=%d new=%d",
				cachedGen, newGen,
			)
		}
	}
	return c.metadata.Save(state)
}

// publishCommittedState updates the in-memory cache with the committed state.
// This must be called AFTER the committed journal record is durable.
// Before calling this, the caller must ensure:
//   - metadata snapshot is durable (saveStateSnapshot)
//   - committed journal record is durable (journal.Append with StateCommitted)
//
// Must be called with c.mu held.
func (c *Coordinator) publishCommittedState(state metadata.SampleState) {
	c.cachedState = &state
	c.cachedStateSet = true
	_ = c.journal.CompactIfClean()
}

// commitState saves state to disk and updates the in-memory cache.
// Must be called with c.mu held.
// commitState persists state to metadata and updates the in-memory cache.
// It enforces the following invariants before writing:
//
//   - I5 (Metadata Truthfulness): CheckStateInvariants must pass.
//   - I11 (Monotonic Generation): state.Pool.Generation must be strictly
//     greater than the last committed generation.
//
// Violations of either invariant cause commitState to return an error without
// modifying the on-disk metadata or the cache.
func (c *Coordinator) commitState(state metadata.SampleState) (metadata.SnapshotEnvelope, error) {
	// I5: enforce metadata structural invariants before persisting.
	// J3 (replay_required check) is deliberately excluded here because an
	// in-flight transaction at StateMetadataWritten legitimately sets
	// ReplayRequired=true as a crash-recovery breadcrumb. J3 is a
	// post-recovery invariant enforced by Recover(), not by the write path.
	if vs := checkPreCommitInvariants(state); len(vs) > 0 {
		return metadata.SnapshotEnvelope{}, fmt.Errorf(
			"I5: refusing to commit state with %d invariant violation(s): %v",
			len(vs), vs[0])
	}

	// I11: enforce monotonic generation.
	// Generation is derived from the transaction count — each committed
	// transaction increments NewGeneration by 1, so len(Transactions)
	// represents the current committed generation.
	newGen := int64(len(state.Transactions))
	if c.cachedStateSet {
		cachedGen := int64(len(c.cachedState.Transactions))
		if newGen < cachedGen {
			return metadata.SnapshotEnvelope{}, fmt.Errorf(
				"I11: refusing non-monotonic generation: cached=%d new=%d",
				cachedGen, newGen)
		}
	}

	env, err := c.metadata.Save(state)
	if err != nil {
		return metadata.SnapshotEnvelope{}, err
	}
	c.cachedState = &state
	c.cachedStateSet = true

	return env, nil
}

// invalidateCache forces the next operation to reload state from disk.
// Must be called with c.mu held.
func (c *Coordinator) invalidateCache() {
	c.cachedStateSet = false
	c.cachedState = nil
}

func (c *Coordinator) WriteFile(req WriteRequest) (WriteResult, error) {
	if c == nil {
		return WriteResult{}, fmt.Errorf("coordinator is nil")
	}
	lock, err := c.acquireExclusiveOperationLock()
	if err != nil {
		return WriteResult{}, err
	}
	defer lock.release()
	c.mu.Lock()
	defer c.mu.Unlock()
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
	if req.Payload == nil && !req.AllowSynthetic {
		return WriteResult{}, fmt.Errorf(
			"payload is required for production writes; synthetic data requires explicit AllowSynthetic opt-in")
	}

	// Validate compression algorithm
	compAlg := CompressionAlg(req.CompressionAlg)
	if req.CompressionAlg != "" && compAlg != CompressionZstd && compAlg != CompressionLZ4 && compAlg != CompressionSnappy && compAlg != CompressionGZip && compAlg != CompressionXZ {
		return WriteResult{}, fmt.Errorf("unsupported compression algorithm: %s", req.CompressionAlg)
	}

	// A new allocation must never proceed while the journal still contains an
	// interrupted transaction. Otherwise we could allocate extents against stale
	// metadata and collide with extents that recovery has not yet merged.
	if err := c.ensureRecoveredLocked(req.PoolName); err != nil {
		return WriteResult{}, err
	}

	state, err := c.loadState(metadata.PrototypeState(req.PoolName))
	if err != nil {
		return WriteResult{}, fmt.Errorf("load metadata state: %w", err)
	}
	for _, existing := range state.Files {
		if existing.Path == req.LogicalPath {
			return WriteResult{}, fmt.Errorf("file already exists: %s", req.LogicalPath)
		}
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

	// Apply compression if requested and update extent metadata
	if compAlg != CompressionNone {
		for i := range extents {
			originalData := extentData(extents[i], req.Payload)
			compressedData, err := compress(originalData, compAlg)
			if err != nil {
				return WriteResult{}, fmt.Errorf("compress extent %s: %w", extents[i].ExtentID, err)
			}
			extents[i].CompressionAlg = metadata.CompressionAlgorithm(compAlg)
			extents[i].CompressedSize = int64(len(compressedData))
			extents[i].Checksum = digestBytes(compressedData)
			extents[i].ChecksumAlg = ChecksumAlgorithm
		}
		for i := range state.Extents {
			for j := range extents {
				if state.Extents[i].ExtentID == extents[j].ExtentID {
					state.Extents[i].CompressionAlg = extents[j].CompressionAlg
					state.Extents[i].CompressedSize = extents[j].CompressedSize
					state.Extents[i].Checksum = extents[j].Checksum
					state.Extents[i].ChecksumAlg = extents[j].ChecksumAlg
				}
			}
		}
	}

	// For uncompressed extents, compute checksums on raw data
	// For compressed extents, checksums already computed on compressed data above
	if compAlg == CompressionNone {
		applyExtentChecksums(&state, extents, req.Payload)
	}

	if err := writeExtentFilesWithCompression(filepath.Dir(c.metadataPath), extents, req.Payload, compAlg, req.extentWriteLimit); err != nil {
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

	if err := ensureHealthyCommittedParityInputs(c.metadataPath, c.journal, state, extents); err != nil {
		return WriteResult{}, fmt.Errorf("verify committed parity inputs before parity rewrite: %w", err)
	}
	mergeParityGroups(&state, extents)
	if err := writeParityFiles(filepath.Dir(c.metadataPath), &state, extents, req.parityWriteLimit); err != nil {
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
	if _, err := c.journal.Append(withState(baseRecord, StateMetadataWritten)); err != nil {
		return WriteResult{}, fmt.Errorf("append metadata-written record: %w", err)
	}
	if shouldStopAfter(req.FailAfter, StateMetadataWritten) {
		// For crash recovery: save state snapshot before returning to ensure
		// recovery can see the partial transaction state if needed.
		if _, err := c.commitState(state); err != nil {
			return WriteResult{}, fmt.Errorf("save metadata snapshot on stop: %w", err)
		}
		result.FinalState = StateMetadataWritten
		result.ReplayRequired = true
		return result, nil
	}

	committedAt := time.Now().UTC()

	// 1. Mark transaction committed in the state that will be snapshotted.
	upsertTransaction(&state, baseRecord, StateCommitted, false, &committedAt)

	// 2. Persist metadata first, but do NOT publish cachedState yet.
	// This ensures metadata is durable BEFORE the committed record is appended,
	// satisfying invariant I1.
	if _, err := c.saveStateSnapshot(state); err != nil {
		return WriteResult{}, fmt.Errorf("save committed metadata snapshot: %w", err)
	}

	// 3. Only after durable metadata exists, append the committed journal record.
	if _, err := c.journal.Append(withState(baseRecord, StateCommitted)); err != nil {
		return WriteResult{}, fmt.Errorf("append committed record: %w", err)
	}

	// 4. Now the transaction is truly visible - publish cached state.
	// This must happen AFTER both metadata and committed record are durable.
	c.publishCommittedState(state)

	result.FinalState = StateCommitted

	// Phase 3 — Enforce invariants for the data touched by this write. The write
	// path must prove that its own extents/parity are durable and self-consistent,
	// but it must not fail because of unrelated preexisting corruption elsewhere
	// in the pool; that broader health signal belongs to scrub/startup admission.
	rootDir := filepath.Dir(c.metadataPath)
	if violations := CheckTargetedWriteIntegrity(rootDir, state, extents); len(violations) > 0 {
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

// applyExtentChecksums computes and stores BLAKE3 checksums for each extent.
// When payload is non-nil, checksums are derived from the corresponding slice
// of real user data. When payload is nil, checksums are computed from
// deterministic synthetic data so that explicitly opted-in demos and tests
// remain self-consistent.
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
// that explicitly opted-in demo and test paths remain self-consistent without
// requiring real input.
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

func ensureHealthyCommittedParityInputs(metadataPath string, journal *Store, state metadata.SampleState, newExtents []metadata.Extent) error {
	newExtentSet := make(map[string]bool, len(newExtents))
	touchedGroups := make(map[string]bool, len(newExtents))
	for _, extent := range newExtents {
		newExtentSet[extent.ExtentID] = true
		touchedGroups[extent.ParityGroupID] = true
	}
	for _, extent := range state.Extents {
		if !touchedGroups[extent.ParityGroupID] || newExtentSet[extent.ExtentID] {
			continue
		}
		if _, repaired, err := verifyExtent(metadataPath, journal, state, extent, true, ""); err != nil {
			return err
		} else if repaired {
			// The committed member was repaired before parity recomputation, so the
			// ensuing parity file will be derived from authoritative bytes rather
			// than folding latent corruption into the new parity image.
		}
	}
	return nil
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
//
// I3 (Post-write readback): after every extent write, the bytes are read back
// from disk and compared to the expected checksum. This catches storage layers
// that silently accept a write but persist different bytes. Without this guard,
// a corrupted new extent would poison parity XOR computation and destroy the
// parity group's recoverability permanently.
func writeExtentFiles(rootDir string, extents []metadata.Extent, payload []byte, limit int) error {
	return writeExtentFilesWithCompression(rootDir, extents, payload, CompressionNone, limit)
}

// writeExtentFilesWithCompression writes each extent's data to disk with optional compression.
// If CompressionAlg is not CompressionNone, the extent data is compressed before writing.
// For mirrored extents, a copy is also written to the mirror disk.
func writeExtentFilesWithCompression(rootDir string, extents []metadata.Extent, payload []byte, compAlg CompressionAlg, limit int) error {
	written := 0
	for _, extent := range extents {
		if limit > 0 && written >= limit {
			return fmt.Errorf("injected crash: wrote %d of %d extents", written, len(extents))
		}
		if err := validateExtentRelativePath(extent.PhysicalLocator.RelativePath); err != nil {
			return fmt.Errorf("extent %s has invalid path: %w", extent.ExtentID, err)
		}
		targetPath := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)

		var intended []byte
		if extent.CompressionAlg != "" && extent.CompressionAlg != metadata.CompressionNone {
			originalData := extentData(extent, payload)
			compressedData, err := compress(originalData, CompressionAlg(extent.CompressionAlg))
			if err != nil {
				return fmt.Errorf("compress extent file %s: %w", extent.ExtentID, err)
			}
			intended = compressedData
		} else {
			intended = extentData(extent, payload)
		}

		if err := replaceSyncFile(targetPath, intended, 0o600); err != nil {
			return fmt.Errorf("write extent file %s: %w", targetPath, err)
		}
		if extent.Checksum != "" {
			if err := verifyOnDiskCompressedExtentBytes(targetPath, extent); err != nil {
				return fmt.Errorf("I3: post-write readback failed for %s: %w", extent.ExtentID, err)
			}
		}

		if extent.ProtectionClass == metadata.ExtentMirrored && extent.MirrorDiskID != "" {
			mirrorPath := writeMirrorRelativePath(rootDir, extent.PhysicalLocator.RelativePath, extent.MirrorDiskID)
			if err := replaceSyncFile(mirrorPath, intended, 0o600); err != nil {
				return fmt.Errorf("write mirror extent file %s: %w", mirrorPath, err)
			}
			if extent.MirrorChecksum != "" {
				if err := verifyOnDiskMirrorExtentBytes(mirrorPath, extent); err != nil {
					return fmt.Errorf("I3: post-write mirror readback failed for %s: %w", extent.ExtentID, err)
				}
			}
		}

		written++
	}
	return nil
}

// writeMirrorRelativePath computes the mirror copy path for an extent.
func writeMirrorRelativePath(rootDir, primaryPath, mirrorDiskID string) string {
	dir := filepath.Dir(primaryPath)
	ext := filepath.Ext(primaryPath)
	base := filepath.Base(primaryPath)
	mirrorDir := filepath.Join(rootDir, "mirror", mirrorDiskID, dir)
	return filepath.Join(mirrorDir, base+".mirror"+ext)
}

// verifyOnDiskMirrorExtentBytes verifies the mirror extent data on disk.
func verifyOnDiskMirrorExtentBytes(path string, extent metadata.Extent) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if extent.CompressedSize > 0 && int64(len(data)) != extent.CompressedSize {
		return fmt.Errorf("mirror compressed length mismatch: committed_compressed=%d disk=%d", extent.CompressedSize, len(data))
	}
	if extent.MirrorChecksum != "" && digestBytes(data) != extent.MirrorChecksum {
		return fmt.Errorf("mirror checksum mismatch: expected=%s got=%s", extent.MirrorChecksum, digestBytes(data))
	}
	if extent.MirrorChecksum == "" && extent.Checksum != "" && digestBytes(data) != extent.Checksum {
		return fmt.Errorf("mirror checksum mismatch: expected=%s got=%s", extent.Checksum, digestBytes(data))
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

func verifyOnDiskExtentBytes(path string, extent metadata.Extent) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if int64(len(data)) != extent.Length {
		return fmt.Errorf("length mismatch: committed=%d disk=%d", extent.Length, len(data))
	}
	if digestBytes(data) != extent.Checksum {
		return fmt.Errorf("checksum mismatch: expected=%s got=%s", extent.Checksum, digestBytes(data))
	}
	return nil
}

// verifyOnDiskCompressedExtentBytes verifies compressed extent data on disk.
// For compressed extents, the on-disk size is CompressedSize and the checksum
// is computed on the compressed data.
func verifyOnDiskCompressedExtentBytes(path string, extent metadata.Extent) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if extent.CompressedSize > 0 && int64(len(data)) != extent.CompressedSize {
		return fmt.Errorf("compressed length mismatch: committed_compressed=%d disk=%d", extent.CompressedSize, len(data))
	}
	if digestBytes(data) != extent.Checksum {
		return fmt.Errorf("checksum mismatch: expected=%s got=%s", extent.Checksum, digestBytes(data))
	}
	return nil
}

func verifyOnDiskParityBytes(path string, expectedChecksum string, expectedLen int) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) != expectedLen {
		return fmt.Errorf("length mismatch: expected=%d disk=%d", expectedLen, len(data))
	}
	if digestBytes(data) != expectedChecksum {
		return fmt.Errorf("checksum mismatch: expected=%s got=%s", expectedChecksum, digestBytes(data))
	}
	return nil
}

func writeParityFiles(rootDir string, state *metadata.SampleState, extents []metadata.Extent, limit int) error {
	if len(extents) == 0 {
		return nil
	}
	if state == nil {
		return fmt.Errorf("state is nil")
	}

	parityDir := filepath.Join(rootDir, "parity")
	if err := ensureDir(parityDir, 0o755); err != nil {
		return fmt.Errorf("create parity directory: %w", err)
	}

	groups := make(map[string][]metadata.Extent)
	groupOrder := make([]string, 0)
	for _, extent := range state.Extents {
		for _, target := range extents {
			if extent.ParityGroupID == target.ParityGroupID {
				if _, seen := groups[extent.ParityGroupID]; !seen {
					groupOrder = append(groupOrder, extent.ParityGroupID)
				}
				groups[extent.ParityGroupID] = append(groups[extent.ParityGroupID], extent)
				break
			}
		}
	}

	written := 0
	for _, groupID := range groupOrder {
		if limit > 0 && written >= limit {
			return fmt.Errorf("injected crash: wrote %d of %d parity groups", written, len(groupOrder))
		}
		members := groups[groupID]
		maxLen := 0
		memberData := make([][]byte, 0, len(members))

		// Build a checksum index for fast lookup during verification.
		checksumByID := make(map[string]string, len(members))
		// For compressed extents, we need to track that checksums are on compressed data
		// but parity verification needs to verify decompressed data
		for _, m := range members {
			if m.CompressionAlg != "" && m.CompressionAlg != metadata.CompressionNone {
				// For compressed extents, verify the compressed data checksum matches
				// The extent's Checksum is already set to the compressed data checksum
				checksumByID[m.ExtentID] = m.Checksum
			} else {
				checksumByID[m.ExtentID] = m.Checksum
			}
		}

		for _, member := range members {
			if err := validateExtentRelativePath(member.PhysicalLocator.RelativePath); err != nil {
				return fmt.Errorf("member extent %s has invalid path: %w", member.ExtentID, err)
			}
			contentPath := filepath.Join(rootDir, member.PhysicalLocator.RelativePath)
			data, err := os.ReadFile(contentPath)
			if err != nil {
				return fmt.Errorf("read extent file %s for parity: %w", contentPath, err)
			}
			// For compressed extents, decompress before parity computation.
			// Parity is always computed on uncompressed data.
			// Skip checksum verification for compressed extents - they were already
			// verified at write time via verifyOnDiskCompressedExtentBytes.
			if member.CompressionAlg != "" && member.CompressionAlg != metadata.CompressionNone {
				decompressed, err := decompress(data, CompressionAlg(member.CompressionAlg))
				if err != nil {
					return fmt.Errorf("decompress extent %s for parity: %w", member.ExtentID, err)
				}
				data = decompressed
			} else {
				// I3/I9: verify each member's checksum before XOR-ing into parity.
				// If a member is corrupt, using it for parity computation would poison
				// the parity group and destroy all other members' recoverability.
				if expected := checksumByID[member.ExtentID]; expected != "" {
					normed := normalizeExtentLength(data, member.Length)
					if digestBytes(normed) != expected {
						return fmt.Errorf("I3: parity input checksum mismatch for extent %s in group %s: "+
							"member is corrupt and must be repaired before parity can be recomputed",
							member.ExtentID, groupID)
					}
					data = normed
				}
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
		if err := replaceSyncFile(parityPath, parityData, 0o600); err != nil {
			return fmt.Errorf("write parity file %s: %w", parityPath, err)
		}
		// I4: post-write readback — verify the parity bytes now on disk match
		// the computed checksum before allowing the transaction to proceed.
		// If the storage layer accepted the write but stored different bytes,
		// we must detect it here or the parity group becomes permanently
		// unrecoverable (all members would reconstruct to corrupted data).
		if err := verifyOnDiskParityBytes(parityPath, parityChecksum, maxLen); err != nil {
			return fmt.Errorf("I4: post-write parity readback failed for group %s: %w", groupID, err)
		}
		for i := range state.ParityGroups {
			if state.ParityGroups[i].ParityGroupID == groupID {
				state.ParityGroups[i].ParityChecksum = parityChecksum
			}
		}
		written++
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
