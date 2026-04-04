package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/rtparityd/rtparityd/internal/metadata"
)

type WriteRequest struct {
	PoolName    string `json:"pool_name"`
	LogicalPath string `json:"logical_path"`
	SizeBytes   int64  `json:"size_bytes"`
	FailAfter   State  `json:"fail_after,omitempty"`
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

	baseRecord := Record{
		TxID:              txID,
		Timestamp:         startedAt,
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

	applyExtentChecksums(&state, extents, txID, req.LogicalPath)
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

func applyExtentChecksums(state *metadata.SampleState, extents []metadata.Extent, txID, logicalPath string) {
	checksums := make(map[string]string, len(extents))
	for i := range extents {
		seed := fmt.Sprintf("%s|%s|%s|%d|%d", txID, logicalPath, extents[i].ExtentID, extents[i].LogicalOffset, extents[i].Length)
		sum := sha256.Sum256([]byte(seed))
		checksum := hex.EncodeToString(sum[:])
		extents[i].Checksum = checksum
		checksums[extents[i].ExtentID] = checksum
	}

	for i := range state.Extents {
		if checksum, ok := checksums[state.Extents[i].ExtentID]; ok {
			state.Extents[i].Checksum = checksum
			state.Extents[i].ChecksumAlg = "blake3"
		}
	}
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
