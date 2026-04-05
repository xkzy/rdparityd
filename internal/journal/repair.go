package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	repairPathPrefixExtent = "repair://extent/"
	repairPathPrefixParity = "repair://parity/"
)

func isRepairRecord(record Record) bool {
	if record.File != nil || len(record.Extents) == 0 {
		return false
	}
	return strings.HasPrefix(record.LogicalPath, repairPathPrefixExtent) ||
		strings.HasPrefix(record.LogicalPath, repairPathPrefixParity)
}

func repairKind(record Record) string {
	switch {
	case strings.HasPrefix(record.LogicalPath, repairPathPrefixExtent):
		return "extent"
	case strings.HasPrefix(record.LogicalPath, repairPathPrefixParity):
		return "parity"
	default:
		return ""
	}
}

func extentRepairPath(extentID string) string {
	return repairPathPrefixExtent + extentID
}

func parityRepairPath(groupID string) string {
	return repairPathPrefixParity + groupID
}

func shouldStopRepairAfter(target, current State) bool {
	return target != "" && target == current
}

func repairBaseRecord(poolName, logicalPath string, extents []metadata.Extent) Record {
	affected := make([]string, 0, len(extents))
	for _, extent := range extents {
		affected = append(affected, extent.ExtentID)
	}
	return Record{
		TxID:              fmt.Sprintf("tx-repair-%d", time.Now().UTC().UnixNano()),
		Timestamp:         time.Now().UTC(),
		PoolName:          poolName,
		LogicalPath:       logicalPath,
		Extents:           append([]metadata.Extent(nil), extents...),
		AffectedExtentIDs: affected,
	}
}

func runExtentRepair(metadataPath string, journal *Store, state metadata.SampleState, extent metadata.Extent, failAfter State) ([]byte, bool, error) {
	rootDir := filepath.Dir(metadataPath)
	rebuilt, err := reconstructExtent(rootDir, state, extent)
	if err != nil {
		return nil, false, err
	}

	record := repairBaseRecord(state.Pool.Name, extentRepairPath(extent.ExtentID), []metadata.Extent{extent})
	if _, err := journal.Append(withState(record, StatePrepared)); err != nil {
		return nil, false, fmt.Errorf("append extent repair prepared record: %w", err)
	}
	if shouldStopRepairAfter(failAfter, StatePrepared) {
		return nil, false, fmt.Errorf("injected repair crash after %s", StatePrepared)
	}

	path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
	if err := replaceSyncFile(path, rebuilt, 0o600); err != nil {
		return nil, false, fmt.Errorf("rewrite healed extent: %w", err)
	}
	// I3/I9: post-write readback — verify the bytes now on disk match the
	// expected checksum before journaling data-written. This catches torn
	// writes and storage-layer bit flips that occur during the write itself.
	if written, readErr := os.ReadFile(path); readErr != nil {
		return nil, false, fmt.Errorf("I3: post-repair readback failed for %s: %w", extent.ExtentID, readErr)
	} else if digestBytes(normalizeExtentLength(written, extent.Length)) != extent.Checksum {
		return nil, false, fmt.Errorf("I3: post-repair checksum mismatch for extent %s — disk did not accept the correct bytes", extent.ExtentID)
	}
	if _, err := journal.Append(withState(record, StateDataWritten)); err != nil {
		return nil, false, fmt.Errorf("append extent repair data-written record: %w", err)
	}
	if shouldStopRepairAfter(failAfter, StateDataWritten) {
		return nil, false, fmt.Errorf("injected repair crash after %s", StateDataWritten)
	}

	if _, err := journal.Append(withState(record, StateCommitted)); err != nil {
		return nil, false, fmt.Errorf("append extent repair committed record: %w", err)
	}
	return rebuilt, true, nil
}

func runParityRepair(metadataPath string, journal *Store, state *metadata.SampleState, groupID string, extents []metadata.Extent, failAfter State) (bool, error) {
	if state == nil {
		return false, fmt.Errorf("state is nil")
	}
	if len(extents) == 0 {
		return false, fmt.Errorf("no extents provided for parity repair")
	}

	sort.Slice(extents, func(i, j int) bool {
		if extents[i].LogicalOffset == extents[j].LogicalOffset {
			return extents[i].ExtentID < extents[j].ExtentID
		}
		return extents[i].LogicalOffset < extents[j].LogicalOffset
	})

	record := repairBaseRecord(state.Pool.Name, parityRepairPath(groupID), extents)
	if _, err := journal.Append(withState(record, StatePrepared)); err != nil {
		return false, fmt.Errorf("append parity repair prepared record: %w", err)
	}
	if shouldStopRepairAfter(failAfter, StatePrepared) {
		return false, fmt.Errorf("injected repair crash after %s", StatePrepared)
	}

	if err := writeParityFiles(filepath.Dir(metadataPath), state, extents, 0); err != nil {
		return false, fmt.Errorf("rewrite parity group %s: %w", groupID, err)
	}
	if _, err := journal.Append(withState(record, StateDataWritten)); err != nil {
		return false, fmt.Errorf("append parity repair data-written record: %w", err)
	}
	if shouldStopRepairAfter(failAfter, StateDataWritten) {
		return false, fmt.Errorf("injected repair crash after %s", StateDataWritten)
	}

	if _, err := journal.Append(withState(record, StateCommitted)); err != nil {
		return false, fmt.Errorf("append parity repair committed record: %w", err)
	}
	return true, nil
}

func rollForwardRepair(metadataPath string, journal *Store, state *metadata.SampleState, txRecords []Record) (bool, error) {
	if state == nil {
		return false, fmt.Errorf("state is nil")
	}
	if len(txRecords) == 0 {
		return false, fmt.Errorf("empty repair transaction record set")
	}

	last := effectiveRecoveryRecord(txRecords)
	if !isRepairRecord(last) {
		return false, fmt.Errorf("record %s is not a repair transaction", last.TxID)
	}
	if len(last.Extents) == 0 {
		return false, fmt.Errorf("repair transaction %s has no extents", last.TxID)
	}

	metadataChanged := false
	switch repairKind(last) {
	case "extent":
		target := last.Extents[0]
		path := filepath.Join(filepath.Dir(metadataPath), target.PhysicalLocator.RelativePath)
		if last.State == StatePrepared {
			rebuilt, err := reconstructExtent(filepath.Dir(metadataPath), *state, target)
			if err != nil {
				return false, fmt.Errorf("reconstruct extent %s during repair replay: %w", target.ExtentID, err)
			}
			if err := replaceSyncFile(path, rebuilt, 0o600); err != nil {
				return false, fmt.Errorf("rewrite extent %s during repair replay: %w", target.ExtentID, err)
			}
			if _, err := journal.Append(withState(last, StateDataWritten)); err != nil {
				return false, fmt.Errorf("append extent repair data-written replay record: %w", err)
			}
			last.State = StateDataWritten
		}
		if last.State == StateDataWritten || last.State == StateReplayRequired {
			if err := ensureExtentFiles(filepath.Dir(metadataPath), *state, []metadata.Extent{target}); err != nil {
				return false, fmt.Errorf("verify extent %s during repair replay: %w", target.ExtentID, err)
			}
			if _, err := journal.Append(withState(last, StateCommitted)); err != nil {
				return false, fmt.Errorf("append extent repair committed replay record: %w", err)
			}
		}
		return metadataChanged, nil
	case "parity":
		groupID := strings.TrimPrefix(last.LogicalPath, repairPathPrefixParity)
		if groupID == "" {
			return false, fmt.Errorf("missing parity group id in repair transaction %s", last.TxID)
		}
		beforeChecksums := parityChecksumsByGroup(*state)
		if last.State == StatePrepared {
			if err := writeParityFiles(filepath.Dir(metadataPath), state, last.Extents, 0); err != nil {
				return false, fmt.Errorf("rewrite parity group %s during repair replay: %w", groupID, err)
			}
			if _, err := journal.Append(withState(last, StateDataWritten)); err != nil {
				return false, fmt.Errorf("append parity repair data-written replay record: %w", err)
			}
			last.State = StateDataWritten
		}
		if last.State == StateDataWritten || last.State == StateReplayRequired {
			for _, group := range state.ParityGroups {
				if group.ParityGroupID == groupID {
					if err := verifyParityGroup(filepath.Dir(metadataPath), group); err != nil {
						return false, fmt.Errorf("verify parity group %s during repair replay: %w", groupID, err)
					}
					break
				}
			}
			if _, err := journal.Append(withState(last, StateCommitted)); err != nil {
				return false, fmt.Errorf("append parity repair committed replay record: %w", err)
			}
		}
		metadataChanged = parityChecksumsChanged(beforeChecksums, *state)
		return metadataChanged, nil
	default:
		return false, fmt.Errorf("unknown repair transaction type for tx %s", last.TxID)
	}
}

func parityChecksumsByGroup(state metadata.SampleState) map[string]string {
	checksums := make(map[string]string, len(state.ParityGroups))
	for _, group := range state.ParityGroups {
		checksums[group.ParityGroupID] = group.ParityChecksum
	}
	return checksums
}

func parityChecksumsChanged(before map[string]string, state metadata.SampleState) bool {
	for _, group := range state.ParityGroups {
		if before[group.ParityGroupID] != group.ParityChecksum {
			return true
		}
	}
	return false
}
