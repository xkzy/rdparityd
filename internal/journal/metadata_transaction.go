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

import (
	"fmt"
	"strings"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	metadataPathPrefixScrub   = "metadata://scrub/"
	metadataPathPrefixRebuild = "metadata://rebuild/"
)

func isMetadataTransaction(record Record) bool {
	if record.File != nil || len(record.Extents) > 0 {
		return false
	}
	return strings.HasPrefix(record.LogicalPath, metadataPathPrefixScrub) ||
		strings.HasPrefix(record.LogicalPath, metadataPathPrefixRebuild)
}

func metadataKind(record Record) string {
	switch {
	case strings.HasPrefix(record.LogicalPath, metadataPathPrefixScrub):
		return "scrub"
	case strings.HasPrefix(record.LogicalPath, metadataPathPrefixRebuild):
		return "rebuild"
	default:
		return ""
	}
}

func scrubTransactionPath(runID string) string {
	return metadataPathPrefixScrub + runID
}

func rebuildTransactionPath(diskID string) string {
	return metadataPathPrefixRebuild + diskID
}

func logScrubStart(journal *Store, poolName string, runID string) error {
	record := Record{
		TxID:        fmt.Sprintf("tx-scrub-%d", time.Now().UTC().UnixNano()),
		Timestamp:   time.Now().UTC(),
		PoolName:    poolName,
		LogicalPath: scrubTransactionPath(runID),
	}
	_, err := journal.Append(withState(record, StatePrepared))
	return err
}

func logScrubComplete(journal *Store, poolName string, runID string) error {
	record := Record{
		TxID:        fmt.Sprintf("tx-scrub-complete-%d", time.Now().UTC().UnixNano()),
		Timestamp:   time.Now().UTC(),
		PoolName:    poolName,
		LogicalPath: scrubTransactionPath(runID),
	}
	_, err := journal.Append(withState(record, StateCommitted))
	return err
}

func logRebuildStart(journal *Store, poolName string, diskID string) error {
	record := Record{
		TxID:        fmt.Sprintf("tx-rebuild-start-%d", time.Now().UTC().UnixNano()),
		Timestamp:   time.Now().UTC(),
		PoolName:    poolName,
		LogicalPath: rebuildTransactionPath(diskID),
	}
	_, err := journal.Append(withState(record, StatePrepared))
	return err
}

func logRebuildComplete(journal *Store, poolName string, diskID string) error {
	record := Record{
		TxID:        fmt.Sprintf("tx-rebuild-complete-%d", time.Now().UTC().UnixNano()),
		Timestamp:   time.Now().UTC(),
		PoolName:    poolName,
		LogicalPath: rebuildTransactionPath(diskID),
	}
	_, err := journal.Append(withState(record, StateCommitted))
	return err
}

func rollForwardMetadataTransaction(metadataPath string, state *metadata.SampleState, txRecords []Record) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if len(txRecords) == 0 {
		return fmt.Errorf("empty metadata transaction record set")
	}

	last := effectiveRecoveryRecord(txRecords)
	if !isMetadataTransaction(last) {
		return fmt.Errorf("record %s is not a metadata transaction", last.TxID)
	}

	switch metadataKind(last) {
	case "scrub":
		runID := strings.TrimPrefix(last.LogicalPath, metadataPathPrefixScrub)
		if runID == "" {
			return fmt.Errorf("missing scrub run id in metadata transaction %s", last.TxID)
		}
		if last.State == StatePrepared {
			return fmt.Errorf("scrub transaction %s started but never completed; marking as failed to retry", last.TxID)
		}
		return nil
	case "rebuild":
		diskID := strings.TrimPrefix(last.LogicalPath, metadataPathPrefixRebuild)
		if diskID == "" {
			return fmt.Errorf("missing disk id in rebuild metadata transaction %s", last.TxID)
		}
		if last.State == StatePrepared {
			return fmt.Errorf("rebuild transaction %s started but never completed; will retry on next rebuild request", last.TxID)
		}
		return nil
	default:
		return fmt.Errorf("unknown metadata transaction type for tx %s", last.TxID)
	}
}
