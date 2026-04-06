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

// fault_injection.go — Test fault injection helpers for ZFS-level failure testing.
// These helpers enable deterministic corruption, truncation, and failure simulation
// for the comprehensive failure matrix tests.

import (
	"fmt"
	"os"
	"path/filepath"
)

// JournalCorruptionHelper provides methods to corrupt journal records at specific byte positions.
type JournalCorruptionHelper struct {
	journalPath string
}

func NewJournalCorruptionHelper(journalPath string) *JournalCorruptionHelper {
	return &JournalCorruptionHelper{journalPath: journalPath}
}

// TornPrepareHeader corrupts the magic/version bytes of the first record's header.
func (jch *JournalCorruptionHelper) TornPrepareHeader() error {
	data, err := os.ReadFile(jch.journalPath)
	if err != nil || len(data) < 8 {
		return fmt.Errorf("cannot corrupt prepare header: %w", err)
	}
	// Flip magic bytes [4:8]
	for i := 4; i < 8; i++ {
		data[i] ^= 0xFF
	}
	return os.WriteFile(jch.journalPath, data, 0o600)
}

// TornPreparePayload corrupts the TxID within the first record's payload.
func (jch *JournalCorruptionHelper) TornPreparePayload() error {
	data, err := os.ReadFile(jch.journalPath)
	if err != nil || len(data) < 100 {
		return fmt.Errorf("cannot corrupt prepare payload: %w", err)
	}
	// Flip bytes in the middle of payload (approximate TxID location)
	data[80] ^= 0xFF
	return os.WriteFile(jch.journalPath, data, 0o600)
}

// InvalidChecksum corrupts the record checksum in the header.
func (jch *JournalCorruptionHelper) InvalidChecksum(recordIndex int) error {
	data, err := os.ReadFile(jch.journalPath)
	if err != nil || len(data) < 72 {
		return fmt.Errorf("cannot corrupt checksum: %w", err)
	}
	// Checksum is at [40:72] in the header
	if recordIndex == 0 {
		data[40] ^= 0xFF // First record checksum
	}
	return os.WriteFile(jch.journalPath, data, 0o600)
}

// RemoveCommitMarker deletes the final commit record from the journal.
func (jch *JournalCorruptionHelper) RemoveCommitMarker() error {
	data, err := os.ReadFile(jch.journalPath)
	if err != nil {
		return fmt.Errorf("cannot remove commit: %w", err)
	}
	if len(data) < 4 {
		return fmt.Errorf("journal too short")
	}
	// Truncate to remove the last record (approximate).
	// A real implementation would parse records, but for testing
	// we can simply truncate ~200 bytes to remove the commit record.
	if len(data) >= 200 {
		return os.WriteFile(jch.journalPath, data[:len(data)-200], 0o600)
	}
	return nil
}

// TruncateJournal truncates the journal file to simulate torn write.
func (jch *JournalCorruptionHelper) TruncateJournal(bytes int64) error {
	return os.Truncate(jch.journalPath, bytes)
}

// DiskFailureHelper simulates disk failures by removing or corrupting extent files.
type DiskFailureHelper struct {
	rootDir string
	extents []string // relative paths to extent files
}

func NewDiskFailureHelper(rootDir string) *DiskFailureHelper {
	return &DiskFailureHelper{
		rootDir: rootDir,
		extents: []string{},
	}
}

// RegisterExtent adds a relative path to the list of extent files under management.
func (dfh *DiskFailureHelper) RegisterExtent(relativePath string) {
	dfh.extents = append(dfh.extents, relativePath)
}

// RemoveExtentFile simulates a disk losing an extent by deleting the file.
func (dfh *DiskFailureHelper) RemoveExtentFile(index int) error {
	if index < 0 || index >= len(dfh.extents) {
		return fmt.Errorf("extent index out of range")
	}
	path := filepath.Join(dfh.rootDir, dfh.extents[index])
	return os.Remove(path)
}

// CorruptExtentFile corrupts an extent by flipping every byte.
func (dfh *DiskFailureHelper) CorruptExtentFile(index int) error {
	if index < 0 || index >= len(dfh.extents) {
		return fmt.Errorf("extent index out of range")
	}
	path := filepath.Join(dfh.rootDir, dfh.extents[index])
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for i := range data {
		data[i] ^= 0xFF
	}
	return os.WriteFile(path, data, 0o600)
}

// RemoveParityFile removes a parity file to simulate parity disk failure.
func (dfh *DiskFailureHelper) RemoveParityFile(groupID string) error {
	path := filepath.Join(dfh.rootDir, "parity", groupID+".bin")
	return os.Remove(path)
}

// MetadataCorruptionHelper corrupts the metadata snapshot.
type MetadataCorruptionHelper struct {
	metadataPath string
}

func NewMetadataCorruptionHelper(metadataPath string) *MetadataCorruptionHelper {
	return &MetadataCorruptionHelper{metadataPath: metadataPath}
}

// TornHeader corrupts the metadata header magic/version.
func (mch *MetadataCorruptionHelper) TornHeader() error {
	data, err := os.ReadFile(mch.metadataPath)
	if err != nil || len(data) < 8 {
		return fmt.Errorf("cannot corrupt metadata header: %w", err)
	}
	// Flip magic bytes [0:4]
	for i := 0; i < 4; i++ {
		data[i] ^= 0xFF
	}
	return os.WriteFile(mch.metadataPath, data, 0o600)
}

// InvalidStateChecksum corrupts the state checksum.
func (mch *MetadataCorruptionHelper) InvalidStateChecksum() error {
	data, err := os.ReadFile(mch.metadataPath)
	if err != nil || len(data) < 72 {
		return fmt.Errorf("cannot corrupt state checksum: %w", err)
	}
	// State checksum is at [40:72]
	data[40] ^= 0xFF
	return os.WriteFile(mch.metadataPath, data, 0o600)
}

// TruncateMetadata truncates metadata file to simulate torn write.
func (mch *MetadataCorruptionHelper) TruncateMetadata(bytes int64) error {
	return os.Truncate(mch.metadataPath, bytes)
}
