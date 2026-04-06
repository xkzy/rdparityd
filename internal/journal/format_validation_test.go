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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestValidateOnDiskFormatsHealthyPool(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("validate-formats-"), 65536)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/test.bin",
		AllowSynthetic: true,
		Payload:        payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if !result.ok() {
		t.Fatalf("expected no errors, got: %v", result.Errors)
	}
}

func TestValidateOnDiskFormatsMissingMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "missing-metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	// missing metadata is a warning (pool uninitialized), not an error
	if result.ok() && len(result.Warnings) == 0 {
		t.Fatal("expected warnings for missing metadata file")
	}
}

func TestValidateOnDiskFormatsTamperedMetadata(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/test.bin",
		AllowSynthetic: true,
		SizeBytes:      1024,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	// Tamper with the metadata payload (after 72-byte header).
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if len(data) > 72 {
		data[72] ^= 0xFF
	}
	if err := os.WriteFile(metaPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if result.ok() {
		t.Fatal("expected error for tampered metadata")
	}
}

func TestValidateOnDiskFormatsMissingExtentFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("extent-data-"), 65536)
	writeResult, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/test.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}

	extentRelPath := writeResult.Extents[0].PhysicalLocator.RelativePath
	if err := os.Remove(filepath.Join(dir, extentRelPath)); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if result.ok() {
		t.Fatal("expected error for missing extent file")
	}
}

func TestValidateOnDiskFormatsMissingJournal(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "no-journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/test.bin",
		AllowSynthetic: true,
		SizeBytes:      512,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	// Remove journal — should trigger a warning, not an error.
	if err := os.Remove(journalPath); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if !result.ok() {
		t.Fatalf("expected no errors for missing journal, got: %v", result.Errors)
	}
}

func TestValidateOnDiskFormatsCorruptedExtentChecksum(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("extent-checksum-corruption-"), 1024)
	writeResult, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/corrupt.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}

	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if len(data) > 0 {
		data[0] ^= 0xFF
	}
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if result.ok() {
		t.Fatal("expected error for corrupted extent")
	}
}

func TestValidateRebuildProgressFileValidFormat(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("rebuild-progress-"), 65536)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/rebuild/test.bin",
		AllowSynthetic: true,
		Payload:        payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	progress := RebuildProgress{
		DiskID:           "disk-01",
		CompletedExtents: []string{},
	}
	if len(state.Extents) > 0 {
		progress.CompletedExtents = []string{state.Extents[0].ExtentID}
	}
	if err := saveRebuildProgress(metaPath, progress); err != nil {
		t.Fatalf("saveRebuildProgress returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if !result.ok() {
		t.Fatalf("expected no errors with valid rebuild progress, got: %v", result.Errors)
	}
}

func TestValidateOnDiskFormatsParityChecksum(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("parity-checksum-test-"), 65536)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/data/parity.bin",
		AllowSynthetic: true,
		Payload:        payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	// Corrupt one parity file (if any parity groups exist).
	if len(state.ParityGroups) > 0 {
		pg := state.ParityGroups[0]
		parityPath := filepath.Join(dir, "parity", pg.ParityGroupID+".bin")
		data, err := os.ReadFile(parityPath)
		if err != nil {
			t.Fatalf("ReadFile parity returned error: %v", err)
		}
		if len(data) > 0 {
			data[0] ^= 0xAA
		}
		if err := os.WriteFile(parityPath, data, 0o600); err != nil {
			t.Fatalf("WriteFile parity returned error: %v", err)
		}
		result := ValidateOnDiskFormats(dir, metaPath, journalPath)
		if result.ok() {
			t.Fatal("expected error for corrupted parity file")
		}
	}
}

func TestValidateRebuildProgressFileValidFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")

	// Create a valid progress file using saveRebuildProgress, then copy it
	// with the naming convention expected by validateRebuildProgressFile.
	progress := RebuildProgress{
		DiskID:           "disk-01",
		CompletedExtents: []string{"extent-001", "extent-002"},
	}
	if err := saveRebuildProgress(metaPath, progress); err != nil {
		t.Fatalf("saveRebuildProgress returned error: %v", err)
	}

	// Copy the file to the rebuild-progress-*.bin naming convention.
	srcPath := progressPath(metaPath, "disk-01")
	dstPath := filepath.Join(dir, "rebuild-progress-disk-01.bin")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("ReadFile progress returned error: %v", err)
	}
	if err := os.WriteFile(dstPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile progress copy returned error: %v", err)
	}

	if err := validateRebuildProgressFile(dstPath); err != nil {
		t.Fatalf("validateRebuildProgressFile returned error: %v", err)
	}
}

func TestValidateRebuildProgressFileTooShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rebuild-progress-bad.bin")
	if err := os.WriteFile(path, []byte("RBLD"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if err := validateRebuildProgressFile(path); err == nil {
		t.Fatal("expected error for too-short progress file")
	}
}

func TestValidateRebuildProgressFileWrongMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rebuild-progress-bad.bin")
	data := make([]byte, rebuildProgressHdr)
	copy(data[0:4], "XXXX") // wrong magic
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if err := validateRebuildProgressFile(path); err == nil {
		t.Fatal("expected error for wrong magic")
	}
}

func TestValidateRebuildProgressFileInViaValidateOnDisk(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/f.bin",
		AllowSynthetic: true,
		SizeBytes:      512,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	// Write a corrupt progress file with the expected naming pattern.
	badPath := filepath.Join(dir, "rebuild-progress-disk-01.bin")
	corrupt := make([]byte, rebuildProgressHdr)
	copy(corrupt[0:4], rebuildProgressMagic)
	// Leave hash as zeros — it won't match the (empty) payload.
	if err := os.WriteFile(badPath, corrupt, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result := ValidateOnDiskFormats(dir, metaPath, journalPath)
	if result.ok() {
		t.Fatal("expected error for corrupt rebuild progress file")
	}
}
