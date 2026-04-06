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
	"context"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestProtectionStatus_OneDisk(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	result, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	status, err := coord.ProtectionStatus()
	if err != nil {
		t.Fatalf("ProtectionStatus failed: %v", err)
	}

	if status.DiskCount != 4 {
		t.Errorf("expected 4 disks (prototype state), got %d", status.DiskCount)
	}
	if status.DataDiskCount != 2 {
		t.Errorf("expected 2 data disks, got %d", status.DataDiskCount)
	}
	if status.ParityDiskCount != 1 {
		t.Errorf("expected 1 parity disk, got %d", status.ParityDiskCount)
	}
	if status.ExtentCounts[metadata.ExtentChecksumOnly] != len(result.Extents) {
		t.Errorf("expected %d checksum-only extents, got %d",
			len(result.Extents), status.ExtentCounts[metadata.ExtentChecksumOnly])
	}
}

func TestProtectionState(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	state, err := coord.ProtectionState()
	if err != nil {
		t.Fatalf("ProtectionState failed: %v", err)
	}

	if state != metadata.ProtectionParity {
		t.Errorf("expected parity (prototype has parity disk), got %s", state)
	}
}

func TestExtentProtectionClass(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	result, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	class, err := coord.ExtentProtectionClass(result.Extents[0].ExtentID)
	if err != nil {
		t.Fatalf("ExtentProtectionClass failed: %v", err)
	}

	if class != metadata.ExtentChecksumOnly {
		t.Errorf("expected checksum_only, got %s", class)
	}
}

func TestExtentProtectionClass_NotFound(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.ExtentProtectionClass("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent extent")
	}
}

func TestSetPoolProtectionStateRejectsInvalidValue(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	if err := coord.SetPoolProtectionState(metadata.PoolProtectionState("invalid")); err == nil {
		t.Fatal("expected invalid protection state to be rejected")
	}
}

func TestSetPoolProtectionStateUnsupportedForValidValue(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	if err := coord.SetPoolProtectionState(metadata.ProtectionParity); err == nil {
		t.Fatal("expected protection state mutation to be unsupported")
	}
}
