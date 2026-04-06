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
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestRenameFileForcesRecoveryBeforeMetadataMutation(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	basePayload := makePayload(1024, 0x11)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "rename-recovery-guard",
		LogicalPath:    "/test/base.bin",
		AllowSynthetic: true,
		Payload:        basePayload,
	}); err != nil {
		t.Fatalf("WriteFile base: %v", err)
	}

	pendingPayload := makePayload(2048, 0x22)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "rename-recovery-guard",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        pendingPayload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if !res.ReplayRequired {
		t.Fatal("expected replay-required pending write")
	}

	renamer := NewCoordinator(metaPath, journalPath)
	if _, err := renamer.RenameFile("/test/base.bin", "/test/base-renamed.bin"); err != nil {
		t.Fatalf("RenameFile should force recovery before rename: %v", err)
	}

	readPending, err := renamer.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile pending after rename-triggered recovery: %v", err)
	}
	if !bytes.Equal(readPending.Data, pendingPayload) {
		t.Fatal("pending file data mismatch after rename-triggered recovery")
	}
	if _, err := renamer.ReadFile("/test/base.bin"); err == nil {
		t.Fatal("old path should not exist after rename")
	}
	readRenamed, err := renamer.ReadFile("/test/base-renamed.bin")
	if err != nil {
		t.Fatalf("ReadFile renamed file: %v", err)
	}
	if !bytes.Equal(readRenamed.Data, basePayload) {
		t.Fatal("renamed file data mismatch")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after rename: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after rename-triggered recovery: %+v", summary)
	}
}

func TestAddDiskForcesRecoveryBeforeMetadataMutation(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	pendingPayload := makePayload(1024, 0x33)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "disk-recovery-guard",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        pendingPayload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if !res.ReplayRequired {
		t.Fatal("expected replay-required pending write")
	}

	mut := NewCoordinator(metaPath, journalPath)
	if err := mut.AddDisk("disk-03", "33333333-aaaa-bbbb-cccc-555555555555", metadata.DiskRoleData, "/mnt/data03", 16<<30); err != nil {
		t.Fatalf("AddDisk should force recovery before metadata mutation: %v", err)
	}

	readPending, err := mut.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile pending after AddDisk-triggered recovery: %v", err)
	}
	if !bytes.Equal(readPending.Data, pendingPayload) {
		t.Fatal("pending file data mismatch after AddDisk-triggered recovery")
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("load metadata after AddDisk: %v", err)
	}
	found := false
	for _, d := range state.Disks {
		if d.DiskID == "disk-03" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("added disk not present in metadata after recovery+AddDisk")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after AddDisk: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after AddDisk-triggered recovery: %+v", summary)
	}
}
