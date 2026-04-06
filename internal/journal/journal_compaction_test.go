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
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestJournalCompactionAfterCommit(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	for i := 1; i <= 5; i++ {
		_, err := coord.WriteFile(context.Background(), WriteRequest{
			PoolName:       "compact-test",
			LogicalPath:    "/test/f" + string(rune('0'+i)) + ".bin",
			AllowSynthetic: true,
			SizeBytes:      4096,
		})
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatalf("stat journal: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected journal to be empty after compaction, got %d bytes", info.Size())
	}
	t.Logf("✓ journal compacted to zero bytes after 5 committed writes")

	// Metadata must still be fully intact.
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load: %v", err)
	}
	if len(state.Files) != 5 {
		t.Fatalf("expected 5 files in metadata, got %d", len(state.Files))
	}
	if vs := CheckStateInvariants(state); len(vs) > 0 {
		t.Fatalf("state invariants violated: %v", vs[0])
	}
}

func TestJournalCompactionNotAppliedWhileTransactionIncomplete(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "compact-incomplete",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatalf("stat journal: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("journal must NOT be compacted when an incomplete transaction is present")
	}
	t.Logf("✓ journal not compacted (%d bytes) while transaction incomplete", info.Size())
}

func TestJournalCompactionThenRecovery(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "compact-recovery",
		LogicalPath:    "/test/committed.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("first write: %v", err)
	}

	_, err = coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "compact-recovery",
		LogicalPath:    "/test/partial.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("partial write: %v", err)
	}

	// Recovery should handle the partial write correctly even though the journal
	// may have been compacted after the first committed write.
	rec := NewCoordinator(metaPath, journalPath)
	result, err := rec.RecoverWithState(metadata.PrototypeState("compact-recovery"))
	if err != nil {
		t.Fatalf("RecoverWithState: %v", err)
	}
	if len(result.RecoveredTxIDs) == 0 {
		t.Fatal("expected the partial write to be recovered")
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after recovery: %+v", summary)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load: %v", err)
	}
	if len(state.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(state.Files))
	}
}

func TestJournalCompactionIdemptotent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "compact-idempotent",
		LogicalPath:    "/test/file.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Journal should be compacted now. Compacting again must be a no-op.
	store := NewStore(journalPath)
	if err := store.CompactIfClean(); err != nil {
		t.Fatalf("second CompactIfClean: %v", err)
	}
	if err := store.CompactIfClean(); err != nil {
		t.Fatalf("third CompactIfClean: %v", err)
	}

	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected journal still empty, got %d bytes", info.Size())
	}
	t.Logf("✓ CompactIfClean is idempotent on empty journal")
}
