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
)

func TestReadFileForcesRecoveryWhenJournalReplayIsPending(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	payload := makePayload(4096, 0x5a)
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "pre-read-recovery",
		LogicalPath:    "/test/pending.bin",
		AllowSynthetic: true,
		Payload:        payload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile pending: %v", err)
	}
	if res.FinalState != StateDataWritten || !res.ReplayRequired {
		t.Fatalf("expected StateDataWritten + replay required, got state=%s replay=%v", res.FinalState, res.ReplayRequired)
	}

	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay before read: %v", err)
	}
	if !summary.RequiresReplay {
		t.Fatalf("expected journal to require replay before read, got %+v", summary)
	}

	reader := NewCoordinator(metaPath, journalPath)
	read, err := reader.ReadFile("/test/pending.bin")
	if err != nil {
		t.Fatalf("ReadFile should force recovery and succeed: %v", err)
	}
	if !bytes.Equal(read.Data, payload) {
		t.Fatal("ReadFile returned wrong data after pre-read recovery")
	}

	summary, err = NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("Replay after read-triggered recovery: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after read-triggered recovery: %+v", summary)
	}
}
