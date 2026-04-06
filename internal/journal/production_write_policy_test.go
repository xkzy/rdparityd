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

func TestWriteFileRejectsImplicitSyntheticPayload(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/prod/reject.bin",
		SizeBytes:   4096,
	})
	if err == nil {
		t.Fatal("expected implicit synthetic write to be rejected")
	}
	if got := err.Error(); got == "" || !bytes.Contains([]byte(got), []byte("AllowSynthetic")) {
		t.Fatalf("expected AllowSynthetic guidance in error, got: %v", err)
	}
}

func TestWriteFileAllowsExplicitSyntheticOptIn(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	result, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/prod/explicit-synth.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("expected explicit synthetic write to succeed, got: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed state, got %q", result.FinalState)
	}
}
