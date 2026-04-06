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

func TestPoolNameReturnsDemoWhenNoMetadata(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	name, err := coord.PoolName()
	if err != nil {
		t.Fatalf("PoolName returned error: %v", err)
	}
	if name != "demo" {
		t.Fatalf("expected %q, got %q", "demo", name)
	}
}

func TestPoolNameReturnsPoolNameAfterWrite(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	payload := bytes.Repeat([]byte("hello"), 1024)
	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "mypool",
		LogicalPath:    "/file.txt",
		AllowSynthetic: true,
		Payload:        payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	name, err := coord.PoolName()
	if err != nil {
		t.Fatalf("PoolName returned error: %v", err)
	}
	if name != "mypool" {
		t.Fatalf("expected %q, got %q", "mypool", name)
	}
}

func TestRootDirReturnsMetadataDirectory(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta", "metadata.bin")
	coord := NewCoordinator(metaPath, filepath.Join(dir, "journal.bin"))

	rootDir := coord.RootDir()
	want := filepath.Join(dir, "meta")
	if rootDir != want {
		t.Fatalf("expected %q, got %q", want, rootDir)
	}
}

func TestReadMetaReturnsEmptyStateWhenNoFile(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta returned error: %v", err)
	}
	if state.Pool.Name != "" {
		t.Fatalf("expected empty pool name, got %q", state.Pool.Name)
	}
}

func TestReadMetaReturnsCachedStateAfterWrite(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	if _, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/file.txt",
		AllowSynthetic: true,
		SizeBytes:      512,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta returned error: %v", err)
	}
	if len(state.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(state.Files))
	}
	if state.Files[0].Path != "/file.txt" {
		t.Fatalf("unexpected file path: %q", state.Files[0].Path)
	}
}
