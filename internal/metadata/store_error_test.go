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

package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsTruncatedData(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "truncated.bin")

	data := []byte("RTPM\x00\x01\x00\x00")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store := &Store{path: path}
	_, err := store.Load()
	if err == nil {
		t.Error("Load should reject truncated data")
	}
}

func TestLoadRejectsBadMagic(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "badmagic.bin")

	data := []byte("XXXX\x00\x01\x00\x00\x00\x00\x00\x00")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store := &Store{path: path}
	_, err := store.Load()
	if err == nil {
		t.Error("Load should reject bad magic")
	}
}

func TestLoadRejectsWrongVersion(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "wrongversion.bin")

	data := []byte("RTPM\x00\x99\x00\x00")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store := &Store{path: path}
	_, err := store.Load()
	if err == nil {
		t.Error("Load should reject unsupported version")
	}
}

func TestLoadRejectsMissingFile(t *testing.T) {
	store := &Store{path: "/nonexistent/path/metadata.bin"}
	_, err := store.Load()
	if err == nil {
		t.Error("Load should reject missing file")
	}
}
