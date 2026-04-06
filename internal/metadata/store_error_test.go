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
