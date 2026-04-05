package journal

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestRenameFileSucceeds(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/a/file.txt",
		AllowSynthetic: true,
		SizeBytes:   1024,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coord.RenameFile("/a/file.txt", "/b/renamed.txt")
	if err != nil {
		t.Fatalf("RenameFile returned error: %v", err)
	}
	if result.OldPath != "/a/file.txt" {
		t.Fatalf("unexpected old path: %q", result.OldPath)
	}
	if result.NewPath != "/b/renamed.txt" {
		t.Fatalf("unexpected new path: %q", result.NewPath)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta returned error: %v", err)
	}
	found := false
	for _, f := range state.Files {
		if f.Path == "/b/renamed.txt" {
			found = true
		}
		if f.Path == "/a/file.txt" {
			t.Fatal("old path still present after rename")
		}
	}
	if !found {
		t.Fatal("renamed file not found at new path")
	}
}

func TestRenameFileSamePathIsNoOp(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/same.txt",
		AllowSynthetic: true,
		SizeBytes:   512,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coord.RenameFile("/same.txt", "/same.txt")
	if err != nil {
		t.Fatalf("RenameFile same->same returned error: %v", err)
	}
	if result.OldPath != "/same.txt" || result.NewPath != "/same.txt" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRenameFileNotFound(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/exists.txt",
		AllowSynthetic: true,
		SizeBytes:   512,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err = coord.RenameFile("/missing.txt", "/other.txt")
	if err == nil {
		t.Fatal("expected error for missing source file")
	}
}

func TestRenameFileDestinationExists(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	for _, path := range []string{"/src.txt", "/dst.txt"} {
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, AllowSynthetic: true, SizeBytes: 512}); err != nil {
			t.Fatalf("WriteFile(%q) returned error: %v", path, err)
		}
	}

	_, err := coord.RenameFile("/src.txt", "/dst.txt")
	if err == nil {
		t.Fatal("expected error when destination already exists")
	}
}

func TestRenameFileEmptyPaths(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	if _, err := coord.RenameFile("", "/new.txt"); err == nil {
		t.Fatal("expected error for empty old path")
	}
	if _, err := coord.RenameFile("/old.txt", ""); err == nil {
		t.Fatal("expected error for empty new path")
	}
}

func TestOverwriteFileReturnsNotSupported(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	err := coord.OverwriteFile("/file.txt", 0, []byte("data"))
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got: %v", err)
	}
}

func TestTruncateFileReturnsNotSupported(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	err := coord.TruncateFile("/file.txt", 0)
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got: %v", err)
	}
}

func TestGrowFileReturnsNotSupported(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	err := coord.GrowFile("/file.txt", []byte("more data"))
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got: %v", err)
	}
}
