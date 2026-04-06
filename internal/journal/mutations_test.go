package journal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRenameFileSucceeds(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/a/file.txt",
		AllowSynthetic: true,
		SizeBytes:      1024,
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

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/same.txt",
		AllowSynthetic: true,
		SizeBytes:      512,
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

	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/exists.txt",
		AllowSynthetic: true,
		SizeBytes:      512,
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
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, AllowSynthetic: true, SizeBytes: 512}); err != nil {
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

func TestOverwriteFileBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/file.bin"
	if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, AllowSynthetic: true, SizeBytes: 512}); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	result, err := coord.OverwriteFile(path, 0, []byte("modified"))
	if err != nil {
		t.Fatalf("OverwriteFile failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
	if len(result.Extents) == 0 {
		t.Error("expected modified extents")
	}

	readResult, err := coord.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	expected := "modified"
	for i := 0; i < len(expected) && i < len(readResult.Data); i++ {
		if readResult.Data[i] != expected[i] {
			t.Errorf("byte mismatch at position %d: got %d want %d", i, readResult.Data[i], expected[i])
			break
		}
	}
}

func TestTruncateFileBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/file.bin"
	// Use 2 full extents (default 1MB each) then truncate to 1 extent
	originalSize := 2 * 1024 * 1024
	if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, AllowSynthetic: true, SizeBytes: int64(originalSize)}); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Truncate to 1 extent (1MB)
	result, err := coord.TruncateFile(path, 1024*1024)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}

	readResult, err := coord.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if readResult.File.SizeBytes != 1024*1024 {
		t.Errorf("expected size %d, got %d", 1024*1024, readResult.File.SizeBytes)
	}
}

func TestGrowFileBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/file.bin"
	original := []byte("original content")
	if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: original}); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	appended := []byte(" appended")
	result, err := coord.GrowFile(path, appended)
	if err != nil {
		t.Fatalf("GrowFile failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}

	readResult, err := coord.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	expected := len(original) + len(appended)
	if readResult.File.SizeBytes != int64(expected) {
		t.Errorf("expected size %d, got %d", expected, readResult.File.SizeBytes)
	}
}

func TestDeleteFileBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	path := "/test/delete_me.bin"
	_, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    path,
		AllowSynthetic: true,
		SizeBytes:      1024,
	})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	stateBefore, _ := coord.ReadMeta()
	extentsBefore := len(stateBefore.Extents)

	result, err := coord.DeleteFile(path)
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
	if result.RemovedExtents == 0 {
		t.Error("expected extents to be removed")
	}

	stateAfter, _ := coord.ReadMeta()
	for _, f := range stateAfter.Files {
		if f.Path == path {
			t.Error("file still exists after delete")
		}
	}

	if len(stateAfter.Extents) >= extentsBefore {
		t.Error("extents not removed from metadata")
	}
}

func TestDeleteFileNotFound(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.DeleteFile("/nonexistent.bin")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}
