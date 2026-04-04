package metadata

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreSaveLoadRoundTrip(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "metadata.json"))
	state := PrototypeState("demo")

	allocator := NewAllocator(&state)
	_, extents, err := allocator.AllocateFile("/shares/media/movie.mkv", 2*state.Pool.ExtentSizeBytes+123)
	if err != nil {
		t.Fatalf("AllocateFile returned error: %v", err)
	}
	if len(extents) != 3 {
		t.Fatalf("expected 3 extents, got %d", len(extents))
	}

	saved, err := store.Save(state)
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if saved.StateChecksum == "" {
		t.Fatal("expected state checksum to be set")
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if loaded.Pool.Name != "demo" {
		t.Fatalf("unexpected pool name: %q", loaded.Pool.Name)
	}
	if len(loaded.Files) != 1 || len(loaded.Extents) != 3 {
		t.Fatalf("unexpected loaded state sizes: files=%d extents=%d", len(loaded.Files), len(loaded.Extents))
	}
}

func TestStoreLoadRejectsTamperedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.json")
	store := NewStore(path)
	state := PrototypeState("demo")
	if _, err := store.Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	mutated := strings.Replace(string(contents), "\"name\": \"demo\"", "\"name\": \"tampered\"", 1)
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err = store.Load()
	if err == nil {
		t.Fatal("expected checksum validation error")
	}
}

func TestAllocatorUsesOnlineDataDisksAndSplitsByExtentSize(t *testing.T) {
	state := PrototypeState("demo")
	allocator := NewAllocator(&state)

	file, extents, err := allocator.AllocateFile("/shares/docs/archive.tar", 2*state.Pool.ExtentSizeBytes+1)
	if err != nil {
		t.Fatalf("AllocateFile returned error: %v", err)
	}
	if file.SizeBytes != 2*state.Pool.ExtentSizeBytes+1 {
		t.Fatalf("unexpected file size: %d", file.SizeBytes)
	}
	if len(extents) != 3 {
		t.Fatalf("expected 3 extents, got %d", len(extents))
	}
	if extents[0].DataDiskID == extents[1].DataDiskID {
		t.Fatalf("expected allocator to spread early extents across data disks, got %q twice", extents[0].DataDiskID)
	}
	if extents[2].LogicalOffset != 2*state.Pool.ExtentSizeBytes {
		t.Fatalf("unexpected final logical offset: %d", extents[2].LogicalOffset)
	}
	if extents[2].Length != 1 {
		t.Fatalf("expected tail extent length of 1, got %d", extents[2].Length)
	}
}
