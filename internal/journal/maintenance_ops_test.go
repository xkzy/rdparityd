package journal

import (
	"path/filepath"
	"testing"
)

func TestTrimBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	state.Pool.FilesystemType = "btrfs"

	result, err := coord.Trim()
	if err != nil {
		t.Fatalf("Trim failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
}

func TestDefragBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	state.Pool.FilesystemType = "btrfs"

	result, err := coord.Defrag()
	if err != nil {
		t.Fatalf("Defrag failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
}

func TestSnapshotBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	state.Pool.FilesystemType = "btrfs"

	result, err := coord.Snapshot("test-snapshot")
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
	if result.Name != "test-snapshot" {
		t.Errorf("expected snapshot name 'test-snapshot', got %q", result.Name)
	}
}

func TestSnapshotAutoName(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	state.Pool.FilesystemType = "btrfs"

	result, err := coord.Snapshot("")
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if result.TxID == "" {
		t.Error("expected non-empty txid")
	}
	if result.Name == "" {
		t.Error("expected auto-generated snapshot name")
	}
}
