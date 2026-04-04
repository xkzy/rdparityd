package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorReadFileVerifiesHealthyData(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	_, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/read.bin",
		SizeBytes:   8192,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coordinator.ReadFile("/shares/demo/read.bin")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !result.Verified {
		t.Fatal("expected verified read result")
	}
	if result.BytesRead != 8192 {
		t.Fatalf("expected 8192 bytes read, got %d", result.BytesRead)
	}
	if len(result.HealedExtentIDs) != 0 {
		t.Fatalf("expected no self-heal events, got %#v", result.HealedExtentIDs)
	}
}

func TestCoordinatorReadFileSelfHealsCorruptedExtent(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/heal.bin",
		SizeBytes:   8192,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	extentPath := filepath.Join(filepath.Dir(metadataPath), writeResult.Extents[0].PhysicalLocator.RelativePath)
	original, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	corrupted := append([]byte(nil), original...)
	corrupted[0] ^= 0xFF
	if err := os.WriteFile(extentPath, corrupted, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result, err := coordinator.ReadFile("/shares/demo/heal.bin")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !result.Verified {
		t.Fatal("expected verified read result after self-heal")
	}
	if len(result.HealedExtentIDs) != 1 {
		t.Fatalf("expected 1 self-healed extent, got %#v", result.HealedExtentIDs)
	}

	repaired, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !bytes.Equal(repaired, original) {
		t.Fatal("expected repaired extent to match the original bytes")
	}
}
