package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Gate 10 mandatory coverage: scrub repair path.
// This test must remain non-skipped. It verifies that scrub(repair=true)
// detects a corrupted extent, repairs it from parity, reports the healed
// extent, and restores correct user-visible data.
func TestGate10_ScrubRepairPathRepairsCorruptedExtent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("scrub-repair-path-"), 65536)
	_, err := coord.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/scrub/repair.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta returned error: %v", err)
	}
	if len(state.Extents) == 0 {
		t.Fatal("expected at least one extent")
	}

	target := state.Extents[0]
	extentPath := filepath.Join(dir, target.PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile extent returned error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("extent file is empty")
	}
	data[0] ^= 0x5a
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("Corrupt extent write returned error: %v", err)
	}

	result, err := coord.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub(true) returned error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy scrub after repair, got issues: %+v", result.Issues)
	}
	if result.HealedCount < 1 {
		t.Fatalf("expected at least one healed extent, got %d", result.HealedCount)
	}
	found := false
	for _, id := range result.HealedExtentIDs {
		if id == target.ExtentID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected healed extent %s in result, got %v", target.ExtentID, result.HealedExtentIDs)
	}

	readResult, err := coord.ReadFile("/scrub/repair.bin")
	if err != nil {
		t.Fatalf("ReadFile after scrub repair returned error: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("read data mismatch after scrub repair")
	}
}
