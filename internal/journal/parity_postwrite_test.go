package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestI4_PostWriteParityReadback(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.log")

	coord := NewCoordinator(metaPath, journalPath)
	payload := []byte("test data for parity readback verification")
	result, err := coord.WriteFile(WriteRequest{
		PoolName:    "test-i4",
		LogicalPath: "/test/parity-readback.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed, got %s", result.FinalState)
	}

	state, err := coord.metadata.Load()
	if err != nil {
		t.Fatalf("Load metadata: %v", err)
	}

	for _, group := range state.ParityGroups {
		if len(group.MemberExtentIDs) == 0 {
			continue
		}
		parityPath := filepath.Join(dir, "parity", group.ParityGroupID+".bin")
		data, err := os.ReadFile(parityPath)
		if err != nil {
			t.Fatalf("read parity file %s: %v", parityPath, err)
		}
		checksum := digestBytes(data)
		if checksum != group.ParityChecksum {
			t.Fatalf("parity checksum mismatch for group %s: expected=%s got=%s",
				group.ParityGroupID, group.ParityChecksum, checksum)
		}
	}

	t.Log("I4: post-write parity readback verified")
}
