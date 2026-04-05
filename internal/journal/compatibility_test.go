package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCompatibility_DifferentFilesystemTypes(t *testing.T) {
	filesystemTypes := []string{"ext4", "xfs", "btrfs"}

	for _, fsType := range filesystemTypes {
		t.Run(fsType, func(t *testing.T) {
			dir := t.TempDir()
			coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

			state, err := coord.ReadMeta()
			if err != nil {
				t.Fatalf("ReadMeta failed: %v", err)
			}
			state.Pool.FilesystemType = fsType

			path := "/test/" + fsType + ".bin"
			payload := []byte("test data for " + fsType)

			result, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
			if err != nil {
				t.Fatalf("WriteFile failed on %s: %v", fsType, err)
			}

			if len(result.Extents) == 0 {
				t.Errorf("No extents created for %s", fsType)
			}

			readResult, err := coord.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile failed on %s: %v", fsType, err)
			}

			if !readResult.Verified {
				t.Errorf("Read not verified on %s", fsType)
			}

			if string(readResult.Data) != string(payload) {
				t.Errorf("Data mismatch on %s", fsType)
			}
		})
	}
}

func TestCompatibility_ExtentPathsValid(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	testPaths := []string{
		"/simple.txt",
		"/deep/nested/path/file.bin",
		"/shares/media/videos/test.mp4",
		"/123numeric.bin",
		"/special-chars_underscore.bin",
	}

	for _, path := range testPaths {
		payload := []byte("data for " + path)
		result, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload})
		if err != nil {
			t.Fatalf("WriteFile failed for %s: %v", path, err)
		}

		for _, ext := range result.Extents {
			extPath := filepath.Join(dir, ext.PhysicalLocator.RelativePath)
			if _, err := os.Stat(extPath); os.IsNotExist(err) {
				t.Errorf("Extent file does not exist: %s", extPath)
			}
		}

		readResult, err := coord.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile failed for %s: %v", path, err)
		}

		if string(readResult.Data) != string(payload) {
			t.Errorf("Data mismatch for %s", path)
		}
	}
}

func TestCompatibility_RecoveryWithDifferentStates(t *testing.T) {
	dir := t.TempDir()

	for scenario := 0; scenario < 3; scenario++ {
		metaPath := filepath.Join(dir, fmt.Sprintf("metadata_%d.bin", scenario))
		journalPath := filepath.Join(dir, fmt.Sprintf("journal_%d.bin", scenario))

		coord := NewCoordinator(metaPath, journalPath)

		path := "/test/recovery_" + string(rune(scenario)) + ".bin"
		_, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: []byte("test data")})
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		coord2 := NewCoordinator(metaPath, journalPath)
		recovery, err := coord2.Recover()
		if err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}

		if len(recovery.RecoveredTxIDs) != 0 {
			t.Errorf("Scenario %d: expected 0 recovered transactions, got %d", scenario, len(recovery.RecoveredTxIDs))
		}

		readResult, err := coord2.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile after recovery failed: %v", err)
		}

		if string(readResult.Data) != "test data" {
			t.Errorf("Data mismatch after recovery")
		}
	}
}

func TestCompatibility_LargeExtentCount(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	extentCount := 100
	for i := 0; i < extentCount; i++ {
		path := fmt.Sprintf("/test/extent_%03d.bin", i)
		payload := []byte("data for extent " + string(rune(i)))
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	if len(state.Extents) != extentCount {
		t.Errorf("Expected %d extents, got %d", extentCount, len(state.Extents))
	}

	violations := CheckStateInvariants(state)
	if len(violations) > 0 {
		t.Errorf("Invariant violations: %v", violations[0])
	}
}

func TestCompatibility_ParityGroupBoundaries(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	maxGroupSize := 8
	writeCount := maxGroupSize + 2
	for i := 0; i < writeCount; i++ {
		path := "/test/group_" + string(rune(i)) + ".bin"
		payload := []byte("data for group test " + string(rune(i)))
		if _, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	state, err = coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta after writes failed: %v", err)
	}

	if len(state.ParityGroups) < 2 {
		t.Errorf("Expected at least 2 parity groups for %d writes", writeCount)
	}

	for _, group := range state.ParityGroups {
		if len(group.MemberExtentIDs) > maxGroupSize {
			t.Errorf("Parity group %s has %d members (max %d)", group.ParityGroupID, len(group.MemberExtentIDs), maxGroupSize)
		}
	}
}
