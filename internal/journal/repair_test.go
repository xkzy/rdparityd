package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestRunParityRepairSucceeds(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("parity-repair-test-"), 65536)
	writeResult, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/data/parity-repair.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Skip("no extents for parity repair test")
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(state.ParityGroups) == 0 {
		t.Skip("no parity groups for parity repair test")
	}

	// Corrupt the parity file to force repair.
	pg := state.ParityGroups[0]
	parityPath := filepath.Join(dir, "parity", pg.ParityGroupID+".bin")
	data, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("ReadFile parity returned error: %v", err)
	}
	if len(data) > 0 {
		data[0] ^= 0xAA
	}
	if err := os.WriteFile(parityPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile parity returned error: %v", err)
	}

	journal := NewStore(journalPath)
	extents := make([]metadata.Extent, 0)
	for _, e := range state.Extents {
		if e.ParityGroupID == pg.ParityGroupID {
			extents = append(extents, e)
		}
	}

	repaired, err := runParityRepair(metaPath, journal, &state, pg.ParityGroupID, extents, "")
	if err != nil {
		t.Fatalf("runParityRepair returned error: %v", err)
	}
	if !repaired {
		t.Fatal("expected repaired=true")
	}
}

func TestRunParityRepairNilState(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.bin")
	journal := NewStore(journalPath)

	_, err := runParityRepair(filepath.Join(dir, "meta.bin"), journal, nil, "pg-1", []metadata.Extent{{ExtentID: "e1"}}, "")
	if err == nil {
		t.Fatal("expected error for nil state")
	}
}

func TestRunParityRepairNoExtents(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.bin")
	journal := NewStore(journalPath)
	state := metadata.PrototypeState("demo")

	_, err := runParityRepair(filepath.Join(dir, "meta.bin"), journal, &state, "pg-1", nil, "")
	if err == nil {
		t.Fatal("expected error for empty extents")
	}
}

func TestParityChecksumsByGroup(t *testing.T) {
	state := metadata.SampleState{
		ParityGroups: []metadata.ParityGroup{
			{ParityGroupID: "pg-1", ParityChecksum: "checksum-a"},
			{ParityGroupID: "pg-2", ParityChecksum: "checksum-b"},
		},
	}

	checksums := parityChecksumsByGroup(state)
	if len(checksums) != 2 {
		t.Fatalf("expected 2 checksums, got %d", len(checksums))
	}
	if checksums["pg-1"] != "checksum-a" {
		t.Fatalf("unexpected checksum for pg-1: %q", checksums["pg-1"])
	}
	if checksums["pg-2"] != "checksum-b" {
		t.Fatalf("unexpected checksum for pg-2: %q", checksums["pg-2"])
	}
}

func TestParityChecksumsChanged(t *testing.T) {
	before := map[string]string{
		"pg-1": "old-checksum",
		"pg-2": "same-checksum",
	}
	state := metadata.SampleState{
		ParityGroups: []metadata.ParityGroup{
			{ParityGroupID: "pg-1", ParityChecksum: "new-checksum"},
			{ParityGroupID: "pg-2", ParityChecksum: "same-checksum"},
		},
	}

	if !parityChecksumsChanged(before, state) {
		t.Fatal("expected changed=true when pg-1 checksum differs")
	}

	before["pg-1"] = "new-checksum"
	if parityChecksumsChanged(before, state) {
		t.Fatal("expected changed=false when all checksums are the same")
	}
}

func TestRollForwardRepairNilState(t *testing.T) {
	dir := t.TempDir()
	journal := NewStore(filepath.Join(dir, "journal.bin"))

	_, err := rollForwardRepair(filepath.Join(dir, "meta.bin"), journal, nil, []Record{{}})
	if err == nil {
		t.Fatal("expected error for nil state")
	}
}

func TestRollForwardRepairEmptyRecords(t *testing.T) {
	dir := t.TempDir()
	journal := NewStore(filepath.Join(dir, "journal.bin"))
	state := metadata.PrototypeState("demo")

	_, err := rollForwardRepair(filepath.Join(dir, "meta.bin"), journal, &state, nil)
	if err == nil {
		t.Fatal("expected error for empty records")
	}
}

func TestExtentsForParityGroupsReturnsMatchingExtents(t *testing.T) {
	state := metadata.SampleState{
		Extents: []metadata.Extent{
			{ExtentID: "e1", ParityGroupID: "pg-1"},
			{ExtentID: "e2", ParityGroupID: "pg-2"},
			{ExtentID: "e3", ParityGroupID: "pg-1"},
			{ExtentID: "e4", ParityGroupID: "pg-3"},
		},
	}

	groupIDs := map[string]struct{}{"pg-1": {}}
	result := extentsForParityGroups(state, groupIDs)
	if len(result) != 2 {
		t.Fatalf("expected 2 extents for pg-1, got %d", len(result))
	}
	for _, e := range result {
		if e.ParityGroupID != "pg-1" {
			t.Fatalf("unexpected parity group id: %q", e.ParityGroupID)
		}
	}
}

func TestExtentsForParityGroupsEmptyGroupIDs(t *testing.T) {
	state := metadata.SampleState{
		Extents: []metadata.Extent{
			{ExtentID: "e1", ParityGroupID: "pg-1"},
		},
	}

	result := extentsForParityGroups(state, map[string]struct{}{})
	if len(result) != 0 {
		t.Fatalf("expected 0 extents for empty group set, got %d", len(result))
	}
}

func TestRepairKind(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{repairPathPrefixExtent + "ext-1", "extent"},
		{repairPathPrefixParity + "pg-1", "parity"},
		{"/data/file.txt", ""},
	}
	for _, tc := range cases {
		record := Record{LogicalPath: tc.path, Extents: []metadata.Extent{{ExtentID: "e1"}}}
		got := repairKind(record)
		if got != tc.want {
			t.Errorf("repairKind(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestIsRepairRecord(t *testing.T) {
	extents := []metadata.Extent{{ExtentID: "e1"}}
	cases := []struct {
		record Record
		want   bool
	}{
		{Record{LogicalPath: repairPathPrefixExtent + "e1", Extents: extents}, true},
		{Record{LogicalPath: repairPathPrefixParity + "pg-1", Extents: extents}, true},
		{Record{LogicalPath: repairPathPrefixExtent + "e1"}, false},                          // no extents
		{Record{LogicalPath: repairPathPrefixExtent + "e1", Extents: extents, File: &metadata.FileRecord{}}, false}, // has file
		{Record{LogicalPath: "/data/file.txt", Extents: extents}, false},                     // wrong prefix
	}
	for _, tc := range cases {
		got := isRepairRecord(tc.record)
		if got != tc.want {
			t.Errorf("isRepairRecord(%q) = %v, want %v", tc.record.LogicalPath, got, tc.want)
		}
	}
}
