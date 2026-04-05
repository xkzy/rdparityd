package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// writeAndCommit runs a full WriteFile transaction and returns the result.
func writeAndCommit(t *testing.T, metadataPath, journalPath, logicalPath string, sizeBytes int64) WriteResult {
	t.Helper()
	result, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: logicalPath,
		SizeBytes:   sizeBytes,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	return result
}

// loadState loads the metadata state from disk.
func loadState(t *testing.T, metadataPath string) metadata.SampleState {
	t.Helper()
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load returned error: %v", err)
	}
	return state
}

// assertNoViolations fails the test if any violations are present.
func assertNoViolations(t *testing.T, vs []InvariantViolation) {
	t.Helper()
	for _, v := range vs {
		t.Errorf("unexpected invariant violation: %s", v.Error())
	}
}

// assertViolation fails the test unless at least one violation with the given
// code is present.
func assertViolation(t *testing.T, vs []InvariantViolation, code string) {
	t.Helper()
	for _, v := range vs {
		if v.Code == code {
			return
		}
	}
	t.Errorf("expected invariant violation with code %q, got %v", code, vs)
}

// ─── CheckStateInvariants: clean-state baseline ───────────────────────────────

func TestStateInvariantsPassAfterCommit(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/clean.bin", (1<<20)+100)

	state := loadState(t, metadataPath)
	assertNoViolations(t, CheckStateInvariants(state))
}

func TestIntegrityInvariantsPassAfterCommit(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/integrity.bin", (1<<20)+100)

	state := loadState(t, metadataPath)
	assertNoViolations(t, CheckIntegrityInvariants(dir, state))
}

func TestJournalInvariantsPassAfterCommit(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/journal.bin", 4096)

	records, err := NewStore(journalPath).Load()
	if err != nil {
		t.Fatalf("journal.Load returned error: %v", err)
	}
	assertNoViolations(t, CheckJournalInvariants(records))
}

// ─── E1: extent checksum vs on-disk data ─────────────────────────────────────

func TestE1DetectsCorruptExtentData(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	result := writeAndCommit(t, metadataPath, journalPath, "/shares/demo/e1.bin", 4096)
	state := loadState(t, metadataPath)

	// Corrupt the first byte of the first extent file.
	extentPath := filepath.Join(dir, result.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	vs := CheckIntegrityInvariants(dir, state)
	assertViolation(t, vs, "E1")
}

func TestE1DetectsMissingExtentFile(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	result := writeAndCommit(t, metadataPath, journalPath, "/shares/demo/e1-missing.bin", 4096)
	state := loadState(t, metadataPath)

	extentPath := filepath.Join(dir, result.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}

	vs := CheckIntegrityInvariants(dir, state)
	assertViolation(t, vs, "E1")
}

// ─── E2: checksum algorithm ───────────────────────────────────────────────────

func TestE2DetectsWrongChecksumAlg(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/e2.bin", 4096)
	state := loadState(t, metadataPath)

	// Tamper with the checksum_alg field.
	state.Extents[0].ChecksumAlg = "blake3"

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "E2")
}

// ─── E3: positive extent length ───────────────────────────────────────────────

func TestE3DetectsZeroLength(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/e3.bin", 4096)
	state := loadState(t, metadataPath)

	state.Extents[0].Length = 0

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "E3")
}

// ─── M1: extent FileID must exist ─────────────────────────────────────────────

func TestM1DetectsOrphanedExtent(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/m1.bin", 4096)
	state := loadState(t, metadataPath)

	// Remove the file but leave the extent.
	state.Files = nil

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "M1")
}

// ─── M2: extent DataDiskID must exist ────────────────────────────────────────

func TestM2DetectsUnknownDisk(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/m2.bin", 4096)
	state := loadState(t, metadataPath)

	// Point the extent at a nonexistent disk.
	state.Extents[0].DataDiskID = "disk-does-not-exist"

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "M2")
}

// ─── M3: unique extent IDs ────────────────────────────────────────────────────

func TestM3DetectsDuplicateExtentID(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/m3.bin", 4096)
	state := loadState(t, metadataPath)

	// Inject a duplicate.
	dup := state.Extents[0]
	state.Extents = append(state.Extents, dup)

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "M3")
}

// ─── P1: parity group cross-references ───────────────────────────────────────

func TestP1DetectsDanglingParityGroupID(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/p1.bin", 4096)
	state := loadState(t, metadataPath)

	// Point the extent at a group that does not exist.
	state.Extents[0].ParityGroupID = "pg-does-not-exist"

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "P1")
}

func TestP1DetectsDanglingMemberExtentID(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/p1b.bin", 4096)
	state := loadState(t, metadataPath)

	// Inject a ghost member into the first parity group.
	state.ParityGroups[0].MemberExtentIDs = append(
		state.ParityGroups[0].MemberExtentIDs, "extent-ghost")

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "P1")
}

// ─── P2: parity file checksum ─────────────────────────────────────────────────

func TestP2DetectsCorruptParityFile(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/p2.bin", 4096)
	state := loadState(t, metadataPath)

	// Corrupt the parity file for the first group.
	parityPath := filepath.Join(dir, "parity", state.ParityGroups[0].ParityGroupID+".bin")
	data, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(parityPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	vs := CheckIntegrityInvariants(dir, state)
	assertViolation(t, vs, "P2")
}

// ─── P3: parity equals XOR of members ────────────────────────────────────────

func TestP3DetectsStaleParityAfterExtentChange(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	result := writeAndCommit(t, metadataPath, journalPath, "/shares/demo/p3.bin", 4096)
	state := loadState(t, metadataPath)

	// Silently overwrite an extent file with different content WITHOUT updating
	// parity. This simulates an out-of-band write that bypasses the journal,
	// making parity stale.
	extentPath := filepath.Join(dir, result.Extents[0].PhysicalLocator.RelativePath)
	altData := make([]byte, 4096)
	for i := range altData {
		altData[i] = 0xAB
	}
	// Update the metadata checksum to match the new data and persist the state
	// so E1 does not fire (we want to isolate P3). Persisting also makes the
	// test robust if CheckIntegrityInvariants ever loads state from disk.
	state.Extents[0].Checksum = digestBytes(altData)
	if _, err := metadata.NewStore(metadataPath).Save(state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if err := os.WriteFile(extentPath, altData, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	vs := CheckIntegrityInvariants(dir, state)
	assertViolation(t, vs, "P3")
}

// ─── P4: no two extents in the same group on the same disk ───────────────────

func TestP4DetectsDiskAliasingInParityGroup(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Write enough data to get multiple extents (> 1 MiB default extent size).
	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/p4.bin", (1<<20)+100)
	state := loadState(t, metadataPath)

	// Find the parity group that has at least two members, or create a violation
	// by injecting a second extent on the same disk into an existing group.
	firstExtent := state.Extents[0]

	// Inject a ghost extent that lives on the same disk as the first extent but
	// is registered in the same parity group — this is the invariant violation.
	ghost := metadata.Extent{
		ExtentID:      "extent-ghost-p4",
		FileID:        firstExtent.FileID,
		DataDiskID:    firstExtent.DataDiskID, // same disk — violation
		ParityGroupID: firstExtent.ParityGroupID,
		Length:        firstExtent.Length,
		ChecksumAlg:   ChecksumAlgorithm,
	}
	state.Extents = append(state.Extents, ghost)
	for i := range state.ParityGroups {
		if state.ParityGroups[i].ParityGroupID == firstExtent.ParityGroupID {
			state.ParityGroups[i].MemberExtentIDs = append(
				state.ParityGroups[i].MemberExtentIDs, ghost.ExtentID)
			break
		}
	}

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "P4")
}

// ─── J1: journal record checksums ────────────────────────────────────────────

func TestJ1DetectsTamperedJournalRecord(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/j1.bin", 4096)

	// Reload the records via the Store; the raw journal file was verified on load.
	// To test J1 we build a record with a wrong checksum directly.
	badRecords := []Record{
		{
			Magic:           RecordMagic,
			Version:         RecordVersion,
			TxID:            "tx-tampered",
			State:           StatePrepared,
			PayloadChecksum: "aaaa",
			RecordChecksum:  "bbbb",
		},
	}

	vs := CheckJournalInvariants(badRecords)
	assertViolation(t, vs, "J1")
}

// ─── J2: state machine transitions ───────────────────────────────────────────

func TestJ2DetectsInvalidStateTransition(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Write one real commit so the journal has valid records.
	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/j2.bin", 4096)

	// Inject a record with an illegal transition (committed → prepared).
	badRecords := []Record{
		{
			Magic:   RecordMagic,
			Version: RecordVersion,
			TxID:    "tx-bad-transition",
			State:   StateCommitted,
		},
		{
			Magic:   RecordMagic,
			Version: RecordVersion,
			TxID:    "tx-bad-transition",
			State:   StatePrepared,
		},
	}
	// Seal the records so J1 does not fire (we want to isolate J2).
	for i := range badRecords {
		sealed, err := sealRecord(badRecords[i])
		if err != nil {
			t.Fatalf("sealRecord returned error: %v", err)
		}
		badRecords[i] = sealed
	}

	vs := CheckJournalInvariants(badRecords)
	assertViolation(t, vs, "J2")
}

// ─── J3: no replay-required flag after recovery ───────────────────────────────

func TestJ3DetectsLingeringReplayRequired(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	writeAndCommit(t, metadataPath, journalPath, "/shares/demo/j3.bin", 4096)
	state := loadState(t, metadataPath)

	// Simulate a transaction that was not properly cleaned up.
	state.Transactions[0].ReplayRequired = true

	vs := CheckStateInvariants(state)
	assertViolation(t, vs, "J3")
}

func TestJ3PassesAfterSuccessfulRecovery(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Write a transaction that crashes at data-written.
	_, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/j3-crash.bin",
		SizeBytes:   4096,
		FailAfter:   StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	// Run recovery.
	if _, err := NewCoordinator(metadataPath, journalPath).Recover(); err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}

	// J3 should pass after recovery.
	state := loadState(t, metadataPath)
	vs := CheckStateInvariants(state)
	for _, v := range vs {
		if v.Code == "J3" {
			t.Errorf("unexpected J3 violation after recovery: %s", v.Error())
		}
	}
}
