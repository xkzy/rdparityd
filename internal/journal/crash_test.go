package journal

// crash_test.go — Phase 3: crash-injection integration tests.
//
// Every test in this file exercises the full write → crash → recover cycle.
// After each recovery the test runs CheckIntegrityInvariants so the reader can
// see exactly which invariant a regression would break.
//
// Organisation
//   TestCrashAtEveryStateAllInvariantsPassAfterRecovery   — table-driven: crash×state
//   TestCrashReadbackAfterRecovery                         — bytes round-trip after recovery
//   TestRecoveryIsIdempotent                               — two Recover calls are safe
//   TestMultipleCrashedWritesAllHealedByOneRecover         — N in-flight crashes, one Recover
//   TestCommittedWriteUnaffectedByCrashedFollower          — first commit stable after crash of second
//   TestBitRotAfterCommitHealedByScrub                    — E1 → Scrub → invariants + readback
//   TestParityCorruptionHealedByScrub                     — P2/P3 → Scrub → invariants
//   TestAllCrashPointsAcrossPayloadSizes                  — crash × size matrix

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── helpers local to this file ──────────────────────────────────────────────

// crashAndRecover writes a file, crashes at crashAfter, then calls Recover on a
// fresh coordinator (simulating a process restart). It returns the write result
// and the recovery result so callers can make further assertions.
func crashAndRecover(
	t *testing.T,
	dir string,
	logicalPath string,
	payload []byte,
	sizeBytes int64,
	crashAfter State,
) (WriteResult, RecoveryResult) {
	t.Helper()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Step 1: Write with injected crash.
	writeResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: logicalPath,
		Payload:     payload,
		SizeBytes:   sizeBytes,
		FailAfter:   crashAfter,
	})
	if err != nil {
		t.Fatalf("WriteFile (crash=%s): %v", crashAfter, err)
	}
	if writeResult.FinalState != crashAfter {
		t.Fatalf("expected final state %s after crash injection, got %s", crashAfter, writeResult.FinalState)
	}

	// Step 2: Simulate process restart by creating a new coordinator.
	recoveryResult, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover (crash=%s): %v", crashAfter, err)
	}
	return writeResult, recoveryResult
}

// assertInvariantsClean runs the full integrity check and fails the test if any
// violations are found.
func assertInvariantsClean(t *testing.T, dir string) {
	t.Helper()
	metadataPath := filepath.Join(dir, "metadata.json")
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load: %v", err)
	}
	vs := CheckIntegrityInvariants(dir, state)
	for _, v := range vs {
		t.Errorf("invariant violation after recovery: %s", v.Error())
	}

	records, err := NewStore(filepath.Join(dir, "journal.log")).Load()
	if err != nil {
		t.Fatalf("journal.Load: %v", err)
	}
	for _, v := range CheckJournalInvariants(records) {
		t.Errorf("journal invariant violation: %s", v.Error())
	}
}

// assertJournalClean fails the test if the journal has any incomplete
// transactions after recovery.
func assertJournalClean(t *testing.T, dir string) {
	t.Helper()
	summary, err := NewStore(filepath.Join(dir, "journal.log")).Replay()
	if err != nil {
		t.Fatalf("journal.Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal still requires replay after recovery: %+v", summary)
	}
}

// makePayload creates a repeating byte pattern of exactly n bytes.
func makePayload(n int, seed byte) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((int(seed) + i) % 251)
	}
	return data
}

// ─── TestCrashAtEveryStateAllInvariantsPassAfterRecovery ──────────────────────

// crashStateCase defines a single scenario in the table-driven crash test.
type crashStateCase struct {
	name       string
	crashAfter State
	// whether the transaction should appear in RecoveredTxIDs or AbortedTxIDs
	expectRecovered bool
}

var crashStateCases = []crashStateCase{
	{
		name:            "crash-after-prepared",
		crashAfter:      StatePrepared,
		expectRecovered: false, // prepared → abort (no data on disk yet)
	},
	{
		name:            "crash-after-data-written",
		crashAfter:      StateDataWritten,
		expectRecovered: true,
	},
	{
		name:            "crash-after-parity-written",
		crashAfter:      StateParityWritten,
		expectRecovered: true,
	},
	{
		name:            "crash-after-metadata-written",
		crashAfter:      StateMetadataWritten,
		expectRecovered: true,
	},
}

func TestCrashAtEveryStateAllInvariantsPassAfterRecovery(t *testing.T) {
	// Use a payload size that generates exactly two extents so parity is always
	// exercised.
	const size = (1 << 20) + 512
	payload := makePayload(size, 7)

	for _, tc := range crashStateCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			_, recoveryResult := crashAndRecover(
				t, dir, "/shares/demo/"+tc.name+".bin", payload, 0, tc.crashAfter,
			)

			if tc.expectRecovered {
				if len(recoveryResult.RecoveredTxIDs) != 1 {
					t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
				}
			} else {
				if len(recoveryResult.AbortedTxIDs) != 1 {
					t.Fatalf("expected 1 aborted tx, got %v", recoveryResult.AbortedTxIDs)
				}
			}

			assertJournalClean(t, dir)
			assertInvariantsClean(t, dir)
		})
	}
}

// ─── TestCrashReadbackAfterRecovery ──────────────────────────────────────────

// After crashing at a recoverable state and running recovery, the file's
// content should be exactly the bytes that were passed to WriteFile.
func TestCrashReadbackAfterRecovery(t *testing.T) {
	recoverableStates := []State{
		StateDataWritten,
		StateParityWritten,
		StateMetadataWritten,
	}
	const size = (1 << 20) + 256
	payload := makePayload(size, 13)

	for _, crashAfter := range recoverableStates {
		crashAfter := crashAfter
		t.Run(string(crashAfter), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			metadataPath := filepath.Join(dir, "metadata.json")
			journalPath := filepath.Join(dir, "journal.log")

			crashAndRecover(t, dir, "/shares/demo/readback.bin", payload, 0, crashAfter)

			// Read back the file on a fresh coordinator.
			readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/demo/readback.bin")
			if err != nil {
				t.Fatalf("ReadFile after recovery: %v", err)
			}
			if !readResult.Verified {
				t.Fatal("expected verified read after recovery")
			}
			if readResult.BytesRead != int64(len(payload)) {
				t.Fatalf("expected %d bytes, got %d", len(payload), readResult.BytesRead)
			}
			if !bytes.Equal(readResult.Data, payload) {
				// Find first difference for a useful error message.
				for i := range payload {
					if readResult.Data[i] != payload[i] {
						t.Fatalf("data mismatch at byte %d after %s crash+recovery: want %02x got %02x",
							i, crashAfter, payload[i], readResult.Data[i])
					}
				}
			}
		})
	}
}

// ─── TestRecoveryIsIdempotent ─────────────────────────────────────────────────

// Running Recover twice must be a no-op: invariants still pass, the journal
// contains no new incomplete transactions, and no data is corrupted.
func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	payload := makePayload((1<<20)+100, 17)

	// First: crash at data-written.
	_, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/idempotent.bin",
		Payload:     payload,
		FailAfter:   StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// First Recover.
	if _, err := NewCoordinator(metadataPath, journalPath).Recover(); err != nil {
		t.Fatalf("first Recover: %v", err)
	}

	// Second Recover — should be a no-op.
	if _, err := NewCoordinator(metadataPath, journalPath).Recover(); err != nil {
		t.Fatalf("second Recover: %v", err)
	}

	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)

	// Data must still be readable and correct.
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/demo/idempotent.bin")
	if err != nil {
		t.Fatalf("ReadFile after double recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after double recovery")
	}
}

// ─── TestMultipleCrashedWritesAllHealedByOneRecover ───────────────────────────

// A single Recover() call must handle multiple incomplete transactions in the
// journal simultaneously. Each crashed write must use unique extent IDs so that
// they don't overwrite each other's on-disk data.
//
// Scenario:
//  1. Commit baseline write (primes allocator, saves metadata)
//  2. Crash write A at prepared (abort candidate) — no files written
//  3. Crash write B at parity-written (roll-forward candidate) — files + parity on disk
//     Both A and B allocate the same "next" extents from the committed baseline,
//     but A never writes any files, so B's on-disk data is authoritative.
//  4. One Recover() heals both: A is aborted, B is rolled forward.
//  5. Baseline and B are both readable; A is absent.
func TestMultipleCrashedWritesAllHealedByOneRecover(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	// Step 1: commit a baseline write so the metadata is persisted and the
	// allocator counter is advanced past the extents used here.
	basePayload := makePayload(1<<20, 1)
	baseResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/multi/base.bin",
		Payload:     basePayload,
	})
	if err != nil || baseResult.FinalState != StateCommitted {
		t.Fatalf("baseline WriteFile: err=%v state=%s", err, baseResult.FinalState)
	}

	// Step 2: crash write A at prepared — only a journal record, no files.
	payloadA := makePayload((1<<20)+100, 5)
	_, err = NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/multi/a.bin",
		Payload:     payloadA,
		FailAfter:   StatePrepared,
	})
	if err != nil {
		t.Fatalf("crash write A: %v", err)
	}

	// Step 3: crash write B at parity-written — files and parity are on disk.
	// B allocates the same "next free" extents as A (since A never committed
	// metadata), but because A wrote no files B's data is the sole content on
	// disk for those extent IDs.
	payloadB := makePayload((1<<20)+200, 7)
	_, err = NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/multi/b.bin",
		Payload:     payloadB,
		FailAfter:   StateParityWritten,
	})
	if err != nil {
		t.Fatalf("crash write B: %v", err)
	}

	// Step 4: single Recover() call.
	result, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.RecoveredTxIDs) != 1 {
		t.Errorf("expected 1 recovered tx, got %v", result.RecoveredTxIDs)
	}
	if len(result.AbortedTxIDs) != 1 {
		t.Errorf("expected 1 aborted tx, got %v", result.AbortedTxIDs)
	}

	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)

	// Step 5: data integrity checks.
	coordinator := NewCoordinator(metadataPath, journalPath)

	readBase, err := coordinator.ReadFile("/shares/multi/base.bin")
	if err != nil {
		t.Fatalf("ReadFile base.bin: %v", err)
	}
	if !bytes.Equal(readBase.Data, basePayload) {
		t.Fatal("base.bin data corrupted by multi-crash recovery")
	}

	readB, err := coordinator.ReadFile("/shares/multi/b.bin")
	if err != nil {
		t.Fatalf("ReadFile b.bin after recovery: %v", err)
	}
	if !bytes.Equal(readB.Data, payloadB) {
		t.Fatal("b.bin data mismatch after recovery")
	}

	// a.bin was aborted — it must not be visible.
	if _, err := coordinator.ReadFile("/shares/multi/a.bin"); err == nil {
		t.Fatal("expected a.bin to be absent after abort, but ReadFile succeeded")
	}
}

// ─── TestCommittedWriteUnaffectedByCrashedFollower ────────────────────────────

// A committed write must be byte-for-byte stable after a subsequent write
// crashes and is recovered.
func TestCommittedWriteUnaffectedByCrashedFollower(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	// First write — committed cleanly.
	firstPayload := makePayload((1<<20)+100, 3)
	firstResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/first.bin",
		Payload:     firstPayload,
	})
	if err != nil || firstResult.FinalState != StateCommitted {
		t.Fatalf("first WriteFile failed or not committed: err=%v state=%s", err, firstResult.FinalState)
	}

	// Second write — crashes at parity-written.
	secondPayload := makePayload((1<<20)+200, 5)
	_, err = coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/second.bin",
		Payload:     secondPayload,
		FailAfter:   StateParityWritten,
	})
	if err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}

	// Recovery heals the second write.
	if _, err := NewCoordinator(metadataPath, journalPath).Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	assertInvariantsClean(t, dir)

	// First file must still round-trip correctly.
	newCoord := NewCoordinator(metadataPath, journalPath)
	readResult, err := newCoord.ReadFile("/shares/demo/first.bin")
	if err != nil {
		t.Fatalf("ReadFile first.bin after recovery: %v", err)
	}
	if !bytes.Equal(readResult.Data, firstPayload) {
		t.Fatal("first committed file corrupted by recovery of second file")
	}

	// Second file must also be readable after recovery.
	readResult2, err := newCoord.ReadFile("/shares/demo/second.bin")
	if err != nil {
		t.Fatalf("ReadFile second.bin after recovery: %v", err)
	}
	if !bytes.Equal(readResult2.Data, secondPayload) {
		t.Fatal("second recovered file has wrong content")
	}
}

// ─── TestBitRotAfterCommitHealedByScrub ──────────────────────────────────────

// After a successful commit, silently corrupting an extent file should be
// detected by E1 and fully healed by Scrub(repair=true).
func TestBitRotAfterCommitHealedByScrub(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	payload := makePayload((1<<20)+100, 11)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/bitrot.bin",
		Payload:     payload,
	})
	if err != nil || writeResult.FinalState != StateCommitted {
		t.Fatalf("WriteFile: err=%v state=%s", err, writeResult.FinalState)
	}

	// Corrupt the first extent by flipping every byte.
	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	original, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent: %v", err)
	}
	corrupted := make([]byte, len(original))
	for i, b := range original {
		corrupted[i] = ^b
	}
	if err := os.WriteFile(extentPath, corrupted, 0o600); err != nil {
		t.Fatalf("write corrupted extent: %v", err)
	}

	// E1 should now fire.
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load: %v", err)
	}
	vs := CheckIntegrityInvariants(dir, state)
	foundE1 := false
	for _, v := range vs {
		if v.Code == "E1" {
			foundE1 = true
		}
	}
	if !foundE1 {
		t.Fatal("expected E1 violation after bit-rot, got none")
	}

	// Scrub with repair=true must heal the extent.
	scrubResult, err := coordinator.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if scrubResult.HealedCount == 0 {
		t.Fatalf("expected Scrub to heal at least one extent, got %+v", scrubResult)
	}

	// All invariants must now pass.
	assertInvariantsClean(t, dir)

	// And the file must read back correctly.
	readResult, err := coordinator.ReadFile("/shares/demo/bitrot.bin")
	if err != nil {
		t.Fatalf("ReadFile after scrub: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after bit-rot + scrub repair")
	}
}

// ─── TestParityCorruptionHealedByScrub ───────────────────────────────────────

// Silently corrupting the parity file must be detected by P2 and P3, and
// healed by Scrub(repair=true) without affecting the data extents.
func TestParityCorruptionHealedByScrub(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coordinator := NewCoordinator(metadataPath, journalPath)

	payload := makePayload((1<<20)+100, 19)
	writeResult, err := coordinator.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/paritycorrupt.bin",
		Payload:     payload,
	})
	if err != nil || writeResult.FinalState != StateCommitted {
		t.Fatalf("WriteFile: err=%v state=%s", err, writeResult.FinalState)
	}

	// Corrupt the parity file by flipping every byte.
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load: %v", err)
	}
	if len(state.ParityGroups) == 0 {
		t.Fatal("no parity groups in state")
	}
	parityPath := filepath.Join(dir, "parity", state.ParityGroups[0].ParityGroupID+".bin")
	parityData, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("read parity: %v", err)
	}
	for i := range parityData {
		parityData[i] = ^parityData[i]
	}
	if err := os.WriteFile(parityPath, parityData, 0o600); err != nil {
		t.Fatalf("write corrupted parity: %v", err)
	}

	// P2 and P3 should now fire.
	state2, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load: %v", err)
	}
	vs := CheckIntegrityInvariants(dir, state2)
	foundP := false
	for _, v := range vs {
		if v.Code == "P2" || v.Code == "P3" {
			foundP = true
		}
	}
	if !foundP {
		t.Fatal("expected P2 or P3 violation after parity corruption, got none")
	}

	// Scrub with repair must regenerate parity from data extents.
	scrubResult, err := coordinator.Scrub(true)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if scrubResult.HealedCount == 0 {
		t.Fatalf("expected Scrub to heal at least one parity group, got %+v", scrubResult)
	}

	assertInvariantsClean(t, dir)

	// Data extents must be unaffected — file reads back correctly.
	readResult, err := coordinator.ReadFile("/shares/demo/paritycorrupt.bin")
	if err != nil {
		t.Fatalf("ReadFile after parity scrub: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after parity corruption + scrub")
	}
}

// ─── TestAllCrashPointsAcrossPayloadSizes ────────────────────────────────────

// Matrix test: every crash point × three payload sizes.
// Each cell must produce a system where all invariants pass after recovery,
// and recoverable cells must produce readable, correct data.
func TestAllCrashPointsAcrossPayloadSizes(t *testing.T) {
	payloadSizes := []struct {
		label string
		size  int
	}{
		{"tiny-512b", 512},
		{"single-extent-1mib", 1 << 20},
		{"multi-extent-2mib+", (1 << 20) * 2},
	}

	for _, pz := range payloadSizes {
		pz := pz
		for _, tc := range crashStateCases {
			tc := tc
			testName := fmt.Sprintf("%s/%s", pz.label, tc.name)
			t.Run(testName, func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				metadataPath := filepath.Join(dir, "metadata.json")
				journalPath := filepath.Join(dir, "journal.log")

				payload := makePayload(pz.size, 23)

				_, err := NewCoordinator(metadataPath, journalPath).WriteFile(WriteRequest{
					PoolName:    "demo",
					LogicalPath: "/shares/demo/matrix.bin",
					Payload:     payload,
					FailAfter:   tc.crashAfter,
				})
				if err != nil {
					t.Fatalf("WriteFile: %v", err)
				}

				if _, err := NewCoordinator(metadataPath, journalPath).Recover(); err != nil {
					t.Fatalf("Recover: %v", err)
				}

				assertJournalClean(t, dir)
				assertInvariantsClean(t, dir)

				if !tc.expectRecovered {
					return // prepared → aborted, file was never written
				}

				readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/demo/matrix.bin")
				if err != nil {
					t.Fatalf("ReadFile: %v", err)
				}
				if !bytes.Equal(readResult.Data, payload) {
					t.Fatalf("data mismatch in %s: got %d bytes", testName, readResult.BytesRead)
				}
			})
		}
	}
}
