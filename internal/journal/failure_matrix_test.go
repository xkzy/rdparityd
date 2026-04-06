package journal

// failure_matrix_test.go — Category A crash boundary tests (A1-A14).
//
// This file implements comprehensive crash-injection tests that exercise boundary
// conditions across the full write lifecycle. Each test follows the pattern:
//   1. WriteFile with crash injection (crash at specific state or during sub-phase)
//   2. Recover() on a fresh coordinator (simulating process restart)
//   3. CheckIntegrityInvariants() to verify recovery maintained consistency
//   4. Data round-trip verification for recoverable states

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── Helper functions ────────────────────────────────────────────────────────

func crashAndRecover(t *testing.T, dir string, logicalPath string, payload []byte, sizeBytes int64, crashAfter State) (WriteResult, RecoveryResult) {
	t.Helper()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	writeResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    logicalPath,
		AllowSynthetic: true,
		Payload:        payload,
		SizeBytes:      sizeBytes,
		FailAfter:      crashAfter,
	})
	if err != nil {
		t.Fatalf("WriteFile (crash=%s): %v", crashAfter, err)
	}
	if writeResult.FinalState != crashAfter {
		t.Fatalf("expected final state %s, got %s", crashAfter, writeResult.FinalState)
	}
	recoveryResult, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover (crash=%s): %v", crashAfter, err)
	}
	return writeResult, recoveryResult
}

func assertInvariantsClean(t *testing.T, dir string) {
	t.Helper()
	metadataPath := filepath.Join(dir, "metadata.json")
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
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

func makePayload(n int, seed byte) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((int(seed) + i) % 251)
	}
	return data
}

// ─── Category A: Crash Boundary Tests ────────────────────────────────────────

// TestA1_EmptyJournalNoPrepareRecord: Verify system boots cleanly with no journal records.
func TestA1_EmptyJournalNoPrepareRecord(t *testing.T) {
	dir := t.TempDir()
	assertJournalClean(t, dir)
}

// TestA2_CrashAfterPrepareSmallPayload: Crash after prepare with small payload.
func TestA2_CrashAfterPrepareSmallPayload(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload(512, 11)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a2.bin", payload, 0, StatePrepared)
	if len(recoveryResult.AbortedTxIDs) != 1 {
		t.Fatalf("expected 1 aborted tx, got %v", recoveryResult.AbortedTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
}

// TestA3_CrashAfterPrepareMultiExtent: Crash after prepare with multi-extent payload.
func TestA3_CrashAfterPrepareMultiExtent(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+512, 13)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a3.bin", payload, 0, StatePrepared)
	if len(recoveryResult.AbortedTxIDs) != 1 {
		t.Fatalf("expected 1 aborted tx, got %v", recoveryResult.AbortedTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
}

// TestA4_CrashMidDataWriteExtentLimit1: Crash during extent file write using extentWriteLimit=1.
func TestA4_CrashMidDataWriteExtentLimit1(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	payload := makePayload((1<<20)+100, 17)
	writeResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:         "demo",
		LogicalPath:      "/shares/test/a4.bin",
		AllowSynthetic:   true,
		Payload:          payload,
		extentWriteLimit: 1,
	})
	if err == nil {
		t.Fatalf("expected WriteFile to return error due to crash injection")
	}
	if writeResult.FinalState == StateCommitted {
		t.Fatalf("expected WriteFile to not reach committed state")
	}
	recoveryResult, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recoveryResult.AbortedTxIDs)+len(recoveryResult.RecoveredTxIDs) == 0 {
		t.Fatalf("expected recovery to handle the incomplete transaction")
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
}

// TestA6_CrashAfterDataWritten: Crash after StateDataWritten.
func TestA6_CrashAfterDataWritten(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+256, 19)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a6.bin", payload, 0, StateDataWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/a6.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after A6")
	}
}

// TestA7_CrashMidParityWriteParityLimit1: Crash during parity file write using parityWriteLimit=1.
func TestA7_CrashMidParityWriteParityLimit1(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	payload := makePayload((2<<20)+100, 23)
	_, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:         "demo",
		LogicalPath:      "/shares/test/a7.bin",
		AllowSynthetic:   true,
		Payload:          payload,
		parityWriteLimit: 1,
	})
	if err == nil {
		t.Fatalf("expected WriteFile to return error due to crash injection")
	}
	recoveryResult, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recoveryResult.AbortedTxIDs)+len(recoveryResult.RecoveredTxIDs) == 0 {
		t.Fatalf("expected recovery to handle the incomplete transaction")
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
}

// TestA9_StateTransitionPreparedToDataWritten: Verify state transition prep->data.
func TestA9_StateTransitionPreparedToDataWritten(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+100, 29)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a9.bin", payload, 0, StateDataWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
}

// TestA10_StateTransitionDataWrittenToParityWritten: Verify state transition data->parity.
func TestA10_StateTransitionDataWrittenToParityWritten(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+200, 31)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a10.bin", payload, 0, StateParityWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/a10.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after A10")
	}
}

// TestA11_StateTransitionParityWrittenToMetadataWritten: Verify state transition parity->metadata.
func TestA11_StateTransitionParityWrittenToMetadataWritten(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+300, 37)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a11.bin", payload, 0, StateMetadataWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/a11.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after A11")
	}
}

// TestA12_StateTransitionMetadataWrittenToCommitted: Happy path - clean commit.
func TestA12_StateTransitionMetadataWrittenToCommitted(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	payload := makePayload((1<<20)+400, 41)
	writeResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/test/a12.bin",
		AllowSynthetic: true,
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if writeResult.FinalState != StateCommitted {
		t.Fatalf("expected StateCommitted, got %s", writeResult.FinalState)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/a12.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch in A12")
	}
}

// TestA13_CrashAfterParityMetadataIntegrity: Crash after parity, verify metadata integrity.
func TestA13_CrashAfterParityMetadataIntegrity(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+500, 43)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a13.bin", payload, 0, StateParityWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	metadataPath := filepath.Join(dir, "metadata.json")
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		t.Fatalf("metadata.Load: %v", err)
	}
	if len(state.ParityGroups) == 0 {
		t.Fatalf("expected parity groups, got none")
	}
	parityViolations := CheckIntegrityInvariants(dir, state)
	for _, v := range parityViolations {
		if v.Kind == "parity" {
			t.Errorf("unexpected parity violation after A13: %s", v.Error())
		}
	}
}

// TestA14_CrashAfterMetadataDataIntegrity: Crash after metadata, verify data integrity.
func TestA14_CrashAfterMetadataDataIntegrity(t *testing.T) {
	dir := t.TempDir()
	payload := makePayload((1<<20)+600, 47)
	_, recoveryResult := crashAndRecover(t, dir, "/shares/test/a14.bin", payload, 0, StateMetadataWritten)
	if len(recoveryResult.RecoveredTxIDs) != 1 {
		t.Fatalf("expected 1 recovered tx, got %v", recoveryResult.RecoveredTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/a14.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !readResult.Verified {
		t.Errorf("expected verified=true after A14 recovery")
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch in A14")
	}
}

// TestAllCategoryACrashPoints: Table-driven test combining crash points × payload sizes.
func TestAllCategoryACrashPoints(t *testing.T) {
	testCases := []struct {
		name       string
		payloadLen int
		crashState State
		expectRec  bool
		expectRead bool
	}{
		{"A2-prepared-512b", 512, StatePrepared, false, false},
		{"A3-prepared-1mib", 1 << 20, StatePrepared, false, false},
		{"A6-data-written-512b", 512, StateDataWritten, true, true},
		{"A6-data-written-1mib", 1 << 20, StateDataWritten, true, true},
		{"A6-data-written-2mib", 2 << 20, StateDataWritten, true, true},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			metadataPath := filepath.Join(dir, "metadata.json")
			journalPath := filepath.Join(dir, "journal.log")
			payload := makePayload(tc.payloadLen, 53)
			_, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
				PoolName:       "demo",
				LogicalPath:    "/shares/test/" + tc.name + ".bin",
				AllowSynthetic: true,
				Payload:        payload,
				FailAfter:      tc.crashState,
			})
			if err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			recoveryResult, err := NewCoordinator(metadataPath, journalPath).Recover()
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if tc.expectRec {
				if len(recoveryResult.RecoveredTxIDs) == 0 {
					t.Fatalf("expected recovered tx, got none: %+v", recoveryResult)
				}
			} else {
				if len(recoveryResult.AbortedTxIDs) == 0 {
					t.Fatalf("expected aborted tx, got none: %+v", recoveryResult)
				}
			}
			assertJournalClean(t, dir)
			assertInvariantsClean(t, dir)
			if !tc.expectRead {
				return
			}
			readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/" + tc.name + ".bin")
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if !bytes.Equal(readResult.Data, payload) {
				t.Fatalf("data mismatch in %s", tc.name)
			}
		})
	}
}

// TestA_RecoveryIdempotentAfterCrash: Verify calling Recover() twice is safe.
func TestA_RecoveryIdempotentAfterCrash(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	payload := makePayload((1<<20)+100, 59)
	_, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/test/idempotent.bin",
		AllowSynthetic: true,
		Payload:        payload,
		FailAfter:      StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	result1, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	result2, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	// Second recover should be idempotent—same tx count or zero (already recovered).
	if len(result1.RecoveredTxIDs) > 0 && len(result2.RecoveredTxIDs) > 0 {
		if len(result1.RecoveredTxIDs) != len(result2.RecoveredTxIDs) {
			t.Fatalf("recovered tx counts differ: first=%d second=%d", len(result1.RecoveredTxIDs), len(result2.RecoveredTxIDs))
		}
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	readResult, err := NewCoordinator(metadataPath, journalPath).ReadFile("/shares/test/idempotent.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(readResult.Data, payload) {
		t.Fatal("data mismatch after double recovery")
	}
}

// TestA_MultipleCrashedWritesRecoveredTogether: Verify one Recover() handles multiple crashes.
func TestA_MultipleCrashedWritesRecoveredTogether(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	basePayload := makePayload((1 << 20), 61)
	baseResult, err := NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/multi/base.bin",
		AllowSynthetic: true,
		Payload:        basePayload,
	})
	if err != nil || baseResult.FinalState != StateCommitted {
		t.Fatalf("baseline WriteFile: err=%v state=%s", err, baseResult.FinalState)
	}
	payloadA := makePayload((1<<20)+100, 67)
	_, err = NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/multi/a.bin",
		AllowSynthetic: true,
		Payload:        payloadA,
		FailAfter:      StatePrepared,
	})
	if err != nil {
		t.Fatalf("crash write A: %v", err)
	}
	payloadB := makePayload((1<<20)+200, 71)
	_, err = NewCoordinator(metadataPath, journalPath).WriteFile(context.Background(), WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/multi/b.bin",
		AllowSynthetic: true,
		Payload:        payloadB,
		FailAfter:      StateParityWritten,
	})
	if err != nil {
		t.Fatalf("crash write B: %v", err)
	}
	result, err := NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.RecoveredTxIDs) != 1 {
		t.Errorf("expected 1 recovered tx, got %v", result.RecoveredTxIDs)
	}
	// New invariant: WriteFile now forces recovery/abort of any replay-required
	// journal state before allocating new extents. So the earlier prepared write
	// may already have been aborted during the second write's preflight recovery,
	// leaving nothing additional to abort in this final explicit Recover().
	if len(result.AbortedTxIDs) > 1 {
		t.Errorf("expected at most 1 aborted tx, got %v", result.AbortedTxIDs)
	}
	assertJournalClean(t, dir)
	assertInvariantsClean(t, dir)
	coordinator := NewCoordinator(metadataPath, journalPath)
	readBase, err := coordinator.ReadFile("/shares/multi/base.bin")
	if err != nil {
		t.Fatalf("ReadFile base.bin: %v", err)
	}
	if !bytes.Equal(readBase.Data, basePayload) {
		t.Fatal("base.bin corrupted")
	}
	readB, err := coordinator.ReadFile("/shares/multi/b.bin")
	if err != nil {
		t.Fatalf("ReadFile b.bin: %v", err)
	}
	if !bytes.Equal(readB.Data, payloadB) {
		t.Fatal("b.bin corrupted")
	}
	if _, err := coordinator.ReadFile("/shares/multi/a.bin"); err == nil {
		t.Fatal("expected a.bin to not exist after abort")
	}
}
