package journal

import (
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestValidateRecordSequenceEmpty(t *testing.T) {
	err := ValidateRecordSequence(nil)
	if err == nil {
		t.Fatal("expected error for empty sequence")
	}
}

func TestValidateRecordSequenceValidNormalTransaction(t *testing.T) {
	now := time.Now().UTC()
	records := []Record{
		{TxID: "tx-1", Timestamp: now, State: StatePrepared},
		{TxID: "tx-1", Timestamp: now, State: StateDataWritten},
		{TxID: "tx-1", Timestamp: now, State: StateParityWritten},
		{TxID: "tx-1", Timestamp: now, State: StateMetadataWritten},
		{TxID: "tx-1", Timestamp: now, State: StateCommitted},
	}

	if err := ValidateRecordSequence(records); err != nil {
		t.Fatalf("expected no error for valid sequence, got: %v", err)
	}
}

func TestValidateRecordSequenceInvalidTransition(t *testing.T) {
	now := time.Now().UTC()
	records := []Record{
		{TxID: "tx-1", Timestamp: now, State: StatePrepared},
		{TxID: "tx-1", Timestamp: now, State: StateCommitted}, // skips required steps
	}

	if err := ValidateRecordSequence(records); err == nil {
		t.Fatal("expected error for invalid transition prepared -> committed")
	}
}

func TestValidateRecordSequenceRepairTransaction(t *testing.T) {
	now := time.Now().UTC()
	// Repair records use a different valid sequence: Prepared -> DataWritten -> Committed
	records := []Record{
		{
			TxID:        "tx-repair-1",
			Timestamp:   now,
			State:       StatePrepared,
			LogicalPath: repairPathPrefixExtent + "ext-1",
			Extents:     []metadata.Extent{{ExtentID: "ext-1"}},
		},
		{
			TxID:        "tx-repair-1",
			Timestamp:   now,
			State:       StateDataWritten,
			LogicalPath: repairPathPrefixExtent + "ext-1",
			Extents:     []metadata.Extent{{ExtentID: "ext-1"}},
		},
		{
			TxID:        "tx-repair-1",
			Timestamp:   now,
			State:       StateCommitted,
			LogicalPath: repairPathPrefixExtent + "ext-1",
			Extents:     []metadata.Extent{{ExtentID: "ext-1"}},
		},
	}

	if err := ValidateRecordSequence(records); err != nil {
		t.Fatalf("expected no error for valid repair sequence, got: %v", err)
	}
}

func TestValidateRecordSequenceRepairPreparedToReplayRequired(t *testing.T) {
	now := time.Now().UTC()
	records := []Record{
		{
			TxID:        "tx-repair-2",
			Timestamp:   now,
			State:       StatePrepared,
			LogicalPath: repairPathPrefixExtent + "ext-2",
			Extents:     []metadata.Extent{{ExtentID: "ext-2"}},
		},
		{
			TxID:        "tx-repair-2",
			Timestamp:   now,
			State:       StateReplayRequired,
			LogicalPath: repairPathPrefixExtent + "ext-2",
			Extents:     []metadata.Extent{{ExtentID: "ext-2"}},
		},
		{
			TxID:        "tx-repair-2",
			Timestamp:   now,
			State:       StateCommitted,
			LogicalPath: repairPathPrefixExtent + "ext-2",
			Extents:     []metadata.Extent{{ExtentID: "ext-2"}},
		},
	}

	if err := ValidateRecordSequence(records); err != nil {
		t.Fatalf("expected no error for repair replay-required sequence, got: %v", err)
	}
}

func TestValidateSequenceValidTransitions(t *testing.T) {
	valid := [][]State{
		{StatePrepared, StateDataWritten, StateParityWritten, StateMetadataWritten, StateCommitted},
		{StatePrepared, StateAborted},
		{StatePrepared, StateReplayRequired, StateCommitted},
		{StateDataWritten, StateParityWritten},
	}

	for _, seq := range valid {
		if err := ValidateSequence(seq); err != nil {
			t.Errorf("expected no error for sequence %v, got: %v", seq, err)
		}
	}
}

func TestValidateSequenceInvalidTransitions(t *testing.T) {
	invalid := [][]State{
		{StatePrepared, StateCommitted},
		{StateCommitted, StatePrepared},
	}

	for _, seq := range invalid {
		if err := ValidateSequence(seq); err == nil {
			t.Errorf("expected error for sequence %v", seq)
		}
	}
}

func TestValidateSequenceEmpty(t *testing.T) {
	if err := ValidateSequence(nil); err == nil {
		t.Fatal("expected error for empty sequence")
	}
}

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from State
		to   State
		want bool
	}{
		{StatePrepared, StateDataWritten, true},
		{StatePrepared, StateAborted, true},
		{StatePrepared, StateCommitted, false},
		{StateDataWritten, StateParityWritten, true},
		{StateDataWritten, StateAborted, true},
		{StateParityWritten, StateMetadataWritten, true},
		{StateMetadataWritten, StateCommitted, true},
		{StateCommitted, StatePrepared, false},
	}

	for _, tc := range cases {
		got := CanTransition(tc.from, tc.to)
		if got != tc.want {
			t.Errorf("CanTransition(%q, %q) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}
