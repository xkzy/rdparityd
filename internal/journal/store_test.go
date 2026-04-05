package journal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreAppendAndLoadRoundTrip(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "journal.log"))

	written, err := store.Append(Record{
		TxID:              "tx-1",
		State:             StatePrepared,
		Timestamp:         time.Unix(100, 0).UTC(),
		OldGeneration:     1,
		NewGeneration:     2,
		AffectedExtentIDs: []string{"extent-1"},
	})
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if written.RecordChecksum == "" {
		t.Fatal("expected a record checksum to be populated")
	}

	records, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].TxID != "tx-1" || records[0].State != StatePrepared {
		t.Fatalf("unexpected record loaded: %#v", records[0])
	}
}

func TestStoreLoadRejectsTamperedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	store := NewStore(path)

	_, err := store.Append(Record{
		TxID:      "tx-2",
		State:     StatePrepared,
		Timestamp: time.Unix(200, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	// The payload starts after the 4-byte RecordLen prefix and the 72-byte
	// binary header (offset 76). Flip a byte in the payload so the BLAKE3
	// record hash no longer matches, triggering a checksum mismatch on Load.
	if len(contents) <= 76 {
		t.Fatalf("journal file too short to tamper (%d bytes)", len(contents))
	}
	contents[76] ^= 0xFF
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err = store.Load()
	if err == nil {
		t.Fatal("expected checksum validation error")
	}
}

func TestReplaySummarizesIncompleteTransactions(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "journal.log"))
	entries := []Record{
		{TxID: "tx-complete", State: StatePrepared, Timestamp: time.Unix(300, 0).UTC()},
		{TxID: "tx-complete", State: StateDataWritten, Timestamp: time.Unix(301, 0).UTC()},
		{TxID: "tx-complete", State: StateParityWritten, Timestamp: time.Unix(302, 0).UTC()},
		{TxID: "tx-complete", State: StateMetadataWritten, Timestamp: time.Unix(303, 0).UTC()},
		{TxID: "tx-complete", State: StateCommitted, Timestamp: time.Unix(304, 0).UTC()},
		{TxID: "tx-replay", State: StatePrepared, Timestamp: time.Unix(400, 0).UTC()},
		{TxID: "tx-replay", State: StateDataWritten, Timestamp: time.Unix(401, 0).UTC()},
	}
	for _, entry := range entries {
		if _, err := store.Append(entry); err != nil {
			t.Fatalf("Append returned error: %v", err)
		}
	}

	summary, err := store.Replay()
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if !summary.RequiresReplay {
		t.Fatal("expected replay to be required")
	}
	if len(summary.IncompleteTxIDs) != 1 || summary.IncompleteTxIDs[0] != "tx-replay" {
		t.Fatalf("unexpected incomplete transactions: %#v", summary.IncompleteTxIDs)
	}
	if len(summary.Actions) != 1 {
		t.Fatalf("expected 1 replay action, got %d", len(summary.Actions))
	}
	if summary.Actions[0].Outcome != StateReplayRequired {
		t.Fatalf("expected replay-required outcome, got %q", summary.Actions[0].Outcome)
	}
	if !strings.Contains(summary.Actions[0].Recommendation, "parity") {
		t.Fatalf("expected parity guidance, got %q", summary.Actions[0].Recommendation)
	}
}
