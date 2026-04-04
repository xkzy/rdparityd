package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rtparityd/rtparityd/internal/journal"
)

func TestLoadRuntimeStateMarksReplayRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	store := journal.NewStore(path)

	entries := []journal.Record{
		{TxID: "tx-replay", State: journal.StatePrepared, Timestamp: time.Unix(100, 0).UTC()},
		{TxID: "tx-replay", State: journal.StateDataWritten, Timestamp: time.Unix(101, 0).UTC()},
	}
	for _, entry := range entries {
		if _, err := store.Append(entry); err != nil {
			t.Fatalf("Append returned error: %v", err)
		}
	}

	state := loadRuntimeState("demo", path, filepath.Join(t.TempDir(), "metadata.json"))
	if state.healthStatus() != "degraded" {
		t.Fatalf("expected degraded health, got %q", state.healthStatus())
	}
	if !state.JournalSummary.RequiresReplay {
		t.Fatal("expected replay to be required")
	}
}

func TestLoadRuntimeStateCreatesMetadataSnapshot(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")

	state := loadRuntimeState("demo", journalPath, metadataPath)
	if state.MetadataPath != metadataPath {
		t.Fatalf("unexpected metadata path: %q", state.MetadataPath)
	}
	if _, err := os.Stat(metadataPath); err != nil {
		t.Fatalf("expected metadata snapshot to exist: %v", err)
	}
	if len(state.Prototype.Disks) == 0 {
		t.Fatal("expected prototype disks to be loaded")
	}
}

func TestJournalEndpointReturnsReplaySummary(t *testing.T) {
	state := runtimeState{
		MetadataPath: "/tmp/test-metadata.json",
		JournalPath:  "/tmp/test-journal.log",
		JournalSummary: journal.ReplaySummary{
			TotalRecords:    2,
			RequiresReplay:  true,
			IncompleteTxIDs: []string{"tx-replay"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/journal", nil)
	rr := httptest.NewRecorder()
	newMux(state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["journal_path"] != "/tmp/test-journal.log" {
		t.Fatalf("unexpected journal path: %#v", body["journal_path"])
	}
}
