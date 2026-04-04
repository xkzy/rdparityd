package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/rtparityd/rtparityd/internal/journal"
)

func TestLoadRuntimeStateRollsForwardReplayRequiredWrite(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	result, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/startup-replay.bin",
		SizeBytes:   4096,
		FailAfter:   journal.StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if result.FinalState != journal.StateDataWritten {
		t.Fatalf("expected data-written state, got %q", result.FinalState)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	if state.healthStatus() != "ok" {
		t.Fatalf("expected startup replay to recover the pool, got %q", state.healthStatus())
	}
	if state.JournalSummary.RequiresReplay {
		t.Fatalf("expected replay to be resolved, got %#v", state.JournalSummary)
	}
	if len(state.Prototype.Files) != 1 || len(state.Prototype.Extents) == 0 {
		t.Fatalf("expected recovered metadata in runtime state, got files=%d extents=%d", len(state.Prototype.Files), len(state.Prototype.Extents))
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
