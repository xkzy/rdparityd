package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/journal"
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
	newMux(&state).ServeHTTP(rr, req)

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

func TestReadEndpointReturnsVerificationResult(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	_, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/http-read.bin",
		SizeBytes:   4096,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodGet, "/v1/read?path=/shares/demo/http-read.bin", nil)
	rr := httptest.NewRecorder()
	newMux(&state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["verified"] != true {
		t.Fatalf("expected verified=true, got %#v", body["verified"])
	}
}

func TestScrubEndpointReturnsRepairSummary(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/http-scrub.bin",
		SizeBytes:   4096,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	extentPath := filepath.Join(filepath.Dir(metadataPath), writeResult.Extents[0].PhysicalLocator.RelativePath)
	corrupted, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	corrupted[0] ^= 0xFF
	if err := os.WriteFile(extentPath, corrupted, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodPost, "/v1/scrub?repair=true", nil)
	rr := httptest.NewRecorder()
	newMux(&state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["healthy"] != true {
		t.Fatalf("expected healthy=true, got %#v", body["healthy"])
	}
	if body["healed_count"] != float64(1) {
		t.Fatalf("expected healed_count=1, got %#v", body["healed_count"])
	}
}

func TestScrubHistoryEndpointReturnsPersistedRuns(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	_, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/http-scrub-history.bin",
		SizeBytes:   4096,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := coordinator.Scrub(false); err != nil {
		t.Fatalf("Scrub returned error: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodGet, "/v1/scrub/history", nil)
	rr := httptest.NewRecorder()
	newMux(&state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["count"] != float64(1) {
		t.Fatalf("expected count=1, got %#v", body["count"])
	}
	history, ok := body["history"].([]any)
	if !ok || len(history) != 1 {
		t.Fatalf("expected one history entry, got %#v", body["history"])
	}
}

func TestRebuildEndpointRestoresMissingDataDiskExtent(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/http-rebuild.bin",
		SizeBytes:   (1 << 20) + 64,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	targetExtent := writeResult.Extents[0]
	extentPath := filepath.Join(filepath.Dir(metadataPath), targetExtent.PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodPost, "/v1/rebuild?disk="+targetExtent.DataDiskID, nil)
	rr := httptest.NewRecorder()
	newMux(&state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["healthy"] != true {
		t.Fatalf("expected healthy=true, got %#v", body["healthy"])
	}
	if body["extents_rebuilt"] != float64(1) {
		t.Fatalf("expected extents_rebuilt=1, got %#v", body["extents_rebuilt"])
	}
}

func TestRebuildAllEndpointRestoresMultipleDisks(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/shares/demo/http-rebuild-all.bin",
		SizeBytes:   (3 << 20) + 99,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	usedDisks := make(map[string]bool)
	usedGroups := make(map[string]bool)
	removed := 0
	for _, extent := range writeResult.Extents {
		if usedDisks[extent.DataDiskID] || usedGroups[extent.ParityGroupID] {
			continue
		}
		extentPath := filepath.Join(filepath.Dir(metadataPath), extent.PhysicalLocator.RelativePath)
		if err := os.Remove(extentPath); err != nil {
			t.Fatalf("Remove returned error: %v", err)
		}
		usedDisks[extent.DataDiskID] = true
		usedGroups[extent.ParityGroupID] = true
		removed++
	}
	if removed < 2 {
		t.Fatalf("expected recoverable missing extents across multiple disks, got %d", removed)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodPost, "/v1/rebuild/all", nil)
	rr := httptest.NewRecorder()
	newMux(&state).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["healthy"] != true {
		t.Fatalf("expected healthy=true, got %#v", body["healthy"])
	}
	if body["disks_rebuilt"] != float64(2) {
		t.Fatalf("expected disks_rebuilt=2, got %#v", body["disks_rebuilt"])
	}
	if body["extents_rebuilt"] != float64(2) {
		t.Fatalf("expected extents_rebuilt=2, got %#v", body["extents_rebuilt"])
	}
}
