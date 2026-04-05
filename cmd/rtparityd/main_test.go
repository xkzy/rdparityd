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
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/startup-replay.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      journal.StateDataWritten,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if result.FinalState != journal.StateDataWritten {
		t.Fatalf("expected data-written state, got %q", result.FinalState)
	}

	// Check what's in the journal before recovery
	store := journal.NewStore(journalPath)
	records, _ := store.Load()
	t.Logf("DEBUG: records in journal before recovery: %d", len(records))
	for i, r := range records {
		t.Logf("DEBUG: record %d: tx=%s state=%s", i, r.TxID, r.State)
	}

	t.Logf("DEBUG: loading runtime state...")
	state := loadRuntimeState("demo", journalPath, metadataPath)
	t.Logf("DEBUG: startupError=%q, RequiresReplay=%v, Status=%q",
		state.StartupError, state.JournalSummary.RequiresReplay, state.StartupAdmission.Status)
	t.Logf("DEBUG: Prototype files=%d, extents=%d", len(state.Prototype.Files), len(state.Prototype.Extents))
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
	if state.StartupAdmission.Status != "degraded" && state.StartupAdmission.Status != "ok" {
		t.Fatalf("expected first boot to be admitted, got %#v", state.StartupAdmission)
	}
}

func TestLoadRuntimeStateRefusesCorruptMetadataEmptyJournal(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	if err := os.WriteFile(metadataPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("write corrupt metadata: %v", err)
	}
	if err := os.WriteFile(journalPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write empty journal: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	if state.StartupAdmission.Status != "refuse" {
		t.Fatalf("expected refused startup, got %#v", state.StartupAdmission)
	}
	if state.healthStatus() != "error" {
		t.Fatalf("expected healthStatus=error, got %q", state.healthStatus())
	}
}

func TestLoadRuntimeStateAllowsRecoverableDegradedDiskFailure(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coord := journal.NewCoordinator(metadataPath, journalPath)

	writeResult, err := coord.WriteFile(journal.WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/degraded.bin",
		AllowSynthetic: true,
		SizeBytes:      1<<20 + 17,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected extents")
	}
	missingExtentPath := filepath.Join(filepath.Dir(metadataPath), writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(missingExtentPath); err != nil {
		t.Fatalf("remove extent: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	if state.StartupAdmission.Status != "degraded" {
		t.Fatalf("expected degraded startup, got %#v", state.StartupAdmission)
	}
	if state.healthStatus() != "degraded" {
		t.Fatalf("expected healthStatus=degraded, got %q", state.healthStatus())
	}
}

func TestJournalEndpointReturnsReplaySummary(t *testing.T) {
	state := &runtimeState{
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

func TestDiagnosticsEndpointReportsRecoveryState(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	writeResult, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-diagnostics.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Fatal("expected extents")
	}

	// Mark one disk failed in metadata so operator diagnostics can surface it.
	if err := coordinator.FailDisk(writeResult.Extents[0].DataDiskID); err != nil {
		t.Fatalf("FailDisk returned error: %v", err)
	}

	// Corrupt one parity file so parity mismatch state is surfaced clearly.
	state, err := coordinator.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta returned error: %v", err)
	}
	if len(state.ParityGroups) == 0 {
		t.Fatal("expected parity groups")
	}
	parityPath := filepath.Join(filepath.Dir(metadataPath), "parity", state.ParityGroups[0].ParityGroupID+".bin")
	parityBytes, err := os.ReadFile(parityPath)
	if err != nil {
		t.Fatalf("Read parity file returned error: %v", err)
	}
	parityBytes[0] ^= 0xff
	if err := os.WriteFile(parityPath, parityBytes, 0o600); err != nil {
		t.Fatalf("Corrupt parity write returned error: %v", err)
	}

	stateRT := loadRuntimeState("demo", journalPath, metadataPath)
	// Explicitly inject an interrupted transaction summary to prove the handler
	// surfaces it directly instead of requiring log inspection.
	stateRT.JournalSummary.RequiresReplay = true
	stateRT.JournalSummary.IncompleteTxIDs = []string{"tx-interrupted"}

	req := httptest.NewRequest(http.MethodGet, "/v1/diagnostics", nil)
	rr := httptest.NewRecorder()
	newMux(stateRT).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if body["journal_state"] != "dirty" {
		t.Fatalf("expected journal_state=dirty, got %#v", body["journal_state"])
	}
	if body["parity_mismatch_state"] != true {
		t.Fatalf("expected parity_mismatch_state=true, got %#v", body["parity_mismatch_state"])
	}
	failedDisks, ok := body["failed_disks"].([]any)
	if !ok || len(failedDisks) == 0 {
		t.Fatalf("expected failed_disks to be non-empty, got %#v", body["failed_disks"])
	}
	interrupted, ok := body["interrupted_transactions"].([]any)
	if !ok || len(interrupted) != 1 || interrupted[0] != "tx-interrupted" {
		t.Fatalf("expected interrupted transaction diagnostics, got %#v", body["interrupted_transactions"])
	}
}

func TestReadEndpointReturnsVerificationResult(t *testing.T) {
	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	journalPath := filepath.Join(t.TempDir(), "journal.log")
	coordinator := journal.NewCoordinator(metadataPath, journalPath)

	_, err := coordinator.WriteFile(journal.WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-read.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state := loadRuntimeState("demo", journalPath, metadataPath)
	req := httptest.NewRequest(http.MethodGet, "/v1/read?path=/shares/demo/http-read.bin", nil)
	rr := httptest.NewRecorder()
	newMux(state).ServeHTTP(rr, req)

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
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-scrub.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
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
	newMux(state).ServeHTTP(rr, req)

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
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-scrub-history.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
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
	newMux(state).ServeHTTP(rr, req)

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
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-rebuild.bin",
		AllowSynthetic: true,
		SizeBytes:      (1 << 20) + 64,
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
	newMux(state).ServeHTTP(rr, req)

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
		PoolName:       "demo",
		LogicalPath:    "/shares/demo/http-rebuild-all.bin",
		AllowSynthetic: true,
		SizeBytes:      (3 << 20) + 99,
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
	newMux(state).ServeHTTP(rr, req)

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
