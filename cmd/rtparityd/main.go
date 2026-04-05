package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
)

type runtimeState struct {
	// mu protects concurrent access to all mutable runtimeState fields.
	// Handlers must hold mu.RLock for reads and mu.Lock for writes to
	// Prototype and JournalSummary.
	mu               sync.RWMutex
	StartedAt        time.Time              `json:"started_at"`
	Prototype        metadata.SampleState   `json:"prototype"`
	MetadataPath     string                 `json:"metadata_path"`
	JournalPath      string                 `json:"journal_path"`
	Recovery         journal.RecoveryResult `json:"recovery"`
	JournalSummary   journal.ReplaySummary  `json:"journal_summary"`
	StartupError     string                 `json:"startup_error,omitempty"`
	StartupAdmission startupAdmission       `json:"startup_admission"`
}

// startupAdmission is the centralized startup/open decision for the daemon.
// Status values:
//   - "ok": safe to start normally
//   - "degraded": safe to start, but operator attention is required
//   - "refuse": must not start serving requests
//
// A refused startup is a hard gate: the daemon exits before ListenAndServe.
type startupAdmission struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
}

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	poolName := flag.String("pool-name", "demo", "prototype pool name")
	journalPath := flag.String("journal-path", "/tmp/rtparityd/journal.log", "journal path to inspect during startup replay")
	metadataPath := flag.String("metadata-path", "/tmp/rtparityd/metadata.json", "metadata snapshot path for the prototype state")
	flag.Parse()

	state := loadRuntimeState(*poolName, *journalPath, *metadataPath)
	if state.StartupError != "" {
		log.Printf("startup inspection reported errors: %s", state.StartupError)
	} else {
		log.Printf("loaded metadata snapshot %s with %d file(s) and %d extent(s)", state.MetadataPath, len(state.Prototype.Files), len(state.Prototype.Extents))
		if len(state.Recovery.RecoveredTxIDs) > 0 {
			log.Printf("startup replay recovered %d transaction(s): %v", len(state.Recovery.RecoveredTxIDs), state.Recovery.RecoveredTxIDs)
		}
		if len(state.Recovery.AbortedTxIDs) > 0 {
			log.Printf("startup replay aborted %d transaction(s): %v", len(state.Recovery.AbortedTxIDs), state.Recovery.AbortedTxIDs)
		}
		if state.JournalSummary.RequiresReplay {
			log.Printf("journal replay still required for %d transaction(s): %v", len(state.JournalSummary.IncompleteTxIDs), state.JournalSummary.IncompleteTxIDs)
		} else {
			log.Printf("journal clean at %s with %d record(s)", state.JournalPath, state.JournalSummary.TotalRecords)
		}
	}

	if state.StartupAdmission.Status == "refuse" {
		log.Fatalf("refusing startup: %s", strings.Join(state.StartupAdmission.Reasons, "; "))
	}

	log.Printf("startup admission=%s", state.StartupAdmission.Status)
	if len(state.StartupAdmission.Reasons) > 0 {
		log.Printf("startup reasons: %s", strings.Join(state.StartupAdmission.Reasons, "; "))
	}

	log.Printf("starting rtparityd prototype on %s", *listen)
	if err := http.ListenAndServe(*listen, newMux(state)); err != nil {
		log.Fatal(err)
	}
}

func isRecoverableMissingFileError(err string) bool {
	return strings.Contains(err, "file missing or unreadable")
}

func allRecoverableMissingFileErrors(errs []string) bool {
	if len(errs) == 0 {
		return false
	}
	for _, err := range errs {
		if !isRecoverableMissingFileError(err) {
			return false
		}
	}
	return true
}

func evaluateStartupAdmission(state *runtimeState) startupAdmission {
	reasons := make([]string, 0)
	rootDir := filepath.Dir(state.MetadataPath)
	formatResult := journal.ValidateOnDiskFormats(rootDir, state.MetadataPath, state.JournalPath)
	analysis := journal.AnalyzeMultiDiskFailures(rootDir, state.Prototype)

	if state.StartupError != "" {
		reasons = append(reasons, state.StartupError)
	}
	if state.JournalSummary.RequiresReplay {
		reasons = append(reasons, "journal still requires replay after startup recovery")
	}
	if analysis != nil && !analysis.RecoveryIsPossible {
		reasons = append(reasons, analysis.RecommendedAction)
	}

	degradedReasons := make([]string, 0)
	if len(formatResult.Errors) > 0 {
		if analysis != nil && analysis.RecoveryIsPossible && len(analysis.FailedDisks) > 0 && allRecoverableMissingFileErrors(formatResult.Errors) {
			degradedReasons = append(degradedReasons, formatResult.Errors...)
		} else {
			reasons = append(reasons, formatResult.Errors...)
		}
	}
	if len(reasons) > 0 {
		return startupAdmission{Status: "refuse", Reasons: reasons}
	}

	if analysis != nil && len(analysis.FailedDisks) > 0 {
		degradedReasons = append(degradedReasons, "recoverable failed disks present")
	}
	if len(formatResult.Warnings) > 0 {
		degradedReasons = append(degradedReasons, formatResult.Warnings...)
	}
	if len(degradedReasons) > 0 {
		return startupAdmission{Status: "degraded", Reasons: degradedReasons}
	}
	return startupAdmission{Status: "ok"}
}

func loadRuntimeState(poolName, journalPath, metadataPath string) *runtimeState {
	state := &runtimeState{
		StartedAt:    time.Now().UTC(),
		Prototype:    metadata.PrototypeState(poolName),
		MetadataPath: metadataPath,
		JournalPath:  journalPath,
	}

	recovery, err := journal.NewCoordinator(metadataPath, journalPath).RecoverWithState(metadata.PrototypeState(poolName))
	if err != nil {
		state.StartupError = joinStartupError(state.StartupError, "recovery: "+err.Error())
	} else {
		state.Recovery = recovery
	}

	loadedState, err := metadata.NewStore(metadataPath).LoadOrCreate(metadata.PrototypeState(poolName))
	if err != nil {
		state.StartupError = joinStartupError(state.StartupError, "metadata: "+err.Error())
	} else {
		state.Prototype = loadedState
	}

	summary, err := journal.NewStore(journalPath).Replay()
	if err != nil {
		state.StartupError = joinStartupError(state.StartupError, "journal: "+err.Error())
		return state
	}
	state.JournalSummary = summary
	state.StartupAdmission = evaluateStartupAdmission(state)
	return state
}

func joinStartupError(current, next string) string {
	if current == "" {
		return next
	}
	return current + "; " + next
}

func (s *runtimeState) healthStatus() string {
	switch s.StartupAdmission.Status {
	case "refuse":
		return "error"
	case "degraded":
		return "degraded"
	default:
		return "ok"
	}
}

func diagnosticsView(state *runtimeState) map[string]any {
	state.mu.RLock()
	proto := state.Prototype
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	recovery := state.Recovery
	summary := state.JournalSummary
	startupError := state.StartupError
	admission := state.StartupAdmission
	status := state.healthStatus()
	state.mu.RUnlock()

	rootDir := filepath.Dir(metadataPath)
	formatResult := journal.ValidateOnDiskFormats(rootDir, metadataPath, journalPath)
	analysis := journal.AnalyzeMultiDiskFailures(rootDir, proto)
	violations := journal.CheckIntegrityInvariants(rootDir, proto)

	failedDiskSet := make(map[string]struct{})
	for _, disk := range proto.Disks {
		if disk.HealthStatus != "" && disk.HealthStatus != "online" {
			failedDiskSet[disk.DiskID] = struct{}{}
		}
	}
	for _, diskID := range analysis.FailedDisks {
		failedDiskSet[diskID] = struct{}{}
	}
	failedDisks := make([]string, 0, len(failedDiskSet))
	for diskID := range failedDiskSet {
		failedDisks = append(failedDisks, diskID)
	}

	rebuildProgressFiles, _ := filepath.Glob(filepath.Join(rootDir, "rebuild-*.progress"))

	metadataState := "ok"
	if _, err := metadata.NewStore(metadataPath).Load(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			metadataState = "missing"
		} else {
			metadataState = "corrupt"
		}
	} else if len(recovery.RecoveredTxIDs) > 0 || len(recovery.AbortedTxIDs) > 0 {
		metadataState = "reconciled-from-journal"
	}

	journalState := "clean"
	if startupError != "" && strings.Contains(startupError, "journal:") {
		journalState = "corrupt"
	} else if summary.RequiresReplay {
		journalState = "dirty"
	}

	parityIssues := make([]string, 0)
	corruptionFindings := make([]string, 0)
	for _, v := range violations {
		switch v.Code {
		case "P2", "P3":
			parityIssues = append(parityIssues, v.Error())
			corruptionFindings = append(corruptionFindings, v.Error())
		case "E1", "J1":
			corruptionFindings = append(corruptionFindings, v.Error())
		}
	}
	corruptionFindings = append(corruptionFindings, formatResult.Errors...)

	var lastScrub any
	if n := len(proto.ScrubHistory); n > 0 {
		lastScrub = proto.ScrubHistory[n-1]
	}

	return map[string]any{
		"status":                    status,
		"startup_admission":         admission,
		"pool_state":                status,
		"metadata_state":            metadataState,
		"journal_state":             journalState,
		"journal_requires_replay":   summary.RequiresReplay,
		"interrupted_transactions":  summary.IncompleteTxIDs,
		"recovered_transactions":    recovery.RecoveredTxIDs,
		"aborted_transactions":      recovery.AbortedTxIDs,
		"failed_disks":              failedDisks,
		"rebuild_in_progress_files": rebuildProgressFiles,
		"last_scrub":                lastScrub,
		"parity_mismatch_state":     len(parityIssues) > 0,
		"parity_issues":             parityIssues,
		"corruption_findings":       corruptionFindings,
		"format_warnings":           formatResult.Warnings,
		"multi_disk_failure":        analysis,
		"startup_error":             startupError,
		"metadata_path":             metadataPath,
		"journal_path":              journalPath,
	}
}

func newMux(state *runtimeState) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		state.mu.RLock()
		scrubHistory := state.Prototype.ScrubHistory
		poolExtentSize := state.Prototype.Pool.ExtentSizeBytes
		poolParityMode := state.Prototype.Pool.ParityMode
		metadataPath := state.MetadataPath
		managedFiles := len(state.Prototype.Files)
		allocatedExtents := len(state.Prototype.Extents)
		recoveredTxs := len(state.Recovery.RecoveredTxIDs)
		journalPath := state.JournalPath
		journalRecords := state.JournalSummary.TotalRecords
		journalReplay := state.JournalSummary.RequiresReplay
		startedAt := state.StartedAt
		startupError := state.StartupError
		status := state.healthStatus()
		state.mu.RUnlock()

		var lastScrub any
		if len(scrubHistory) > 0 {
			lastScrub = scrubHistory[len(scrubHistory)-1]
		}

		state.mu.RLock()
		failedDisks := 0
		parityIssues := 0
		for _, disk := range state.Prototype.Disks {
			if disk.HealthStatus != "online" {
				failedDisks++
			}
		}
		for _, group := range state.Prototype.ParityGroups {
			if group.ParityChecksum == "" {
				parityIssues++
			}
		}
		state.mu.RUnlock()

		scrubFailures := 0
		rebuildFailures := 0
		metadataIssues := 0
		for i := len(scrubHistory) - 1; i >= 0 && scrubFailures+rebuildFailures < 10; i-- {
			if scrubHistory[i].FailedCount > 0 {
				scrubFailures++
			}
		}

		alertLevel := "ok"
		if failedDisks > 0 || parityIssues > 0 || scrubFailures > 0 {
			alertLevel = "warning"
		}
		if startupError != "" {
			alertLevel = "critical"
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"name":                    "rtparityd",
			"status":                  status,
			"alert_level":             alertLevel,
			"version":                 "0.1.0-prototype",
			"default_extent_bytes":    poolExtentSize,
			"parity_mode":             poolParityMode,
			"metadata_path":           metadataPath,
			"managed_files":           managedFiles,
			"allocated_extents":       allocatedExtents,
			"recovered_transactions":  recoveredTxs,
			"journal_path":            journalPath,
			"journal_records":         journalRecords,
			"journal_requires_replay": journalReplay,
			"scrub_runs":              len(scrubHistory),
			"last_scrub":              lastScrub,
			"failed_disks":            failedDisks,
			"parity_issues":           parityIssues,
			"scrub_failures":          scrubFailures,
			"rebuild_failures":        rebuildFailures,
			"metadata_issues":         metadataIssues,
			"timestamp":               time.Now().UTC(),
			"started_at":              startedAt,
			"startup_error":           startupError,
		})
	})

	mux.HandleFunc("/v1/pools", func(w http.ResponseWriter, r *http.Request) {
		state.mu.RLock()
		pool := state.Prototype.Pool
		state.mu.RUnlock()
		writeJSON(w, http.StatusOK, []metadata.Pool{pool})
	})

	mux.HandleFunc("/v1/design", func(w http.ResponseWriter, r *http.Request) {
		state.mu.RLock()
		extentSize := state.Prototype.Pool.ExtentSizeBytes
		parityMode := state.Prototype.Pool.ParityMode
		state.mu.RUnlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"language":             "go",
			"extent_size_bytes":    extentSize,
			"parity_mode":          parityMode,
			"max_parity_group":     8,
			"metadata_device_hint": "dedicated SSD",
			"journal_format":       journal.RecordMagic,
		})
	})

	mux.HandleFunc("/v1/journal", func(w http.ResponseWriter, r *http.Request) {
		state.mu.RLock()
		journalPath := state.JournalPath
		status := state.healthStatus()
		recovery := state.Recovery
		summary := state.JournalSummary
		startupError := state.StartupError
		state.mu.RUnlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"journal_path":  journalPath,
			"status":        status,
			"recovery":      recovery,
			"summary":       summary,
			"startup_error": startupError,
		})
	})

	mux.HandleFunc("/v1/metadata", func(w http.ResponseWriter, r *http.Request) {
		state.mu.RLock()
		metadataPath := state.MetadataPath
		status := state.healthStatus()
		files := state.Prototype.Files
		extents := state.Prototype.Extents
		parityGroups := state.Prototype.ParityGroups
		transactions := state.Prototype.Transactions
		scrubHistory := state.Prototype.ScrubHistory
		startupError := state.StartupError
		state.mu.RUnlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"metadata_path": metadataPath,
			"status":        status,
			"files":         files,
			"extents":       extents,
			"parity_groups": parityGroups,
			"transactions":  transactions,
			"scrub_history": scrubHistory,
			"startup_error": startupError,
		})
	})

	mux.HandleFunc("/v1/diagnostics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "method not allowed",
			})
			return
		}
		writeJSON(w, http.StatusOK, diagnosticsView(state))
	})

	mux.HandleFunc("/v1/read", func(w http.ResponseWriter, r *http.Request) {
		logicalPath := r.URL.Query().Get("path")
		if logicalPath == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "missing required query parameter: path",
			})
			return
		}

		state.mu.RLock()
		metadataPath := state.MetadataPath
		journalPath := state.JournalPath
		state.mu.RUnlock()

		result, err := journal.NewCoordinator(metadataPath, journalPath).ReadFile(logicalPath)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "file not found") {
				status = http.StatusNotFound
			}
			writeJSON(w, status, map[string]any{
				"path":  logicalPath,
				"error": err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"path":              logicalPath,
			"verified":          result.Verified,
			"bytes_read":        result.BytesRead,
			"content_checksum":  result.ContentChecksum,
			"healed_extent_ids": result.HealedExtentIDs,
			"file":              result.File,
		})
	})

	mux.HandleFunc("/v1/scrub/history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "method not allowed",
			})
			return
		}

		state.mu.RLock()
		metadataPath := state.MetadataPath
		poolName := state.Prototype.Pool.Name
		state.mu.RUnlock()

		if loadedState, err := metadata.NewStore(metadataPath).LoadOrCreate(metadata.PrototypeState(poolName)); err == nil {
			state.mu.Lock()
			state.Prototype = loadedState
			state.mu.Unlock()
		}

		state.mu.RLock()
		scrubHistory := state.Prototype.ScrubHistory
		state.mu.RUnlock()

		writeJSON(w, http.StatusOK, map[string]any{
			"metadata_path": metadataPath,
			"count":         len(scrubHistory),
			"history":       scrubHistory,
		})
	})

	mux.HandleFunc("/v1/scrub", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "method not allowed",
			})
			return
		}

		repairValue := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("repair")))
		repair := repairValue == "true" || repairValue == "1" || (repairValue == "" && r.Method == http.MethodPost)

		state.mu.RLock()
		metadataPath := state.MetadataPath
		journalPath := state.JournalPath
		poolName := state.Prototype.Pool.Name
		state.mu.RUnlock()

		result, err := journal.NewCoordinator(metadataPath, journalPath).Scrub(repair)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"repair": repair,
				"error":  err.Error(),
			})
			return
		}

		if loadedState, err := metadata.NewStore(metadataPath).LoadOrCreate(metadata.PrototypeState(poolName)); err == nil {
			state.mu.Lock()
			state.Prototype = loadedState
			state.mu.Unlock()
		}
		if summary, err := journal.NewStore(journalPath).Replay(); err == nil {
			state.mu.Lock()
			state.JournalSummary = summary
			state.mu.Unlock()
		}

		writeJSON(w, http.StatusOK, result)
	})

	mux.HandleFunc("/v1/rebuild/all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "method not allowed",
			})
			return
		}

		state.mu.RLock()
		metadataPath := state.MetadataPath
		journalPath := state.JournalPath
		state.mu.RUnlock()

		result, err := journal.NewCoordinator(metadataPath, journalPath).RebuildAllDataDisks()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"error": err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusOK, result)
	})

	mux.HandleFunc("/v1/rebuild", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "method not allowed",
			})
			return
		}

		diskID := strings.TrimSpace(r.URL.Query().Get("disk"))
		if diskID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "missing required query parameter: disk",
			})
			return
		}

		state.mu.RLock()
		metadataPath := state.MetadataPath
		journalPath := state.JournalPath
		state.mu.RUnlock()

		result, err := journal.NewCoordinator(metadataPath, journalPath).RebuildDataDisk(diskID)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "no extents mapped") {
				status = http.StatusNotFound
			}
			writeJSON(w, status, map[string]any{
				"disk":  diskID,
				"error": err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusOK, result)
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(value)
}
