package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rtparityd/rtparityd/internal/journal"
	"github.com/rtparityd/rtparityd/internal/metadata"
)

type runtimeState struct {
	StartedAt      time.Time              `json:"started_at"`
	Prototype      metadata.SampleState   `json:"prototype"`
	MetadataPath   string                 `json:"metadata_path"`
	JournalPath    string                 `json:"journal_path"`
	Recovery       journal.RecoveryResult `json:"recovery"`
	JournalSummary journal.ReplaySummary  `json:"journal_summary"`
	StartupError   string                 `json:"startup_error,omitempty"`
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

	log.Printf("starting rtparityd prototype on %s", *listen)
	if err := http.ListenAndServe(*listen, newMux(state)); err != nil {
		log.Fatal(err)
	}
}

func loadRuntimeState(poolName, journalPath, metadataPath string) runtimeState {
	state := runtimeState{
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
	return state
}

func joinStartupError(current, next string) string {
	if current == "" {
		return next
	}
	return current + "; " + next
}

func (s runtimeState) healthStatus() string {
	if s.StartupError != "" {
		return "error"
	}
	if s.JournalSummary.RequiresReplay {
		return "degraded"
	}
	return "ok"
}

func newMux(state runtimeState) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		var lastScrub any
		if len(state.Prototype.ScrubHistory) > 0 {
			lastScrub = state.Prototype.ScrubHistory[len(state.Prototype.ScrubHistory)-1]
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"name":                    "rtparityd",
			"status":                  state.healthStatus(),
			"version":                 "0.1.0-prototype",
			"default_extent_bytes":    state.Prototype.Pool.ExtentSizeBytes,
			"parity_mode":             state.Prototype.Pool.ParityMode,
			"metadata_path":           state.MetadataPath,
			"managed_files":           len(state.Prototype.Files),
			"allocated_extents":       len(state.Prototype.Extents),
			"recovered_transactions":  len(state.Recovery.RecoveredTxIDs),
			"journal_path":            state.JournalPath,
			"journal_records":         state.JournalSummary.TotalRecords,
			"journal_requires_replay": state.JournalSummary.RequiresReplay,
			"scrub_runs":              len(state.Prototype.ScrubHistory),
			"last_scrub":              lastScrub,
			"timestamp":               time.Now().UTC(),
			"started_at":              state.StartedAt,
			"startup_error":           state.StartupError,
		})
	})

	mux.HandleFunc("/v1/pools", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []metadata.Pool{state.Prototype.Pool})
	})

	mux.HandleFunc("/v1/design", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"language":             "go",
			"extent_size_bytes":    state.Prototype.Pool.ExtentSizeBytes,
			"parity_mode":          state.Prototype.Pool.ParityMode,
			"max_parity_group":     8,
			"metadata_device_hint": "dedicated SSD",
			"journal_format":       journal.RecordMagic,
		})
	})

	mux.HandleFunc("/v1/journal", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"journal_path":  state.JournalPath,
			"status":        state.healthStatus(),
			"recovery":      state.Recovery,
			"summary":       state.JournalSummary,
			"startup_error": state.StartupError,
		})
	})

	mux.HandleFunc("/v1/metadata", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"metadata_path": state.MetadataPath,
			"status":        state.healthStatus(),
			"files":         state.Prototype.Files,
			"extents":       state.Prototype.Extents,
			"parity_groups": state.Prototype.ParityGroups,
			"transactions":  state.Prototype.Transactions,
			"scrub_history": state.Prototype.ScrubHistory,
			"startup_error": state.StartupError,
		})
	})

	mux.HandleFunc("/v1/read", func(w http.ResponseWriter, r *http.Request) {
		logicalPath := r.URL.Query().Get("path")
		if logicalPath == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "missing required query parameter: path",
			})
			return
		}

		result, err := journal.NewCoordinator(state.MetadataPath, state.JournalPath).ReadFile(logicalPath)
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

		if loadedState, err := metadata.NewStore(state.MetadataPath).LoadOrCreate(metadata.PrototypeState(state.Prototype.Pool.Name)); err == nil {
			state.Prototype = loadedState
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"metadata_path": state.MetadataPath,
			"count":         len(state.Prototype.ScrubHistory),
			"history":       state.Prototype.ScrubHistory,
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

		result, err := journal.NewCoordinator(state.MetadataPath, state.JournalPath).Scrub(repair)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"repair": repair,
				"error":  err.Error(),
			})
			return
		}

		if loadedState, err := metadata.NewStore(state.MetadataPath).LoadOrCreate(metadata.PrototypeState(state.Prototype.Pool.Name)); err == nil {
			state.Prototype = loadedState
		}
		if summary, err := journal.NewStore(state.JournalPath).Replay(); err == nil {
			state.JournalSummary = summary
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

		result, err := journal.NewCoordinator(state.MetadataPath, state.JournalPath).RebuildDataDisk(diskID)
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
