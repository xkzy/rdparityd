package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/rtparityd/rtparityd/internal/journal"
	"github.com/rtparityd/rtparityd/internal/metadata"
)

type runtimeState struct {
	StartedAt      time.Time             `json:"started_at"`
	Prototype      metadata.SampleState  `json:"prototype"`
	MetadataPath   string                `json:"metadata_path"`
	JournalPath    string                `json:"journal_path"`
	JournalSummary journal.ReplaySummary `json:"journal_summary"`
	StartupError   string                `json:"startup_error,omitempty"`
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
		if state.JournalSummary.RequiresReplay {
			log.Printf("journal replay required for %d transaction(s): %v", len(state.JournalSummary.IncompleteTxIDs), state.JournalSummary.IncompleteTxIDs)
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
		writeJSON(w, http.StatusOK, map[string]any{
			"name":                    "rtparityd",
			"status":                  state.healthStatus(),
			"version":                 "0.1.0-prototype",
			"default_extent_bytes":    state.Prototype.Pool.ExtentSizeBytes,
			"parity_mode":             state.Prototype.Pool.ParityMode,
			"metadata_path":           state.MetadataPath,
			"managed_files":           len(state.Prototype.Files),
			"allocated_extents":       len(state.Prototype.Extents),
			"journal_path":            state.JournalPath,
			"journal_records":         state.JournalSummary.TotalRecords,
			"journal_requires_replay": state.JournalSummary.RequiresReplay,
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
			"startup_error": state.StartupError,
		})
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
