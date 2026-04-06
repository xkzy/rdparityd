package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
	"github.com/xkzy/rdparityd/internal/metrics"
)

const (
	socketPerm    = 0o660
	daemonVersion = "0.1.0"
)

type runtimeState struct {
	mu               sync.RWMutex
	StartedAt        time.Time
	Prototype        metadata.SampleState
	MetadataPath     string
	JournalPath      string
	Recovery         journal.RecoveryResult
	JournalSummary   journal.ReplaySummary
	StartupError     string
	StartupAdmission startupAdmission
}

type startupAdmission struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
}

type command struct {
	ID     string          `json:"id"`
	Op     string          `json:"op"`
	Params json.RawMessage `json:"params,omitempty"`
}

type response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func main() {
	socketPath := flag.String("socket", "/var/run/rtparityd/rtparityd.sock", "unix socket path for control interface")
	poolName := flag.String("pool-name", "demo", "prototype pool name")
	journalPath := flag.String("journal-path", "/var/lib/rtparityd/journal.bin", "journal path")
	metadataPath := flag.String("metadata-path", "/var/lib/rtparityd/metadata.bin", "metadata snapshot path")
	flag.Parse()

	if !filepath.IsAbs(*socketPath) {
		log.Fatalf("socket path must be absolute: %s", *socketPath)
	}
	if !filepath.IsAbs(*journalPath) {
		log.Fatalf("journal-path must be absolute: %s", *journalPath)
	}
	if !filepath.IsAbs(*metadataPath) {
		log.Fatalf("metadata-path must be absolute: %s", *metadataPath)
	}
	if *poolName == "" {
		log.Fatalf("pool-name is required")
	}
	if strings.ContainsAny(*poolName, "/\\:") {
		log.Fatalf("pool-name must not contain path separators: %s", *poolName)
	}
	if filepath.Dir(*metadataPath) == filepath.Dir(*journalPath) {
		log.Printf("warning: metadata and journal share the same directory")
	}

	if err := os.MkdirAll(filepath.Dir(*socketPath), 0o755); err != nil {
		log.Fatalf("create socket directory: %v", err)
	}
	if err := os.Remove(*socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("remove existing socket: %v", err)
	}

	listener, err := net.Listen("unix", *socketPath)
	if err != nil {
		log.Fatalf("listen on socket: %v", err)
	}
	if err := os.Chmod(*socketPath, socketPerm); err != nil {
		log.Fatalf("set socket permissions: %v", err)
	}
	defer os.Remove(*socketPath)

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

	log.Printf("rtparityd listening on %s", *socketPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed") {
				break
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConnection(conn, state)
	}
}

func handleConnection(conn net.Conn, state *runtimeState) {
	defer conn.Close()

	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	for {
		var cmd command
		if err := dec.Decode(&cmd); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			sendError(enc, "", fmt.Errorf("decode command: %w", err))
			return
		}

		resp := response{ID: cmd.ID}
		result, err := handleCommand(cmd.Op, cmd.Params, state)
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.Result = result
		}

		if err := enc.Encode(resp); err != nil {
			log.Printf("encode response: %v", err)
			return
		}
	}
}

func sendError(enc *json.Encoder, id string, err error) {
	enc.Encode(response{ID: id, Error: err.Error()})
}

func handleCommand(op string, params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	switch op {
	case "health":
		return healthOp(state)
	case "metrics":
		return metricsOp()
	case "write":
		return writeOp(params, state)
	case "read":
		return readOp(params, state)
	case "scrub":
		return scrubOp(params, state)
	case "rebuild":
		return rebuildOp(params, state)
	case "rebuild-all":
		return rebuildAllOp(state)
	case "status":
		return statusOp(state)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func healthOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	startupError := state.StartupError
	status := state.healthStatus()
	state.mu.RUnlock()

	failedDisks := 0
	for _, disk := range proto.Disks {
		if disk.HealthStatus != "" && disk.HealthStatus != "online" {
			failedDisks++
		}
	}

	alertLevel := "ok"
	if failedDisks > 0 {
		alertLevel = "warning"
	}
	if startupError != "" {
		alertLevel = "critical"
	}

	result := map[string]any{
		"name":    "rtparityd",
		"status":  status,
		"version": daemonVersion,
		"alert":   alertLevel,
		"uptime":  time.Since(state.StartedAt).Seconds(),
	}
	return json.Marshal(result)
}

func metricsOp() (json.RawMessage, error) {
	rec := metrics.Global()
	snap := rec.Snapshot()
	return json.Marshal(snap)
}

type writeParams struct {
	Path      string `json:"path"`
	Payload   []byte `json:"payload,omitempty"`
	Size      int64  `json:"size"`
	Synthetic bool   `json:"synthetic,omitempty"`
}

func writeOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p writeParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.Path == "" {
		return nil, errors.New("path is required")
	}
	if p.Payload == nil && !p.Synthetic && p.Size <= 0 {
		return nil, errors.New("payload or size required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	req := journal.WriteRequest{
		PoolName:       "demo",
		LogicalPath:    p.Path,
		Payload:        p.Payload,
		SizeBytes:      p.Size,
		AllowSynthetic: p.Synthetic,
	}

	m := metrics.Global()
	m.ActiveWrites.Inc()
	m.WriteOps.Inc()
	defer m.ActiveWrites.Dec()

	start := time.Now()
	defer func() { m.WriteLatency.Record(time.Since(start)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := coord.WriteFile(ctx, req)
	if err != nil {
		m.WriteErrors.Inc()
		return nil, err
	}
	return json.Marshal(result)
}

type readParams struct {
	Path string `json:"path"`
}

func readOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p readParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.Path == "" {
		return nil, errors.New("path is required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	m.ActiveReads.Inc()
	m.ReadOps.Inc()
	defer m.ActiveReads.Dec()

	start := time.Now()
	defer func() { m.ReadLatency.Record(time.Since(start)) }()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	result, err := coord.ReadFile(p.Path)
	if err != nil {
		m.ReadErrors.Inc()
		return nil, err
	}
	return json.Marshal(result)
}

type scrubParams struct {
	Repair bool `json:"repair"`
}

func scrubOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p scrubParams
	if err := json.Unmarshal(params, &p); err != nil {
		p.Repair = true
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	m.ScrubOps.Inc()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	result, err := coord.ScrubContext(ctx, p.Repair)
	m.ScrubLatency.Record(time.Since(start))
	if err != nil {
		m.ScrubErrors.Inc()
		return nil, err
	}
	return json.Marshal(result)
}

type rebuildParams struct {
	DiskID string `json:"disk"`
}

func rebuildOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p rebuildParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.DiskID == "" {
		return nil, errors.New("disk is required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	m.RebuildOps.Inc()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	result, err := coord.RebuildDataDiskContext(ctx, p.DiskID)
	m.RebuildLatency.Record(time.Since(start))
	if err != nil {
		m.RebuildErrors.Inc()
		return nil, err
	}
	return json.Marshal(result)
}

func rebuildAllOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	m.RebuildOps.Inc()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	result, err := coord.RebuildAllDataDisksContext(ctx)
	m.RebuildLatency.Record(time.Since(start))
	if err != nil {
		m.RebuildErrors.Inc()
		return nil, err
	}
	return json.Marshal(result)
}

func statusOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	metadataPath := state.MetadataPath
	recovery := state.Recovery
	summary := state.JournalSummary
	status := state.healthStatus()
	state.mu.RUnlock()

	rootDir := filepath.Dir(metadataPath)
	violations := journal.CheckIntegrityInvariants(rootDir, proto)

	failedDiskSet := make(map[string]struct{})
	for _, disk := range proto.Disks {
		if disk.HealthStatus != "" && disk.HealthStatus != "online" {
			failedDiskSet[disk.DiskID] = struct{}{}
		}
	}
	failedDisks := make([]string, 0, len(failedDiskSet))
	for diskID := range failedDiskSet {
		failedDisks = append(failedDisks, diskID)
	}

	result := map[string]any{
		"status":                  status,
		"startup_admission":       state.StartupAdmission,
		"pool_state":              status,
		"managed_files":           len(proto.Files),
		"allocated_extents":       len(proto.Extents),
		"journal_records":         summary.TotalRecords,
		"journal_requires_replay": summary.RequiresReplay,
		"recovered_transactions":  len(recovery.RecoveredTxIDs),
		"aborted_transactions":    len(recovery.AbortedTxIDs),
		"failed_disks":            failedDisks,
		"invariant_violations":    len(violations),
		"uptime_seconds":          time.Since(state.StartedAt).Seconds(),
	}
	return json.Marshal(result)
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

	recovery, err := journal.NewCoordinator(metadataPath, journalPath).Recover()
	if err != nil {
		state.StartupError = joinStartupError(state.StartupError, "recovery: "+err.Error())
	} else {
		state.Recovery = recovery
	}

	loadedState, found, err := loadMetadataIfPresent(metadataPath)
	if err != nil {
		state.StartupError = joinStartupError(state.StartupError, "metadata: "+err.Error())
	} else if found {
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

func loadMetadataIfPresent(metadataPath string) (metadata.SampleState, bool, error) {
	loadedState, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return metadata.SampleState{}, false, nil
		}
		return metadata.SampleState{}, false, err
	}
	return loadedState, true, nil
}

func joinStartupError(current, next string) string {
	if current == "" {
		return next
	}
	return current + "; " + next
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
