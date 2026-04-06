/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"

	"github.com/xkzy/rdparityd/internal/audit"
	"github.com/xkzy/rdparityd/internal/config"
	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/logger"
	"github.com/xkzy/rdparityd/internal/metadata"
	"github.com/xkzy/rdparityd/internal/metrics"

	"github.com/shirou/gopsutil/host"
)

const (
	socketPerm        = 0o660
	daemonVersion     = "0.1.0"
	daemonBuildDate   = "unknown"
	daemonGitCommit   = "unknown"
	maxRequestSize    = 1024 * 1024             // 1MB max JSON request
	maxWriteSize      = 1024 * 1024 * 1024 * 10 // 10GB max write size
	connectionTimeout = 30 * time.Second        // connection read/write timeout
	idleTimeout       = 5 * time.Minute         // idle connection timeout
	maxConnections    = 32                      // max concurrent connections
	memoryThresholdMB = 1024                    // 1GB memory threshold for warnings
)

var (
	requireRootUser *bool
)

func enableKeepalive(conn net.Conn) {
	if uc, ok := conn.(*net.UnixConn); ok {
		rawConn, err := uc.SyscallConn()
		if err != nil {
			return
		}
		rawConn.Control(func(fd uintptr) {
			unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
			unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 10)
			unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 3)
		})
	}
}

const idempotencyTTL = 10 * time.Minute

func checkIdempotency(state *runtimeState, key string) (json.RawMessage, string, bool) {
	if key == "" {
		return nil, "", false
	}
	state.idempotencyMu.Lock()
	defer state.idempotencyMu.Unlock()
	entry, ok := state.idempotencyCache[key]
	if !ok {
		return nil, "", false
	}
	if time.Since(entry.Timestamp) > idempotencyTTL {
		delete(state.idempotencyCache, key)
		return nil, "", false
	}
	return entry.Result, entry.Err, true
}

func storeIdempotency(state *runtimeState, key string, result json.RawMessage, err error) {
	if key == "" {
		return
	}
	state.idempotencyMu.Lock()
	defer state.idempotencyMu.Unlock()
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	state.idempotencyCache[key] = idempotencyEntry{
		Result:    result,
		Err:       errStr,
		Timestamp: time.Now(),
	}
}

func cleanupIdempotencyCache(state *runtimeState) {
	state.idempotencyMu.Lock()
	defer state.idempotencyMu.Unlock()
	now := time.Now()
	for key, entry := range state.idempotencyCache {
		if now.Sub(entry.Timestamp) > idempotencyTTL {
			delete(state.idempotencyCache, key)
		}
	}
}

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
	writeLimiter     *rate.Limiter
	readLimiter      *rate.Limiter
	cfg              *config.Config
	cfgMu            sync.RWMutex
	ShuttingDown     bool
	Draining         bool
	configReloaded   chan struct{}
	shutdownCh       chan struct{}
	inFlight         int64
	idempotencyCache map[string]idempotencyEntry
	idempotencyMu    sync.Mutex
}

type idempotencyEntry struct {
	Result    json.RawMessage
	Err       string
	Timestamp time.Time
}

type startupAdmission struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
}

type command struct {
	ID             string          `json:"id"`
	Op             string          `json:"op"`
	Params         json.RawMessage `json:"params,omitempty"`
	Seq            int64           `json:"seq,omitempty"`
	Ts             int64           `json:"ts,omitempty"`
	TraceID        string          `json:"trace_id,omitempty"`
	CacheHint      bool            `json:"cache_hint,omitempty"`
	Version        string          `json:"version,omitempty"`
	TimeoutMs      int64           `json:"timeout_ms,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

const APIVersion = "v1"

type batchRequest struct {
	ID       string    `json:"id"`
	Commands []command `json:"commands"`
}

type batchResponse struct {
	ID        string     `json:"id"`
	Responses []response `json:"responses"`
}

type response struct {
	ID         string          `json:"id"`
	Seq        int64           `json:"seq,omitempty"`
	Ts         int64           `json:"ts,omitempty"`
	TraceID    string          `json:"trace_id,omitempty"`
	Version    string          `json:"version,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	ErrorCode  int             `json:"error_code,omitempty"`
	Suggestion string          `json:"suggestion,omitempty"`
	LatencyMs  int64           `json:"latency_ms,omitempty"`
}

const (
	ErrCodeNone           = 0
	ErrCodeInvalidParams  = 1001
	ErrCodeNotFound       = 1002
	ErrCodeInternalError  = 1003
	ErrCodeRateLimited    = 1004
	ErrCodeTimeout        = 1005
	ErrCodeNotImplemented = 1006
	ErrCodePermission     = 1007
	ErrCodeInvalidState   = 1008
)

type ErrorWithCode struct {
	Code       int
	Message    string
	Suggestion string
}

func (e *ErrorWithCode) Error() string {
	return e.Message
}

func NewErrorWithCode(code int, msg string) error {
	return &ErrorWithCode{Code: code, Message: msg}
}

func NewErrorWithSuggestion(code int, msg, suggestion string) error {
	return &ErrorWithCode{Code: code, Message: msg, Suggestion: suggestion}
}

func generateRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func main() {
	configPath := flag.String("config", "", "path to config file (optional)")
	socketPath := flag.String("socket", "/var/run/rtparityd/rtparityd.sock", "unix socket path for control interface")
	adminSocketPath := flag.String("admin-socket", "", "unix socket path for admin interface (optional)")
	pidPath := flag.String("pid-file", "/var/run/rtparityd/rtparityd.pid", "pid file path")
	poolName := flag.String("pool-name", "demo", "prototype pool name")
	journalPath := flag.String("journal-path", "/var/lib/rtparityd/journal.bin", "journal path")
	metadataPath := flag.String("metadata-path", "/var/lib/rtparityd/metadata.bin", "metadata snapshot path")
	requireRootUser = flag.Bool("require-root", true, "require root user for socket connections (disable for non-root CLI)")
	showVersion := flag.Bool("version", false, "show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("rtparityd %s\n", daemonVersion)
		os.Exit(0)
	}

	var cfg *config.Config
	if *configPath != "" {
		var err error
		cfg, err = config.Load(*configPath)
		if err != nil {
			logger.Error("failed to load config", "error", err)
			os.Exit(1)
		}
		logger.Info("loaded config file", "path", *configPath)
	} else {
		cfg = config.Default()
	}

	logger.SetMinLevel(logger.Level(cfg.Logging.Level))
	logger.SetJSON(cfg.Logging.JSON)

	if cfg.Audit.Enabled {
		if err := audit.Enable(cfg.Audit.Path); err != nil {
			logger.Error("failed to enable audit logging", "error", err)
		} else {
			logger.Info("audit logging enabled", "path", cfg.Audit.Path)
		}
	}

	pName := *poolName
	if pName == "" {
		pName = cfg.Global.PoolName
	}
	jPath := *journalPath
	if jPath == "" {
		jPath = cfg.Global.JournalPath
	}
	mPath := *metadataPath
	if mPath == "" {
		mPath = cfg.Global.MetadataPath
	}
	sPath := *socketPath
	if sPath == "" {
		sPath = cfg.Server.SocketPath
	}

	if !filepath.IsAbs(sPath) {
		logger.Error("socket path must be absolute", "path", sPath)
		os.Exit(1)
	}
	if !filepath.IsAbs(jPath) {
		logger.Error("journal-path must be absolute", "path", jPath)
		os.Exit(1)
	}
	if !filepath.IsAbs(mPath) {
		logger.Error("metadata-path must be absolute", "path", mPath)
		os.Exit(1)
	}
	if pName == "" {
		logger.Error("pool-name is required")
		os.Exit(1)
	}
	if strings.ContainsAny(pName, "/\\:") {
		logger.Error("pool-name must not contain path separators", "pool_name", pName)
		os.Exit(1)
	}
	if filepath.Dir(mPath) == filepath.Dir(jPath) {
		logger.Warn("metadata and journal share the same directory")
	}

	if err := os.MkdirAll(filepath.Dir(sPath), 0o755); err != nil {
		logger.Error("create socket directory", "error", err)
		os.Exit(1)
	}
	if err := os.Remove(sPath); err != nil && !os.IsNotExist(err) {
		logger.Error("remove existing socket", "error", err)
		os.Exit(1)
	}

	listener, err := net.Listen("unix", sPath)
	if err != nil {
		logger.Error("listen on socket", "error", err)
		os.Exit(1)
	}
	if err := os.Chmod(sPath, socketPerm); err != nil {
		logger.Error("set socket permissions", "error", err)
		os.Exit(1)
	}
	defer os.Remove(sPath)

	state := loadRuntimeState(pName, jPath, mPath, cfg)
	if state.StartupError != "" {
		logger.Warn("startup inspection reported errors", "error", state.StartupError)
	} else {
		logger.Info("loaded metadata snapshot", "path", state.MetadataPath, "files", len(state.Prototype.Files), "extents", len(state.Prototype.Extents))
		if len(state.Recovery.RecoveredTxIDs) > 0 {
			logger.Info("startup replay recovered transactions", "count", len(state.Recovery.RecoveredTxIDs), "txids", state.Recovery.RecoveredTxIDs)
		}
		if len(state.Recovery.AbortedTxIDs) > 0 {
			logger.Info("startup replay aborted transactions", "count", len(state.Recovery.AbortedTxIDs), "txids", state.Recovery.AbortedTxIDs)
		}
		if state.JournalSummary.RequiresReplay {
			logger.Info("journal replay required", "count", len(state.JournalSummary.IncompleteTxIDs))
		} else {
			logger.Info("journal clean", "path", state.JournalPath, "records", state.JournalSummary.TotalRecords)
		}
	}

	if state.StartupAdmission.Status == "refuse" {
		logger.Error("refusing startup", "reasons", strings.Join(state.StartupAdmission.Reasons, "; "))
		os.Exit(1)
	}

	logger.Info("startup admission status", "status", state.StartupAdmission.Status)
	if len(state.StartupAdmission.Reasons) > 0 {
		logger.Warn("startup admission reasons", "reasons", strings.Join(state.StartupAdmission.Reasons, "; "))
	}

	pidStr := fmt.Sprintf("%d", os.Getpid())
	if err := os.WriteFile(*pidPath, []byte(pidStr), 0o644); err != nil {
		logger.Error("failed to write pid file", "error", err)
		os.Exit(1)
	}

	logger.Info("rtparityd listening", "socket", sPath, "pid", pidStr)

	var adminListener net.Listener
	if *adminSocketPath != "" {
		if !filepath.IsAbs(*adminSocketPath) {
			logger.Error("admin-socket path must be absolute", "path", *adminSocketPath)
			os.Exit(1)
		}
		if err := os.MkdirAll(filepath.Dir(*adminSocketPath), 0o755); err != nil {
			logger.Error("create admin socket directory", "error", err)
			os.Exit(1)
		}
		if err := os.Remove(*adminSocketPath); err != nil && !os.IsNotExist(err) {
			logger.Error("remove existing admin socket", "error", err)
			os.Exit(1)
		}
		adminListener, err = net.Listen("unix", *adminSocketPath)
		if err != nil {
			logger.Error("listen on admin socket", "error", err)
			os.Exit(1)
		}
		if err := os.Chmod(*adminSocketPath, 0o660); err != nil {
			logger.Error("set admin socket permissions", "error", err)
			os.Exit(1)
		}
		defer os.Remove(*adminSocketPath)
		logger.Info("admin socket listening", "socket", *adminSocketPath)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down")
		state.mu.Lock()
		state.ShuttingDown = true
		state.Draining = true
		state.mu.Unlock()
		listener.Close()
		if adminListener != nil {
			adminListener.Close()
		}
	}()

	go func() {
		<-state.shutdownCh
		logger.Info("waiting for in-flight requests to complete")
		for {
			inFlight := atomic.LoadInt64(&state.inFlight)
			if inFlight == 0 {
				break
			}
			logger.Info("in-flight requests", "count", inFlight)
			time.Sleep(500 * time.Millisecond)
		}
		logger.Info("shutdown complete")
		os.Remove(*pidPath)
		os.Exit(0)
	}()

	connSem := make(chan struct{}, maxConnections)

	go monitorMemory()
	go monitorMaintenanceWindow(state)
	go monitorAlerts(state)

	if cfg != nil && cfg.Scrub.Enabled {
		go startScrubScheduler(cfg, state)
	}

	if cfg != nil && cfg.Global.SnapshotIntervalHrs > 0 {
		go startSnapshotScheduler(cfg, state)
	}

	if *configPath != "" {
		go watchConfigFile(*configPath, state)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed") {
					break
				}
				logger.Error("accept error", "error", err)
				continue
			}

			state.mu.RLock()
			shuttingDown := state.ShuttingDown
			admission := state.StartupAdmission
			state.mu.RUnlock()

			if shuttingDown || admission.Status == "refuse" {
				conn.Close()
				if admission.Status == "refuse" {
					logger.Warn("rejecting connection: startup not ready")
				}
				continue
			}

			m := metrics.Global()
			m.ConnectionsAccepted.Inc()

			connSem <- struct{}{}
			go func() {
				handleConnection(conn, state)
				<-connSem
			}()
		}
	}()

	if adminListener != nil {
		for {
			conn, err := adminListener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed") {
					break
				}
				logger.Error("admin accept error", "error", err)
				continue
			}
			go handleAdminConnection(conn, state)
		}
	}

	<-sigCh
	logger.Info("rtparityd stopped")
}

func sendAlert(state *runtimeState, alertType string, message string) {
	state.cfgMu.RLock()
	cfg := state.cfg
	state.cfgMu.RUnlock()

	if cfg == nil || !cfg.Alert.Enabled || cfg.Alert.URL == "" {
		return
	}

	payload := map[string]any{
		"alert_type": alertType,
		"message":    message,
		"pool_name":  cfg.Global.PoolName,
		"version":    daemonVersion,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, cfg.Alert.URL, bytes.NewReader(body))
	if err != nil {
		logger.Error("failed to create alert request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("failed to send alert", "error", err)
		return
	}
	resp.Body.Close()
	logger.Info("alert sent", "type", alertType)
}

func monitorAlerts(state *runtimeState) {
	if state.cfg == nil || !state.cfg.Alert.Enabled {
		return
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	var lastStartupError string
	var lastDiskFailures int

	for {
		select {
		case <-state.shutdownCh:
			return
		case <-ticker.C:
			cleanupIdempotencyCache(state)

			state.mu.RLock()
			startupError := state.StartupError
			failedDisks := 0
			for _, disk := range state.Prototype.Disks {
				if disk.HealthStatus == "failed" {
					failedDisks++
				}
			}
			shuttingDown := state.ShuttingDown
			state.mu.RUnlock()

			if startupError != "" && startupError != lastStartupError {
				sendAlert(state, "startup_error", startupError)
				lastStartupError = startupError
			}

			if failedDisks > 0 && failedDisks != lastDiskFailures {
				sendAlert(state, "disk_failure", fmt.Sprintf("%d disks failed", failedDisks))
				lastDiskFailures = failedDisks
			}

			if shuttingDown && !shuttingDown {
				sendAlert(state, "shutdown", "daemon is shutting down")
			}
		}
	}
}

func monitorMemory() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		memUsedMB := memStats.Alloc / 1024 / 1024
		if memUsedMB > memoryThresholdMB {
			logger.Warn("high memory usage", "memory_mb", memUsedMB, "threshold_mb", memoryThresholdMB)
		}
	}
}

func watchConfigFile(configPath string, state *runtimeState) {
	var lastModTime time.Time
	if fi, err := os.Stat(configPath); err == nil {
		lastModTime = fi.ModTime()
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-state.shutdownCh:
			return
		case <-ticker.C:
			fi, err := os.Stat(configPath)
			if err != nil {
				continue
			}
			if !fi.ModTime().Equal(lastModTime) {
				lastModTime = fi.ModTime()
				logger.Info("config file changed, triggering reload", "path", configPath)
				params := fmt.Sprintf(`{"config_path": "%s"}`, configPath)
				_, _ = reloadConfigOp(json.RawMessage(params), state)
			}
		}
	}
}

func monitorMaintenanceWindow(state *runtimeState) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-state.shutdownCh:
			return
		case now := <-ticker.C:
			hour := now.Hour()
			isMaintenanceHour := hour >= 2 && hour < 5
			logger.Debug("maintenance window check", "hour", hour, "maintenance", isMaintenanceHour)
		}
	}
}

func startScrubScheduler(cfg *config.Config, state *runtimeState) {
	interval := time.Duration(cfg.Scrub.IntervalHours) * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("scrub scheduler started", "interval_hours", cfg.Scrub.IntervalHours)

	for {
		select {
		case <-state.shutdownCh:
			logger.Info("scrub scheduler stopped")
			return
		case now := <-ticker.C:
			hour := now.Hour()
			if hour >= cfg.Scrub.StartHour && hour < cfg.Scrub.EndHour {
				logger.Info("scheduled scrub starting")
				state.mu.RLock()
				metadataPath := state.MetadataPath
				journalPath := state.JournalPath
				state.mu.RUnlock()

				coord := journal.NewCoordinator(metadataPath, journalPath)
				_, err := coord.Scrub(false)
				if err != nil {
					logger.Error("scheduled scrub failed", "error", err)
				} else {
					logger.Info("scheduled scrub completed")
				}
			} else {
				logger.Debug("scrub skipped: outside maintenance window", "hour", hour)
			}
		}
	}
}

func startSnapshotScheduler(cfg *config.Config, state *runtimeState) {
	if cfg.Global.SnapshotIntervalHrs <= 0 {
		return
	}

	interval := time.Duration(cfg.Global.SnapshotIntervalHrs) * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("snapshot scheduler started", "interval_hours", cfg.Global.SnapshotIntervalHrs)

	for {
		select {
		case <-state.shutdownCh:
			logger.Info("snapshot scheduler stopped")
			return
		case <-ticker.C:
			state.mu.RLock()
			proto := state.Prototype
			metadataPath := state.MetadataPath
			state.mu.RUnlock()

			store := metadata.NewStore(metadataPath)
			_, err := store.Save(proto)
			if err != nil {
				logger.Error("scheduled snapshot failed", "error", err)
			} else {
				logger.Info("scheduled snapshot completed")
			}
		}
	}
}

func handleConnection(conn net.Conn, state *runtimeState) {
	defer conn.Close()

	if !checkPeerCredentials(conn) {
		logger.Warn("rejected connection: invalid peer credentials")
		return
	}

	enableKeepalive(conn)

	metrics.Global().ActiveConnections.Inc()
	defer metrics.Global().ActiveConnections.Dec()

	state.cfgMu.RLock()
	readTimeout := connectionTimeout
	if state.cfg != nil && state.cfg.Server.ReadTimeout > 0 {
		readTimeout = time.Duration(state.cfg.Server.ReadTimeout) * time.Second
	}
	state.cfgMu.RUnlock()

	conn.SetDeadline(time.Now().Add(readTimeout))
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetWriteDeadline(time.Now().Add(readTimeout))

	dec := json.NewDecoder(io.LimitReader(conn, maxRequestSize))
	enc := json.NewEncoder(conn)

	lastActivity := time.Now()

	for {
		var cmd command
		if err := dec.Decode(&cmd); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			sendError(enc, "", fmt.Errorf("decode command: %w", err))
			return
		}

		if cmd.ID == "" {
			cmd.ID = generateRequestID()
		}

		lastActivity = time.Now()
		conn.SetReadDeadline(time.Now().Add(connectionTimeout))
		conn.SetWriteDeadline(time.Now().Add(connectionTimeout))

		start := time.Now()
		resp := response{ID: cmd.ID, Seq: cmd.Seq, Ts: cmd.Ts, TraceID: cmd.TraceID, Version: APIVersion}

		state.cfgMu.RLock()
		maxTimeoutMs := int64(0)
		slowThreshold := int64(5000)
		if state.cfg != nil {
			maxTimeoutMs = state.cfg.Server.MaxTimeoutMs
			slowThreshold = state.cfg.Limits.SlowQueryThreshold
		}
		state.cfgMu.RUnlock()

		var cancel context.CancelFunc
		ctx := context.Background()
		if cmd.TimeoutMs > 0 {
			if cmd.TimeoutMs > maxTimeoutMs && maxTimeoutMs > 0 {
				cmd.TimeoutMs = maxTimeoutMs
			}
			ctx, cancel = context.WithTimeout(ctx, time.Duration(cmd.TimeoutMs)*time.Millisecond)
			defer cancel()
		}

		select {
		case <-ctx.Done():
			sendError(enc, cmd.ID, NewErrorWithCode(ErrCodeTimeout, "request timeout: "+ctx.Err().Error()))
			continue
		default:
		}

		if cmd.Op == "write" || cmd.Op == "bulk-write" {
			if cached, errStr, ok := checkIdempotency(state, cmd.IdempotencyKey); ok {
				if errStr != "" {
					sendError(enc, cmd.ID, errors.New(errStr))
				} else {
					resp.Result = cached
					resp.LatencyMs = time.Since(start).Milliseconds()
					if err := enc.Encode(resp); err != nil {
						logger.Error("encode response", "error", err)
						return
					}
				}
				continue
			}
		}

		atomic.AddInt64(&state.inFlight, 1)
		result, err := handleCommand(ctx, cmd.Op, cmd.Params, state)
		atomic.AddInt64(&state.inFlight, -1)
		resp.LatencyMs = time.Since(start).Milliseconds()

		m := metrics.Global()
		m.CommandsProcessed.Inc()

		if resp.LatencyMs > slowThreshold {
			logger.Warn("slow query",
				"op", cmd.Op,
				"latency_ms", resp.LatencyMs,
				"threshold_ms", slowThreshold,
				"trace_id", cmd.TraceID,
			)
		}

		if err != nil {
			resp.Error = err.Error()
			if e, ok := err.(*ErrorWithCode); ok {
				resp.ErrorCode = e.Code
				resp.Suggestion = e.Suggestion
			} else {
				resp.ErrorCode = ErrCodeInternalError
			}
		} else {
			resp.Result = result
		}

		if (cmd.Op == "write" || cmd.Op == "bulk-write") && cmd.IdempotencyKey != "" {
			storeIdempotency(state, cmd.IdempotencyKey, result, err)
		}

		if err := enc.Encode(resp); err != nil {
			logger.Error("encode response", "error", err)
			return
		}

		state.cfgMu.RLock()
		idle := idleTimeout
		if state.cfg != nil && state.cfg.Server.IdleTimeout > 0 {
			idle = time.Duration(state.cfg.Server.IdleTimeout) * time.Second
		}
		state.cfgMu.RUnlock()

		if time.Since(lastActivity) > idle {
			logger.Info("connection idle timeout, closing")
			return
		}
	}
}

func handleAdminConnection(conn net.Conn, state *runtimeState) {
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		logger.Warn("admin connection is not a Unix socket")
		return
	}

	var ucred *unix.Ucred
	var err error
	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		logger.Error("failed to get raw conn", "error", err)
		return
	}

	err = rawConn.Control(func(fd uintptr) {
		ucred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil {
		logger.Error("failed to get peer credentials", "error", err)
		return
	}

	if ucred.Uid == 0 {
		logger.Info("admin connection from root", "pid", ucred.Pid)
	} else {
		logger.Warn("admin connection rejected: non-root", "uid", ucred.Uid)
		return
	}

	conn.SetDeadline(time.Now().Add(60 * time.Second))
	dec := json.NewDecoder(io.LimitReader(conn, maxRequestSize))
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

		if cmd.ID == "" {
			cmd.ID = generateRequestID()
		}

		conn.SetDeadline(time.Now().Add(60 * time.Second))

		start := time.Now()
		resp := response{ID: cmd.ID, Seq: cmd.Seq, Ts: cmd.Ts, TraceID: cmd.TraceID}
		result, err := handleAdminCommand(context.Background(), cmd.Op, cmd.Params, state)
		resp.LatencyMs = time.Since(start).Milliseconds()
		if err != nil {
			resp.Error = err.Error()
			if e, ok := err.(*ErrorWithCode); ok {
				resp.ErrorCode = e.Code
				resp.Suggestion = e.Suggestion
			} else {
				resp.ErrorCode = ErrCodeInternalError
			}
		} else {
			resp.Result = result
		}

		if err := enc.Encode(resp); err != nil {
			logger.Error("encode response", "error", err)
			return
		}
	}
}

func handleAdminCommand(ctx context.Context, op string, params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	switch op {
	case "shutdown":
		audit.Log("admin_shutdown", "admin", "requested", "")
		return adminShutdownOp(state)
	case "drain":
		audit.Log("admin_drain", "admin", "requested", "")
		return adminDrainOp(state)
	case "status":
		return adminStatusOp(state)
	case "force-gc":
		audit.Log("admin_force_gc", "admin", "requested", "")
		return adminForceGCOp()
	default:
		return nil, fmt.Errorf("unknown admin operation: %s", op)
	}
}

func adminShutdownOp(state *runtimeState) (json.RawMessage, error) {
	logger.Warn("admin shutdown requested")
	state.Draining = true
	state.ShuttingDown = true
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(state.shutdownCh)
	}()
	return json.Marshal(map[string]any{"status": "shutting_down"})
}

func adminDrainOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.Lock()
	state.Draining = true
	state.ShuttingDown = true
	state.mu.Unlock()
	logger.Warn("drain mode enabled")
	return json.Marshal(map[string]any{"status": "draining"})
}

func adminStatusOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return json.Marshal(map[string]any{
		"uptime_seconds": time.Since(state.StartedAt).Seconds(),
		"shutting_down":  state.ShuttingDown,
		"memory_mb":      memStats.Alloc / 1024 / 1024,
		"goroutines":     runtime.NumGoroutine(),
		"gc_count":       memStats.NumGC,
		"files":          len(state.Prototype.Files),
		"extents":        len(state.Prototype.Extents),
	})
}

func adminForceGCOp() (json.RawMessage, error) {
	before := time.Now()
	runtime.GC()
	return json.Marshal(map[string]any{
		"status":      "gc_completed",
		"duration_ms": time.Since(before).Milliseconds(),
	})
}

func checkPeerCredentials(conn net.Conn) bool {
	if !*requireRootUser {
		return true
	}

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		logger.Warn("connection is not a Unix socket, rejecting")
		return false
	}

	var ucred *unix.Ucred
	var err error
	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		logger.Error("failed to get raw conn", "error", err)
		return false
	}

	err = rawConn.Control(func(fd uintptr) {
		ucred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil {
		logger.Error("failed to get peer credentials", "error", err)
		return false
	}

	if ucred == nil || ucred.Uid != 0 {
		logger.Warn("rejected: non-root user connected")
		return false
	}

	return true
}

func sendError(enc *json.Encoder, id string, err error) {
	errCode := ErrCodeInternalError
	suggestion := ""
	if e, ok := err.(*ErrorWithCode); ok {
		errCode = e.Code
		suggestion = e.Suggestion
	}
	enc.Encode(response{ID: id, Error: err.Error(), ErrorCode: errCode, Suggestion: suggestion})
}

func handleCommand(ctx context.Context, op string, params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	switch op {
	case "health":
		return healthOp(state)
	case "liveness":
		return livenessOp(state)
	case "readiness":
		return readinessOp(state)
	case "metrics":
		return metricsOp()
	case "write":
		return writeOp(params, state)
	case "bulk-write":
		return bulkWriteOp(params, state)
	case "read":
		return readOp(params, state)
	case "bulk-read":
		return bulkReadOp(params, state)
	case "scrub":
		return scrubOp(params, state)
	case "rebuild":
		return rebuildOp(params, state)
	case "rebuild-all":
		return rebuildAllOp(state)
	case "status":
		return statusOp(state)
	case "backup":
		return backupOp(params, state)
	case "restore":
		return restoreOp(params, state)
	case "export-metadata":
		return exportMetadataOp(params, state)
	case "import-metadata":
		return importMetadataOp(params, state)
	case "reload-config":
		return reloadConfigOp(params, state)
	case "disk-stats":
		return diskStatsOp(state)
	case "prometheus":
		return prometheusOp()
	case "version":
		return versionOp()
	case "scrub-history":
		return scrubHistoryOp(state)
	case "pool-stats":
		return poolStatsOp(state)
	case "memory-pool-stats":
		return memoryPoolStatsOp()
	case "locks":
		return locksOp(state)
	case "ping":
		return pingOp()
	case "check-consistency":
		return checkConsistencyOp(state)
	case "repair-metadata":
		return repairMetadataOp(state)
	case "compact-journal":
		return compactJournalOp(state)
	case "export-journal":
		return exportJournalOp(params, state)
	case "migrate-disk":
		return migrateDiskOp(params, state)
	case "recovery-status":
		return recoveryStatusOp(state)
	case "host-info":
		return hostInfoOp()
	case "system-stats":
		return systemStatsOp(state)
	case "validate-config":
		return validateConfigOp(params, state)
	case "operations":
		return operationsOp()
	case "batch":
		return batchOp(params, state)
	case "state-dump":
		return stateDumpOp(state)
	case "config-get":
		return configGetOp(state)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func healthOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	startupError := state.StartupError
	status := state.healthStatus()
	journalSummary := state.JournalSummary
	state.mu.RUnlock()

	failedDisks := 0
	degradedDisks := 0
	for _, disk := range proto.Disks {
		if disk.HealthStatus != "" && disk.HealthStatus != "online" {
			if disk.HealthStatus == "failed" {
				failedDisks++
			} else {
				degradedDisks++
			}
		}
	}

	alertLevel := "ok"
	if failedDisks > 0 || startupError != "" {
		alertLevel = "critical"
	} else if degradedDisks > 0 {
		alertLevel = "warning"
	}

	components := map[string]any{
		"journal": map[string]any{
			"status":          "ok",
			"records":         journalSummary.TotalRecords,
			"requires_replay": journalSummary.RequiresReplay,
		},
		"metadata": map[string]any{
			"status":  "ok",
			"files":   len(proto.Files),
			"extents": len(proto.Extents),
			"disks":   len(proto.Disks),
		},
		"disk": map[string]any{
			"status":   "ok",
			"total":    len(proto.Disks),
			"failed":   failedDisks,
			"degraded": degradedDisks,
		},
		"memory": map[string]any{
			"status": "ok",
		},
	}

	if failedDisks > 0 {
		components["disk"].(map[string]any)["status"] = "critical"
	} else if degradedDisks > 0 {
		components["disk"].(map[string]any)["status"] = "degraded"
	}

	if startupError != "" {
		components["journal"].(map[string]any)["status"] = "error"
		components["metadata"].(map[string]any)["status"] = "error"
		alertLevel = "critical"
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	result := map[string]any{
		"name":              "rtparityd",
		"status":            status,
		"version":           daemonVersion,
		"alert":             alertLevel,
		"uptime_seconds":    time.Since(state.StartedAt).Seconds(),
		"files_managed":     len(proto.Files),
		"extents_allocated": len(proto.Extents),
		"disks_total":       len(proto.Disks),
		"disks_failed":      failedDisks,
		"journal_records":   journalSummary.TotalRecords,
		"requires_replay":   journalSummary.RequiresReplay,
		"startup_error":     startupError,
		"shutting_down":     state.ShuttingDown,
		"components":        components,
		"runtime": map[string]any{
			"goroutines":     runtime.NumGoroutine(),
			"memory_mb":      memStats.Alloc / 1024 / 1024,
			"total_alloc_mb": memStats.TotalAlloc / 1024 / 1024,
			"sys_mb":         memStats.Sys / 1024 / 1024,
			"gc_count":       memStats.NumGC,
		},
	}
	return json.Marshal(result)
}

func livenessOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()

	if state.ShuttingDown {
		return json.Marshal(map[string]any{"status": "not alive", "reason": "shutting_down"})
	}

	return json.Marshal(map[string]any{"status": "alive"})
}

func readinessOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	admission := state.StartupAdmission
	startupError := state.StartupError
	requiresReplay := state.JournalSummary.RequiresReplay
	draining := state.Draining
	state.mu.RUnlock()

	if draining {
		return json.Marshal(map[string]any{
			"status": "not ready",
			"reason": "draining",
		})
	}

	if admission.Status == "refuse" {
		return json.Marshal(map[string]any{
			"status": "not ready",
			"reason": "startup refused",
			"errors": admission.Reasons,
		})
	}

	if startupError != "" {
		return json.Marshal(map[string]any{
			"status": "not ready",
			"reason": "startup error",
			"errors": []string{startupError},
		})
	}

	if requiresReplay {
		return json.Marshal(map[string]any{
			"status": "not ready",
			"reason": "journal requires replay",
		})
	}

	return json.Marshal(map[string]any{"status": "ready"})
}

func stateDumpOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return json.Marshal(map[string]any{
		"daemon": map[string]any{
			"version":        daemonVersion,
			"started_at":     state.StartedAt,
			"uptime_seconds": time.Since(state.StartedAt).Seconds(),
		},
		"runtime": map[string]any{
			"goroutines":     runtime.NumGoroutine(),
			"memory_mb":      memStats.Alloc / 1024 / 1024,
			"total_alloc_mb": memStats.TotalAlloc / 1024 / 1024,
			"gc_count":       memStats.NumGC,
		},
		"state": map[string]any{
			"shutting_down":  state.ShuttingDown,
			"startup_status": state.StartupAdmission.Status,
			"startup_error":  state.StartupError,
		},
		"limits": map[string]any{
			"write_rate":  state.writeLimiter.Limit(),
			"write_burst": state.writeLimiter.Burst(),
			"read_rate":   state.readLimiter.Limit(),
			"read_burst":  state.readLimiter.Burst(),
		},
	})
}

func configGetOp(state *runtimeState) (json.RawMessage, error) {
	state.cfgMu.RLock()
	cfg := state.cfg
	state.cfgMu.RUnlock()

	if cfg == nil {
		return json.Marshal(map[string]any{
			"status": "no config loaded",
		})
	}

	return json.Marshal(map[string]any{
		"status": "ok",
		"config": map[string]any{
			"pool_name":     cfg.Global.PoolName,
			"journal_path":  cfg.Global.JournalPath,
			"metadata_path": cfg.Global.MetadataPath,
			"server": map[string]any{
				"socket_path":       cfg.Server.SocketPath,
				"require_root_user": cfg.Server.RequireRootUser,
				"read_timeout_sec":  cfg.Server.ReadTimeout,
				"idle_timeout_sec":  cfg.Server.IdleTimeout,
				"max_timeout_ms":    cfg.Server.MaxTimeoutMs,
			},
			"limits": map[string]any{
				"max_request_size_bytes":  cfg.Limits.MaxRequestSize,
				"max_write_size_bytes":    cfg.Limits.MaxWriteSize,
				"write_rate_limit":        cfg.Limits.WriteRateLimit,
				"write_burst":             cfg.Limits.WriteBurst,
				"read_rate_limit":         cfg.Limits.ReadRateLimit,
				"read_burst":              cfg.Limits.ReadBurst,
				"slow_query_threshold_ms": cfg.Limits.SlowQueryThreshold,
			},
			"logging": map[string]any{
				"level": cfg.Logging.Level,
				"json":  cfg.Logging.JSON,
			},
			"scrub": map[string]any{
				"enabled":        cfg.Scrub.Enabled,
				"interval_hours": cfg.Scrub.IntervalHours,
				"start_hour":     cfg.Scrub.StartHour,
				"end_hour":       cfg.Scrub.EndHour,
			},
		},
	})
}

func metricsOp() (json.RawMessage, error) {
	rec := metrics.Global()
	snap := rec.Snapshot()
	return json.Marshal(snap)
}

func prometheusOp() (json.RawMessage, error) {
	rec := metrics.Global()
	output := rec.Prometheus()
	return json.Marshal(map[string]any{"metrics": output})
}

func versionOp() (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"version":    daemonVersion,
		"build_date": daemonBuildDate,
		"git_commit": daemonGitCommit,
	})
}

func pingOp() (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"status": "pong",
		"mode":   "async",
	})
}

func hostInfoOp() (json.RawMessage, error) {
	info, err := host.Info()
	if err != nil {
		return nil, fmt.Errorf("get host info: %w", err)
	}

	uptime, _ := host.Uptime()

	return json.Marshal(map[string]any{
		"hostname":     info.Hostname,
		"os":           info.OS,
		"platform":     info.Platform,
		"platform_ver": info.PlatformVersion,
		"arch":         info.KernelArch,
		"kernel":       info.KernelVersion,
		"uptime_sec":   uptime,
	})
}

func systemStatsOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	state.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	onlineDisks := 0
	failedDisks := 0
	for _, d := range proto.Disks {
		if d.HealthStatus == "online" {
			onlineDisks++
		} else {
			failedDisks++
		}
	}

	return json.Marshal(map[string]any{
		"runtime": map[string]any{
			"goroutines":     runtime.NumGoroutine(),
			"memory_mb":      memStats.Alloc / 1024 / 1024,
			"total_alloc_mb": memStats.TotalAlloc / 1024 / 1024,
			"gc_count":       memStats.NumGC,
		},
		"storage": map[string]any{
			"total_disks":   len(proto.Disks),
			"online_disks":  onlineDisks,
			"failed_disks":  failedDisks,
			"total_files":   len(proto.Files),
			"total_extents": len(proto.Extents),
		},
	})
}

type validateConfigParams struct {
	ConfigPath string `json:"config_path"`
}

func validateConfigOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p validateConfigParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.ConfigPath == "" {
		return nil, errors.New("config_path is required")
	}

	cfg, err := config.Load(p.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return json.Marshal(map[string]any{
		"status":      "valid",
		"config_path": p.ConfigPath,
		"pool_name":   cfg.Global.PoolName,
	})
}

func operationsOp() (json.RawMessage, error) {
	ops := map[string][]string{
		"info":        {"ping", "version", "health", "host-info", "system-stats", "operations"},
		"metrics":     {"metrics", "prometheus", "disk-stats", "pool-stats", "memory-pool-stats", "locks", "config-get"},
		"maintenance": {"check-consistency", "repair-metadata", "compact-journal", "export-journal", "validate-config"},
		"recovery":    {"migrate-disk", "recovery-status", "scrub", "scrub-history", "rebuild", "rebuild-all"},
		"data":        {"backup", "restore", "export-metadata", "import-metadata"},
		"io":          {"write", "read", "bulk-write", "bulk-read", "reload-config"},
	}
	count := 0
	for _, list := range ops {
		count += len(list)
	}
	return json.Marshal(map[string]any{
		"operations": ops,
		"total":      count,
	})
}

func batchOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var req batchRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("parse batch request: %w", err)
	}
	if len(req.Commands) == 0 {
		return nil, errors.New("commands required")
	}
	if len(req.Commands) > 100 {
		return nil, errors.New("max 100 commands per batch")
	}

	responses := make([]response, 0, len(req.Commands))
	for _, cmd := range req.Commands {
		start := time.Now()
		resp := response{ID: cmd.ID, Seq: cmd.Seq, Ts: cmd.Ts, TraceID: cmd.TraceID, Version: APIVersion}
		result, err := handleCommand(context.Background(), cmd.Op, cmd.Params, state)
		resp.LatencyMs = time.Since(start).Milliseconds()

		state.cfgMu.RLock()
		slowThreshold := int64(5000)
		if state.cfg != nil {
			slowThreshold = state.cfg.Limits.SlowQueryThreshold
		}
		state.cfgMu.RUnlock()
		if resp.LatencyMs > slowThreshold {
			logger.Warn("slow query",
				"op", cmd.Op,
				"latency_ms", resp.LatencyMs,
				"threshold_ms", slowThreshold,
				"trace_id", cmd.TraceID,
			)
		}

		if err != nil {
			resp.Error = err.Error()
			if e, ok := err.(*ErrorWithCode); ok {
				resp.ErrorCode = e.Code
				resp.Suggestion = e.Suggestion
			} else {
				resp.ErrorCode = ErrCodeInternalError
			}
		} else {
			resp.Result = result
		}
		responses = append(responses, resp)
	}

	return json.Marshal(batchResponse{ID: req.ID, Responses: responses})
}

func checkConsistencyOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	metadataPath := state.MetadataPath
	state.mu.RUnlock()

	rootDir := filepath.Dir(metadataPath)
	violations := journal.CheckIntegrityInvariants(rootDir, state.Prototype)

	return json.Marshal(map[string]any{
		"status":      "checked",
		"violations":  len(violations),
		"details":     violations,
		"metadata_ok": len(violations) == 0,
	})
}

func repairMetadataOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.Lock()
	state.ShuttingDown = true
	state.mu.Unlock()

	logger.Warn("repair-metadata: marking for maintenance mode")

	state.mu.RLock()
	metadataPath := state.MetadataPath
	state.mu.RUnlock()

	return json.Marshal(map[string]any{
		"status":        "repair_initiated",
		"note":          "metadata repair requires manual intervention",
		"metadata_path": metadataPath,
	})
}

func compactJournalOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	journalPath := state.JournalPath
	state.mu.RUnlock()

	store := journal.NewStore(journalPath)
	if err := store.CompactIfClean(); err != nil {
		return nil, fmt.Errorf("compact journal: %w", err)
	}

	return json.Marshal(map[string]any{
		"status":  "compacted",
		"journal": journalPath,
	})
}

type exportJournalParams struct {
	OutputPath  string `json:"output_path"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
}

func exportJournalOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p exportJournalParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.OutputPath == "" {
		return nil, errors.New("output_path is required")
	}

	state.mu.RLock()
	journalPath := state.JournalPath
	state.mu.RUnlock()

	store := journal.NewStore(journalPath)
	summary, err := store.Replay()
	if err != nil {
		return nil, fmt.Errorf("replay journal: %w", err)
	}

	exportData := map[string]any{
		"exported_at":     time.Now().UTC().Format(time.RFC3339),
		"journal_path":    journalPath,
		"total_records":   summary.TotalRecords,
		"requires_replay": summary.RequiresReplay,
		"incomplete_txs":  len(summary.IncompleteTxIDs),
	}

	data, _ := json.MarshalIndent(exportData, "", "  ")
	if err := os.WriteFile(p.OutputPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write export: %w", err)
	}

	return json.Marshal(map[string]any{
		"status":      "exported",
		"output_path": p.OutputPath,
		"records":     summary.TotalRecords,
	})
}

type migrateDiskParams struct {
	SourceDisk string `json:"source_disk"`
	TargetDisk string `json:"target_disk"`
}

func migrateDiskOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p migrateDiskParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.SourceDisk == "" || p.TargetDisk == "" {
		return nil, errors.New("source_disk and target_disk are required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	coord := journal.NewCoordinator(metadataPath, journalPath)
	_, err := coord.RebuildDataDisk(p.SourceDisk)
	if err != nil {
		return nil, fmt.Errorf("migrate disk: %w", err)
	}

	return json.Marshal(map[string]any{
		"status": "migration_started",
		"source": p.SourceDisk,
		"target": p.TargetDisk,
		"note":   "data will be migrated in background",
	})
}

func recoveryStatusOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	recovery := state.Recovery
	summary := state.JournalSummary
	state.mu.RUnlock()

	return json.Marshal(map[string]any{
		"recovery": map[string]any{
			"attempted_txs": len(recovery.AttemptedTxIDs),
			"recovered_txs": len(recovery.RecoveredTxIDs),
			"aborted_txs":   len(recovery.AbortedTxIDs),
		},
		"journal": map[string]any{
			"total_records":   summary.TotalRecords,
			"requires_replay": summary.RequiresReplay,
			"incomplete_txs":  len(summary.IncompleteTxIDs),
		},
		"status": "analyzed",
	})
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
	if p.Size > maxWriteSize {
		return nil, fmt.Errorf("size exceeds maximum allowed (%d bytes)", maxWriteSize)
	}
	if p.Size < 0 {
		return nil, errors.New("size must be non-negative")
	}

	state.mu.RLock()
	limiter := state.writeLimiter
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	if err := limiter.Wait(context.Background()); err != nil {
		return nil, NewErrorWithCode(ErrCodeRateLimited, "rate limit exceeded")
	}

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

	writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := coord.WriteFile(writeCtx, req)
	if err != nil {
		m.WriteErrors.Inc()
		audit.Log("write", "", "failed", err.Error())
		return nil, err
	}
	audit.Log("write", "", "success", p.Path)
	return json.Marshal(result)
}

type bulkWriteParams struct {
	Paths     []string `json:"paths"`
	Payload   []byte   `json:"payload,omitempty"`
	Size      int64    `json:"size"`
	Synthetic bool     `json:"synthetic,omitempty"`
}

func bulkWriteOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p bulkWriteParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if len(p.Paths) == 0 {
		return nil, errors.New("paths are required")
	}
	if len(p.Paths) > 100 {
		return nil, errors.New("max 100 paths per bulk operation")
	}

	state.mu.RLock()
	limiter := state.writeLimiter
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	results := make([]map[string]any, 0, len(p.Paths))
	errors := make([]string, 0, len(p.Paths))

	for _, path := range p.Paths {
		if err := limiter.Wait(context.Background()); err != nil {
			errors = append(errors, "rate limited")
			results = append(results, map[string]any{"path": path, "error": "rate limited"})
			continue
		}

		coord := journal.NewCoordinator(metadataPath, journalPath)
		req := journal.WriteRequest{
			PoolName:       "demo",
			LogicalPath:    path,
			Payload:        p.Payload,
			SizeBytes:      p.Size,
			AllowSynthetic: p.Synthetic,
		}

		m.WriteOps.Inc()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := coord.WriteFile(ctx, req)
		cancel()

		if err != nil {
			m.WriteErrors.Inc()
			errors = append(errors, err.Error())
			results = append(results, map[string]any{"path": path, "error": err.Error()})
		} else {
			results = append(results, map[string]any{"path": path, "status": "written"})
		}
	}

	return json.Marshal(map[string]any{
		"total":   len(p.Paths),
		"results": results,
		"errors":  errors,
	})
}

type bulkReadParams struct {
	Paths []string `json:"paths"`
}

func bulkReadOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p bulkReadParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if len(p.Paths) == 0 {
		return nil, errors.New("paths are required")
	}
	if len(p.Paths) > 100 {
		return nil, errors.New("max 100 paths per bulk operation")
	}

	state.mu.RLock()
	limiter := state.readLimiter
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	m := metrics.Global()
	results := make([]map[string]any, 0, len(p.Paths))

	for _, path := range p.Paths {
		if err := limiter.Wait(context.Background()); err != nil {
			results = append(results, map[string]any{"path": path, "error": "rate limited"})
			continue
		}

		coord := journal.NewCoordinator(metadataPath, journalPath)
		m.ReadOps.Inc()

		result, err := coord.ReadFile(path)
		if err != nil {
			m.ReadErrors.Inc()
			results = append(results, map[string]any{"path": path, "error": err.Error()})
		} else {
			results = append(results, map[string]any{
				"path":             path,
				"bytes_read":       result.BytesRead,
				"verified":         result.Verified,
				"content_checksum": result.ContentChecksum,
			})
		}
	}

	return json.Marshal(map[string]any{
		"total":   len(p.Paths),
		"results": results,
	})
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
	limiter := state.readLimiter
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	if err := limiter.Wait(context.Background()); err != nil {
		return nil, fmt.Errorf("rate limit: %w", err)
	}

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
		audit.Log("read", "", "failed", err.Error())
		return nil, err
	}
	audit.Log("read", "", "success", p.Path)
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

func diskStatsOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	state.mu.RUnlock()

	disks := make([]map[string]any, 0, len(proto.Disks))
	for _, d := range proto.Disks {
		extentsOnDisk := 0
		bytesOnDisk := int64(0)
		for _, e := range proto.Extents {
			if e.DataDiskID == d.DiskID || e.MirrorDiskID == d.DiskID {
				extentsOnDisk++
				bytesOnDisk += e.Length
			}
		}

		disks = append(disks, map[string]any{
			"disk_id":        d.DiskID,
			"uuid":           d.UUID,
			"role":           d.Role,
			"health_status":  d.HealthStatus,
			"capacity_bytes": d.CapacityBytes,
			"free_bytes":     d.FreeBytes,
			"used_bytes":     bytesOnDisk,
			"extents_count":  extentsOnDisk,
			"generation":     d.Generation,
			"mountpoint":     d.Mountpoint,
		})
	}

	parityGroups := make([]map[string]any, 0, len(proto.ParityGroups))
	for _, pg := range proto.ParityGroups {
		parityGroups = append(parityGroups, map[string]any{
			"parity_group_id":   pg.ParityGroupID,
			"parity_disk_id":    pg.ParityDiskID,
			"member_extent_ids": pg.MemberExtentIDs,
		})
	}

	return json.Marshal(map[string]any{
		"disks":         disks,
		"parity_groups": parityGroups,
		"total_disks":   len(proto.Disks),
		"total_groups":  len(proto.ParityGroups),
	})
}

func scrubHistoryOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	state.mu.RUnlock()

	type scrubRunSummary struct {
		RunID          string    `json:"run_id"`
		StartedAt      time.Time `json:"started_at"`
		CompletedAt    time.Time `json:"completed_at,omitempty"`
		Status         string    `json:"status"`
		FilesChecked   int       `json:"files_checked"`
		ExtentsChecked int       `json:"extents_checked"`
		HealedCount    int       `json:"healed_count"`
		FailedCount    int       `json:"failed_count"`
		IssueCount     int       `json:"issue_count"`
	}

	runs := make([]scrubRunSummary, 0, len(proto.ScrubHistory))
	for _, s := range proto.ScrubHistory {
		status := "completed"
		if s.CompletedAt.IsZero() {
			status = "in_progress"
		}
		runs = append(runs, scrubRunSummary{
			RunID:          s.RunID,
			StartedAt:      s.StartedAt,
			CompletedAt:    s.CompletedAt,
			Status:         status,
			FilesChecked:   s.FilesChecked,
			ExtentsChecked: s.ExtentsChecked,
			HealedCount:    s.HealedCount,
			FailedCount:    s.FailedCount,
			IssueCount:     s.IssueCount,
		})
	}

	return json.Marshal(map[string]any{
		"scrub_runs": runs,
		"total":      len(runs),
	})
}

func poolStatsOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	proto := state.Prototype
	state.mu.RUnlock()

	totalCapacity := int64(0)
	totalFree := int64(0)
	for _, d := range proto.Disks {
		totalCapacity += d.CapacityBytes
		totalFree += d.FreeBytes
	}

	totalExtents := 0
	totalExtentBytes := int64(0)
	for _, e := range proto.Extents {
		totalExtents++
		totalExtentBytes += e.Length
	}

	protectionClassCounts := make(map[string]int)
	for _, e := range proto.Extents {
		protectionClassCounts[string(e.ProtectionClass)]++
	}

	return json.Marshal(map[string]any{
		"pool": map[string]any{
			"name":       proto.Pool.Name,
			"version":    proto.Pool.Version,
			"created_at": proto.Pool.CreatedAt,
		},
		"capacity": map[string]any{
			"total_bytes":  totalCapacity,
			"free_bytes":   totalFree,
			"used_bytes":   totalCapacity - totalFree,
			"used_percent": float64(totalCapacity-totalFree) / float64(totalCapacity) * 100,
		},
		"extents": map[string]any{
			"count":       totalExtents,
			"total_bytes": totalExtentBytes,
		},
		"files": map[string]any{
			"count": len(proto.Files),
		},
		"disks": map[string]any{
			"total":    len(proto.Disks),
			"data":     countDisksByRole(proto.Disks, "data"),
			"parity":   countDisksByRole(proto.Disks, "parity"),
			"mirror":   countDisksByRole(proto.Disks, "mirror"),
			"metadata": countDisksByRole(proto.Disks, "metadata"),
		},
		"protection_classes": protectionClassCounts,
	})
}

func countDisksByRole(disks []metadata.Disk, role string) int {
	count := 0
	for _, d := range disks {
		if strings.ToLower(string(d.Role)) == strings.ToLower(role) {
			count++
		}
	}
	return count
}

func locksOp(state *runtimeState) (json.RawMessage, error) {
	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	lockFilePath := metadataPath + ".lock"

	return json.Marshal(map[string]any{
		"lock_file":      lockFilePath,
		"metadata_path":  metadataPath,
		"journal_path":   journalPath,
		"exclusive_lock": true,
	})
}

func memoryPoolStatsOp() (json.RawMessage, error) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return json.Marshal(map[string]any{
		"heap": map[string]any{
			"alloc_bytes":       memStats.Alloc,
			"total_alloc_bytes": memStats.TotalAlloc,
			"sys_bytes":         memStats.Sys,
			"heap_objects":      memStats.HeapObjects,
		},
		"gc": map[string]any{
			"num_gc":          memStats.NumGC,
			"last_gc_time_ns": memStats.LastGC,
			"pause_total_ns":  memStats.PauseTotalNs,
		},
		"goroutines": map[string]any{
			"count": runtime.NumGoroutine(),
		},
	})
}

type backupParams struct {
	OutputPath string `json:"output_path"`
}

func backupOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p backupParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.OutputPath == "" {
		return nil, errors.New("output_path is required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	state.mu.Lock()
	proto := state.Prototype
	state.mu.Unlock()

	backupDir := p.OutputPath
	if err := os.MkdirAll(backupDir, 0o750); err != nil {
		return nil, fmt.Errorf("create backup directory: %w", err)
	}

	metadataBackup := filepath.Join(backupDir, "metadata.bin")
	if err := copyFile(metadataPath, metadataBackup); err != nil {
		return nil, fmt.Errorf("backup metadata: %w", err)
	}

	journalBackup := filepath.Join(backupDir, "journal.bin")
	if err := copyFile(journalPath, journalBackup); err != nil {
		return nil, fmt.Errorf("backup journal: %w", err)
	}

	manifest := map[string]any{
		"version":       daemonVersion,
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
		"metadata_path": metadataPath,
		"journal_path":  journalPath,
		"files_count":   len(proto.Files),
		"extents_count": len(proto.Extents),
		"disks_count":   len(proto.Disks),
	}
	manifestData, _ := json.MarshalIndent(manifest, "", "  ")
	os.WriteFile(filepath.Join(backupDir, "manifest.json"), manifestData, 0o644)

	logger.Info("backup completed", "output_dir", backupDir)
	audit.Log("backup", "", "success", backupDir)

	return json.Marshal(map[string]any{
		"backup_dir": backupDir,
		"files":      len(proto.Files),
		"extents":    len(proto.Extents),
		"disks":      len(proto.Disks),
	})
}

type restoreParams struct {
	InputPath string `json:"input_path"`
}

func restoreOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p restoreParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.InputPath == "" {
		return nil, errors.New("input_path is required")
	}

	state.mu.RLock()
	metadataPath := state.MetadataPath
	journalPath := state.JournalPath
	state.mu.RUnlock()

	backupDir := p.InputPath

	metadataBackup := filepath.Join(backupDir, "metadata.bin")
	if _, err := os.Stat(metadataBackup); err != nil {
		return nil, fmt.Errorf("backup metadata not found: %w", err)
	}

	journalBackup := filepath.Join(backupDir, "journal.bin")
	if _, err := os.Stat(journalBackup); err != nil {
		return nil, fmt.Errorf("backup journal not found: %w", err)
	}

	metadataTemp := metadataPath + ".restore.tmp"
	if err := copyFile(metadataBackup, metadataTemp); err != nil {
		return nil, fmt.Errorf("restore metadata: %w", err)
	}
	if err := os.Rename(metadataTemp, metadataPath); err != nil {
		return nil, fmt.Errorf("replace metadata: %w", err)
	}

	journalTemp := journalPath + ".restore.tmp"
	if err := copyFile(journalBackup, journalTemp); err != nil {
		return nil, fmt.Errorf("restore journal: %w", err)
	}
	if err := os.Rename(journalTemp, journalPath); err != nil {
		return nil, fmt.Errorf("replace journal: %w", err)
	}

	logger.Info("restore completed", "backup_dir", backupDir)
	audit.Log("restore", "", "success", backupDir)

	return json.Marshal(map[string]any{
		"status":     "restored",
		"backup_dir": backupDir,
	})
}

type exportParams struct {
	OutputPath string `json:"output_path"`
	Format     string `json:"format"`
}

func exportMetadataOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p exportParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.OutputPath == "" {
		return nil, errors.New("output_path is required")
	}

	state.mu.RLock()
	proto := state.Prototype
	state.mu.RUnlock()

	var data []byte
	var err error
	if p.Format == "json" || p.Format == "" {
		data, err = json.MarshalIndent(proto, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal json: %w", err)
		}
	} else {
		return nil, errors.New("unsupported format: use json")
	}

	if err := os.WriteFile(p.OutputPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write file: %w", err)
	}

	logger.Info("metadata exported", "path", p.OutputPath, "format", p.Format)

	return json.Marshal(map[string]any{
		"output_path": p.OutputPath,
		"files":       len(proto.Files),
		"extents":     len(proto.Extents),
	})
}

type importParams struct {
	InputPath string `json:"input_path"`
}

func importMetadataOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p importParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}
	if p.InputPath == "" {
		return nil, errors.New("input_path is required")
	}

	data, err := os.ReadFile(p.InputPath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var proto metadata.SampleState
	if err := json.Unmarshal(data, &proto); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}

	state.mu.Lock()
	state.Prototype = proto
	state.mu.Unlock()

	logger.Info("metadata imported", "path", p.InputPath)

	return json.Marshal(map[string]any{
		"status":  "imported",
		"files":   len(proto.Files),
		"extents": len(proto.Extents),
	})
}

type reloadConfigParams struct {
	ConfigPath string `json:"config_path"`
}

func reloadConfigOp(params json.RawMessage, state *runtimeState) (json.RawMessage, error) {
	var p reloadConfigParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}

	if p.ConfigPath == "" {
		return nil, errors.New("config_path is required")
	}

	newCfg, err := config.Load(p.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	state.cfgMu.Lock()
	state.cfg = newCfg
	state.writeLimiter = rate.NewLimiter(rate.Limit(newCfg.Limits.WriteRateLimit), newCfg.Limits.WriteBurst)
	state.readLimiter = rate.NewLimiter(rate.Limit(newCfg.Limits.ReadRateLimit), newCfg.Limits.ReadBurst)
	state.cfgMu.Unlock()

	logger.SetMinLevel(logger.Level(newCfg.Logging.Level))
	logger.SetJSON(newCfg.Logging.JSON)

	select {
	case state.configReloaded <- struct{}{}:
	default:
	}

	logger.Info("config reloaded", "path", p.ConfigPath)

	return json.Marshal(map[string]any{
		"status":      "reloaded",
		"config_path": p.ConfigPath,
		"write_rate":  newCfg.Limits.WriteRateLimit,
		"read_rate":   newCfg.Limits.ReadRateLimit,
		"log_level":   newCfg.Logging.Level,
	})
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	return os.Chmod(dstFile.Name(), srcInfo.Mode())
}

func (s *runtimeState) healthStatus() string {
	if s.Draining {
		return "draining"
	}
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

func loadRuntimeState(poolName, journalPath, metadataPath string, cfg *config.Config) *runtimeState {
	writeRate := 10
	writeBurst := 20
	readRate := 50
	readBurst := 100
	if cfg != nil {
		writeRate = cfg.Limits.WriteRateLimit
		writeBurst = cfg.Limits.WriteBurst
		readRate = cfg.Limits.ReadRateLimit
		readBurst = cfg.Limits.ReadBurst
	}

	state := &runtimeState{
		StartedAt:        time.Now().UTC(),
		Prototype:        metadata.PrototypeState(poolName),
		MetadataPath:     metadataPath,
		JournalPath:      journalPath,
		writeLimiter:     rate.NewLimiter(rate.Limit(writeRate), writeBurst),
		readLimiter:      rate.NewLimiter(rate.Limit(readRate), readBurst),
		cfg:              cfg,
		configReloaded:   make(chan struct{}, 1),
		shutdownCh:       make(chan struct{}),
		idempotencyCache: make(map[string]idempotencyEntry),
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
