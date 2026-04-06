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

package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	value uint64
}

func (c *Counter) Inc() {
	atomic.AddUint64(&c.value, 1)
}

func (c *Counter) Add(n uint64) {
	atomic.AddUint64(&c.value, n)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.value)
}

func (c *Counter) Reset() {
	atomic.StoreUint64(&c.value, 0)
}

type Gauge struct {
	value int64
}

func (g *Gauge) Set(v int64) {
	atomic.StoreInt64(&g.value, v)
}

func (g *Gauge) Inc() {
	atomic.AddInt64(&g.value, 1)
}

func (g *Gauge) Dec() {
	atomic.AddInt64(&g.value, -1)
}

func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

type Histogram struct {
	mu      sync.Mutex
	counts  []uint64
	buckets []time.Duration
}

func NewHistogram(buckets []time.Duration) *Histogram {
	if len(buckets) == 0 {
		buckets = []time.Duration{
			1 * time.Millisecond,
			5 * time.Millisecond,
			10 * time.Millisecond,
			50 * time.Millisecond,
			100 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			5 * time.Second,
			10 * time.Second,
		}
	}
	return &Histogram{
		buckets: buckets,
		counts:  make([]uint64, len(buckets)+1),
	}
}

func (h *Histogram) Record(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.buckets {
		if d < h.buckets[i] {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.buckets)]++
}

func (h *Histogram) Counts() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]uint64, len(h.counts))
	copy(cp, h.counts)
	return cp
}

func (h *Histogram) Buckets() []time.Duration {
	return h.buckets
}

func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.counts {
		h.counts[i] = 0
	}
}

type Recorder struct {
	WriteOps        *Counter
	WriteErrors     *Counter
	WriteLatency    *Histogram
	ReadOps         *Counter
	ReadErrors      *Counter
	ReadLatency     *Histogram
	ScrubOps        *Counter
	ScrubErrors     *Counter
	ScrubLatency    *Histogram
	RebuildOps      *Counter
	RebuildErrors   *Counter
	RebuildLatency  *Histogram
	RecoverOps      *Counter
	RecoverErrors   *Counter
	RecoveryLatency *Histogram

	ActiveWrites *Gauge
	ActiveReads  *Gauge

	DiskErrors  *Counter
	DiskLatency *Histogram

	ConnectionsAccepted *Counter
	CommandsProcessed   *Counter

	ActiveConnections *Gauge

	startTime time.Time
	startedAt int64
}

var global = &Recorder{
	WriteOps:            &Counter{},
	WriteErrors:         &Counter{},
	WriteLatency:        NewHistogram(nil),
	ReadOps:             &Counter{},
	ReadErrors:          &Counter{},
	ReadLatency:         NewHistogram(nil),
	ScrubOps:            &Counter{},
	ScrubErrors:         &Counter{},
	ScrubLatency:        NewHistogram(nil),
	RebuildOps:          &Counter{},
	RebuildErrors:       &Counter{},
	RebuildLatency:      NewHistogram(nil),
	RecoverOps:          &Counter{},
	RecoverErrors:       &Counter{},
	RecoveryLatency:     NewHistogram(nil),
	ActiveWrites:        &Gauge{},
	ActiveReads:         &Gauge{},
	DiskErrors:          &Counter{},
	DiskLatency:         NewHistogram(nil),
	ConnectionsAccepted: &Counter{},
	CommandsProcessed:   &Counter{},
	ActiveConnections:   &Gauge{},
	startTime:           time.Now(),
}

func Global() *Recorder {
	atomic.StoreInt64(&global.startedAt, global.startTime.Unix())
	return global
}

func (r *Recorder) Reset() {
	r.WriteOps.Reset()
	r.WriteErrors.Reset()
	r.WriteLatency.Reset()
	r.ReadOps.Reset()
	r.ReadErrors.Reset()
	r.ReadLatency.Reset()
	r.ScrubOps.Reset()
	r.ScrubErrors.Reset()
	r.ScrubLatency.Reset()
	r.RebuildOps.Reset()
	r.RebuildErrors.Reset()
	r.RebuildLatency.Reset()
	r.RecoverOps.Reset()
	r.RecoverErrors.Reset()
	r.RecoveryLatency.Reset()
	r.ActiveWrites.Set(0)
	r.ActiveReads.Set(0)
}

type Snapshot struct {
	WriteOps            uint64        `json:"write_ops"`
	WriteErrors         uint64        `json:"write_errors"`
	WriteLatency        histogramSnap `json:"write_latency_ms"`
	ReadOps             uint64        `json:"read_ops"`
	ReadErrors          uint64        `json:"read_errors"`
	ReadLatency         histogramSnap `json:"read_latency_ms"`
	ScrubOps            uint64        `json:"scrub_ops"`
	ScrubErrors         uint64        `json:"scrub_errors"`
	ScrubLatency        histogramSnap `json:"scrub_latency_ms"`
	RebuildOps          uint64        `json:"rebuild_ops"`
	RebuildErrors       uint64        `json:"rebuild_errors"`
	RebuildLatency      histogramSnap `json:"rebuild_latency_ms"`
	RecoverOps          uint64        `json:"recover_ops"`
	RecoverErrors       uint64        `json:"recover_errors"`
	RecoveryLatency     histogramSnap `json:"recovery_latency_ms"`
	ActiveWrites        int64         `json:"active_writes"`
	ActiveReads         int64         `json:"active_reads"`
	ActiveConnections   int64         `json:"active_connections"`
	ConnectionsAccepted uint64        `json:"connections_accepted"`
	CommandsProcessed   uint64        `json:"commands_processed"`
	UptimeSec           int64         `json:"uptime_seconds"`
}

type histogramSnap struct {
	Buckets []time.Duration `json:"buckets"`
	Counts  []uint64        `json:"counts"`
}

func (r *Recorder) Snapshot() Snapshot {
	started := atomic.LoadInt64(&global.startedAt)
	var uptime int64
	if started > 0 {
		uptime = time.Now().Unix() - started
	}

	return Snapshot{
		WriteOps:            r.WriteOps.Value(),
		WriteErrors:         r.WriteErrors.Value(),
		WriteLatency:        histogramSnap{r.WriteLatency.Buckets(), r.WriteLatency.Counts()},
		ReadOps:             r.ReadOps.Value(),
		ReadErrors:          r.ReadErrors.Value(),
		ReadLatency:         histogramSnap{r.ReadLatency.Buckets(), r.ReadLatency.Counts()},
		ScrubOps:            r.ScrubOps.Value(),
		ScrubErrors:         r.ScrubErrors.Value(),
		ScrubLatency:        histogramSnap{r.ScrubLatency.Buckets(), r.ScrubLatency.Counts()},
		RebuildOps:          r.RebuildOps.Value(),
		RebuildErrors:       r.RebuildErrors.Value(),
		RebuildLatency:      histogramSnap{r.RebuildLatency.Buckets(), r.RebuildLatency.Counts()},
		RecoverOps:          r.RecoverOps.Value(),
		RecoverErrors:       r.RecoverErrors.Value(),
		RecoveryLatency:     histogramSnap{r.RecoveryLatency.Buckets(), r.RecoveryLatency.Counts()},
		ActiveWrites:        r.ActiveWrites.Value(),
		ActiveReads:         r.ActiveReads.Value(),
		ActiveConnections:   r.ActiveConnections.Value(),
		ConnectionsAccepted: r.ConnectionsAccepted.Value(),
		CommandsProcessed:   r.CommandsProcessed.Value(),
		UptimeSec:           uptime,
	}
}

func (r *Recorder) Uptime() time.Duration {
	started := atomic.LoadInt64(&r.startedAt)
	if started == 0 {
		return 0
	}
	return time.Since(time.Unix(started, 0))
}

func (r *Recorder) Prometheus() string {
	snap := r.Snapshot()
	lines := []string{
		"# HELP rtparityd_write_ops_total Total write operations",
		"# TYPE rtparityd_write_ops_total counter",
		fmt.Sprintf("rtparityd_write_ops_total %d", snap.WriteOps),
		"# HELP rtparityd_write_errors_total Total write errors",
		"# TYPE rtparityd_write_errors_total counter",
		fmt.Sprintf("rtparityd_write_errors_total %d", snap.WriteErrors),
		"# HELP rtparityd_read_ops_total Total read operations",
		"# TYPE rtparityd_read_ops_total counter",
		fmt.Sprintf("rtparityd_read_ops_total %d", snap.ReadOps),
		"# HELP rtparityd_read_errors_total Total read errors",
		"# TYPE rtparityd_read_errors_total counter",
		fmt.Sprintf("rtparityd_read_errors_total %d", snap.ReadErrors),
		"# HELP rtparityd_scrub_ops_total Total scrub operations",
		"# TYPE rtparityd_scrub_ops_total counter",
		fmt.Sprintf("rtparityd_scrub_ops_total %d", snap.ScrubOps),
		"# HELP rtparityd_scrub_errors_total Total scrub errors",
		"# TYPE rtparityd_scrub_errors_total counter",
		fmt.Sprintf("rtparityd_scrub_errors_total %d", snap.ScrubErrors),
		"# HELP rtparityd_rebuild_ops_total Total rebuild operations",
		"# TYPE rtparityd_rebuild_ops_total counter",
		fmt.Sprintf("rtparityd_rebuild_ops_total %d", snap.RebuildOps),
		"# HELP rtparityd_rebuild_errors_total Total rebuild errors",
		"# TYPE rtparityd_rebuild_errors_total counter",
		fmt.Sprintf("rtparityd_rebuild_errors_total %d", snap.RebuildErrors),
		"# HELP rtparityd_active_writes Currently active write operations",
		"# TYPE rtparityd_active_writes gauge",
		fmt.Sprintf("rtparityd_active_writes %d", snap.ActiveWrites),
		"# HELP rtparityd_active_reads Currently active read operations",
		"# TYPE rtparityd_active_reads gauge",
		fmt.Sprintf("rtparityd_active_reads %d", snap.ActiveReads),
		"# HELP rtparityd_active_connections Currently active connections",
		"# TYPE rtparityd_active_connections gauge",
		fmt.Sprintf("rtparityd_active_connections %d", snap.ActiveConnections),
		"# HELP rtparityd_connections_accepted_total Total accepted connections",
		"# TYPE rtparityd_connections_accepted_total counter",
		fmt.Sprintf("rtparityd_connections_accepted_total %d", snap.ConnectionsAccepted),
		"# HELP rtparityd_commands_processed_total Total commands processed",
		"# TYPE rtparityd_commands_processed_total counter",
		fmt.Sprintf("rtparityd_commands_processed_total %d", snap.CommandsProcessed),
		"# HELP rtparityd_uptime_seconds Daemon uptime in seconds",
		"# TYPE rtparityd_uptime_seconds gauge",
		fmt.Sprintf("rtparityd_uptime_seconds %d", snap.UptimeSec),
	}
	return strings.Join(lines, "\n")
}
