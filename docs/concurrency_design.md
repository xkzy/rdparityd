# rtparityd Concurrent IO Design

## Executive Summary

**Can rtparityd safely improve speed using concurrency across all drives?**

Yes, with careful design. The hard limits are:
- Single parity disk is inherently bottlenecked (serial write)
- fsync must be serialized per disk (OS limitation)
- Metadata updates must be serialized (coordinator design)
- Parity group updates must be serialized per group

This design achieves:
- Parallel reads from multiple data disks
- Parallel data writes (different extents, different disks)
- Parallel scrub/rebuild with bounded workers
- Safe parity coordination through per-group locking

---

## 1. Concurrent Read Path

### 1.1 Current Bottleneck
- Reads are sequential: one extent at a time
- No parallelism across disks
- No read-ahead

### 1.2 Concurrency Design

```
                    ┌─────────────────────────────────────────┐
                    │           ReadFile(path)               │
                    │        (serial, holds mu briefly)       │
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │    findFileExtents(state, path)       │
                    │    Returns: file, sorted extents[]     │
                    │    O(F) scan + O(E) scan               │
                    └──────────────┬──────────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
    ┌────▼────────────┐    ┌──────▼─────────┐    ┌────────▼─────────┐
    │  Extent 0        │    │   Extent 1     │    │   Extent N      │
    │  (Disk A)       │    │   (Disk B)     │    │   (Disk A)      │
    │  (async read)   │    │  (async read)  │    │  (async read)   │
    └────┬────────────┘    └──────┬─────────┘    └────────┬─────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                    ┌──────────────▼──────────────────────────┐
                    │     Reassemble & Verify                 │
                    │  - merge in logical offset order         │
                    │  - verify checksum per extent           │
                    │  - repair if needed (read parity)       │
                    └─────────────────────────────────────────┘
```

### 1.3 Per-Disk Queue Model

```go
// DiskScheduler dispatches IO to per-disk worker pools.
type DiskScheduler struct {
    // One worker pool per disk for bounded parallelism
    pools map[string]*DiskWorkerPool
    
    // Global request queue for fair scheduling
    requestCh chan *ReadRequest
    
    // Semaphore per disk to limit concurrent IO
    diskSemaphore map[string]chan struct{}
}

type DiskWorkerPool struct {
    diskID     string
    workerCh   chan func()  // bounded workers
    resultCh   chan error
    maxWorkers int
}

// NewDiskScheduler creates a scheduler with bounded workers per disk.
func NewDiskScheduler(maxWorkersPerDisk int) *DiskScheduler {
    return &DiskScheduler{
        pools:        make(map[string]*DiskWorkerPool),
        requestCh:   make(chan *ReadRequest, 1024),
        diskSemaphore: make(map[string]chan struct{}),
    }
}
```

### 1.4 Request Scheduling

```go
type ReadRequest struct {
    Extent    metadata.Extent
    Path      string  // for reassembly ordering
    Offset    int64   // logical offset for ordering
    ResultCh  chan *ExtentReadResult
}

type ExtentReadResult struct {
    Data     []byte
    Verified bool
    Healed   bool
    Error    error
}
```

**Scheduling strategy:**
1. Group extents by disk
2. Dispatch to per-disk worker pool
3. Use bounded semaphore per disk (e.g., 4 concurrent reads per disk)
4. Reassemble in logical offset order using indexed channel results

### 1.5 Read-Ahead Strategy

```go
type ReadAheadCache struct {
    maxBytes   int64
    currentOff int64
    requests   map[int64]*readAheadRequest
    
    // Prefetch window
    windowSize int64 = 4 * 1024 * 1024  // 4MB
}

// On ReadFile, check if sequential access pattern detected
// If yes, spawn background reads for next N extents
```

### 1.6 Checksum Verification Points

```
Data Read → Checksum Verify → (if bad) Parity Reconstruct → Verify → Use
```

All verification happens after parallel reads complete, before returning to caller.

### 1.7 Read-Repair Behavior

```go
// If checksum fails:
// 1. Read all OTHER extents in the same parity group
// 2. Read parity extent  
// 3. XOR to reconstruct missing/corrupt extent
// 4. Verify reconstructed checksum
// 5. If OK, write back to disk (async, fire-and-forget)
// 6. Return reconstructed data to caller
```

### 1.8 Correctness Risks

| Risk | Mitigation |
|------|------------|
| Reading stale data after metadata update | Hold metadata lock briefly during extent lookup |
| Returning partial data on error | All-or-nothing: verify all checksums or return error |
| Race with write | Write path uses Coordinator.mu; reads wait for exclusive lock |
| Read-after-write inconsistency | ensureRecoveredLocked before read |

### 1.9 Invariants

- **RC1**: Reads must wait for in-flight writes to complete (acquire shared lock)
- **RC2**: No extent may be returned without checksum verification
- **RC3**: Reassembly must maintain logical file order
- **RC4**: Read-repair must verify reconstructed data before returning

---

## 2. Concurrent Write Path

### 2.1 Current Bottleneck
- All writes serialized through Coordinator.mu
- Each extent written sequentially
- Parity computed after all data writes complete

### 2.2 Concurrency Design

**Key insight**: Different extents on different disks can write in parallel. Parity must be serialized per group.

```
Stage 1: Append Prepared    ──────────────────────────────►
Stage 2: Parallel Data Writes (different disks OK!)      ──►
         Extent 0 (Disk A) ─┐
         Extent 1 (Disk B) ──┼─► can run in parallel
         Extent 2 (Disk A) ─┘
Stage 3: Parity Write (serial per group)                  ──►
Stage 4: Save Metadata (serial)                           ──►
Stage 5: Append Committed (serial)                        ──►
Stage 6: Publish Cache (serial)                           ──►
```

### 2.3 Which Stages May Run Concurrently

| Stage | Can Parallelize? | With What? |
|-------|------------------|------------|
| Journal Append (Prepared) | No | Must be first |
| Data Writes | Yes | Different extents, different disks |
| Data fsyncs | Yes (across disks) | Each disk independent |
| Parity Writes | Yes (different groups) | Per-group serialization |
| Metadata Save | No | Must be after all data+parity |
| Commit Record | No | Must be last durability step |

### 2.4 Data Write Parallelization

```go
type ParallelWriteState struct {
    Extents     []metadata.Extent
    DataWrites  []chan error  // one per extent
    ParityGroup map[string]bool  // which groups touched
}

// writeExtentsInParallel writes multiple extents to different disks concurrently.
func (c *Coordinator) writeExtentsInParallel(
    rootDir string, 
    extents []metadata.Extent, 
    payload []byte,
) error {
    var wg sync.WaitGroup
    errors := make([]error, len(extents))
    
    for i, ext := range extents {
        wg.Add(1)
        go func(i int, ext metadata.Extent) {
            defer wg.Done()
            // Write to disk (each disk has its own file)
            errors[i] = writeExtentFile(rootDir, ext, payload)
        }(i, ext)
    }
    
    wg.Wait()
    
    // Check for errors
    for _, err := range errors {
        if err != nil {
            return err  // partial failure handling needed
        }
    }
    return nil
}
```

### 2.5 Parity Update Coordination

**Critical**: Multiple extents in the SAME parity group must NOT write parity simultaneously.

```go
// PerParityGroup lock prevents concurrent parity updates for same group.
type ParityGroupLocker struct {
    locks map[string]*sync.Mutex
    mu    sync.Mutex
}

func (p *ParityGroupLocker) LockGroup(groupID string) func() {
    p.mu.Lock()
    if p.locks[groupID] == nil {
        p.locks[groupID] = &sync.Mutex{}
    }
    m := p.locks[groupID]
    p.mu.Unlock()
    
    m.Lock()
    return func() { m.Unlock() }
}
```

**Algorithm**:
1. Group extents by parity group
2. For each group with new extents:
   - Acquire group lock
   - Read existing parity for group
   - XOR new extent data with existing parity
   - Write new parity
   - Release group lock
3. Groups can be processed in parallel (different groups = no conflict)

### 2.6 Parity Disk Bottleneck Handling

**Inherent limitation**: Single parity disk sees serialized writes.

**Mitigation strategies**:
1. **Batch parity writes**: If multiple groups' extents are written, compute all parity first, then write sequentially
2. **Async parity write**: Write data, schedule parity, return to user faster (but must complete before commit)
3. **Parity write pipelining**: While writing parity for group N, preparing data for group N+1

```go
type WritePipeline struct {
    // Stage 1: Data write (parallel across disks)
    dataCh chan *DataWriteTask
    
    // Stage 2: Parity prepare (in-memory, parallel across groups)
    parityCh chan *ParityTask
    
    // Stage 3: Parity write (serial to parity disk)
    parityWriteCh chan *ParityWriteTask
}
```

### 2.7 Batching Opportunities

**Multiple file writes can batch**:
- Batch journal appends ( Prepared records)
- Batch fsync calls (one per disk, not per extent)
- Batch metadata save (one snapshot, multiple files)

**Limitation**: Each WriteFile is currently independent. Future: group commit for multiple simultaneous writes.

### 2.8 Lock Model for Writes

```go
func (c *Coordinator) WriteFile(req WriteRequest) {
    // 1. Acquire exclusive operation lock (cross-process)
    lock, _ := c.acquireExclusiveOperationLock()
    defer lock.release()
    
    // 2. Acquire coordinator mutex (serializes all mutations)
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // 3. Recovery guard
    c.ensureRecoveredLocked(...)
    
    // 4. Allocate extents (serial, metadata)
    file, extents := allocator.Allocate(...)
    
    // 5. Append Prepared record (serial, journal)
    journal.Append(Prepared)
    
    // 6. PARALLEL data writes (different disks OK!)
    var wg sync.WaitGroup
    for _, ext := range extents {
        wg.Add(1)
        go func(ext metadata.Extent) {
            defer wg.Done()
            writeExtentToDisk(ext, data)
        }(ext)
    }
    wg.Wait()
    
    // 7. SERIAL parity writes (per-group lock)
    for groupID := range touchedGroups {
        parityGroupLock.Lock(groupID)
        computeAndWriteParity(groupID)
        parityGroupLock.Unlock(groupID)
    }
    
    // 8. SERIAL metadata save
    saveStateSnapshot(state)
    
    // 9. SERIAL commit record
    journal.Append(Committed)
    
    // 10. SERIAL cache publish
    publishCommittedState(state)
}
```

### 2.9 Correctness Risks

| Risk | Mitigation |
|------|------------|
| Inconsistent parity | Per-group parity lock prevents races |
| Partial data on crash | All data written before any parity; rollback on error |
| False commit | Metadata must be saved before commit record |
| Lost updates | Each stage must complete before next starts |
| Deadlock | Always acquire coordinator.mu before group locks |

### 2.10 Invariants

- **WC1**: No two concurrent writes may update the same parity group without serialization
- **WC2**: All data extents for a transaction must be written before parity computation begins
- **WC3**: Parity must be verified (readback check) before commit proceeds
- **WC4**: A transaction is committed only after metadata snapshot is durable AND commit record is durable
- **WC5**: Coordinator.mu must be held before any lock acquisition in write path

---

## 3. Scheduler Design

### 3.1 Design Goals
- Avoid starving user IO
- Avoid overloading one disk
- Bounded parallelism
- Priority: user reads > user writes > scrub > rebuild

### 3.2 Scheduler Architecture

```go
type IOScheduler struct {
    // Per-disk worker pools (bounded)
    diskPools map[string]*DiskWorkerPool
    
    // Priority queues
    userReadCh   chan *ReadRequest
    userWriteCh  chan *WriteRequest
    scrubCh      chan *ScrubRequest
    rebuildCh    chan *RebuildRequest
    
    // Semaphores per disk
    diskLimits map[string]semaphore
    
    // Background workers
    wg sync.WaitGroup
}

type Priority int
const (
    PriorityUserRead Priority = iota
    PriorityUserWrite
    PriorityScrub
    PriorityRebuild
)
```

### 3.3 Scheduling Algorithm

```
1. Dispatch new request to priority queue
2. Scheduler loop selects from highest-priority non-empty queue
3. Check disk semaphore (bounded IO per disk)
4. If available, dispatch to per-disk worker pool
5. If not, request queued until semaphore available

// Fairness: round-robin across priority levels after N requests
// Prevent starvation: lower priority gets guaranteed slot every M requests
```

### 3.4 Per-Disk Limits

```go
const (
    MaxConcurrentReadsPerDisk  = 4
    MaxConcurrentWritesPerDisk = 2
    MaxScrubWorkersPerDisk      = 2
    MaxRebuildWorkersPerDisk    = 2
)

// Semaphore per disk limits concurrency
type semaphore chan struct{}

func (s semaphore) Acquire() { s <- struct{}{} }
func (s semaphore) Release() { <-s }
```

### 3.5 Priority and Fairness

| Operation | Priority | Bandwidth Guarantee |
|-----------|----------|---------------------|
| User Read | Highest | Always served before background |
| User Write | High | Always served before scrub/rebuild |
| Scrub | Medium | 50% of idle disk bandwidth |
| Rebuild | Low | 25% of idle disk bandwidth |

**Implementation**: Use weighted round-robin with token bucket for background ops.

### 3.6 Foreground vs Background Isolation

```go
// Separate channels prevent background ops from starving foreground
type IOScheduler struct {
    // Immediate dispatch channels (no queuing)
    userReadCh  chan *ReadRequest   // capacity: immediate
    userWriteCh chan *WriteRequest  // capacity: immediate
    
    // Background queues (with depth limits)
    scrubCh     chan *ScrubRequest   // capacity: 100
    rebuildCh   chan *RebuildRequest  // capacity: 100
}
```

---

## 4. Parallel Scrub Design

### 4.1 Current Bottleneck
- Sequential extent checking
- No parallelism
- Full pool lock during scrub

### 4.2 Concurrency Design

```
Scrub Phase 1: Extent Verification (parallel)
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Disk A      │ │ Disk B      │ │ Disk C      │
│ Extent 0    │ │ Extent 1    │ │ Extent 2    │
│ Extent 3    │ │ Extent 4    │ │ Extent 5    │
└─────────────┘ └─────────────┘ └─────────────┘

Scrub Phase 2: Parity Verification (per-group, serial within group)
┌─────────────┐ ┌─────────────┐
│ Group A     │ │ Group B     │
│ Read data   │ │ Read data   │
│ Read parity │ │ Read parity │
│ XOR verify  │ │ XOR verify  │
└─────────────┘ └─────────────┘
```

### 4.3 Batch Size and Worker Model

```go
type ScrubConfig struct {
    // Workers per disk for extent verification
    ExtentWorkersPerDisk int
    
    // Batch size for checksum verification
    VerifyBatchSize int = 32
    
    // Throttle: pause between batches
    ThrottleDuration time.Duration = 100 * time.Millisecond
    
    // Max disk bandwidth for scrub (0 = unlimited)
    MaxBandwidthMBps int
}

func (c *Coordinator) ScrubParallel(cfg ScrubConfig) error {
    // 1. Load state (serial)
    state, _ := c.loadState(...)
    
    // 2. Group extents by disk
    extentsByDisk := groupExtentsByDisk(state.Extents)
    
    // 3. Launch per-disk workers
    var wg sync.WaitGroup
    for diskID, extents := range extentsByDisk {
        wg.Add(1)
        go func(diskID string, extents []metadata.Extent) {
            defer wg.Done()
            c.scrubDiskExtents(diskID, extents, cfg)
        }(diskID, extents)
    }
    wg.Wait()
    
    // 4. Parity verification (serial per group)
    for _, group := range state.ParityGroups {
        verifyParityGroup(group)
    }
    
    return nil
}
```

### 4.4 Throttling

```go
func (s *ScrubWorker) throttle() {
    if s.cfg.ThrottleDuration > 0 {
        time.Sleep(s.cfg.ThrottleDuration)
    }
    // Also check if foreground IO is waiting
    select {
    case <-s.foregroundWaitCh:
        // Yield to foreground, resume after delay
    default:
    }
}
```

### 4.5 Repair Verification

```go
// After repairing an extent:
// 1. Read repaired extent
// 2. Verify checksum matches metadata
// 3. Log repair result
// 4. Continue to next extent

func (s *ScrubWorker) repairAndVerify(ext metadata.Extent) error {
    // Repair from parity
    data := reconstructFromParity(ext)
    
    // Write back
    writeExtent(ext, data)
    
    // Verify immediately (read back and check)
    verified, err := verifyExtentChecksum(ext)
    if err != nil || !verified {
        return fmt.Errorf("repair verification failed for %s", ext.ExtentID)
    }
    
    return nil
}
```

### 4.6 Correctness Invariants

- **SC1**: Scrub must not block user IO indefinitely (throttling required)
- **SC2**: Repaired extents must be verified before marking complete
- **SC3**: Progress must be persisted after each extent (resume support)
- **SC4**: Scrub must hold no locks that block user operations

---

## 5. Parallel Rebuild Design

### 5.1 Current Bottleneck
- Sequential extent reconstruction
- No parallelism

### 5.2 Concurrency Design

```
Rebuild for Disk A (failed):
┌─────────────────────────────────────────────────────────────┐
│  Extent scheduling (by parity group + offset)              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                   │
│  │ Group A  │ │ Group B  │ │ Group A  │                   │
│  │ Offset 0 │ │ Offset 1 │ │ Offset 2 │                   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                   │
└───────┼────────────┼────────────┼──────────────────────────┘
        │            │            │
┌───────▼────────────▼────────────▼──────────────────────────┐
│  Parallel reconstruction from surviving disks             │
│  - Read data extents from Disk B, Disk C                   │
│  - Read parity from Parity disk                             │
│  - XOR to reconstruct                                       │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  Verify and write back                                      │
│  - Verify checksum matches metadata                         │
│  - Write to replacement disk                                │
│  - Persist progress                                          │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 Extent Scheduling

```go
type RebuildScheduler struct {
    // Group extents by parity group, then sort by offset
    // This ensures deterministic ordering and proper dependency handling
    
    queue []*RebuildTask
    
    // Semaphore limits concurrent rebuilds
    maxConcurrent int
}

func (r *RebuildScheduler) nextTask() *RebuildTask {
    // Always return next in sorted order
    // Dependencies: if Group A has N extents, process in offset order
    // No cross-group dependencies
    return r.queue[r.index]
}
```

### 5.4 Dependency Model

**Key insight**: Within a parity group, no dependencies between different extents.
- Each extent can be reconstructed independently
- Different parity groups can be processed in parallel
- BUT: must process in order within group to detect gaps properly

```go
func (r *RebuildScheduler) canRunParallel(t1, t2 *RebuildTask) bool {
    // Parallel if different parity groups
    return t1.ParityGroupID != t2.ParityGroupID
}
```

### 5.5 Commit Model

```go
// After rebuilding all extents for a disk:
// 1. Verify ALL reconstructed extents match metadata checksums
// 2. Update metadata (mark disk as online)
// 3. Persist final progress
// 4. Return result

func (c *Coordinator) commitRebuild(diskID string) error {
    // 1. Final verification
    for _, ext := range rebuiltExtents {
        if !verifyChecksum(ext) {
            return fmt.Errorf("final verification failed")
        }
    }
    
    // 2. Update metadata
    state.Disks[diskID].HealthStatus = "online"
    c.saveStateSnapshot(state)
    
    // 3. Persist completion
    progress := RebuildProgress{DiskID: diskID, CompletedExtents: nil}
    saveRebuildProgress(..., progress)
    
    return nil
}
```

### 5.6 Progress Persistence

```go
// After each extent is rebuilt and verified:
func (s *RebuildWorker) onExtentComplete(ext metadata.Extent) {
    s.progress.CompletedExtents = append(s.progress.CompletedExtents, ext.ExtentID)
    s.progress.LastUpdated = time.Now().UTC()
    
    // Persist every N extents (not every single one, for performance)
    if len(s.progress.CompletedExtents)%10 == 0 {
        saveRebuildProgress(s.metadataPath, s.progress)
    }
}
```

### 5.7 Correctness Invariants

- **RC1**: Each reconstructed extent must be checksum-verified before writing
- **RC2**: Progress must be persisted before returning (crash-safe)
- **RC3**: Rebuild must skip already-completed extents (idempotent)
- **RC4**: Disk must remain marked offline until full rebuild commits

---

## 6. Concurrency Invariants Summary

### 6.1 Read Invariants

| ID | Invariant | Enforcement |
|----|-----------|--------------|
| RC1 | Reads wait for in-flight writes | Shared lock in ReadFile |
| RC2 | No unchecked extent returned | Checksum verification mandatory |
| RC3 | Reassembly preserves order | Indexed results + ordered merge |
| RC4 | Read-repair verified before return | Post-repair checksum check |

### 6.2 Write Invariants

| ID | Invariant | Enforcement |
|----|-----------|--------------|
| WC1 | No concurrent parity updates for same group | Per-group mutex |
| WC2 | All data written before any parity | Pipeline stage order |
| WC3 | Parity verified before commit | Readback + checksum |
| WC4 | Transaction committed only when both metadata AND journal durable | Fixed commit order |
| WC5 | Coordinator.mu acquired before any other lock | Lock ordering |

### 6.3 Scheduler Invariants

| ID | Invariant | Enforcement |
|----|-----------|--------------|
| SC1 | User IO not starved | Priority queues + guarantees |
| SC2 | Disk overload prevented | Per-disk semaphores |
| SC3 | Background ops bounded | Channel depth limits |

### 6.4 Scrub/Rebuild Invariants

| ID | Invariant | Enforcement |
|----|-----------|--------------|
| BC1 | Repair verified before completing | Checksum re-verify |
| BC2 | Progress persisted durably | fsync after each batch |
| BC3 | Idempotent restart | Skip completed in progress file |

---

## 7. Locking and Ownership Model

### 7.1 Lock Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    Coordinator.mu                           │
│               (serializes all mutations)                    │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
┌───────────────┐ ┌──────────────┐ ┌────────────────┐
│  ParityGroup  │ │   DiskPool   │ │   Journal      │
│    Locks      │ │   Scheduler  │ │    Lock        │
│ (per-group)   │ │  (per-disk)  │ │  (serial)      │
└───────────────┘ └──────────────┘ └────────────────┘
```

### 7.2 Lock Acquisition Order

**Safe order** (must follow exactly):
1. `Coordinator.mu` (always first)
2. `ParityGroup locks` (in sorted order to prevent deadlock)
3. `Journal.Append` (no lock needed, serializes internally)
4. `Metadata.Save` (no lock needed, serializes via Coordinator.mu)
5. Release in reverse order

### 7.3 Per-Disk Locks

```go
type DiskPool struct {
    disks map[string]*Disk
    
    // Per-disk synchronization
    diskLocks map[string]*sync.RWMutex
}

type Disk struct {
    DiskID      string
    Path        string
    Role        string  // "data" or "parity"
    
    // For concurrent access
    readSem  chan struct{}  // bounded reads
    writeSem chan struct{}  // bounded writes
}
```

### 7.4 Journal Lock

The journal uses its own internal serialization (append-only, single writer).
No external lock needed.

### 7.5 Lock Contention Points

| Location | Expected Contention | Mitigation |
|----------|---------------------|------------|
| Coordinator.mu | High (all mutations) | Minimize hold time |
| ParityGroup locks | Medium | Per-group, rarely contested |
| Per-disk semaphores | Low | Sufficient capacity |

### 7.6 Keeping Lock Scope Small

```go
// BAD: Hold lock during slow IO
func (c *Coordinator) WriteFile(req) {
    c.mu.Lock()
    // SLOW: network call while holding lock
    writeToNetwork(...)
    c.mu.Unlock()
}

// GOOD: Release lock before slow operations
func (c *Coordinator) WriteFile(req) {
    c.mu.Lock()
    state := c.loadState(...)
    c.mu.Unlock()
    
    // Parallel data writes (no lock held)
    writeExtentsInParallel(extents, data)
    
    c.mu.Lock()
    c.saveStateSnapshot(state)
    c.mu.Unlock()
}
```

---

## 8. Performance Strategy

### 8.1 Where Concurrency Helps Most

| Operation | Speedup Potential | Notes |
|-----------|-------------------|-------|
| **Reads** | 3-8x | Parallel across N data disks |
| **Scrub** | N-1x | Parallel across N-1 data disks |
| **Rebuild** | N-1x | Parallel read from surviving disks |
| **Writes** | 1.5-2x | Parallel data, serialized parity |

### 8.2 Where Speed Is Still Limited

| Bottleneck | Limitation | Mitigation |
|------------|------------|-------------|
| **Parity disk** | Single write thread | Batch parity, async pipeline |
| **fsync** | OS serializes | Batch fsyncs, parallel per disk |
| **Metadata serialization** | Coordinator.mu | Minimize hold time |
| **Checksum computation** | CPU bound | Hardware acceleration (BLAKE3) |

### 8.3 Expected Performance Numbers

**Assumptions**:
- 4 data disks + 1 parity disk
- 1MB extent size
- 10Gbps network (if applicable)
- NVMe SSDs

| Operation | Sequential | Concurrent | Speedup |
|-----------|------------|------------|---------|
| Read 1GB | ~400MB/s | ~800MB/s | 2x |
| Write 1GB | ~350MB/s | ~500MB/s | 1.4x |
| Scrub 1TB | ~2 hours | ~30 min | 4x |
| Rebuild 1TB | ~4 hours | ~1 hour | 4x |

### 8.4 Go-Specific Optimizations

```go
// 1. Use sync.Pool for buffers
var extentBufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 1024*1024)  // 1MB
    },
}

// 2. Use goroutine pools (not unbounded spawn)
type WorkerPool struct {
    work chan func()
    wg   sync.WaitGroup
}

// 3. Use channels for backpressure (not blocking)
// 4. Use runtime.GOMAXPROCS for CPU-bound checksum
// 5. Use bufio for disk IO (if not using page cache)
```

---

## 9. Go-Oriented Architecture

### 9.1 New Types

```go
// internal/journal/io.go

// DiskScheduler manages concurrent IO across disks
type DiskScheduler struct {
    pools        map[string]*DiskWorkerPool
    maxWorkers   int
    diskLimits   map[string]semaphore
}

// DiskWorkerPool is a bounded worker pool for one disk
type DiskWorkerPool struct {
    diskID     string
    workCh     chan func()
    resultCh   chan error
    wg         sync.WaitGroup
}

// ReadCoordinator orchestrates parallel reads
type ReadCoordinator struct {
    scheduler *DiskScheduler
    cache     *ReadAheadCache
}

// WriteCoordinator orchestrates parallel writes  
type WriteCoordinator struct {
    scheduler *DiskScheduler
    parityLocks *ParityGroupLocker
}

// IOScheduler manages priorities across all operations
type IOScheduler struct {
    userReadCh   chan *ReadRequest
    userWriteCh  chan *WriteRequest
    scrubCh      chan *ScrubRequest
    rebuildCh    chan *RebuildRequest
    
    // Per-disk pools
    diskPools map[string]*DiskWorkerPool
    
    wg sync.WaitGroup
}
```

### 9.2 API Changes

```go
// New with scheduler
func NewCoordinatorWithIO(metadataPath, journalPath string, maxWorkers int) *Coordinator {
    return &Coordinator{
        metadataPath: metadataPath,
        journalPath:  journalPath,
        metadata:     metadata.NewStore(metadataPath),
        journal:      NewStore(journalPath),
        ioScheduler: NewIOScheduler(maxWorkers),
    }
}

// ReadFile now uses parallel IO
func (c *Coordinator) ReadFile(path string) (ReadResult, error) {
    // Uses ioScheduler for parallel extent reads
}

// WriteFile now uses parallel data writes
func (c *Coordinator) WriteFile(req WriteRequest) (WriteResult, error) {
    // Uses ioScheduler for parallel data writes
}
```

---

## 10. Tests to Add

### 10.1 Read Concurrency Tests

```go
func TestConcurrentReads_DifferentDisks(t *testing.T)
func TestConcurrentReads_SameDisk(t *testing.T)  
func TestReadAhead_SequentialAccess(t *testing.T)
func TestReadRepair_DuringConcurrentRead(t *testing.T)
func TestReadCancelledByWrite(t *testing.T)
```

### 10.2 Write Concurrency Tests

```go
func TestConcurrentWrites_DifferentExtents(t *testing.T)
func TestConcurrentWrites_SameParityGroup(t *testing.T)
func TestConcurrentWrites_MultipleDisks(t *testing.T)
func TestWriteParityRaceCondition(t *testing.T)
func TestWriteParallel_ThenCommit(t *testing.T)
```

### 10.3 Scheduler Tests

```go
func TestScheduler_PriorityInheritance(t *testing.T)
func TestScheduler_DiskLimits(t *testing.T)
func TestScheduler_Fairness(t *testing.T)
func TestScheduler_ScrubDoesntStarveReads(t *testing.T)
```

### 10.4 Scrub/Rebuild Tests

```go
func TestScrubParallel_MultipleDisks(t *testing.T)
func TestRebuildParallel_AllDisks(t *testing.T)
func TestRebuildProgress_PersistsCorrectly(t *testing.T)
func TestRebuildIdempotent_Resume(t *testing.T)
```

---

## 11. Benchmarks to Add

```go
func BenchmarkReadParallel_1Disk(b *testing.B)
func BenchmarkReadParallel_4Disks(b *testing.B)
func BenchmarkWriteParallel_DataOnly(b *testing.B)
func BenchmarkWriteParallel_WithParity(b *testing.B)
func BenchmarkChecksum_BLAKE3(b *testing.B)
func BenchmarkScheduler_Priority(b *testing.B)
```

---

## 12. Implementation Roadmap

### Phase 1: Read Parallelization (Priority: HIGH)
1. Add DiskScheduler and per-disk worker pools
2. Modify ReadFile to use parallel extent reads
3. Add read-ahead cache
4. Tests and benchmarks

### Phase 2: Write Parallelization (Priority: HIGH)
1. Add WriteCoordinator with parallel data write stage
2. Add ParityGroupLocker for safe parity updates
3. Modify WriteFile to use parallel data writes
4. Tests and benchmarks

### Phase 3: Scheduler (Priority: MEDIUM)
1. Add IOScheduler with priority queues
2. Integrate scrub and rebuild into scheduler
3. Add throttling
4. Tests

### Phase 4: Scrub/Rebuild Parallelization (Priority: MEDIUM)
1. Modify Scrub to use parallel workers
2. Modify Rebuild to use parallel reconstruction
3. Add progress persistence
4. Tests and benchmarks

---

## Summary

rtparityd CAN safely improve performance with concurrency:
- Reads: Parallel across disks ✅
- Writes: Parallel data, serialized parity ✅  
- Scrub: Parallel across disks ✅
- Rebuild: Parallel reconstruction ✅

Hard limits remain:
- Single parity disk (serial write inherent)
- fsync serialization (OS limitation)
- Metadata serialization (design choice)

The design preserves all correctness guarantees:
- No false commits
- No torn writes
- No silent corruption
- Crash-safe recovery maintained
