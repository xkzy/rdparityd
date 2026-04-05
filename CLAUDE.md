# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Building and Testing
```bash
# Format all Go code
make fmt

# Run all tests
go test ./...

# Run a specific test
go test ./internal/journal -run "TestWriteAtomicityCommittedStateIsDurable" -v

# Run all tests with verbose output
go test ./... -v
```

### Running Demos and Tools
```bash
# Simulate parity with corruption injection and recovery
go run ./cmd/rtpctl simulate -disks 3 -extents 8 -extent-bytes 4096 -corrupt-disk 1 -corrupt-extent 2

# Run journaled write demo
go run ./cmd/rtpctl write-demo

# Run read demo with verification
go run ./cmd/rtpctl read-demo -metadata-path /tmp/rtparityd-metadata.json -path /shares/demo/write.bin

# Run scrub demo
go run ./cmd/rtpctl scrub-demo -metadata-path /tmp/rtparityd-metadata.json -repair=true

# Run rebuild demo for a specific disk
go run ./cmd/rtpctl rebuild-demo -metadata-path /tmp/rtparityd-metadata.json -disk disk-01

# Check invariants
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.json

# Run the daemon (rtparityd)
go run ./cmd/rtparityd -listen :8080

# Maintenance operations
rtpctl trim -metadata-path=... -journal-path=...
rtpctl defrag -metadata-path=... -journal-path=...
rtpctl snapshot -name=mysnap -metadata-path=... -journal-path=...

# Drive sleep management
rtpctl enable-sleep -timeout=300 -min-active=60
rtpctl disable-sleep
rtpctl wake-disk -disk-id=disk-01
rtpctl sleep-disk -disk-id=disk-01
rtpctl sleep-status
```

### FUSE Mounting
```bash
# Mount the pool as a FUSE filesystem
mkdir -p /tmp/rtparityd-mnt
go run ./cmd/rtpctl mount -mountpoint /tmp/rtparityd-mnt
# Unmount with: fusermount -u /tmp/rtparityd-mnt
```

## High-Level Architecture

`rtparityd` is an extent-based Linux storage engine that provides real-time parity protection, checksum verification, and crash-safe recovery. Files are split into 1 MiB extents distributed across data disks, with a dedicated parity disk storing XOR parity for each parity group.

### Core Components

- **`internal/journal/`**: Transaction management and crash recovery
  - `coordinator.go`: Central coordinator handling writes, reads, scrub, and rebuild operations
  - `store.go`: Append-only journal with per-record checksums
  - `recovery.go`: Journal replay and crash recovery logic
  - `invariants.go`: 15 formal invariants that must always hold (see `docs/invariants.md`)
  - `reader.go`: Read path with checksum verification and automatic repair
  - `scrub.go`: Periodic integrity checking and repair
  - `rebuild.go`: Single-disk recovery from parity
  - `compression.go`: Compression support (zstd, lz4, snappy, gzip, xz)
  - `maintenance_ops.go`: Trim, defrag, snapshot, and drive sleep operations

- **`internal/metadata/`**: Metadata persistence
  - `store.go`: Metadata snapshot storage with BLAKE3 checksums
  - `types.go`: Schema definitions (Pool, Disk, File, Extent, ParityGroup, Transaction)

- **`internal/fusefs/`**: FUSE filesystem layer
  - `fs.go`: POSIX filesystem interface backed by rdparityd

- **`internal/parity/`**: Parity computation and simulation
  - `simulator.go`: XOR parity operations for testing

### Transaction State Machine

Writes progress through these states (see `docs/transaction-state-machine.md`):

```
prepared -> data-written -> parity-written -> metadata-written -> committed
          \-> aborted
          \-> replay-required
```

Each state transition is journaled before the corresponding disk write, enabling crash recovery at any point.

### Key Design Decisions

- **Extent size**: 1 MiB default (configurable per pool)
- **Parity mode**: Single dedicated parity disk (XOR)
- **Checksum algorithm**: BLAKE3
- **Metadata persistence**: Binary checksummed snapshots (magic `RTPM`, BLAKE3-256)
- **Journal format**: Binary records with per-record BLAKE3 checksums
- **Compression**: zstd, lz4, snappy, gzip, xz (optional per-extent)
- **Filesystem type**: Per-pool (btrfs, ext4, xfs, etc.)

### Production Readiness Guarantees

**Durability (Phase 1)**
- All journal appends: `file.Sync()` + `syncDir()`
- All data writes: `replaceSyncFile()` (fsync + directory sync)
- All parity writes: `replaceSyncFile()` (fsync + directory sync)
- Metadata snapshot: `file.Sync()` + `syncDir()`
- See `docs/durability.md` for full ordering

**Crash Recovery (Phase 2)**
- Journal replay at every startup
- Idempotent recovery (safe to re-run)
- `FailAfter` parameter for crash injection testing

**Invariant Enforcement (Phase 3)**
- Pre-commit: structural invariants (M1-M3, E2-E3, P1, P4)
- Post-commit: targeted write integrity (E1, P2, P3)
- Full pool: `CheckIntegrityInvariants()`

**No Synthetic Data in Production (Phase 4)**
- `Payload` required for real writes
- `AllowSynthetic` only for tests/demos
- Synthetic generates fake data that doesn't flow to parity

### Invariants

The system enforces 15 formal invariants documented in `docs/invariants.md`:
- **I1**: Transaction Visibility - writes are only visible after durable commit
- **I2**: No Uncommitted Exposure - readers never see uncommitted data
- **I3**: Data Checksum Integrity - all committed extents verify against checksums
- **I4**: Parity Coverage - parity bytes match XOR of all group members
- **I5**: Metadata Truthfulness - metadata accurately reflects on-disk state
- **I6**: Replay Idempotence - recovery produces same state on multiple runs
- **I7**: Torn Record Detectability - checksums detect incomplete writes
- **I8**: Rebuild Correctness - reconstructed extents verify before acceptance
- **I9**: Scrub Safety - repairs verify sources before writing
- **I10**: Disk Identity - DiskIDs are unique and stable
- **I11**: Monotonic Generation - generation numbers only increase
- **I12**: Single Authoritative Recovery - journal or metadata, never both
- **I13**: Read Correctness - reads return verified bytes or error
- **I14**: Deallocation Safety - extents never deleted while referenced
- **I15**: Observability Truth - status fields accurately reflect health

### Testing Categories

Tests are organized into categories covering specific failure scenarios:
- **Category A**: Crash injection at transaction state boundaries
- **Category B**: Journal corruption and recovery
- **Category D**: Disk failures and multi-disk scenarios
- **Category E**: Rebuild failures
- **Category F**: Full scrub operations
- **Category G**: Metadata corruption and state validation

See `docs/CATEGORY_E_REBUILD_FAILURES.md` for an example of the test documentation format.

### Write Protocol

All writes follow this durable protocol (from `coordinator.go`):

1. Append `prepared` record to journal (fsync + syncDir)
2. Write extent files to data disks (fsync + syncDir)
3. Append `data-written` record (fsync + syncDir)
4. Compute and write parity files (fsync + syncDir)
5. Append `parity-written` record (fsync + syncDir)
6. Update metadata snapshot (fsync + syncDir)
7. Append `metadata-written` record (fsync + syncDir)
8. Append `committed` record (fsync + syncDir)

Each step is fsync'd before advancing. If a crash occurs, recovery resumes from the last durable state.

### Write Durability Ordering

| Step | Action | fsync |
|------|--------|-------|
| 1 | Journal PREPARE | `file.Sync()` + `syncDir()` |
| 2 | Write data extents | `replaceSyncFile()` |
| 3 | Journal DATA_WRITTEN | `file.Sync()` + `syncDir()` |
| 4 | Write parity files | `replaceSyncFile()` |
| 5 | Journal PARITY_WRITTEN | `file.Sync()` + `syncDir()` |
| 6 | Save metadata | `file.Sync()` + `syncDir()` |
| 7 | Journal METADATA_WRITTEN | `file.Sync()` + `syncDir()` |
| 8 | Journal COMMIT | `file.Sync()` + `syncDir()` |

### Read Path

Reads automatically verify checksums and attempt repair from parity if corruption is detected:
1. Load metadata state
2. Read each extent file
3. Verify BLAKE3 checksum
4. On mismatch, attempt reconstruction from parity group
5. Return verified bytes or error

### Failure Detection

The system distinguishes between:
- **Degraded**: Single disk/extent failure, recoverable via parity
- **Unrecoverable**: Multiple failures in same parity group, data loss possible

`AnalyzeMultiDiskFailures()` in `multidisk_failure.go` determines recoverability based on parity group membership.

### On-Disk Format

- **Data disks**: Extent files at `data/ab/cd/extent-<id>.bin` (human-readable)
- **Parity disk**: Parity files at `parity/group-<id>.bin`
- **Metadata device**: Binary snapshot at configurable path with BLAKE3-256 checksum

See `docs/on-disk-format.md` for details.

### Fault Injection

Tests use `FailAfter` parameter in `WriteRequest` to crash at specific states:
- `StatePrepared`: Before data write
- `StateDataWritten`: After data, before parity
- `StateParityWritten`: After parity, before metadata
- `StateMetadataWritten`: After metadata, before commit

Additional injection points exist for rebuild (`rebuildDataDiskWithRepairFailAfter`) and scrub (`scrubWithRepairFailAfter`).
