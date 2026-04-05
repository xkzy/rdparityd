# AGENTS.md

Quick-reference for working in this repo. Read `CLAUDE.md` for full architecture docs.

## Commands

```bash
make fmt          # gofmt -w ./cmd ./internal (required before commits)
make test         # go test ./...
go test ./internal/journal -run "TestWriteAtomicityCommittedStateIsDurable" -v  # single test
go test ./internal/journal -v  # single package (most tests live here)
```

There is no linter beyond `make fmt`. No CI config exists. Run `make fmt` before committing.

## Architecture

**Module**: `github.com/xkzy/rdparityd` (Go 1.25, two deps: go-fuse, blake3)

All correctness-critical persistence is **binary** (big-endian, BLAKE3-256 checksums). `encoding/json` is only used in `cmd/` for HTTP API output and demo pretty-printing — never in `internal/`.

Two binaries:
- `cmd/rtparityd` — HTTP daemon
- `cmd/rtpctl` — CLI tool (sim, demo, mount, check-invariants)

### Package layout

| Package | Role |
|---------|------|
| `internal/journal` | Core engine: coordinator, write pipeline, recovery, scrub, rebuild, repair, invariants. **Most code lives here.** |
| `internal/metadata` | Binary metadata snapshots (magic `RTPM`, v1), type definitions, allocator |
| `internal/fusefs` | FUSE mount layer |
| `internal/parity` | XOR parity simulation (testing only) |

### Key files in `internal/journal/`

- `coordinator.go` — Central orchestrator. Holds `sync.Mutex`, owns write/read/scrub/rebuild paths.
- `store.go` — Append-only binary journal (magic `RTPJ`, per-record BLAKE3-256).
- `recovery.go` — Journal replay and crash recovery.
- `reader.go` — Read path with checksum verification and auto-repair from parity.
- `scrub.go` / `repair.go` — Scrub and repair with their own journalled transactions.
- `rebuild.go` — Single-disk rebuild from parity, with durable progress tracking.
- `invariants.go` — 15 formal invariants (I1–I15), enforced in code.
- `lock.go` — Cross-process `syscall.Flock` advisory lock on metadata path.

### Persistence formats

Both use 72-byte headers with 4-byte magic, uint16 version, BLAKE3-256 checksum:
- **Journal records**: magic `RTPJ`, append-only, torn-write detection via length prefix.
- **Metadata snapshots**: magic `RTPM`, atomic replace (write tmp → rename → dir sync).
- **Rebuild/scrub progress**: magic `RBLD`/`SCRB`, same atomic-replace pattern.

## Write pipeline (5 durably-ordered stages)

```
prepared → data-written → parity-written → metadata-written → committed
```

Each stage: journal append → fsync → disk write → fsync → dir sync. Crash at any point is recoverable via journal replay.

## Concurrency model

- `Coordinator.mu` (`sync.Mutex`) serializes all operations within a process.
- `acquireExclusiveOperationLock()` adds `syscall.Flock` for cross-process safety.
- Both are acquired before any mutation.

## Testing conventions

- **Fault injection**: `FailAfter` field on `WriteRequest` stops the pipeline at a given state. Also `scrubWithRepairFailAfter`, `rebuildDataDiskWithRepairFailAfter`.
- **Fault helpers**: `fault_injection.go` provides `JournalCorruptionHelper`, `DiskFailureHelper`, `MetadataCorruptionHelper`.
- **Test categories**: A (crash at state boundaries), B (journal corruption), D (disk failures), E (rebuild failures), F (full scrub), G (metadata corruption), H (concurrency — disabled).
- **Disabled test files**: `category_c_data_corruption_test.go.disabled`, `category_h_concurrency_test.go.disabled`, `failure_matrix_test.go.disabled`. These compile but are excluded from `go test`.
- Tests use `t.TempDir()` for isolation. No external services needed.

## Things that will bite you

- CLAUDE.md says "JSON snapshots for prototype" — **that is outdated**. Metadata has been fully binary since the store.go rewrite.
- `AllowSynthetic: true` in `WriteRequest` generates deterministic fake extent bytes. Production callers must provide `Payload`. Never set `AllowSynthetic` in non-test code.
- Extent relative paths use a two-level hex hash scheme: `data/ab/cd/extent-<id>.bin`.
- The allocator (`metadata.Allocator`) does not know about existing extents — it only distributes new ones. Parity group membership is assigned at allocation time.
- `replaceSyncFile` (in `fsutil.go`) does atomic write: write to `.tmp` → `fsync` → `rename` → `syncDir`. Use it for all durable file writes.
