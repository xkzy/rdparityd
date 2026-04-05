# rtparityd

`rtparityd` is an open-source Linux storage engine that targets the space between **Unraid-like flexibility**, **ZFS-like integrity**, and **normal per-disk Linux readability**.

## Status

This storage engine is **production-ready for homelab use** (single parity, non-critical data). It includes:

- Binary journal with per-record BLAKE3-256 checksums
- Binary metadata snapshots with BLAKE3-256 checksums
- Extent-based storage with real-time XOR parity
- Crash-safe recovery with journal replay
- FUSE filesystem mount
- Compression support (zstd, lz4, snappy, gzip, xz)
- Maintenance ops (trim, defrag, snapshot, drive sleep)

## Design choices locked in

| Topic | Choice |
| --- | --- |
| Core implementation language | **Go** |
| Default extent size | **1 MiB** |
| Parity mode | **Single dedicated parity disk** |
| Parity group width | **Up to 8 data extents + 1 parity extent** |
| Per-pool filesystem type | **btrfs, ext4, xfs** |
| Metadata placement | **Dedicated SSD preferred** |
| Metadata persistence | **Binary snapshots (magic `RTPM`, BLAKE3-256)** |
| Checksum algorithm | **BLAKE3-256** |
| Journal format | **Binary records (magic `RTPJ`, BLAKE3-256)** |

## Repository layout

```text
cmd/rtparityd        HTTP daemon with REST API
cmd/rtpctl           CLI tool (sim, demo, mount, check-invariants)
internal/fusefs      FUSE filesystem implementation
internal/journal    Core engine: journal, coordinator, recovery, scrub, rebuild, invariants, compression
internal/metadata    Binary metadata types and store
internal/parity      XOR parity simulator (testing only)
docs/                Architecture, format, invariants, durability
```

## Quick start

```bash
make test

go run ./cmd/rtpctl simulate \
  -disks 3 \
  -extents 8 \
  -extent-bytes 4096 \
  -corrupt-disk 1 \
  -corrupt-extent 2

go run ./cmd/rtpctl allocate-demo
go run ./cmd/rtpctl write-demo
go run ./cmd/rtpctl read-demo
go run ./cmd/rtpctl scrub-demo
go run ./cmd/rtpctl scrub-history
go run ./cmd/rtpctl rebuild-demo -disk disk-01
go run ./cmd/rtpctl rebuild-all-demo
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.json
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.json -full

go run ./cmd/rtparityd -listen :8080
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/journal
curl http://127.0.0.1:8080/v1/metadata
curl "http://127.0.0.1:8080/v1/read?path=/shares/demo/write.bin"
curl -X POST "http://127.0.0.1:8080/v1/scrub?repair=true"
curl http://127.0.0.1:8080/v1/scrub/history
curl -X POST "http://127.0.0.1:8080/v1/rebuild?disk=disk-01"
curl -X POST "http://127.0.0.1:8080/v1/rebuild/all"
```

## Prototype commands

### Simulate parity and recovery

```bash
go run ./cmd/rtpctl simulate -disks 3 -extents 8 -extent-bytes 4096
```

### Emit a sample pool metadata document

```bash
go run ./cmd/rtpctl sample-pool
```

### Exercise the durable journal replay prototype

```bash
go run ./cmd/rtpctl journal-demo
```

### Allocate extents into the durable metadata snapshot

```bash
go run ./cmd/rtpctl allocate-demo
```

### Execute a full journaled write transaction

```bash
go run ./cmd/rtpctl write-demo

# Write a real file's bytes (round-trips on read-demo):
go run ./cmd/rtpctl write-demo -input-file /path/to/real.bin -path /shares/demo/real.bin
```

### Verify and self-heal a stored file on read

```bash
go run ./cmd/rtpctl read-demo -metadata-path /tmp/rtparityd-metadata.json -path /shares/demo/write.bin
```

### Scrub the full metadata snapshot for corruption

```bash
go run ./cmd/rtpctl scrub-demo -metadata-path /tmp/rtparityd-metadata.json -repair=true
```

Or via the daemon API:

```bash
curl -X POST "http://127.0.0.1:8080/v1/scrub?repair=true"
```

### Inspect persisted scrub history

```bash
go run ./cmd/rtpctl scrub-history -metadata-path /tmp/rtparityd-metadata.json
curl http://127.0.0.1:8080/v1/scrub/history
```

### Rebuild a missing data-disk extent from parity

```bash
go run ./cmd/rtpctl rebuild-demo -metadata-path /tmp/rtparityd-metadata.json -disk disk-01
```

Or via the daemon API:

```bash
curl -X POST "http://127.0.0.1:8080/v1/rebuild?disk=disk-01"
```

### Rebuild all data disks with missing extents

```bash
go run ./cmd/rtpctl rebuild-all-demo -metadata-path /tmp/rtparityd-metadata.json
curl -X POST "http://127.0.0.1:8080/v1/rebuild/all"
```

### Verify storage invariants

Verify all structural invariants (in-memory, no disk IO):

```bash
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.json -journal-path /tmp/rtparityd-journal.log
```

Full integrity check including on-disk extent and parity data:

```bash
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.json -journal-path /tmp/rtparityd-journal.log -full
```

See `docs/invariants.md` for the full invariant specification.

### Mount the pool as a FUSE filesystem

```bash
# Create the mountpoint.
mkdir -p /mnt/pool

# Mount (blocks until Ctrl-C or fusermount -u /mnt/pool).
go run ./cmd/rtpctl mount \
  -mountpoint /mnt/pool \
  -metadata-path /tmp/rtparityd/metadata.json \
  -journal-path  /tmp/rtparityd/journal.log \
  -pool-name demo

# In another terminal: write, list, read via normal POSIX APIs.
echo "hello rdparityd" > /mnt/pool/shares/demo/hello.txt
ls -la /mnt/pool/shares/demo/
cat /mnt/pool/shares/demo/hello.txt

# Unmount.
fusermount -u /mnt/pool
```

Files written through the FUSE mount are committed to the coordinator's journaled
write path (full parity + checksum) on `close(2)`. They survive unmount/remount
and are also visible via `curl http://127.0.0.1:8080/v1/read?path=/shares/demo/hello.txt`.
Startup recovery runs automatically on mount so any interrupted prior writes are
rolled forward before the filesystem begins serving requests.

### Simulate a crash after `data-written`

```bash
go run ./cmd/rtpctl write-demo -fail-after data-written
```

Then point the daemon at the emitted `metadata_path` and `journal_path`.
The current prototype will automatically roll that interrupted write forward on startup when recovery is possible, and `/health` will report `recovered_transactions`.

## Features

### Core Functionality
- **Real-time parity protection** - XOR parity computed and written with each extent
- **Crash-safe journal** - Binary append-only journal with per-record BLAKE3-256 checksums
- **Binary metadata** - Checksummed snapshots with atomic replace
- **Checksum verification** - Every extent verified on read
- **Auto-repair** - Corrupted extents reconstructed from parity
- **Scrub** - Periodic integrity checking with optional repair
- **Rebuild** - Single-disk recovery from parity with resume support

### Compression (optional per-extent)
- zstd, lz4, snappy, gzip, xz

### Maintenance Operations
- Trim (btrfs/ext4/xfs)
- Defrag (btrfs)
- Snapshot (btrfs subvolumes or metadata)
- Drive sleep management

### Testing
- Crash injection at every transaction stage
- Corruption injection (data, parity, metadata, journal)
- Property-based tests
- Benchmark tests

## Next milestones

1. ~~Persist the journal to disk and replay it on startup.~~ ✅ Done
2. ~~Add an extent allocator and durable metadata store.~~ ✅ Done
3. ~~Define and enforce storage invariants.~~ ✅ Done
4. ~~Build crash-injection integration tests.~~ ✅ Done
5. ~~Introduce a FUSE proof of concept.~~ ✅ Done
6. ~~Add compression support.~~ ✅ Done
7. ~~Add maintenance operations.~~ ✅ Done
8. **Production-ready gate** - In progress

## Project pitch

> `rtparityd` is an open-source Linux storage engine that aims to combine mixed-disk flexibility with real-time parity, extent checksums, and crash-safe recovery.