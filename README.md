# rtparityd

Copyright (C) 2025 rtparityd contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

---

`rtparityd` is an open-source Linux storage engine that targets the space between **Unraid-like flexibility**, **ZFS-like integrity**, and **normal per-disk Linux readability**.

## Status

This storage engine is **production-ready** for the core data path. The supported production feature set is:

- Binary journal with per-record BLAKE3-256 checksums
- Binary metadata snapshots with BLAKE3-256 checksums  
- Extent-based storage with real-time XOR parity
- Crash-safe recovery with journal replay
- FUSE filesystem mount
- Compression support (zstd, lz4, snappy, gzip, xz)
- Read verification and self-heal
- Scrub and rebuild operations
- Metrics and health monitoring via Unix socket control

The following admin features are present in the repo but are **not supported in the current production feature set** and return explicit errors:

- Trim
- Defrag
- Snapshot
- Drive sleep / wake management
- Protection-state mutation

## Design choices locked in

| Topic | Choice |
| --- | --- |
| Core implementation language | **Go** |
| Control interface | **Unix socket** (no HTTP) |
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
cmd/rtparityd        Daemon with Unix socket control (JSON protocol)
cmd/rtpctl           CLI tool (sim, demo, mount, check-invariants)
internal/fusefs      FUSE filesystem implementation
internal/journal    Core engine: journal, coordinator, recovery, scrub, rebuild, invariants, compression
internal/metadata   Binary metadata types and store
internal/metrics     Observability (counters, gauges, histograms)
internal/parity     XOR parity simulator (testing only)
```

## Quick start

```bash
make test

# Run daemon with Unix socket control
go run ./cmd/rtparityd \
  -socket /var/run/rtparityd/rtparityd.sock \
  -metadata-path /var/lib/rtparityd/metadata.bin \
  -journal-path /var/lib/rtparityd/journal.bin

# CLI operations via socket (requires separate client or nc)
# Or use rtpctl for direct file operations:

go run ./cmd/rtpctl allocate-demo
go run ./cmd/rtpctl write-demo
go run ./cmd/rtpctl read-demo
go run ./cmd/rtpctl scrub-demo
go run ./cmd/rtpctl rebuild-demo -disk disk-01
go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd-metadata.bin

# Mount as FUSE filesystem
go run ./cmd/rtpctl mount -mountpoint /mnt/pool
```

## Daemon socket protocol

The daemon exposes a JSON-based Unix socket protocol at `/var/run/rtparityd/rtparityd.sock`:

```bash
# Health check
echo '{"id":"1","op":"health"}' | nc -U /var/run/rtparityd/rtparityd.sock

# Metrics
echo '{"id":"1","op":"metrics"}' | nc -U /var/run/rtparityd/rtparityd.sock

# Write file
echo '{"id":"2","op":"write","params":{"path":"/shares/demo/file.bin","payload":"dGVzdA==","synthetic":false}}' | nc -U /var/run/rtparityd/rtparityd.sock

# Read file
echo '{"id":"3","op":"read","params":{"path":"/shares/demo/file.bin"}}' | nc -U /var/run/rtparityd/rtparityd.sock

# Scrub with repair
echo '{"id":"4","op":"scrub","params":{"repair":true}}' | nc -U /var/run/rtparityd/rtparityd.sock

# Rebuild disk
echo '{"id":"5","op":"rebuild","params":{"disk":"disk-01"}}' | nc -U /var/run/rtparityd/rtparityd.sock

# Status
echo '{"id":"6","op":"status"}' | nc -U /var/run/rtparityd/rtparityd.sock
```

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

### Observability
- **Metrics** - Operation counters, error counts, latency histograms
- **Health** - Pool status, disk health, uptime
- **Status** - Full pool state including invariant violations

### Supported Admin Operations
- Read-only health and diagnostics
- Scrub with optional repair
- Rebuild single disk / rebuild all missing data extents
- Protection status inspection

### Unsupported Admin Operations
- Trim
- Defrag
- Snapshot
- Drive sleep management
- Protection-state mutation

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
8. ~~Production-ready gate~~ ✅ Done

## Project pitch

> `rtparityd` is an open-source Linux storage engine that aims to combine mixed-disk flexibility with real-time parity, extent checksums, and crash-safe recovery.