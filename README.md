# rtparityd

`rtparityd` is an open-source Linux storage engine that targets the space between **Unraid-like flexibility**, **ZFS-like integrity**, and **normal per-disk Linux readability**.

## Status

This repository is now a **Phase 0/1 prototype foundation**. It includes:

- a concrete architecture snapshot,
- an initial on-disk metadata format,
- a transaction failure/replay state machine,
- a Go-based CLI-first scaffold,
- a parity simulator with corruption injection and recovery tests.

## Design choices locked in for the prototype

| Topic | Current choice |
| --- | --- |
| Core implementation language | **Go** |
| Default extent size | **1 MiB** |
| Parity mode | **Single dedicated parity disk** |
| Parity group width | **Up to 8 data extents + 1 parity extent** |
| Data disk filesystems | **XFS / ext4** |
| Metadata placement | **Dedicated SSD preferred** |
| Metadata persistence | **Checksummed SQLite + per-disk cache** |

## Repository layout

```text
cmd/rtparityd        Prototype daemon with HTTP endpoints
cmd/rtpctl           CLI for simulation and sample pool state
internal/journal     Transaction states and validation
internal/metadata    Metadata schema types
internal/parity      XOR parity simulator and tests
docs/                Architecture and format decisions
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

go run ./cmd/rtparityd -listen :8080
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/journal
curl http://127.0.0.1:8080/v1/metadata
curl "http://127.0.0.1:8080/v1/read?path=/shares/demo/write.bin"
curl -X POST "http://127.0.0.1:8080/v1/scrub?repair=true"
curl http://127.0.0.1:8080/v1/scrub/history
curl -X POST "http://127.0.0.1:8080/v1/rebuild?disk=disk-01"
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

### Simulate a crash after `data-written`

```bash
go run ./cmd/rtpctl write-demo -fail-after data-written
```

Then point the daemon at the emitted `metadata_path` and `journal_path`.
The current prototype will automatically roll that interrupted write forward on startup when recovery is possible, and `/health` will report `recovered_transactions`.

## Next milestones

1. Persist the journal to disk and replay it on startup.
2. Add an extent allocator and durable metadata store.
3. Build crash-injection integration tests.
4. Introduce a FUSE proof of concept before any full UI work.

## Project pitch

> `rtparityd` is an open-source Linux storage engine that aims to combine mixed-disk flexibility with real-time parity, extent checksums, and crash-safe recovery.