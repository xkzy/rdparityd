# rtparityd Architecture Snapshot

This repository now contains a **Phase 0/1 CLI-first prototype** for `rtparityd`: an extent-based, Linux-native storage engine that keeps data on ordinary filesystems while adding real-time parity and integrity checks.

## Chosen defaults for the prototype

| Decision | Prototype choice | Why |
| --- | --- | --- |
| Core language | **Go** | Available in the current environment, fast iteration, good fit for daemon + CLI work |
| Extent size | **1 MiB default** | Good balance for media/backups while keeping metadata and parity churn reasonable |
| Parity mode | **Single dedicated parity disk** | Smallest correct v1 slice; dual parity can follow once transaction semantics harden |
| Parity group width | **Up to 8 data extents + 1 parity extent** | Caps read-modify-write cost and keeps rebuild behavior understandable |
| Metadata device | **Dedicated SSD preferred** | Separates random metadata/journal IO from bulk data IO |
| Primary metadata store | **Checksummed SQLite + per-disk recovery cache** | Practical for the prototype while preserving a strong recovery model |

## Core model

1. Files are split into fixed-size extents.
2. Each extent is stored on exactly one data disk.
3. Every extent joins a parity group with at most one member per disk.
4. Writes are journaled before parity or metadata are acknowledged.
5. Reads verify checksums before returning data.
6. Scrub and rebuild operate at the extent level.

## Why Go for the first cut

Rust is still a strong future option for the lower-level data path, but the current repository uses Go because it optimizes for:

- quick daemon and CLI iteration,
- straightforward JSON/HTTP control-plane code,
- easier simulation and fault-injection tooling,
- lower setup friction in the current dev container.

## Immediate milestone plan

### Phase 0
- Validate the parity-group model with a simulator.
- Lock down transaction states and replay rules.
- Freeze the initial on-disk metadata layout.

### Phase 1
- Introduce a durable journal implementation.
- Add an extent allocator and metadata persistence.
- Exercise crash replay in integration tests.

### Phase 2
- Mount a FUSE prototype with checksum verification on reads.
- Drive pool management from the CLI/API before any full web UI work.

## Success criteria for this repo state

The prototype is useful if it can already do the following:

- simulate real-time parity updates,
- inject corruption and show successful recovery,
- document the transaction state machine and replay rules,
- expose a tiny control-plane HTTP surface for early operator workflows.
