# On-disk Metadata Format (v1 prototype)

## Layout

### Data disk

```text
/.rtparity/
  disk.json
  metadata-cache/
    manifest-<generation>.json
  logs/
/data/
  ...managed user content...
```

### Parity disk

```text
/.rtparity/
  parity-map.db
  parity-checksums.json
  tx-journal-cache/
/parity/
  group-<parity_group_id>.bin
```

### Metadata device

```text
/var/lib/rtparityd/
  metadata.db
  metadata.db-wal
  journal.log
  events.jsonl
```

## Primary metadata store

The long-term design still targets a **SQLite/WAL-style metadata device**, but the current repository prototype now persists a **checksummed JSON metadata snapshot** so allocation and startup loading can be exercised immediately.

Current prototype rules:

- Snapshot stored at a configurable path such as `/tmp/rtparityd/metadata.json`.
- The saved state includes a `state_checksum` over the full metadata document.
- Loads fail closed if the checksum does not match.
- Metadata snapshots are still intended to be copied to each data disk under `/.rtparity/metadata-cache/` for later recovery workflows.

## Per-disk manifest

Example `/.rtparity/disk.json`:

```json
{
  "disk_id": "disk-01",
  "uuid": "0b5e2c39-5a2d-4f93-9094-8d6672930f54",
  "role": "data",
  "filesystem_type": "xfs",
  "mountpoint": "/mnt/data01",
  "generation": 12,
  "health_status": "online"
}
```

## Journal record envelope

Each `journal.log` record is append-only and includes:

- `magic`: record signature (`RTPJ`)
- `version`: journal format version
- `tx_id`: transaction identifier
- `state`: `prepared`, `data-written`, `parity-written`, `metadata-written`, `committed`, or `aborted`
- `old_generation` / `new_generation`
- `affected_extent_ids`
- `payload_checksum`
- `record_checksum`

## Extent locator format

For the first prototype, a physical locator is represented as:

```json
{
  "relative_path": "data/ab/cd/extent-000001.bin",
  "offset_bytes": 0,
  "length_bytes": 1048576
}
```

That keeps data disks individually inspectable and readable with normal Linux tooling.
