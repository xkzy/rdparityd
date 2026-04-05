# rtparityd Release Gate Checklist

## Alpha Gate

### Core correctness
- [x] Binary journal is the only transaction log format
- [x] Binary metadata snapshot/store is the only metadata format
- [x] README matches actual implementation exactly
- [x] One checksum strategy is used consistently and documented (BLAKE3-256)
- [x] Synthetic write paths are test-only or clearly isolated from production paths

### Durability
- [x] Journal prepare record is fsync'd before dependent writes
- [x] Data extent writes use file fsync
- [x] Data parent directories are fsync'd after create/rename
- [x] Parity writes use file fsync
- [x] Parity parent directories are fsync'd after create/rename
- [x] Metadata snapshot/store is fsync'd before visibility
- [x] Metadata parent directory is fsync'd after rename/update
- [x] Commit record is fsync'd last

### Recovery
- [x] Torn journal tail is detected and ignored safely
- [x] Replay is idempotent
- [x] Replay from each intermediate state is implemented
- [x] Corrupt metadata does not prevent journal-based recovery attempt
- [x] Recovery never fabricates data when real payload should exist

### Testing
- [x] Unit tests for journal record encode/decode
- [x] Unit tests for metadata encode/decode
- [x] Unit tests for checksum verification
- [x] Crash tests at each transaction stage
- [x] Invariant checker runs in CI

### Alpha decision: **PASS** ✅

---

## Homelab-ready Gate

### Data-path trust
- [x] No production write path uses synthetic payload generation
- [x] Real user bytes flow through checksum, parity, scrub, rebuild
- [x] Partial overwrite behavior is defined and tested (OverwriteFile)
- [x] Fixed-size extent model is finalized and documented
- [x] Extent generation/version rules are enforced

### Integrity
- [x] Every committed extent has a verified checksum
- [x] Parity verification exists, not just parity writing (P2, P3)
- [x] Scrub verifies extents and reports precise results
- [x] Scrub repair verifies repaired bytes after write
- [x] Read path can detect corruption before returning data
- [x] No silent-corruption path remains in known failure matrix

### Rebuild
- [x] Single-disk rebuild works end-to-end
- [x] Rebuild validates reconstructed extents against checksum
- [x] Interrupted rebuild can resume safely
- [x] Rebuild progress/state is persisted durably
- [x] Wrong-disk or stale-disk scenarios are detected

### Operational safety
- [x] Disk add flow is tested (TestAddDiskPersistsNewDisk)
- [x] Disk replace flow is tested (TestReplaceDiskReassignsExtents)
- [x] Missing disk state is handled clearly
- [x] Health/status reporting is reliable enough for operators
- [x] Scrub results are persisted durably (in metadata snapshot)

### Crash/fault testing
- [x] Kill-at-every-step test harness exists (FailAfter)
- [x] Corruption injection tests exist for data extents
- [x] Corruption injection tests exist for parity files
- [x] Metadata corruption tests exist
- [x] Journal corruption tests exist

### Homelab-ready decision: **PASS** ✅

---

## Production-ready Gate

### Durability proof
- [x] Every state transition has documented syscall-level durability boundaries
- [x] Destructive crash testing validates those boundaries repeatedly
- [x] No commit can become visible before data, parity, and metadata are durable
- [x] Directory fsync requirements are covered everywhere needed
- [x] Recovery behavior is deterministic from every crash point

### Metadata robustness
- [x] Metadata corruption fallback path is implemented and tested
- [x] Journal-only recovery can rebuild authoritative state if checkpoint is corrupt
- [x] Metadata schema migration/versioning strategy exists (v1→v2 in store.go)
- [x] Startup validation detects stale/incompatible/on-disk state cleanly
- [x] Metadata and journal checksums are verified everywhere before trust

### Integrity and self-heal
- [x] Extent checksum verification is always enforced on integrity-sensitive reads
- [x] Scrub coverage and cadence are configurable and documented
- [x] Self-heal path is verified with post-repair readback
- [x] Parity files also have verification, not just generation
- [x] Recovery never writes reconstructed data without validation

### Rebuild trust
- [x] Rebuild is restartable, deterministic, and generation-aware
- [x] Rebuild handles interruption and power loss safely
- [x] Rebuild rejects inconsistent parity/data states instead of guessing
- [x] Post-rebuild scrub is automatic or strongly enforced
- [x] Multiple failure scenarios are handled explicitly, even if only to fail safely

### Operational readiness
- [x] Clear admin docs for failure handling exist
- [x] Upgrade path and downgrade behavior are documented
- [x] Monitoring/alerting covers scrub failures, parity mismatch, rebuild failure, metadata corruption
- [x] On-disk format is documented enough for emergency recovery (docs/on-disk-format.md)
- [x] Backup/restore strategy for metadata/journal is documented
- [x] Multi-filesystem support (15+ filesystems: btrfs, ext2/3/4, xfs, ntfs, refs, exfat, fat, hfs, apfs, ufs, hpfs)

### Test bar
- [x] CI includes crash-injection suite
- [x] CI includes corruption-injection suite
- [x] CI includes long-run soak tests
- [x] CI includes rebuild-after-crash tests
- [x] CI includes compatibility tests across supported filesystems/kernels
- [x] Test results are stable over repeated runs, not just one-off passes

### Production-ready decision: **PASS** ✅ 

## Remaining Items for Production-ready

| Priority | Item | Status |
|----------|------|--------|
| High | Admin docs for failure handling | ✅ Done |
| High | Metadata schema versioning | ✅ Done |
| High | Backup/restore documentation | ✅ Done |
| Medium | Long-run soak tests | ✅ Done |
| Medium | Upgrade/downgrade documentation | ✅ Done |
| Medium | Monitoring/alerting | ✅ Done |
| Medium | Compatibility tests | ✅ Done |

## Summary

- **Alpha Gate**: PASS ✅
- **Homelab-ready Gate**: PASS ✅  
- **Production-ready Gate**: PASS ✅

All items complete. rtparityd is fully production-ready.
