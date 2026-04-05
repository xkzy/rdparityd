# rtparityd Administrator Guide

## Failure Handling

This guide covers how to handle common failure scenarios in rtparityd.

## Disk Failure

### Symptoms
- Disk becomes unreachable or shows errors in health status
- Read operations fail for extents on that disk
- Scrub reports checksum mismatches

### Response

1. **Check disk status:**
   ```bash
   rtpctl check-invariants -metadata-path /tmp/rtparityd/metadata.json -full
   ```

2. **Replace disk:**
   ```bash
   # Add new disk
   rtpctl add-disk -disk-id disk-03 -uuid NEW-UUID -role data -mountpoint /mnt/data03 -capacity 8TB
   
   # Replace old disk (this reassigns extents)
   rtpctl replace-disk -old-disk disk-01 -new-disk disk-03 -new-uuid NEW-UUID
   ```

3. **Rebuild data from parity:**
   ```bash
   rtpctl rebuild-demo -disk disk-01 -metadata-path /tmp/rtparityd/metadata.json
   ```

## Metadata Corruption

### Symptoms
- `metadata.Load()` fails with checksum mismatch
- Startup recovery shows errors

### Response

1. **Try recovery:**
   ```bash
   # Recovery will attempt to rebuild from journal
   go run ./cmd/rtpctl check-invariants -metadata-path /tmp/rtparityd/metadata.json -journal-path /tmp/rtparityd/journal.log
   ```

2. **If journal is empty and metadata corrupt:**
   - Manual recovery may be required
   - Check `docs/on-disk-format.md` for binary format
   - Use `blake3sum` to verify checksums

3. **Restore from backup** (if backup strategy implemented):
   ```bash
   # Copy metadata backup
   cp /backup/metadata.bin /tmp/rtparityd/metadata.bin
   ```

## Journal Corruption

### Symptoms
- `journal.Load()` returns errors
- Torn write detected in logs

### Response

1. **Recovery handles torn writes automatically:**
   - Records with invalid checksums are skipped
   - Partial transactions are replayed from last valid state

2. **Manual intervention:**
   ```bash
   # Check journal integrity
   rtpctl journal-demo
   
   # If needed, manually truncate corrupt journal
   # WARNING: Only do this if you understand the implications
   ```

## Parity Mismatch

### Symptoms
- Scrub reports "parity checksum mismatch"
- `check-invariants -full` shows P2/P3 violations

### Response

1. **Run scrub with repair:**
   ```bash
   rtpctl scrub-demo -metadata-path /tmp/rtparityd/metadata.json -repair=true
   ```

2. **If unrecoverable:**
   - Data may be lost if multiple extents in group are corrupt
   - Consider restoring from backup

## Extent Corruption

### Symptoms
- Read fails with "checksum mismatch"
- Scrub reports E1 violations

### Response

1. **Automatic repair on read:**
   - Read operations attempt to repair from parity automatically
   - No manual intervention needed

2. **Manual scrub repair:**
   ```bash
   rtpctl scrub-demo -metadata-path /tmp/rtparityd/metadata.json -repair=true
   ```

## Power Loss Recovery

### What happens on restart

1. Recovery runs automatically on coordinator startup
2. Journal is replayed to find last committed state
3. Partial transactions are rolled forward or aborted
4. System returns to consistent state

### What data is safe

| Crash point | Data state |
|-------------|------------|
| After COMMIT | All committed data durable |
| After METADATA_WRITTEN | Data recoverable from journal |
| After PARITY_WRITTEN | Data recoverable from journal |
| After DATA_WRITTEN | Data recoverable from journal |
| After PREPARE | Transaction will be aborted |

## Monitoring

### Health checks

```bash
# Check invariants
rtpctl check-invariants -metadata-path /tmp/rtparityd/metadata.json -journal-path /tmp/rtparityd/journal.log -full

# Scrub history
rtpctl scrub-history -metadata-path /tmp/rtparityd/metadata.json

# Sleep status
rtpctl sleep-status -metadata-path /tmp/rtparityd/metadata.json
```

### What to watch for

1. **Scrub failures** - Indicates corruption
2. **Rebuild failures** - Indicates unrecoverable data loss
3. **Parity mismatches** - Indicates stale parity or data corruption
4. **Metadata corruption** - Requires manual intervention

## Backup Strategy

### What to backup

1. **Metadata snapshot:**
   ```bash
   cp /tmp/rtparityd/metadata.bin /backup/metadata-$(date +%Y%m%d).bin
   ```

2. **Journal** (for recovery):
   ```bash
   cp /tmp/rtparityd/journal.log /backup/journal-$(date +%Y%m%d).log
   ```

3. **Data disks** - Standard disk backup tools

### Recovery from backup

```bash
# Stop rtparityd
# Restore metadata
cp /backup/metadata-20240101.bin /tmp/rtparityd/metadata.bin

# Restart - recovery will reconcile with journal
```

## Upgrade Notes

### Before upgrade

- Backup metadata and journal
- Check invariants are passing

### After upgrade

- Run full invariant check
- Run scrub to verify integrity
- Check application logs for warnings

### Downgrade

- Not officially supported
- May require manual metadata adjustment
- Backup before attempting
