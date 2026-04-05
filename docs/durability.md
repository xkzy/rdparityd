# Write Durability Guarantees

This document explains the write durability model and guarantees provided by rtparityd.

## Problem Statement

On power loss or system crash, users expect:
1. **No data loss** - committed data must be recoverable
2. **No silent corruption** - checksums must always verify
3. **Correct rebuild** - failed disks must reconstruct exact data

The key challenge: **Linux makes few durability guarantees by default**. Writing a file and returning success does NOT guarantee the data is on persistent storage.

## The Ordering Requirement

For a transaction to be safely committed, we must ensure this exact ordering:

```
1. Journal PREPARE (fsync)
2. Write DATA extents (fsync + dirsync)
3. Journal DATA_WRITTEN (fsync)
4. Write PARITY files (fsync + dirsync)
5. Journal PARITY_WRITTEN (fsync)
6. Save METADATA (fsync + dirsync)
7. Journal METADATA_WRITTEN (fsync)
8. Journal COMMIT (fsync)
```

## Why Each fsync is Required

### 1. Journal PREPARE fsync
**Location**: `internal/journal/store.go:133` (Store.Append)

```go
if _, err := file.Write(data); err != nil {
    return fmt.Errorf("write journal record: %w", err)
}
if err := file.Sync(); err != nil {
    return fmt.Errorf("sync journal: %w", err)
}
if err := syncDir(filepath.Dir(s.path)); err != nil {
    return fmt.Errorf("sync journal directory: %w", err)
}
```

**Why**: Without fsync, a crash could lose the prepare record. Without syncDir, the journal file might not appear in directory listing after crash.

### 2. Data Extents fsync
**Location**: `internal/journal/fsutil.go:72` (replaceSyncFile)

```go
func replaceSyncFile(path string, data []byte, perm os.FileMode) error {
    // ... write to temp file ...
    if err := f.Sync(); err != nil {  // <-- fsync data
        cleanup()
        return fmt.Errorf("sync temp file: %w", err)
    }
    // ... rename ...
    if err := syncDir(dir); err != nil {  // <-- fsync directory
        return fmt.Errorf("sync parent directory: %w", err)
    }
    return nil
}
```

**Why**: `replaceSyncFile` does 3 things:
1. Write to temp file + fsync (durable in kernel buffer)
2. Atomic rename (durable directory entry)
3. fsync directory (durable rename)

Without step 3, a crash after rename could lose the file entirely.

### 3. Journal DATA_WRITTEN fsync
Same as step 1 - ensures crash recovery knows data is written.

### 4. Parity Files fsync
Same as step 2 - ensures parity is recoverable.

### 5. Journal PARITY_WRITTEN fsync
Same as step 1 - ensures crash recovery knows parity is written.

### 6. Metadata fsync
**Location**: `internal/metadata/store.go:114`

```go
if err := file.Sync(); err != nil {
    return fmt.Errorf("sync metadata snapshot: %w", err)
}
// ... rename ...
if err := metaDir.Sync(); err != nil {  // <-- directory sync
    return fmt.Errorf("sync metadata directory: %w", err)
}
```

**Why**: Critical - without this, old metadata could be visible after crash even though new metadata was written.

### 7. Journal COMMIT fsync
Final guarantee that commit record is durable.

## Crash Scenarios

| Crash After | What Survives | Recovery Action |
|-------------|----------------|-----------------|
| Step 1 (PREPARE) | Journal entry | Replay: write data, parity, commit |
| Step 2 (DATA) | Journal + data | Replay: write parity, commit |
| Step 3 (DATA_WRITTEN) | Journal + data | Replay: write parity, commit |
| Step 4 (PARITY) | Journal + data + parity | Replay: write metadata, commit |
| Step 5 (PARITY_WRITTEN) | All above | Replay: write metadata, commit |
| Step 6 (METADATA) | All above | Replay: append commit |
| Step 7 (METADATA_WRITTEN) | All above | Replay: append commit |
| Step 8 (COMMIT) | Everything | No-op (idempotent) |

## Verification

The codebase verifies durability at multiple points:

### Pre-commit Invariants
```go
// coordinator.go:144
func (c *Coordinator) commitState(state metadata.SampleState) {
    if vs := checkPreCommitInvariants(state); len(vs) > 0 {
        return error // refuse to commit corrupt state
    }
    // ...
}
```

Checks: M3 (unique IDs), M1 (file refs exist), M2 (disk refs exist), E2 (checksum alg), E3 (positive length), P1 (parity groups referenced), P4 (no disk aliasing)

### Post-commit Integrity
```go
// coordinator.go:380
if violations := CheckTargetedWriteIntegrity(rootDir, state, extents); len(violations) > 0 {
    return error // fail if committed data doesn't verify
}
```

Checks: E1 (extent checksums), P2 (parity checksums), P3 (parity XOR)

## Tests

### Crash Injection Tests
- `TestWriteAtomicityCommittedStateIsDurable`
- `TestWriteAtomicityFailedStateIsNotDurable`
- `TestWriteAtomicityCrashBetweenCommitAndReturn`
- `TestWriteAtomicityDoubleFailureIsDetected`

### Corruption Injection Tests
- `TestCategoryB_J1TornRecordDetection`
- `TestCategoryB_J2CorruptRecordChecksum`
- `TestCategoryB_J3ReplayWithCorruptState`

### Rebuild Tests
- `TestRebuildChecksumCatchesCorruptionE6`
- `TestRebuildDiskFullE2`
- `TestRebuildSourceExtentCorruptedE3`

## Production vs Test

In production:
- `Payload` field is **required** (no synthetic data)
- `AllowSynthetic` must never be set

In tests:
- `AllowSynthetic: true` allows deterministic fake data
- `FailAfter` parameter injects crashes at each state

## Summary

rtparityd achieves safe durability through:
1. **Ordered fsyncs** - every state transition is durable before proceeding
2. **Directory sync** - ensures file existence survives crash
3. **Journal first** - every disk write preceded by journal record
4. **Invariant enforcement** - refuse to commit corrupt state
5. **Post-commit verification** - verify checksums before returning success
