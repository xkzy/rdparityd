# Category G: Metadata Failure Tests (G1-G10)

## Overview

Category G tests verify recovery behavior when metadata snapshots are corrupted, stale, or inconsistent. The test suite ensures that:

1. **Journal Precedence**: Journal records always take precedence over corrupted metadata
2. **Safe Rejection**: Ambiguous states are safely rejected rather than applied
3. **Recovery Source Selection**: The recovery mechanism correctly selects the appropriate recovery source
4. **Graceful Degradation**: Corrupted metadata doesn't cause unrecoverable failures

## Implementation Location

- **File**: `internal/journal/metadata_failures_test.go`
- **Fault Injection Helper**: `internal/journal/fault_injection.go` - `MetadataCorruptionHelper`
- **Recovery Code**: `internal/journal/recovery.go` - Updated to handle corrupted metadata gracefully

## Test Cases

### G1: TornHeader() - Corrupt Magic Bytes
**Test**: `TestG1_TornHeader`

Simulates corrupted magic bytes in the metadata header:
```go
// Flip magic bytes [0:4] to create invalid header
corruptor.TornHeader()  // XORs bytes [0:4]
```

**Expected Behavior**:
- Metadata validation fails during Load()
- Recovery gracefully falls back to default state
- Journal records are used to reconstruct the state
- File is successfully recovered from journal transaction

**Verification**:
- `len(result.RecoveredTxIDs) > 0` - Transaction recovered from journal
- Recovered file matches original FileID
- No data loss despite corrupted metadata

---

### G2: InvalidStateChecksum() - Corrupt Checksum
**Test**: `TestG2_InvalidStateChecksum`

Simulates corrupted state checksum (BLAKE3 hash mismatch):
```go
// Flip checksum bytes [40:72]
corruptor.InvalidStateChecksum()
```

**Expected Behavior**:
- BLAKE3 validation fails during Load()
- Metadata is rejected as unreliable
- Journal becomes the source of truth
- File and extents are recovered from journal

**Verification**:
- `len(result.RecoveredTxIDs) > 0` - Transaction recovered from journal
- Recovered files are non-empty
- Checksum mismatch detected and handled safely

---

### G3: GenerationRollback
**Test**: `TestG3_GenerationRollback`

Simulates a crash where metadata was not updated with the latest generation:
```
Metadata: Generation 1
Journal:  Generation 2 (newer transaction)
```

**Expected Behavior**:
- Journal's newer generation takes precedence
- Stale metadata generation is recognized
- Newer transaction from journal is applied
- Final state includes changes from newer generation

**Verification**:
- `len(result.RecoveredTxIDs) > 0` - Transaction recovered
- Recovered file count matches newer generation
- Generation mismatch handled correctly

---

### G4: ReferencesNonexistentExtent
**Test**: `TestG4_ReferencesNonexistentExtent`

Simulates metadata referencing an extent that doesn't exist:
```
Metadata Files: [file-000001]
Metadata Extents: [] (removed, inconsistency)
Journal: Contains extent records for file-000001
```

**Expected Behavior**:
- Inconsistency detected between Files and Extents
- Journal transaction becomes the source of truth
- Missing extent is recovered from journal
- Metadata is repaired with correct references

**Verification**:
- Recovery detects reference mismatch
- `len(result.RecoveredTxIDs) > 0` - Journal recovery succeeds
- Extent count matches journal after recovery

---

### G5: ReferencesUncommittedExtent
**Test**: `TestG5_ReferencesUncommittedExtent`

Simulates a race condition where metadata doesn't match uncommitted journal records:
```
Journal: StatePrepared (incomplete)
Metadata: Without the uncommitted extent (safe)
```

**Expected Behavior**:
- Prepared but incomplete transaction is recognized
- Transaction is marked for abortion/rollback
- No partial/inconsistent state is applied
- Safe rejection of ambiguous state

**Verification**:
- `len(result.AbortedTxIDs) > 0` - Incomplete transaction aborted
- Recovery completes safely without applying partial state

---

### G6: CheckpointWrittenButNotDurable
**Test**: `TestG6_CheckpointWrittenButNotDurable`

Simulates a crash where metadata was written but not fsync'd:
```
Metadata File: Partially written (truncated)
Journal: Complete, durable records
```

**Expected Behavior**:
- Truncated/torn metadata file detected
- Load fails gracefully
- Journal provides durable recovery source
- Transaction recovered from journal

**Verification**:
- Metadata loading fails due to truncation
- `len(result.RecoveredTxIDs) > 0` - Journal recovery succeeds
- No data loss despite incomplete metadata write

---

### G7: CorruptedIndex (FileID/ExtentID)
**Test**: `TestG7_CorruptedIndex`

Simulates corruption of File/Extent ID references:
```
Journal:  FileID="file-000001", ExtentID="extent-000001"
Metadata: FileID="corrupted-file-id", ExtentID="corrupted-extent-id"
```

**Expected Behavior**:
- ID mismatch detected during recovery
- Journal's correct IDs take precedence
- Metadata is rebuilt with correct references
- No phantom or corrupted references remain

**Verification**:
- `len(result.RecoveredTxIDs) > 0` - Recovery succeeds
- Recovered FileID matches journal source (file-000001)
- Corrupted IDs not present in recovered state

---

### G8: StaleCheckpointApplied
**Test**: `TestG8_StaleCheckpointApplied`

Simulates stale metadata when newer transactions exist in journal:
```
Metadata: Generation 1 (only first checkpoint)
Journal:  Generation 3 (two newer transactions)
```

**Expected Behavior**:
- Metadata recognized as stale
- Newer journal transactions applied sequentially
- All changes from newer generations recovered
- Final state includes all transactions

**Verification**:
- `len(result.RecoveredTxIDs) >= 2` - Multiple transactions recovered
- Recovered file count >= 2 (both files from newer generations)
- Stale checkpoint identified and superseded

---

### G10: VersionMismatch
**Test**: `TestG10_VersionMismatch`

Simulates metadata version incompatibility:
```
Metadata Version: 0xFFFF (corrupted/incompatible)
Expected Version: 1
Journal: Valid records
```

**Expected Behavior**:
- Version field validation fails
- Metadata rejected as incompatible
- Journal records used for recovery
- File state reconstructed from journal

**Verification**:
- Version mismatch detected
- `len(result.RecoveredTxIDs) > 0` - Journal recovery succeeds
- Recovered state matches journal source

---

## Recovery Precedence Tests

### TestG_JournalPrecedenceOverCorruptedMetadata
Verifies that journal always takes precedence:
- Corrupt metadata in multiple ways
- Verify recovery still succeeds via journal
- Verify correct file is present after recovery

### TestG_SafeRejectionOfAmbiguousMetadata
Verifies safe handling of ambiguous states:
- Create ambiguous corrupted metadata
- Recovery either succeeds with safe default OR returns error
- Never applies undefined/corrupted state

---

## Test Patterns

### Standard Pattern
```go
func TestGx_Description(t *testing.T) {
	// 1. Setup
	tmpdir := t.TempDir()
	coord := NewCoordinator(metaPath, journalPath)
	initialState := metadata.PrototypeState("test-gx")

	// 2. Create committed state
	allocator := metadata.NewAllocator(&initialState)
	file, extents, err := allocator.AllocateFile("/test/file.dat", metadata.DefaultExtentSize)
	
	// 3. Commit to journal
	record := Record{
		TxID: "tx-gx-001",
		State: StateMetadataWritten,
		// ... fields ...
	}
	coord.journal.Append(record)
	coord.commitState(initialState)

	// 4. Apply corruption
	corruptor := NewMetadataCorruptionHelper(metaPath)
	corruptor.SomeCorruptionMethod()

	// 5. Recover
	result, err := coord.Recover()

	// 6. Verify recovery succeeded
	if len(result.RecoveredTxIDs) == 0 {
		t.Fatal("expected recovery from journal")
	}

	// 7. Verify correct state was recovered
	recovered, _ := coord.metadata.Load()
	if len(recovered.Files) == 0 {
		t.Fatal("expected file to be recovered")
	}
}
```

---

## Fault Injection Methods

The `MetadataCorruptionHelper` provides these corruption methods:

### TornHeader()
Flips magic bytes [0:4] to simulate torn header write.

**Binary Format**:
```
[0:4]   Magic (RTPM) - corrupted
[4:6]   Version
[6:8]   Reserved
[8:16]  SavedAt (timestamp)
[16:24] Generation
[24:28] PayloadLen
[28:32] Reserved
[32:40] Reserved
[40:72] StateHash (BLAKE3)
```

### InvalidStateChecksum()
Flips state checksum bytes [40:72] to fail BLAKE3 validation.

### TruncateMetadata(bytes)
Truncates metadata file to simulate partial write.

---

## Recovery Enhancements

The recovery procedure was enhanced to handle corrupted metadata gracefully:

```go
// OLD: Fails if metadata is corrupted
state, err := c.metadata.LoadOrCreate(defaultState)
if err != nil {
    return RecoveryResult{}, fmt.Errorf("load metadata state for recovery: %w", err)
}

// NEW: Falls back to default state, uses journal
state, err := c.metadata.Load()
if err != nil {
    // Metadata is corrupted or missing. Use default state and mark
    // that recovery must use journal records exclusively.
    state = defaultState
}
```

This ensures:
- Corrupted metadata doesn't block recovery
- Journal becomes the exclusive source of truth
- Safe fallback to default state
- No undefined behavior with corrupted data

---

## Validation Checklist

For each test case:
- ✓ Metadata corruption successfully applied
- ✓ Recover() completes without error
- ✓ Recovery identifies corrupted metadata
- ✓ Journal records are used (RecoveredTxIDs not empty)
- ✓ Recovered state matches journal source
- ✓ File and extent references are correct
- ✓ No corrupted IDs/references remain

---

## Future Enhancements

1. **Multi-corruption Tests**: Test multiple simultaneous corruptions
2. **Partial Metadata Tests**: Test metadata with some valid, some corrupted sections
3. **Timing Tests**: Test metadata loaded at exact moment of corruption
4. **Parity Recovery**: Test extent reconstruction when metadata is corrupted
5. **Performance**: Measure recovery time with large metadata files

---

## Integration

These tests integrate with:
- `Coordinator.Recover()` - Recovery orchestration
- `MetadataStore.Load()` - Metadata validation
- `MetadataStore.Save()` - Metadata persistence
- `JournalStore.Load()` - Journal recovery source
- `MetadataCorruptionHelper` - Fault injection

The tests verify the complete recovery pipeline handles metadata failures gracefully and maintains data integrity.
