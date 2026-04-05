# Category G Metadata Failure Tests

## Quick Start

### Files
- **Tests**: `internal/journal/metadata_failures_test.go` (717 lines, 11 test functions)
- **Fault Injection**: `internal/journal/fault_injection.go` (MetadataCorruptionHelper)
- **Recovery**: `internal/journal/recovery.go` (enhanced to handle corrupted metadata)

### Run Tests
```bash
# All tests
go test -v ./internal/journal -run "TestG"

# Specific test
go test -v ./internal/journal -run "TestG1_TornHeader"
```

## Test Summary

| ID | Test | Corruption | Verification |
|----|------|-----------|--------------|
| G1 | TornHeader | Magic bytes [0:4] XORed | Journal recovery succeeds |
| G2 | InvalidStateChecksum | Checksum [40:72] XORed | BLAKE3 validation fails, fallback |
| G3 | GenerationRollback | Metadata gen < journal gen | Newer journal gen applied |
| G4 | ReferencesNonexistentExtent | Extent removed from metadata | Journal repairs reference |
| G5 | ReferencesUncommittedExtent | Prepared but uncommitted tx | Incomplete transaction aborted |
| G6 | CheckpointWrittenButNotDurable | Metadata truncated | Torn write detected, journal used |
| G7 | CorruptedIndex | FileID/ExtentID corrupted | Journal provides authoritative IDs |
| G8 | StaleCheckpointApplied | 2 newer journal transactions | Both transactions applied |
| G10 | VersionMismatch | Version field corrupted | Version validation fails, journal used |
| - | JournalPrecedenceOverCorruptedMetadata | Multiple corruptions | Journal always takes priority |
| - | SafeRejectionOfAmbiguousMetadata | Ambiguous state | Safe handling, no undefined behavior |

## Metadata Format

Header (72 bytes):
```
[0:4]   Magic       "RTPM" (corrupted in G1, G10 corrupts version)
[4:6]   Version     uint16
[6:8]   Reserved    uint16
[8:16]  SavedAt     int64  timestamp
[16:24] Generation  uint64 (used in G3, G8)
[24:28] PayloadLen  uint32
[28:32] Reserved    uint32
[32:40] Reserved    [8]byte
[40:72] StateHash   [32]byte BLAKE3-256 (corrupted in G2)
```

Payload: Binary-encoded SampleState (files, extents, parity groups, etc.)

## How It Works

### Setup Phase
1. Create coordinator with metadata + journal stores
2. Allocate file with extents
3. Commit to journal as StateMetadataWritten
4. Save metadata snapshot

### Corruption Phase
5. Use MetadataCorruptionHelper to apply specific corruption
   - TornHeader() → XOR bytes [0:4]
   - InvalidStateChecksum() → XOR bytes [40:72]
   - TruncateMetadata(n) → Truncate to n bytes

### Recovery Phase
6. Call Recover()
   - Attempts Load() of metadata
   - Falls back to default state if Load() fails
   - Loads journal records
   - Applies each transaction from journal
   - Saves recovered metadata

### Verification Phase
7. Verify recovery succeeded:
   - `len(result.RecoveredTxIDs) > 0` (journal was used)
   - Recovered files match original
   - FileIDs/ExtentIDs are correct
   - No corrupted references remain

## Key Code

### Fault Injection
```go
corruptor := NewMetadataCorruptionHelper(metaPath)
corruptor.TornHeader()              // G1, G10 (version)
corruptor.InvalidStateChecksum()    // G2
corruptor.TruncateMetadata(36)      // G6
```

### Recovery Enhancement
```go
// OLD: Failed on corrupted metadata
state, err := c.metadata.LoadOrCreate(defaultState)

// NEW: Graceful fallback
state, err := c.metadata.Load()
if err != nil {
    state = defaultState  // Use default, journal will fix it
}
```

### Verification
```go
result, err := coord.Recover()
if err != nil {
    t.Fatalf("Recover failed: %v", err)
}

// Journal was used
if len(result.RecoveredTxIDs) == 0 {
    t.Fatal("expected recovery from journal")
}

// Correct state
recovered, _ := coord.metadata.Load()
if len(recovered.Files) == 0 {
    t.Fatal("expected file to be recovered")
}
```

## Test Categories

### Structural Corruption
- **G1**: Magic bytes corrupted
- **G10**: Version field corrupted

### Validation Corruption
- **G2**: Checksum (BLAKE3) corrupted
- **G6**: Torn write (truncation)

### Temporal Anomalies
- **G3**: Generation rollback (stale metadata)
- **G8**: Stale checkpoint (older generation)

### Reference Corruption
- **G4**: Missing extent reference
- **G7**: Corrupted FileID/ExtentID

### Transactional Anomalies
- **G5**: Uncommitted extent (prepared state)

### Precedence Tests
- **JournalPrecedence**: Multiple corruptions
- **SafeRejection**: Ambiguous state handling

## Expected Results

Each test should:
✓ Corrupt metadata without crashing
✓ Call Recover() successfully
✓ Return result with RecoveredTxIDs
✓ Load recovered metadata without error
✓ Have correct FileID/ExtentID in recovered state
✓ Zero data loss (all committed data recoverable)

## Integration

- **Coordinator.Recover()**: Main recovery entry point
- **MetadataStore.Load()**: Metadata validation
- **MetadataStore.Save()**: Persistence
- **JournalStore.Load()**: Journal recovery source
- **MetadataCorruptionHelper**: Fault injection
- **Record/SampleState**: Transaction types

## Documentation

- **CATEGORY_G_TESTS.md**: Detailed test documentation
- **IMPLEMENTATION_SUMMARY.md**: Implementation details
- **CATEGORY_G_README.md**: This file (quick reference)

## Next Steps

1. Run tests: `go test -v ./internal/journal -run "TestG"`
2. Verify all pass without extent file errors
3. Extend tests with extent file creation if needed
4. Add parity recovery tests (G + parity)
5. Performance benchmarks with large metadata

## Maintenance

- Update MetadataCorruptionHelper if binary format changes
- Keep recovery.go graceful error handling
- Add new corruption methods for new scenarios
- Document expected Recovery result fields
