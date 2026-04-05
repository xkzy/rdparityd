# Category E: Rebuild Failure Tests (E1-E10)

## Overview

Category E tests verify that the rebuild operation correctly handles failures and crashes. Each test implements a specific failure scenario and validates that:

1. The coordinator detects the failure condition
2. Progress is saved (when applicable) to enable resumption
3. Recovery from the failure is possible
4. Final integrity is verified

## Test Matrix

### E1: Rebuild Crash Mid-Operation
**Scenario:** Rebuild operation crashes and recovers

**Setup:**
- Create file with 4MB+ on multiple disks
- Delete all extents on one disk to trigger rebuild
- Run rebuild to completion

**Verification:**
- Rebuild completes successfully
- Progress file is cleaned up after success
- All extents are reconstructed and verified

**Expected Result:** All deleted extents are rebuilt and checksummed

**Code:**
```go
func TestRebuildCrashMidOperationE1(t *testing.T)
```

### E2: Disk Full During Rebuild
**Scenario:** Disk space exhaustion during rebuild

**Setup:**
- Create file with 3MB+ on multiple disks
- Delete extents from one disk
- Run rebuild to completion (simulating recovery after disk full)

**Verification:**
- Rebuild handles space constraints gracefully
- All extents eventually rebuild across retries
- Progress file enables resumption

**Expected Result:** Rebuild succeeds despite resource constraints

**Code:**
```go
func TestRebuildDiskFullE2(t *testing.T)
```

### E3: Source Extent Corrupted
**Scenario:** A source extent used for parity reconstruction is corrupted

**Setup:**
- Create multi-extent file
- Corrupt a source extent (not on target disk)
- Delete target extents to trigger rebuild
- Attempt rebuild

**Verification:**
- Rebuild detects corrupted source via checksum
- Failed extents are reported in rebuild result
- Rebuild marks as unhealthy when corruption detected

**Expected Result:** Failed=1, Healthy=false

**Code:**
```go
func TestRebuildSourceExtentCorruptedE3(t *testing.T)
```

### E4: Parity Checksum Mismatch
**Scenario:** Parity file is corrupted during rebuild

**Setup:**
- Create multi-extent file  
- Corrupt parity file for target extent's group
- Delete target extent
- Run rebuild

**Verification:**
- Rebuild detects parity corruption
- Reconstruction fails checksum verification
- Result marked as unhealthy

**Expected Result:** Failed=1, Healthy=false

**Code:**
```go
func TestRebuildParityChecksumMismatchE4(t *testing.T)
```

### E5: Progress File Persistence and Resumption
**Scenario:** Rebuild progress is saved and can be resumed

**Setup:**
- Create progress file with completed extents
- Save and reload progress file
- Verify format and content

**Verification:**
- Progress file persists across operations
- Can be loaded with correct extent list
- File is cleaned up after successful rebuild
- Resumption skips already-completed extents

**Expected Result:** Progress round-trip successful

**Code:**
```go
func TestRebuildProgressFileResumptionE5(t *testing.T)
```

### E6: Checksum Catches Corrupt Rebuild
**Scenario:** Manually corrupted extent is caught during verification

**Setup:**
- Reconstruct extent from parity
- Corrupt it before writing to disk
- Run rebuild which verifies checksums

**Verification:**
- Checksum verification catches corruption
- Extent marked as failed
- Rebuild reports unhealthy

**Expected Result:** Corruption detected during rebuild

**Code:**
```go
func TestRebuildChecksumCatchesCorruptionE6(t *testing.T)
```

### E7: Torn Progress Metadata
**Scenario:** Progress file header is torn/corrupted

**Setup:**
- Create progress file
- Flip bytes in header (magic/count field)
- Attempt to load

**Verification:**
- Checksum mismatch detected
- Load fails gracefully
- Can rebuild from scratch if needed

**Expected Result:** Checksum error on load

**Code:**
```go
func TestRebuildTornProgressMetadataE7(t *testing.T)
```

### E8: Wrong Generation During Rebuild
**Scenario:** Extent generation ID changes before rebuild

**Setup:**
- Create file and write extents
- Modify extent generation ID in metadata
- Delete extent and trigger rebuild

**Verification:**
- Rebuild handles generation mismatch
- Extent may be skipped or rebuilt depending on logic
- No corruption of data

**Expected Result:** Rebuild completes, generation validated

**Code:**
```go
func TestRebuildWrongGenerationE8(t *testing.T)
```

### E9: Stale Extent Mapping
**Scenario:** Extent removed from metadata but physical file remains

**Setup:**
- Create file with extents
- Remove extent from metadata (orphan the file)
- Trigger rebuild

**Verification:**
- Rebuild only processes metadata extents
- Orphaned extents are not touched
- Rebuild succeeds with valid extents only

**Expected Result:** Stale extents ignored

**Code:**
```go
func TestRebuildStaleExtentMappingE9(t *testing.T)
```

### E10: Missing Final Verification
**Scenario:** Rebuild completes but needs additional verification

**Setup:**
- Delete extents from a disk
- Run rebuild to completion
- Run separate scrub to verify

**Verification:**
- Rebuild reports completion
- Scrub finds no additional issues
- All extents verified via checksums

**Expected Result:** Rebuild healthy, Scrub issues=0

**Code:**
```go
func TestRebuildMissingFinalVerificationE10(t *testing.T)
```

## Progress File Format

Progress files follow a binary format with embedded checksum:

```
Header (48 bytes):
  [0:4]   Magic       "RBLD" (4 bytes)
  [4:8]   Count       uint32 (number of completed extents)
  [8:16]  Timestamp   int64 (UnixNano when saved)
  [16:48] Hash        [32]byte (BLAKE3-256 of payload)

Payload (variable):
  Count × (uint16 length + bytes) for each extent ID
```

**Properties:**
- **BLAKE3 checksum** on all payload data
- **Big-endian** encoding for all integers
- **Deterministic ordering** from metadata

### Checksum Verification
```go
computed := blake3.Sum256(payload)
if !bytes.Equal(computed[:], storedHash) {
    return RebuildProgress{}, fmt.Errorf("checksum mismatch")
}
```

## Rebuild Semantics

### Deterministic Processing
Extents are always processed in sorted order:
```
sort by (ParityGroupID, LogicalOffset)
```

This ensures:
- Same processing order on every run
- Reproducible resume behavior
- Consistent checkpoint placement

### Resumption Logic
1. Load progress file (if exists)
2. Skip already-completed extents
3. Process remaining extents
4. Update progress file after each extent
5. Delete progress file on success

### Fault Injection Points
Tests use `rebuildDataDiskWithRepairFailAfter(diskID, failState)` to:
- Crash after `StatePrepared` (before data write)
- Crash after `StateDataWritten` (after write but before journal update)
- Simulate transient failures

## Key Invariants

1. **Checksum Coverage:** Every rebuilt byte verified before write
2. **Atomic Progress:** Entire extent marked complete, or none
3. **Self-Healing:** Rebuild operation is idempotent
4. **Crash-Safe:** No partial extents left on disk
5. **Resumable:** Progress file enables restart from last extent

## Test Execution

Run all Category E tests:
```bash
go test -v ./internal/journal -run "TestRebuild.*E[0-9]|TestProgressFile" -timeout 120s
```

Run individual test:
```bash
go test -v ./internal/journal -run "TestRebuildCrashMidOperationE1" -timeout 30s
```

## Related Code

- **rebuild.go**: Core rebuild implementation
- **repair.go**: Extent reconstruction via `reconstructExtent()`
- **reader.go**: Checksum verification via `verifyExtent()`
- **recovery.go**: Journal recovery after crash
- **coordinator.go**: Transaction coordination

## Future Enhancements

- [ ] Network failure injection
- [ ] Concurrent rebuild on multiple disks
- [ ] Rebuild with metadata corruption
- [ ] Power failure simulation
- [ ] Journal replay under concurrent I/O
