# Category C/F/H Test Skeleton Implementation Summary

**Date**: April 5, 2026  
**Project**: rdparityd (RDP Parity Disk)  
**Purpose**: Comprehensive test suite for failure scenarios in data corruption, scrub operations, and concurrent access

---

## Overview

This document summarizes the implementation of 30 test skeletons across three failure categories:
- **Category C**: Data Corruption (10 tests)
- **Category F**: Full Scrub Operations (10 tests)
- **Category H**: Concurrency & Race Conditions (10 tests)

All tests follow a consistent template with `t.Skip()` to allow the full test suite to execute while implementation proceeds incrementally.

---

## File Locations

```
/Dev/ActiveProject/rdparityd/internal/journal/
├── category_c_data_corruption_test.go      # 10 tests (bit flips, multi-byte corruption, timing)
├── category_f_full_scrub_test.go           # 10 tests (comprehensive scrub, interruption/resumption)
├── category_h_concurrency_test.go          # 10 tests (concurrent writes, repairs, metadata updates)
├── TEST_SKELETON_TEMPLATE.md               # Reference template for consistency
└── CATEGORY_CFH_TEST_STUBS_SUMMARY.md      # This file
```

---

## Test Structure Overview

### Test Skeleton Format

Each test follows this standard structure:

```go
// TestCategoryX_<Name> verifies <what it tests>.
//
// VERIFICATION GOALS:
// - <goal 1>
// - <goal 2>
// - <goal 3>
//
// FAILURE SCENARIOS:
// - <scenario 1>
// - <scenario 2>
//
// EXPECTED OUTCOMES:
// - <outcome 1>
// - <outcome 2>
func TestCategoryX_<Name>(t *testing.T) {
	t.Skip("Category X not yet implemented: <specific reason>")

	// PSEUDOCODE:
	// 1. Setup coordinator and write test data
	// 2. Inject fault or corruption
	// 3. Trigger operation (read/scrub/repair)
	// 4. Verify detection/recovery
	// 5. Assert invariants hold

	root := t.TempDir()
	_ = NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	// Implementation to follow
}
```

### Key Components

1. **Test Name Convention**: `TestCategoryX_<OperationUnderTest>_<FaultType>_<ExpectedOutcome>`
2. **Documentation**: Clear comments explaining verification goals, failure scenarios, and expected outcomes
3. **Pseudocode**: Step-by-step outline for future implementation
4. **t.Skip()**: Allows test to run without failure, indicating planned implementation

---

## Category C: Data Corruption Tests (10 tests)

Tests in Category C focus on detecting and recovering from bit flips, multi-byte corruption, and timing issues around scrub/rebuild/commit operations.

### C1: TestCategoryC_SingleBitFlipDetection
- **Focus**: Single-bit corruption detection
- **Corruption Type**: Bit flip via XOR at specific offset
- **Operation**: Scrub (audit-only mode)
- **Assertion**: FailedCount > 0, extent ID in FailedExtentIDs, Healthy == false

### C2: TestCategoryC_MultiByteTornWrite
- **Focus**: Multi-byte corruption recovery
- **Corruption Type**: 8 consecutive bytes XOR
- **Operation**: Scrub with repair enabled
- **Assertion**: HealedCount == 1, data fully recovered

### C3: TestCategoryC_CorruptionAcrossParityBoundary
- **Focus**: Cross-extent corruption
- **Corruption Type**: Bit flips in 2+ extents
- **Operation**: Scrub with repair
- **Assertion**: FailedCount == 2, HealedCount == 2

### C4: TestCategoryC_CorruptionBeforeCommit
- **Focus**: Corruption in uncommitted transaction
- **Scenario**: Data written, extent corrupted before commit finalized
- **Operation**: Recovery replay
- **Assertion**: File either fully written or not visible (no partial state)

### C5: TestCategoryC_CorruptionDuringRebuild
- **Focus**: Corruption during parity reconstruction
- **Scenario**: Remove extent, corrupt source during rebuild
- **Operation**: Rebuild with corruption detection
- **Assertion**: Rebuild fails safely, destination extent valid

### C6: TestCategoryC_TimingCorruptionVsCommit
- **Focus**: Race between scrub and commit
- **Scenario**: File1 scrub racing with File2 commit
- **Operation**: Both scrub and commit proceed
- **Assertion**: No corruption or invariant violation

### C7: TestCategoryC_CorruptionAfterCommit
- **Focus**: Post-commit corruption detection
- **Scenario**: Extent corrupted on disk after transaction committed
- **Operation**: Scrub in next session
- **Assertion**: Corruption detected, repair transaction created (not replay)

### C8: TestCategoryC_CorruptionInMetadataChecksums
- **Focus**: Metadata checksum corruption
- **Scenario**: Checksum value corrupted in metadata JSON
- **Operation**: Scrub with repair
- **Assertion**: Checksum recalculated and fixed

### C9: TestCategoryC_BitFlipInJournalRecord
- **Focus**: Journal record validation
- **Scenario**: 1-bit flip in journal transaction payload (not checksum)
- **Operation**: Replay on coordinator restart
- **Assertion**: Checksum validation fails, replay stops safely

### C10: TestCategoryC_CorruptionUnderHighLoad
- **Focus**: Corruption detection under concurrent load
- **Scenario**: 10 writers + 10 readers, corruptions during I/O
- **Operation**: Scrub concurrent with active load
- **Assertion**: All corrupted extents detected, no masking from concurrency

---

## Category F: Full Scrub Tests (10 tests)

Tests in Category F verify comprehensive scrub operations, specific corruption detection patterns, and interruption/resumption handling.

### F1: TestCategoryF_FullScrubHealthyPool
- **Focus**: Baseline healthy pool scrub
- **Setup**: 5 files written, no corruption
- **Operation**: Scrub (audit-only)
- **Assertion**: Healthy == true, FilesChecked >= 5, FailedCount == 0

### F2: TestCategoryF_ScrubDetectsMultipleCorruptedExtents
- **Focus**: Multi-extent corruption detection accuracy
- **Setup**: 10 files, 5 random extents corrupted (various patterns)
- **Operation**: Scrub (audit-only)
- **Assertion**: FailedCount == 5, all corrupt extent IDs found, no false positives

### F3: TestCategoryF_ScrubRepairsAllDetectedCorruptions
- **Focus**: Complete repair in single scrub
- **Setup**: 5 files, 3 extents corrupted
- **Operation**: Scrub with repair enabled
- **Assertion**: HealedCount == 3, Healthy == true, all data matches original

### F4: TestCategoryF_ScrubInterruptionAndResumption
- **Focus**: Checkpoint/resumption after crash
- **Setup**: 20 files to create large extent set, 2 extents corrupted
- **Operation**: Scrub interrupted at 50%, then resumed
- **Assertion**: All extents checked exactly once total, both corrupted found

### F5: TestCategoryF_ScrubWithAuditOnlyMode
- **Focus**: Audit-only mode (no modification)
- **Setup**: 2 extents corrupted
- **Operation**: Scrub with repair=false
- **Assertion**: FailedCount > 0, HealedCount == 0, data still corrupted

### F6: TestCategoryF_ScrubPersistsHistoryAndStats
- **Focus**: Scrub history tracking
- **Setup**: Run scrub twice with different outcomes
- **Operation**: Scrub, corrupt different extent, scrub again
- **Assertion**: Both runs in metadata with different timestamps and stats

### F7: TestCategoryF_ScrubDetectsPartialFileCorruption
- **Focus**: Files with mixed health (some extents good, some bad)
- **Setup**: Large file with 3 extents, corrupt extents[0] and [2], keep [1] clean
- **Operation**: Scrub without repair, then with repair
- **Assertion**: FailedCount == 2, extent[1] not in failed list, all repaired

### F8: TestCategoryF_ScrubAcrossMultiplePoolsIsolated
- **Focus**: Pool isolation during scrub
- **Setup**: 2 separate coordinators (pool A and B), corrupt extent in pool A only
- **Operation**: Scrub each pool independently
- **Assertion**: Pool A detects corruption, pool B healthy, repairs isolated

### F9: TestCategoryF_ScrubProgressReporting
- **Focus**: Progress feedback during long-running scrub
- **Setup**: Many files to create 5+ second scrub
- **Operation**: Monitor progress channel during scrub
- **Assertion**: Progress events sent, monotonically increasing, can cancel via context

### F10: TestCategoryF_ScrubRepairDoesntCorruptHealthyExtents
- **Focus**: Repair safety (conservative modification)
- **Setup**: 5 files, corrupt 1 extent in file3
- **Operation**: Scrub with repair
- **Assertion**: Files 1,2,4,5 checksums unchanged, only file 3 modified

---

## Category H: Concurrency Tests (10 tests)

Tests in Category H verify correct handling of concurrent writes, write+repair conflicts, and elimination of race conditions.

### H1: TestCategoryH_ConcurrentWritesToDifferentFiles
- **Focus**: Baseline concurrent write correctness
- **Setup**: 10 goroutines, each writing different file
- **Operation**: All writes proceed simultaneously
- **Assertion**: All files readable with correct payloads, no cross-file corruption

### H2: TestCategoryH_ConcurrentReadsWhileWriting
- **Focus**: Read consistency during concurrent write
- **Setup**: Initial file written, 5 reader goroutines polling
- **Operation**: Writer overwrites file mid-poll
- **Assertion**: Readers see either old or new state (never partial)

### H3: TestCategoryH_ConcurrentWriteToSameFile
- **Focus**: Same-file write conflict detection
- **Setup**: 3 goroutines attempt to write identical path simultaneously
- **Operation**: All writes race
- **Assertion**: Exactly 1 succeeds, others fail with conflict error, final data is one of 3 payloads

### H4: TestCategoryH_WriteAndRepairRaceCondition
- **Focus**: Write + repair operations don't interfere
- **Setup**: Files A and B exist, write C in progress
- **Operation**: While C writing, corrupt B and start repair
- **Assertion**: Both complete successfully, no deadlock or data corruption

### H5: TestCategoryH_ConcurrentScrubAndWrite
- **Focus**: Scrub doesn't block writes
- **Setup**: 10 files, start scrub in background
- **Operation**: While scrub running, write file11
- **Assertion**: Scrub completes, file11 written, no deadlock, results accurate for scanned portion

### H6: TestCategoryH_MultipleRepairsOfDifferentExtents
- **Focus**: Parallel repairs don't interfere
- **Setup**: Corrupt 3 different extents
- **Operation**: Launch 3 repair goroutines concurrently
- **Assertion**: All 3 repairs complete, all extents repaired, no cross-extent corruption

### H7: TestCategoryH_RaceFredomMetadataUpdates
- **Focus**: Concurrent metadata update atomicity
- **Setup**: 5 writer goroutines, all updating metadata
- **Operation**: Race to update metadata simultaneously
- **Assertion**: All 5 file entries present, metadata valid and loadable, no partial JSON

### H8: TestCategoryH_ConcurrentJournalReplay
- **Focus**: Journal replay serialization
- **Setup**: Incomplete journal transaction, trigger replay
- **Operation**: While replay in-flight, launch new write
- **Assertion**: Replay completes before write finishes, no lost transactions, all data persisted

### H9: TestCategoryH_HighConcurrencyStressTest
- **Focus**: System stability under sustained high load
- **Setup**: 20 writers + 10 readers + 5 repair tasks, run 10+ seconds
- **Operation**: All operations active simultaneously
- **Assertion**: No crashes/panics, all ops complete, files readable, no data races (-race flag)

### H10: TestCategoryH_DeadlockFredomInLockingHierarchy
- **Focus**: Lock ordering prevents deadlock
- **Setup**: 3 threads acquiring locks in cycle (A→B→C→A pattern)
- **Operation**: All threads repeat pattern rapidly
- **Assertion**: No deadlock after 100 iterations, all complete successfully, responsive system

---

## Fault Injection Patterns

### Data Corruption (Category C)
```go
// Single bit flip
data[offset] ^= (1 << bitPosition)

// Multi-byte XOR
for i := 0; i < length; i++ {
    data[offset+i] ^= 0xFF
}

// Corrupt checksum
data[checksumOffset] ^= 0xFF
```

### Scrub-Specific (Category F)
```go
// Pause mid-operation
triggerPauseAt(operationState)

// Remove commitment markers
data = data[:len(data)-200]  // Truncate journal

// Simulate progress checkpoint
saveCheckpoint(scannedExtents)
```

### Concurrency (Category H)
```go
// Concurrent writers
var wg sync.WaitGroup
for i := 0; i < N; i++ {
    wg.Add(1)
    go func(id int) {
        coordinator.WriteFile(...)
        wg.Done()
    }(i)
}
wg.Wait()

// Monitor race conditions
go test -race ./internal/journal
```

---

## Common Assertion Patterns

```go
// Corruption detection
if result.FailedCount == 0 { t.Fatal("expected failures detected") }
if !slices.Contains(result.FailedExtentIDs, extentID) { t.Fatal("extent not detected") }

// Repair verification
if result.HealedCount != expected { t.Fatalf("expected %d healed, got %d", expected, result.HealedCount) }
if !bytes.Equal(repaired, original) { t.Fatal("repair data mismatch") }

// Health checks
if !result.Healthy { t.Fatal("expected healthy after repair") }
if len(result.FailedExtentIDs) > 0 { t.Fatal("no failures expected") }

// Invariant checks
if invariantsViolated(state) { t.Fatal("invariants violated") }
```

---

## Implementation Checklist

### Phase 1: Basic Fixtures & Helpers
- [ ] Verify fault injection helpers in `fault_injection.go` work
- [ ] Create test data generators (varied sizes, patterns)
- [ ] Set up corruption verification utilities

### Phase 2: Category C Implementation
- [ ] Implement bit flip detection (C1)
- [ ] Implement multi-byte corruption (C2, C3)
- [ ] Implement transaction state tracking (C4, C5)
- [ ] Implement timing race detection (C6)
- [ ] Implement post-commit scenarios (C7, C8, C9)
- [ ] Implement concurrent load testing (C10)

### Phase 3: Category F Implementation
- [ ] Implement healthy pool baseline (F1)
- [ ] Implement multi-extent detection (F2)
- [ ] Implement complete repair (F3)
- [ ] Implement checkpoint/resumption (F4)
- [ ] Implement audit-only mode (F5)
- [ ] Implement history persistence (F6)
- [ ] Implement partial file handling (F7)
- [ ] Implement pool isolation (F8)
- [ ] Implement progress reporting (F9)
- [ ] Implement repair safety (F10)

### Phase 4: Category H Implementation
- [ ] Implement concurrent writes baseline (H1)
- [ ] Implement read-write consistency (H2)
- [ ] Implement same-file conflict detection (H3)
- [ ] Implement write-repair race handling (H4)
- [ ] Implement scrub-write concurrency (H5)
- [ ] Implement parallel repair safety (H6)
- [ ] Implement metadata update atomicity (H7)
- [ ] Implement journal replay serialization (H8)
- [ ] Implement high-load stress (H9)
- [ ] Implement deadlock-freedom verification (H10)

### Phase 5: Validation & Hardening
- [ ] Run full test suite with `-race` flag
- [ ] Verify all tests reach completion (no hangs)
- [ ] Profile performance of concurrent tests
- [ ] Document any discovered edge cases
- [ ] Create supplementary fault injection scenarios

---

## Running the Tests

### View all skeletons
```bash
cd /Dev/ActiveProject/rdparityd
go test -v ./internal/journal -run "TestCategoryC_|TestCategoryF_|TestCategoryH_" -count=1
```

### Count pending tests
```bash
grep -r "t.Skip.*not yet implemented" internal/journal/category_*.go | wc -l
# Expected: 30 tests
```

### Run with race detection
```bash
go test -race ./internal/journal -run "TestCategoryC_|TestCategoryF_|TestCategoryH_"
```

### Run single test
```bash
go test -v ./internal/journal -run "TestCategoryC_SingleBitFlipDetection"
```

---

## Best Practices for Implementation

1. **Preserve Skeleton Structure**: Keep the VERIFICATION GOALS, FAILURE SCENARIOS, and EXPECTED OUTCOMES comments
2. **Use Existing Helpers**: Leverage `JournalCorruptionHelper` and `DiskFailureHelper` from `fault_injection.go`
3. **Isolate Test Environments**: Always use `t.TempDir()` for filesystem isolation
4. **Assert Clearly**: Each test should have 3-5 clear assertions, not dozens
5. **Document Edge Cases**: Add comments explaining non-obvious test behavior
6. **Maintain Test Independence**: Tests should not depend on execution order
7. **Avoid Hardcoded Timeouts**: Use contexts and channels for timing-sensitive tests
8. **Verify Cleanup**: Ensure temp directories and goroutines are cleaned up

---

## Template Reference

For consistency, all tests should follow the template documented in:
- `TEST_SKELETON_TEMPLATE.md` (comprehensive reference)
- Each test's inline pseudocode section (quick implementation guide)

---

## Contact & Questions

For questions about specific test implementations:
1. Review the pseudocode section of the relevant test
2. Check `TEST_SKELETON_TEMPLATE.md` for patterns
3. Examine similar existing tests in `stress_test.go`, `repair_test.go`, etc.

---

**Total Tests**: 30 (10 per category)  
**Status**: All skeletons in place, ready for phased implementation  
**Compilation**: ✅ Pass (with `t.Skip()`)  
**Race Detection**: Ready for `-race` flag validation  
