# Test Skeleton Template for Categories C, F, H

## Overview
This template provides a consistent structure for implementing test skeletons across all three failure categories. Each test is prefixed with `t.Skip()` to allow the full test suite to run while implementation proceeds.

## Skeleton Format

```go
// TestCategoryX_<TestName> verifies <what it checks>.
//
// VERIFICATION GOALS:
// - <primary assertion>
// - <secondary assertion>
// - <edge case handling>
//
// FAILURE SCENARIOS:
// - <scenario 1>
// - <scenario 2>
//
// EXPECTED OUTCOMES:
// - <desired behavior>
func TestCategoryX_<TestName>(t *testing.T) {
	t.Skip("Category X not yet implemented: <reason>")
	
	// PSEUDOCODE:
	// 1. Setup coordinator and write test file
	// 2. Inject fault/corruption at specific location
	// 3. Trigger operation (read/scrub/repair/write)
	// 4. Verify detection/recovery
	// 5. Assert invariants hold
	
	root := t.TempDir()
	coordinator := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)
	
	// Implementation to follow
}
```

## Key Components

### Test Naming Convention
- `TestCategoryX_<OperationUnderTest>_<FaultType>_<ExpectedOutcome>`
- Example: `TestCategoryC_BitFlip_DetectAndRepair`

### Required Comments
1. **Description**: One-line purpose statement
2. **VERIFICATION GOALS**: Bullet points of what assertions verify
3. **FAILURE SCENARIOS**: Conditions that trigger the fault
4. **EXPECTED OUTCOMES**: Behaviors that must be maintained
5. **PSEUDOCODE**: Step-by-step outline for implementation

### Test Structure Sections
1. **Setup**: Create coordinator, filesystem
2. **Fault Injection**: Corrupt data, simulate timing, introduce races
3. **Trigger Operation**: Read, write, scrub, repair, rebuild
4. **Verify**: Check detection, recovery, invariants
5. **Assert**: Data integrity, state consistency, no data loss

### Fault Injection Patterns

#### Data Corruption (Category C)
```go
// Single bit flip
data[offset] ^= (1 << bitPosition)

// Multi-byte corruption
for i := 0; i < length; i++ {
    data[offset+i] ^= 0xFF
}
```

#### Scrub-Specific (Category F)
```go
// Remove commitment markers
truncateJournal(journalPath, targetSize)

// Interrupt mid-operation
triggerPanicAt(operationState)
```

#### Concurrency (Category H)
```go
// Spawn concurrent writers
var wg sync.WaitGroup
for i := 0; i < N; i++ {
    wg.Add(1)
    go func() { coordinator.WriteFile(...); wg.Done() }()
}
wg.Wait()
```

## Category Guidance

### Category C: Data Corruption (10 tests)
- Bit flips, multi-byte corruption
- Timing around scrub/rebuild/commit
- Detection accuracy, recovery correctness

### Category F: Full Scrub (10 tests)
- Comprehensive scrub operations
- Specific corruption detection
- Interruption and resumption handling

### Category H: Concurrency (10 tests)
- Concurrent writes to same file
- Write + repair conflict resolution
- Race condition elimination

## Common Assertions

```go
// Health checks
if !result.Healthy { t.Fatal("expected healthy after repair") }

// Corruption detection
if result.FailedCount == 0 { t.Fatal("expected failures detected") }

// Recovery verification
if !bytes.Equal(recovered, original) { t.Fatal("data mismatch") }

// Invariant checks
if !invariantsHold(state) { t.Fatal("invariants violated") }
```

## Future Implementation Checklist

- [ ] Replace `t.Skip()` with actual test logic
- [ ] Implement fault injection at specified points
- [ ] Verify assertions with actual coordinator calls
- [ ] Add timing verification where applicable
- [ ] Document any assumptions about journal format
- [ ] Test with multiple pool configurations
- [ ] Verify cleanup of temporary files
