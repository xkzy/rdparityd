# rtparityd — Formal Invariants Specification

**Version:** 1.0  
**Status:** Normative  
**Scope:** Single-node local storage engine with crash recovery, XOR parity, binary journal, and binary metadata store.

---

## Notation

- **"Durable"** means: written to the kernel page cache, fsync'd, and the parent directory entry fsync'd.
- **"Committed"** means: the journal contains a durable `committed` record for that transaction, AND all durability conditions below that state in the write protocol are satisfied.
- **"State"** (unqualified) means the in-memory `metadata.SampleState` assembled by the coordinator at boot or after recovery.
- **"Journal"** means the append-only file read by `Store.Load()` and replayed by `Recover()`.
- **⊕** denotes bitwise XOR.
- **∀** means "for all."
- **∃** means "there exists."
- **→** means "implies."

---

## Invariant Verification Ordering

When running a full integrity check, apply invariants in this order to minimize
redundant IO and to catch structural failures before IO-bound failures:

1. J1, J2 (journal structure — no disk IO)
2. M3, M1, M2 (metadata structure — no disk IO)
3. E2, E3 (extent metadata structure — no disk IO)
4. P1, P4 (parity group structure — no disk IO)
5. J3 (replay flag — in-memory state)
6. I11 (generation monotonicity — in-memory state)
7. E1 (extent checksums — one read per extent)
8. P2, P3 (parity checksums and XOR correctness — reads parity + all members)

Structural invariants (steps 1–6) are cheap enough to run after every
`commitState()`. IO-bound invariants (steps 7–8) are reserved for scrub,
startup validation, and targeted repair paths.

---

## I1 — Transaction Visibility Invariant

**Formal statement:**

```
∀ transaction T:
  visible(T) = true
  ↔
  durable(journal.commit_record(T))
  ∧ durable(all extent files referenced by T)
  ∧ durable(parity files for all parity groups updated by T)
  ∧ durable(metadata snapshot reflecting T as committed)
```

**Why it matters:**  
Prevents the system from presenting a write as complete when any durability condition is missing. A committed transaction that cannot be reconstructed from disk state is a lie.

**Must hold:**  
Always. Including after restart and after recovery.

**Allowed temporary exceptions:**  
None. A partially durable transaction is not committed; it is in-flight.

**How it is checked:**
- `CheckJournalInvariants(records)` — verifies J1 (record checksums valid) and J2 (state machine followed).
- `Recover()` — re-derives committed state from journal; any transaction without a durable `committed` record is either aborted or rolled forward.
- `CheckIntegrityInvariants(rootDir, state)` — verifies E1 (extent bytes match checksum) and P2 (parity bytes match checksum).

**Code paths that can violate it:**
- `WriteFile()` in `coordinator.go`: if it returns success without completing all four fsync steps.
- `commitState()` in `coordinator.go`: if metadata is written but not fsync'd before the commit record is appended.
- `Recover()` in `recovery.go`: if it marks a partially recovered transaction as committed.

**Tests that enforce it:**
- `TestA2_CrashAfterPrepareSmallPayload` — commit record absent; Recover aborts.
- `TestA6_CrashAfterDataWritten` — data fsync done but parity/metadata missing; Recover rolls forward.
- `TestA12_StateTransitionMetadataWrittenToCommitted` — all steps complete; committed record present and data readable.
- `TestAllCategoryACrashPoints` — covers every crash boundary.

**Proposed enforcement gap:**  
None currently. `WriteFile()` follows: `append prepare → write extents → fsync extents → write parity → fsync parity → save metadata → fsync metadata → append committed`. Any `FailAfter` injection tests confirm abort or roll-forward.

---

## I2 — No Uncommitted Exposure Invariant

**Formal statement:**

```
∀ transaction T:
  ¬committed(T) → ¬visible_to_reader(T)

Where visible_to_reader(T) means any of:
  • ReadFile() returns bytes written by T
  • Metadata snapshot references file or extent created by T
  • API/FUSE returns status "complete" for T
```

**Why it matters:**  
Protects the reader from observing writes that disappear after a crash. An uncommitted write that becomes readable is a phantom read.

**Must hold:**  
Always. The in-memory coordinator state may stage an uncommitted transaction, but it must not escape to any reader.

**Allowed temporary exceptions:**  
An in-memory transaction-local view (inside `WriteFile()` before the write path commits) may temporarily hold uncommitted data. This view must not be propagated to the cached state (`cachedState`) or to any reader until the `committed` record is appended.

**How it is checked:**
- `ReadFile()` in `reader.go` reads state from `loadState()`, which returns `cachedState` only if populated by `commitState()`. `commitState()` is called only after the `committed` record is appended.
- `TestG5_ReferencesUncommittedExtent` — verifies that a crashed write is not exposed.
- `TestA3_CrashAfterPrepareMultiExtent` — data files may exist on disk but are not reachable from committed metadata.

**Code paths that can violate it:**
- `coordinator.go:commitState()` — if called before appending the `committed` journal record.
- `coordinator.go:cachedState` — if updated by any path other than `commitState()`.
- `recovery.go:Recover()` — if it exposes a partially recovered transaction as committed.
- `fusefs/fs.go` — if FUSE reads bypass the coordinator's committed state gate.

**Tests that enforce it:**
- `TestG5_ReferencesUncommittedExtent`
- `TestA4_CrashMidDataWriteExtentLimit1` — partial extent written; not visible after recovery.
- `TestA7_CrashMidParityWriteParityLimit1` — parity incomplete; not visible.

**Proposed enforcement gap:**  
`commitState()` is always called after the `committed` record append in `WriteFile()`. The gate is enforced. However, there is no explicit assertion in `commitState()` that verifies the `committed` record is already in the journal before it sets `cachedState`. A defensive check should be added:

```go
// In commitState(): add before updating cachedState
if c.cachedState != nil && !lastJournalRecordIsCommitted(c.journal) {
    panic("invariant I2: commitState called before committed record is durable")
}
```

---

## I3 — Data Checksum Integrity Invariant

**Formal statement:**

```
∀ committed extent E:
  (checksum(E) ≠ "" ∧ state(E) = committed)
  →
  (blake3(read(E.physical_path)) = E.checksum)
  ∨ (health(E) = degraded ∧ reader returns error or reconstructed bytes)
```

**Why it matters:**  
Silent data corruption is the worst failure mode in a storage system. An extent that passes this invariant is provably identical to what was written. An extent that fails must not silently return wrong bytes.

**Must hold:**  
For every committed extent with a non-empty checksum.

**Allowed temporary exceptions:**  
An in-flight extent (inside a non-committed transaction) may have an empty checksum until committed.

**How it is checked:**
- `CheckIntegrityInvariants()` → `checkE1ExtentChecksums()`: reads every extent file and compares to metadata checksum.
- `verifyExtent()` in `reader.go`: called on every read; rejects mismatches by triggering repair.
- `Scrub()` in `scrub.go`: periodically calls `verifyExtent()` for all extents.
- Rebuild path: verifies reconstructed extent checksum before accepting.

**Code paths that can violate it:**
- `writeExtentFiles()` in `coordinator.go`: if it writes wrong bytes or truncated data.
- `runExtentRepair()` in `repair.go`: if the repaired extent is not re-verified after write.
- `rebuildDiskFromState()` in `rebuild.go`: if the rebuilt extent is not checksum-verified before commit.
- Metadata corruption: if `E.checksum` is overwritten with a wrong value.

**Tests that enforce it:**
- `TestD10_DiskReadErrors_CorruptExtentFile` — corrupt extent; read detects and self-heals.
- `TestRebuildSourceExtentCorruptedE3` — corrupt source during rebuild; rejected.
- `TestRebuildChecksumCatchesCorruptionE6` — wrong reconstruction bytes; checksum catches it.
- `TestB4_InvalidChecksumDetected` — corrupt checksum in journal record; rejected on load.
- Scrub tests: `TestCategoryF_*` (skeletons — need implementation).

**Proposed enforcement gap:**  
`runExtentRepair()` writes the repaired extent but does not re-verify the written bytes against the expected checksum. Add a post-write verify:

```go
// In runExtentRepair(), after replaceSyncFile():
written, err := os.ReadFile(path)
if err != nil || digestBytes(written) != extent.Checksum {
    return nil, false, fmt.Errorf("I3: repaired extent %s failed post-write verification", extent.ExtentID)
}
```

---

## I4 — Parity Coverage Invariant

**Formal statement:**

```
∀ committed parity group G with member extents {E₁, E₂, …, Eₙ}:
  blake3(parity_file(G)) = G.parity_checksum
  ∧ read(parity_file(G)) = E₁.bytes ⊕ E₂.bytes ⊕ … ⊕ Eₙ.bytes
  (with each Eᵢ zero-padded to maxLen = max(len(E₁), …, len(Eₙ)))
```

**Why it matters:**  
Parity is the sole mechanism for single-disk-failure recovery. If parity diverges from data, rebuild produces wrong results — plausible but silently incorrect.

**Must hold:**  
For all committed parity groups after transaction commit and after replay.

**Allowed temporary exceptions:**  
During an uncommitted transaction (between `StateDataWritten` and `StateParityWritten`), parity may lag data. This is the only window.

**How it is checked:**
- `checkP2ParityChecksums()` — verifies parity file against stored checksum.
- `checkP3ParityXOR()` — recomputes XOR of member extents and compares to parity file.
- Both are called by `CheckIntegrityInvariants()`.

**Code paths that can violate it:**
- `writeParityFiles()` in `coordinator.go`: if it computes XOR incorrectly, includes wrong members, or fails to fsync.
- `runParityRepair()` in `repair.go`: if parity is rewritten without including all current members.
- Partial fsync: if `StateDataWritten` is recorded but the parity fsync is incomplete.
- `Recover()`: if it reconstructs committed state but does not verify P2/P3.

**Tests that enforce it:**
- `TestRebuildParityChecksumMismatchE4` — stale parity; rebuild detects mismatch.
- `TestA7_CrashMidParityWriteParityLimit1` — crash during parity write; recovery handles without exposing stale parity.
- `TestA10_StateTransitionDataWrittenToParityWritten` — verifies parity written before advancing state.
- `checkP3ParityXOR` called within `assertInvariantsClean` used across all Category A/B/G tests.

**Proposed enforcement gap:**  
`writeParityFiles()` does not verify the written parity bytes after `replaceSyncFile()`. Add a post-write checksum assertion:

```go
// After replaceSyncFile(parityPath, parityData, 0o600):
if digestBytes(parityData) != group.ParityChecksum {
    return fmt.Errorf("I4: parity group %s post-write checksum mismatch", group.ParityGroupID)
}
```

---

## I5 — Metadata Truthfulness Invariant

**Formal statement:**

```
∀ durable metadata snapshot S:
  ∀ extent E ∈ S.Extents:
    ∃ disk D ∈ S.Disks: D.DiskID = E.DataDiskID
    ∧ physical_file_exists(E.PhysicalLocator.RelativePath)
    ∧ (E.ParityGroupID = "" ∨ ∃ G ∈ S.ParityGroups: G.ParityGroupID = E.ParityGroupID)
  ∀ parity group G ∈ S.ParityGroups:
    ∀ M ∈ G.MemberExtentIDs: ∃ E ∈ S.Extents: E.ExtentID = M
  ∀ transaction T ∈ S.Transactions:
    T.NewGeneration > T.OldGeneration
    ∧ T.NewGeneration ≤ current_generation(S)
```

**Why it matters:**  
Metadata is the system's map. If the map lies, every operation that consults it — reads, rebuild, scrub — will act on false premises. A map that references nonexistent extents will cause rebuild to fail or generate wrong data.

**Must hold:**  
Always for any metadata snapshot that has been durably committed.

**Allowed temporary exceptions:**  
In-memory staged metadata inside an uncommitted transaction only. The staged metadata must not be persisted until the transaction commits.

**How it is checked:**
- `CheckStateInvariants(state)`: checks M1 (file IDs exist), M2 (disk IDs exist), M3 (no duplicate extent IDs), P1 (parity group references valid), E2, E3.
- `CheckIntegrityInvariants(rootDir, state)`: additionally checks E1 (files exist and match checksums), P2, P3.
- Called after every `commitState()` in the test suite via `assertInvariantsClean`.

**Code paths that can violate it:**
- `commitState()`: if called with a staged state that references extents not yet written to disk.
- `Recover()`: if `reconcileCommittedTransaction()` merges a transaction whose extents are missing.
- `ReplaceDisk()` in `disk_lifecycle.go`: if it removes a disk record while extents still reference it.
- `RebuildDataDisk()`: if rebuild finalizes without updating metadata to reference the new extent path.

**Tests that enforce it:**
- `TestG4_ReferencesNonexistentExtent` — metadata references missing extent; detected on recovery.
- `TestG1_TornHeader`, `TestG2_InvalidStateChecksum` — corrupt metadata; journal takes precedence.
- `TestG3_GenerationRollback` — stale metadata; journal reconciles forward.
- `CheckStateInvariants` is implicitly tested by every Category A test via `assertInvariantsClean`.

**Proposed enforcement gap:**  
`commitState()` does not call `CheckStateInvariants()` before persisting. Add a guard:

```go
// At the start of commitState():
if vs := CheckStateInvariants(state); len(vs) > 0 {
    return metadata.SnapshotEnvelope{}, fmt.Errorf("I5: refusing to commit state with %d invariant violation(s): %v", len(vs), vs[0])
}
```

---

## I6 — Replay Idempotence Invariant

**Formal statement:**

```
∀ durable (journal J, metadata snapshot S):
  let R(n) = state after applying Recover() n times from (J, S)
  ∀ n ≥ 1: R(n) = R(1)
```

**Why it matters:**  
A storage engine that crashes during recovery must survive a second boot. If applying recovery twice produces different results, the system is unstable after repeated restarts.

**Must hold:**  
Always.

**Allowed temporary exceptions:**  
None.

**How it is checked:**
- `TestA_RecoveryIdempotentAfterCrash` — calls `Recover()` twice; verifies same committed state, same aborted set, and correct data round-trip.
- `TestB6_DuplicateTransactionHandled` — duplicate committed record in journal; recovery applies once, second application is a no-op.
- Generation checks in `reconcileCommittedTransaction()`: `NewGeneration` is idempotent because generation monotonicity prevents re-application of already-applied transactions.

**Code paths that can violate it:**
- `reconcileCommittedTransaction()` in `recovery.go`: if it re-applies an already-committed transaction and double-counts extents or files.
- `rollForwardRepair()` in `recovery.go`: if a repair transaction is re-applied and writes different bytes.
- `mergeRecoveredFile()` in `recovery.go`: if it appends rather than upserts on duplicate file IDs.

**Tests that enforce it:**
- `TestA_RecoveryIdempotentAfterCrash`
- `TestB6_DuplicateTransactionHandled`
- `TestA_MultipleCrashedWritesRecoveredTogether` — multiple incomplete transactions; single recovery handles all; second recovery is a no-op.

**Proposed enforcement gap:**  
`mergeRecoveredFile()` does not check for duplicate FileIDs before appending. A second recovery pass would append a duplicate file record. Fix:

```go
// In mergeRecoveredFile(): replace append with upsert
for i, existing := range state.Files {
    if existing.FileID == file.FileID {
        state.Files[i] = file // upsert
        // also upsert extents...
        return
    }
}
state.Files = append(state.Files, file)
```

---

## I7 — Torn Record Detectability Invariant

**Formal statement:**

```
∀ record R in journal or metadata file:
  well_formed(R) ↔
    magic_bytes(R) = expected_magic
    ∧ version(R) ∈ supported_versions
    ∧ payload_length(R) > 0
    ∧ blake3(payload(R)) = stored_checksum(R)

∀ record R: ¬well_formed(R) → reject(R) and do not interpret as committed state
```

**Why it matters:**  
Power loss can tear any write. A partial journal record or partial metadata page that is interpreted as a valid committed record can corrupt the system's view of what was committed.

**Must hold:**  
Always. Every record read from journal or metadata must be verified before interpretation.

**Allowed temporary exceptions:**  
None. A torn record at the journal tail (end of file) is expected after a crash; it must be truncated, not interpreted.

**How it is checked:**
- `Store.Load()` in `store.go`: calls `validateRecord()` on every decoded record; stops at first invalid record.
- `metadata.NewStore().Load()` in `metadata/store.go`: verifies BLAKE3 checksum of the full payload before accepting the snapshot.
- `checkJ1RecordChecksums()` in `invariants.go`: callable post-load on the full record slice.

**Code paths that can violate it:**
- `Store.Load()`: if it continues past an invalid record instead of truncating.
- `metadata.Load()`: if it accepts a snapshot whose checksum does not match.
- Any future code that calls `Store.Load()` and ignores the error from `validateRecord()`.

**Tests that enforce it:**
- `TestB1_CorruptMagicBytesDetected` — magic bytes flipped; rejected.
- `TestB4_InvalidChecksumDetected` — checksum flipped; rejected.
- `TestB10_TruncatedJournalPartialFsync` — truncated record; load stops at corruption boundary.
- `TestB7_JournalTailGarbageIgnored` — garbage appended after valid records; ignored.
- `TestG1_TornHeader` — metadata magic torn; load fails, journal used instead.

**Proposed enforcement gap:**  
The metadata `Load()` path returns an error on checksum mismatch, but `Recover()` does not currently fall back to journal-only recovery when metadata fails to load — it propagates the error. The recovery path should treat a metadata load failure as "metadata unavailable; use journal only":

```go
// In recovery.go RecoverWithState():
state, err := c.loadState(defaultState)
if err != nil {
    // I7: torn/corrupt metadata; use journal as sole source of truth
    state = defaultState
    // continue with journal replay only
}
```

This is partially implemented but needs explicit test coverage for the fall-back path producing correct state.

---

## I8 — Rebuild Correctness Invariant

**Formal statement:**

```
∀ rebuilt extent E' replacing failed extent E:
  let P = parity_group_of(E)
  let peers = {all committed extents in P except E}

  reconstruction(E') = P.parity_bytes ⊕ (⊕ peer.bytes for peer ∈ peers)
  ∧ blake3(reconstruction(E')) = E.checksum
  ∧ only_if_above_holds: accept(E') and mark E' as committed

∀ E: ¬valid_reconstruction(E) → reject(E) and surface error
```

**Why it matters:**  
Rebuild is the only recovery path for a lost disk. If rebuild generates plausible-but-wrong bytes and marks the extent committed, the corruption is silent and permanent. The checksum verification step is not optional.

**Must hold:**  
For every extent written during a rebuild operation.

**Allowed temporary exceptions:**  
A candidate `E'` may exist on disk transiently before checksum verification. It must not be committed (journal `committed` record written, metadata updated) before verification passes.

**How it is checked:**
- `rebuildDiskFromState()` in `rebuild.go`: calls `reconstructExtent()`, which verifies the reconstruction checksum before returning.
- `reconstructExtent()` in `reader.go`: returns an error if `digestBytes(normalized) != target.Checksum`.
- `CheckIntegrityInvariants()` after rebuild: verifies E1, P2, P3 on all extents including rebuilt ones.

**Code paths that can violate it:**
- `rebuildDiskFromState()`: if it calls `replaceSyncFile()` before `reconstructExtent()` returns success.
- `rollForwardRepair()` in `recovery.go`: if a repair record's extent is committed without re-verifying the rebuild output.
- Wrong-generation metadata: if `rebuildDiskFromState()` reads extents from a stale metadata snapshot, it may XOR wrong peers.

**Tests that enforce it:**
- `TestRebuildChecksumCatchesCorruptionE6` — wrong reconstruction bytes; checksum rejects.
- `TestRebuildSourceExtentCorruptedE3` — source extent corrupt; rebuild detects and fails safely.
- `TestRebuildParityChecksumMismatchE4` — stale parity; detected before rebuild proceeds.
- `TestRebuildWrongGenerationE8` — stale metadata mapping; rebuild uses wrong peers.
- `TestD9_TwoDisksFail_Unrecoverable` — two-disk failure; rebuild correctly refuses.

**Proposed enforcement gap:**  
`rebuildDiskFromState()` does not call `CheckIntegrityInvariants()` after completing all extents — it only verifies each individual extent during reconstruction. Add a post-rebuild integrity check:

```go
// At the end of rebuildDiskFromState(), before returning success:
reloadedState, err := metadata.NewStore(metadataPath).Load()
if err == nil {
    if vs := CheckIntegrityInvariants(filepath.Dir(metadataPath), reloadedState); len(vs) > 0 {
        return result, fmt.Errorf("I8: post-rebuild integrity check failed: %v", vs[0])
    }
}
```

---

## I9 — Scrub Safety Invariant

**Formal statement:**

```
∀ repair action A during scrub:
  let E = extent being repaired
  let source = reconstruction source (parity + peers)

  valid(source) ↔ blake3(source.parity_bytes) = source.parity_checksum
                 ∧ blake3(reconstruction(E, source)) = E.checksum

  A may replace E.file ↔ valid(source)
  ∧ blake3(new_bytes) = E.checksum  (post-write verification)

∀ invalid_source: reject repair, mark extent degraded, surface error — do not replace
```

**Why it matters:**  
Scrub's purpose is to find and fix corruption. If the repair source is itself corrupt or unverified, scrub converts detectable corruption (checksum mismatch) into silent corruption (wrong bytes with matching stored-but-wrong checksum). This is worse than the original failure.

**Must hold:**  
For every repair action. No exceptions.

**Allowed temporary exceptions:**  
None.

**How it is checked:**
- `runExtentRepair()` in `repair.go`: verifies the parity checksum before using parity as a reconstruction source.
- `verifyExtent()` in `reader.go`: verifies the reconstructed result against `E.checksum` before calling `runParityRepair()`.
- `runParityRepair()` in `repair.go`: recomputes parity XOR and verifies the result against the stored group checksum before writing.

**Code paths that can violate it:**
- `runExtentRepair()`: if it skips parity checksum verification when parity is used as reconstruction source.
- `verifyExtent()`: if it calls `runExtentRepair()` and accepts the result without verifying against `E.checksum`.
- `scrubWithRepairFailAfter()` in `scrub.go`: if it calls `verifyExtent()` with `repair=true` and does not verify the extent after repair.

**Tests that enforce it:**
- `TestRecoverReplaysInterruptedReadSelfHeal()` — interrupted self-heal; replay verifies before accepting.
- `TestRecoverReplaysInterruptedScrubParityRepair()` — scrub repair interrupted; verified before accepting.
- `TestD10_DiskReadErrors_CorruptExtentFile` — corrupt extent; scrub self-heals and verifies.

**Proposed enforcement gap:**  
`runExtentRepair()` does not do a post-write read-back to verify the written bytes match the expected checksum. This means a torn repair write could leave a corrupt extent marked as repaired. Implementation:

```go
// In runExtentRepair(), after replaceSyncFile():
if data, err := os.ReadFile(path); err != nil || digestBytes(normalizeExtentLength(data, extent.Length)) != extent.Checksum {
    return nil, false, fmt.Errorf("I9: scrub repair of %s failed post-write verification", extent.ExtentID)
}
```

---

## I10 — Disk Identity Invariant

**Formal statement:**

```
∀ pool P at any point in time:
  ∀ disks D₁, D₂ ∈ P.Disks:
    D₁.DiskID ≠ D₂.DiskID  (no duplicate IDs in committed state)

∀ disk add/replace operation:
  new_disk.DiskID ∉ {D.DiskID for D ∈ current_committed_disks(P)}
  → operation may proceed

∀ disk import at startup:
  imported_disk.generation ≤ current_pool_generation
  ∧ imported_disk.role matches committed role for that DiskID
```

**Why it matters:**  
If two disks claim the same identity, extent placement becomes ambiguous. A replaced disk that returns with stale data would be accepted as the current disk, causing rebuild to mix old and new data.

**Must hold:**  
At import, startup, `AddDisk()`, and `ReplaceDisk()`.

**Allowed temporary exceptions:**  
None.

**How it is checked:**
- `AddDisk()` in `disk_lifecycle.go`: checks for duplicate DiskID before adding.
- `ReplaceDisk()` in `disk_lifecycle.go`: checks that `newDiskID` does not already exist.
- `AnalyzeMultiDiskFailures()` in `multidisk_failure.go`: detects when multiple disks claim the same data.
- `ValidateRecoverabilityInvariants()` in `multidisk_failure.go`: checks for aliasing and stale generation conflicts.

**Code paths that can violate it:**
- `AddDisk()`: duplicate DiskID check is a string equality; does not check UUID at hardware level.
- `ReplaceDisk()`: the old disk's DiskID is removed and the new one added; if the old disk reappears before metadata is committed, both could coexist transiently.
- Startup: no UUID-level challenge against the physical disk device.

**Tests that enforce it:**
- `TestD7_ReplacementDiskPartialRebuild` — replacement disk correctly inserted.
- `TestD8_ReplacementDiskFullRebuildWithParityFail` — replacement with concurrent parity failure.
- `TestD9_TwoDisksFail_Unrecoverable` — multi-disk unrecoverable; identity preserved.
- `TestAnalyzeDualDiskFailureWithParity`, `TestAnalyzeDualDiskFailureWithoutParity` — aliasing detection.

**Proposed enforcement gap:**  
There is no hardware UUID validation. The system uses operator-assigned DiskID strings. Document this explicitly as a v1 limitation:

```
// In docs/invariants.md and in AddDisk() godoc:
// V1 LIMITATION (I10): DiskID uniqueness is enforced only by string equality.
// There is no hardware UUID challenge. Operators must ensure DiskID strings
// are stable and unique. A future implementation should read the disk's
// kernel-assigned UUID (e.g. via blkid or /dev/disk/by-uuid) and reject
// any import where the UUID does not match the committed DiskID binding.
```

---

## I11 — Monotonic Generation Invariant

**Formal statement:**

```
∀ committed transactions T₁, T₂ in temporal order (T₁ before T₂):
  T₁.NewGeneration < T₂.NewGeneration

∀ metadata snapshot S:
  ∀ T ∈ S.Transactions: T.NewGeneration ≤ S.Pool.Generation

∀ recovery operation:
  accepted_state.generation ≥ prior_accepted_state.generation

No state with generation G may be accepted after a state with generation G+k
(k > 0) has been committed, unless an explicit authorized rollback rule fires.
There are no authorized rollback rules in v1.
```

**Why it matters:**  
Generations are the mechanism by which the system distinguishes "this is the current truth" from "this is an older truth." If generations can go backwards, a stale disk reinserted after replacement could override current data.

**Must hold:**  
Always. Both in memory (state transitions) and on disk (metadata snapshots).

**Allowed temporary exceptions:**  
None.

**How it is checked:**
- `coordinator.go:WriteFile()`: `oldGeneration = len(state.Transactions)`, `newGeneration = oldGeneration + 1`. This is monotonic.
- `recovery.go:Recover()`: `reconcileCommittedTransaction()` checks that `record.NewGeneration > current_generation` before applying.
- `checkJ2StateTransitions()`: validates that journal record state sequences are monotonic within a transaction.
- `TestG3_GenerationRollback` — stale metadata with old generation; journal with newer generation takes precedence.

**Code paths that can violate it:**
- `coordinator.go:WriteFile()`: if `loadState()` returns a cached state with stale generation.
- `recovery.go:Recover()`: if two parallel recovery calls race and one applies a stale transaction.
- `metadata.Store.Load()`: if it accepts a snapshot with a generation lower than a previously loaded snapshot.

**Tests that enforce it:**
- `TestG3_GenerationRollback` — stale metadata; journal enforces forward generation.
- `TestG8_StaleCheckpointApplied` — stale snapshot; reconciliation brings state forward.
- `TestB5_StaleGenerationDetected` — stale journal record (wrong OldGeneration); rejected.

**Proposed enforcement gap:**  
`commitState()` does not verify that the new state's generation is strictly greater than the current committed generation. Add:

```go
// In commitState(), before calling metadata.Save():
if newState.Pool.Generation <= c.committedGeneration {
    return metadata.SnapshotEnvelope{}, fmt.Errorf(
        "I11: refusing non-monotonic generation: current=%d, new=%d",
        c.committedGeneration, newState.Pool.Generation)
}
c.committedGeneration = newState.Pool.Generation
```

---

## I12 — Single Authoritative Recovery Invariant

**Formal statement:**

```
At recovery:
  let J = durable journal state
  let S = durable metadata snapshot

  exactly_one_of:
    (1) S is valid ∧ S.generation ≥ last_committed_generation(J)
        → accept S as base, replay any J records with generation > S.generation
    (2) S is invalid (checksum fail, torn, wrong magic)
        → use J as sole source of truth; reconstruct state from committed records
    (3) S is valid but generation < last_committed_generation(J)
        → use J to reconcile forward from S
    (4) J is empty ∧ S is invalid
        → refuse startup with explicit error (cannot determine authoritative state)
    (5) J is present but all records are torn/invalid ∧ S is valid
        → accept S; warn that journal is corrupt beyond recovery boundary

In no case may the system start in an undefined or silently-guessed state.
```

**Why it matters:**  
A storage engine must not guess. If it can start with ambiguous state, it may apply writes on top of wrong committed state, making silent corruption permanent.

**Must hold:**  
At startup, import, and post-crash recovery.

**Allowed temporary exceptions:**  
None. During startup reconciliation, status may be "unknown" until resolution completes, but the system must not become operational with unresolved ambiguity.

**How it is checked:**
- `Recover()` in `recovery.go`: implements rules (1)–(3). Rule (4) is partially implemented: `loadState()` falls back to `defaultState` when metadata is missing; rule (4) only fires if the journal is also empty.
- `TestG_SafeRejectionOfAmbiguousMetadata` — garbage metadata + empty journal; safe rejection.
- `TestG_JournalPrecedenceOverCorruptedMetadata` — corrupt metadata + valid journal; journal wins.

**Code paths that can violate it:**
- `RecoverWithState()`: uses `defaultState` as fallback; if both metadata and journal are corrupt, the system starts with a prototype state rather than refusing.
- `commitState()`: if metadata is written but not verified after write, a torn metadata might be silently treated as valid.

**Tests that enforce it:**
- `TestG_SafeRejectionOfAmbiguousMetadata`
- `TestG_JournalPrecedenceOverCorruptedMetadata`
- `TestG1_TornHeader` through `TestG10_VersionMismatch`

**Proposed enforcement gap:**  
`Recover()` currently uses `defaultState` (the pool prototype) when metadata fails to load and the journal is empty. This silently starts a fresh pool, which is correct behavior for the first-ever boot but dangerous if the metadata file exists but is corrupt. The distinction between "no metadata file" (first boot) and "corrupt metadata file" (dangerous) should be explicit:

```go
// In RecoverWithState():
state, metaErr := c.loadState(defaultState)
if metaErr != nil {
    if os.IsNotExist(metaErr) {
        state = defaultState // first boot: acceptable
    } else {
        // I12: metadata exists but is unreadable — do not silently use prototype
        // Check if journal has committed records to reconstruct from
        if journalEmpty {
            return result, fmt.Errorf("I12: metadata corrupt and journal empty; cannot determine authoritative state")
        }
        state = defaultState // use journal as sole source below
    }
}
```

---

## I13 — Read Correctness Invariant

**Formal statement:**

```
∀ read operation R on logical path P:
  let result = ReadFile(P)

  result.ok = true
  → result.data = committed_bytes(P)
    ∧ ∀ extent E in result: blake3(E.bytes) = E.checksum

  result.ok = false
  → result.error ∈ {ErrNotFound, ErrDegraded, ErrCorrupt, ErrIO}
    ∧ result.data is empty or explicitly marked as unverified

In no case: result.ok = true ∧ result.data ≠ committed_bytes(P)
```

**Why it matters:**  
This is the user-visible contract. A storage engine that sometimes returns wrong bytes and claims success has failed its primary job.

**Must hold:**  
On every `ReadFile()` call for a committed path.

**Allowed temporary exceptions:**  
None.

**How it is checked:**
- `ReadFile()` in `reader.go`: calls `verifyExtent()` for every extent, which rejects checksum mismatches.
- `verifyExtent()`: on mismatch, calls `runExtentRepair()`, which reconstructs from parity and verifies the reconstruction before returning.
- If both the extent and parity reconstruction fail: returns an error; does not return the bad bytes.
- `result.Verified` flag in `ReadResult`: set to `true` only after all extents pass verification.

**Code paths that can violate it:**
- `verifyExtent()` with `repair=false`: returns error but does not return bad bytes (correct).
- `verifyExtent()` with `repair=true`: if `runExtentRepair()` returns bad bytes due to a parity corruption that slips through checksum verification.
- `ReadFile()` setting `Verified=true` before all extents are checked — currently only set at the end after the loop (correct).
- FUSE layer `fs.go`: if it passes `ReadFile()` results through without checking `Verified`.

**Tests that enforce it:**
- `TestD2_DataDiskDisappearsAfterWrite` — missing extent; read self-heals, returns correct bytes.
- `TestD10_DiskReadErrors_CorruptExtentFile` — corrupt extent; self-heal returns correct bytes.
- All Category A tests verify data round-trips after recovery.
- `TestA_MultipleCrashedWritesRecoveredTogether` — multiple crashes; committed file readable with correct bytes.

**Proposed enforcement gap:**  
`ReadFile()` returns `Verified: true` but does not assert `len(content) == file.SizeBytes`. A length mismatch would indicate a silent truncation. Add:

```go
// At the end of readFileWithRepairFailAfter(), before returning:
if int64(len(content)) != file.SizeBytes {
    return ReadResult{}, fmt.Errorf("I13: read length mismatch: expected %d bytes, got %d", file.SizeBytes, len(content))
}
```

---

## I14 — Deallocation Safety Invariant

**Formal statement:**

```
∀ committed extent E reachable from any live committed file F:
  ¬reclaimed(E.physical_path)
  ∧ ¬reclaimed(E.parity_group)

∀ reclaim operation:
  E.physical_path may be deleted only if
    ∄ committed file F: E ∈ extents(F)
    ∧ E.state = "deleted" in committed metadata
```

**Why it matters:**  
A use-after-free in a storage engine is not a segfault — it is silent data corruption. If an extent file is deleted while reachable, future reads see wrong data or errors. Future writes to the same path would produce a stale checksum mismatch.

**Must hold:**  
Always. For all physical extent paths and parity group paths.

**Allowed temporary exceptions:**  
Unreachable extents (not referenced by any committed file) may be garbage pending collection, but they must not be collected while any committed state references them.

**How it is checked:**
- v1 has no delete/overwrite/truncate path (`OverwriteFile()`, `TruncateFile()`, `GrowFile()` all return `ErrNotSupported`). Deallocation is therefore impossible in v1, making this invariant trivially satisfied.
- `checkM1FileIDsExist()` — verifies all extents reference existing files; catches dangling extents.
- Future enforcement: any `DeleteFile()` implementation must first update metadata to remove the file and all its extents (committed), then — and only then — delete the physical files.

**Code paths that can violate it:**
- Does not exist in v1 (append-only model).
- Future `DeleteFile()`: if it deletes physical files before committing the deletion to metadata and journal.
- Future GC/compaction: if it reclaims extent files based on stale reachability information.

**Tests that enforce it:**
- `TestOverwriteFileReturnsNotSupported` — confirms no overwrite path exists in v1.
- `TestTruncateFileReturnsNotSupported`
- `TestGrowFileReturnsNotSupported`
- No deallocation-specific tests needed in v1; required for v2.

**Proposed enforcement gap:**  
None in v1. For v2, document the required atomic deletion protocol:

```
// V2 deletion protocol (I14 compliant):
// 1. Journal: append DeletePrepare record with file/extent IDs.
// 2. Metadata: update file state to "deleting" (atomic commit).
// 3. Physical: delete extent files and parity files.
// 4. Journal: append DeleteCommit record.
// 5. Metadata: remove file and extent records (atomic commit).
// At no point may step 3 precede step 2.
```

---

## I15 — Observability Truth Invariant

**Formal statement:**

```
∀ operator-visible status field F ∈ {pool_health, journal_clean, scrub_result, rebuild_progress, disk_status}:

  F.value = "healthy" → ¬∃ violation V: severity(V) > "degraded"
  F.value = "degraded" → ∃ known failure and recovery path
  F.value = "unknown" → system is in startup reconciliation
  F.value = "healthy" is FORBIDDEN if any of:
    • CheckIntegrityInvariants() returns violations
    • journal has replay_required transactions
    • any disk status is "failed"
    • any extent is known-corrupt without a repair path
```

**Why it matters:**  
Operators make recovery decisions based on what the system reports. A system that reports "healthy" when it is degraded causes operators to delay recovery until data loss becomes irreversible.

**Must hold:**  
Always. After startup, during operation, after scrub, after rebuild.

**Allowed temporary exceptions:**  
During startup reconciliation, `F.value` may be "unknown." It must not be "healthy" until reconciliation completes successfully.

**How it is checked:**
- `ScrubResult.HasCorruption` and `ScrubResult.RepairedExtents` in `scrub.go` — accurately reflects scrub findings.
- `MultiDiskFailureAnalysis.RecoveryIsPossible` in `multidisk_failure.go` — accurately reflects recoverability.
- `FormatValidationResult.IsHealthy` in `format_validation.go` — only `true` if all format checks pass.
- `InvariantViolation` from `CheckIntegrityInvariants()` — surfaces all known violations.

**Code paths that can violate it:**
- `scrubWithRepairFailAfter()` in `scrub.go`: if `ScrubResult.HasCorruption` is not set when a corruption is detected.
- `AnalyzeMultiDiskFailures()` in `multidisk_failure.go`: if `RecoveryIsPossible = true` when it should be `false` for a multi-disk failure.
- `FormatValidationResult.IsHealthy` in `format_validation.go`: if set to `true` before all validators run.
- FUSE/HTTP status endpoint: if it reads from cache that has not been invalidated after an integrity check failure.

**Tests that enforce it:**
- `TestFormatValidationDetectsCorruptMetadata` — `IsHealthy = false` when metadata is corrupt.
- `TestFormatValidationDetectsCorruptExtent` — `IsHealthy = false` when extent corrupt.
- `TestD9_TwoDisksFail_Unrecoverable` — `RecoveryIsPossible = false` for two-disk failure.
- `TestG_SafeRejectionOfAmbiguousMetadata` — ambiguous state; system does not report healthy.

**Proposed enforcement gap:**  
`Scrub()` does not persist its result to the journal. A crash between scrub completion and result reporting would lose the scrub result. The next `GetScrubStatus()` call would return "unknown" instead of the last scrub result. Minimum implementation:

```go
// In scrubWithRepairFailAfter(), after scrub loop completes:
if err := logScrubComplete(c.journal, state.Pool.Name, runID); err != nil {
    // non-fatal: scrub completed, result just not durably logged
    // but status should reflect this uncertainty
    result.StatusCertain = false
}
```

---

## Invariant-to-Code Mapping Table

| Invariant | Checker Function | Source File | Called By |
|-----------|-----------------|-------------|-----------|
| I1 (Transaction Visibility) | `CheckJournalInvariants`, `CheckIntegrityInvariants` | `invariants.go` | `assertInvariantsClean`, `Recover` |
| I2 (No Uncommitted Exposure) | Implicit: `cachedState` gate | `coordinator.go` | All reads via `loadState` |
| I3 (Data Checksum Integrity) | `checkE1ExtentChecksums` (E1) | `invariants.go` | `CheckIntegrityInvariants`, `Scrub` |
| I4 (Parity Coverage) | `checkP2ParityChecksums` (P2), `checkP3ParityXOR` (P3) | `invariants.go` | `CheckIntegrityInvariants`, `Scrub` |
| I5 (Metadata Truthfulness) | `checkM1`, `checkM2`, `checkM3`, `checkP1`, `checkE2`, `checkE3` | `invariants.go` | `CheckStateInvariants` |
| I6 (Replay Idempotence) | `reconcileCommittedTransaction`, generation checks | `recovery.go` | `Recover` |
| I7 (Torn Record Detectability) | `validateRecord`, `checkJ1RecordChecksums` (J1) | `store.go`, `invariants.go` | `Store.Load`, `CheckJournalInvariants` |
| I8 (Rebuild Correctness) | `reconstructExtent` checksum verify | `reader.go` | `rebuildDiskFromState` |
| I9 (Scrub Safety) | `verifyExtent`, `runExtentRepair` checksum | `reader.go`, `repair.go` | `Scrub`, `ReadFile` |
| I10 (Disk Identity) | `AddDisk` duplicate check, `AnalyzeMultiDiskFailures` | `disk_lifecycle.go`, `multidisk_failure.go` | `AddDisk`, `ReplaceDisk`, `Recover` |
| I11 (Monotonic Generation) | `reconcileCommittedTransaction` generation check | `recovery.go` | `Recover` |
| I12 (Single Authoritative Recovery) | `Recover`, `reconcileCommittedTransaction` | `recovery.go` | Startup |
| I13 (Read Correctness) | `verifyExtent`, `ReadFile` length check (proposed) | `reader.go` | `ReadFile` |
| I14 (Deallocation Safety) | N/A — append-only in v1 | — | `ErrNotSupported` stubs |
| I15 (Observability Truth) | `ScrubResult`, `MultiDiskFailureAnalysis`, `FormatValidationResult` | `scrub.go`, `multidisk_failure.go`, `format_validation.go` | Scrub, status endpoints |

---

## Enforcement Gaps Summary

| Gap | Invariant | Severity | Proposed Fix |
|-----|-----------|----------|--------------|
| `commitState()` does not call `CheckStateInvariants()` | I5 | HIGH | Add guard at start of `commitState()` |
| `runExtentRepair()` has no post-write readback | I3, I9 | HIGH | Read-back and verify after `replaceSyncFile` |
| `mergeRecoveredFile()` may upsert on second recovery | I6 | MEDIUM | Already satisfied in current code; keep regression test coverage |
| `rebuildDiskFromState()` has no post-rebuild `CheckIntegrityInvariants` | I8 | MEDIUM | Implemented: full integrity check now runs at end of successful rebuild |
| `Recover()` does not distinguish corrupt vs. missing metadata | I12 | MEDIUM | Implemented: explicit error on corrupt-metadata + empty-journal |
| `ReadFile()` does not verify `len(result) == file.SizeBytes` | I13 | MEDIUM | Implemented: length assertion before return |
| `commitState()` does not enforce monotonic generation | I11 | MEDIUM | Implemented: generation monotonicity enforced from transaction count |
| `Scrub()` result not durably journaled | I15 | LOW | Remaining gap: log scrub completion durably |
| No hardware UUID validation for disk identity | I10 | LOW (v1) | Remaining v1 limitation; implement in v2 |

---

## Required Tests (Gap Coverage)

| Test | Invariant | Status |
|------|-----------|--------|
| `TestI5_CommitStateRejectsDuplicateExtentID` / `TestI5_CommitStateRejectsDanglingFileRef` | I5 | Implemented |
| `TestI3_RepairReadbackVerifiesSunnyDay` | I3, I9 | Implemented |
| `TestI6_MergeRecoveredFileIsUpsertNotAppend` | I6 | Implemented |
| `TestI8_RebuildProducesVerifiedExtents` | I8 | Implemented |
| `TestI12_CorruptMetadataAndEmptyJournalRefusesStart` | I12 | Implemented |
| `TestI13_ReadReturnsExactlyCommittedBytes` | I13 | Implemented |
| `TestI11_CommitStateRejectsNonMonotonicGeneration` | I11 | Implemented |
| All Category A–G tests (existing) | I1–I4, I6, I7, I10–I13, I15 | Passing in prior full-suite validation |

---

*End of rtparityd Formal Invariants Specification v1.0*
