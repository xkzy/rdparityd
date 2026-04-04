# rdparityd Storage Invariants

This document defines the invariants that **must always hold** at every stable
point in the system's lifecycle. A "stable point" is any moment when no
transaction is in flight — immediately after a commit, after crash recovery, or
after a scrub or rebuild completes.

Invariants are grouped by the layer they protect. Each invariant states what
must be true, what happens if it is violated, and how the violation is detected
and repaired.

The Go invariant checker (`internal/journal/invariants.go`) enforces every
invariant listed here and returns structured `InvariantViolation` values that
identify which invariant was broken and which object caused the failure.

---

## Group E — Extent Integrity

### E1 — Extent checksum matches on-disk data

> Every extent that carries a non-empty `checksum` field must have on-disk
> bytes whose SHA-256 hash equals that checksum, truncated or padded to the
> extent's declared `length`.

**Why it matters:**
An extent checksum is the primary guard against silent data corruption. If E1
is violated the system is serving corrupted data.

**What happens on crash:**
If a power loss occurs after the data file is written but before the metadata
snapshot is updated the extent has correct on-disk bytes but no checksum in
metadata. Recovery rolls the transaction forward, recomputes the checksum, and
persists the metadata. E1 is only evaluated against metadata-visible extents,
so a pre-metadata crash does not produce an E1 violation — it produces an
incomplete transaction handled by the journal.

**Can parity become inconsistent?**
Not from an E1 violation alone. Parity and data are written in separate steps.
If the data file is corrupted after both steps succeed, E1 fires and P3 will
also fire because the parity no longer matches the (now corrupt) data. Scrub
with repair heals both.

**Detection:** `CheckIntegrityInvariants` reads each extent file and recomputes
the SHA-256. O(total extent data).

**Repair:** `Coordinator.Scrub(repair=true)` reconstructs the extent from
parity and rewrites the file.

---

### E2 — Extent checksum algorithm is sha256

> Every extent with a non-empty `checksum` must have `checksum_alg` equal to
> `"sha256"`.

**Why it matters:**
A mismatch between the recorded algorithm and the actual algorithm used would
silently invalidate every checksum comparison. Having a single required value
ensures the checker is unambiguous.

**Detection:** `CheckStateInvariants` — pure metadata scan, no IO.

**Repair:** Manual correction. Mixed algorithms indicate a software bug.

---

### E3 — Extent length is positive

> Every extent must have `length > 0`.

**Why it matters:**
A zero-length extent is logically meaningless and would make parity padding
logic ambiguous. The allocator enforces this at allocation time.

**Detection:** `CheckStateInvariants` — pure metadata scan.

**Repair:** The extent should be removed or re-allocated.

---

## Group M — Metadata Consistency

### M1 — Extent FileID references an existing file

> Every extent's `file_id` must correspond to an entry in the pool's `files`
> list.

**Why it matters:**
An orphaned extent (pointing at a deleted or nonexistent file) indicates a
metadata corruption or an incomplete delete operation. The scrub tool has no way
to correctly categorise such an extent.

**Detection:** `CheckStateInvariants` — in-memory cross-reference.

**Repair:** Remove the orphaned extent and its parity contribution.

---

### M2 — Extent DataDiskID references an existing disk

> Every extent's `data_disk_id` must correspond to a disk in the pool's `disks`
> list.

**Why it matters:**
Rebuild and scrub locate extent files by deriving a path relative to the disk's
mountpoint. An unknown disk ID makes the extent unlocatable.

**Detection:** `CheckStateInvariants` — in-memory cross-reference.

**Repair:** Mark the disk as failed and trigger a rebuild from parity.

---

### M3 — Extent IDs are unique

> No two extents may share the same `extent_id`.

**Why it matters:**
The allocator generates IDs sequentially. Duplicates indicate a replay anomaly
or a code defect in the allocator.

**Detection:** `CheckStateInvariants` — in-memory deduplication.

**Repair:** Remove the duplicate; if both are distinct objects one of them
represents a ghost that must be tracked down through the journal.

---

## Group P — Parity Consistency

### P1 — Every extent's parity group exists in metadata

> For each extent that declares a `parity_group_id`, a matching entry must
> exist in the pool's `parity_groups` list. Conversely, every extent ID listed
> in a parity group's `member_extent_ids` must exist in the pool's `extents`
> list.

**Why it matters:**
The parity group is the unit of rebuild. A dangling reference means a missing
or corrupt extent cannot be reconstructed.

**Detection:** `CheckStateInvariants` — in-memory cross-reference (both
directions).

**Repair:** Regenerate the parity group entry from the extents that reference
it, then recompute the parity file.

---

### P2 — Parity group checksum matches on-disk parity data

> Every parity group with a non-empty `parity_checksum` must have an on-disk
> parity file whose SHA-256 equals that checksum.

**Why it matters:**
The parity checksum is the tamper-evident seal on the parity data. If the
parity file is corrupt, reconstructing a failed extent will silently produce
wrong bytes — a far worse outcome than a read error.

**Detection:** `CheckIntegrityInvariants` reads each parity file and
recomputes its SHA-256.

**Repair:** `Coordinator.Scrub(repair=true)` regenerates the parity file from
member extents and updates the checksum in metadata.

---

### P3 — Parity equals XOR of all member extents

> For each parity group, the XOR of all member extent bytes (zero-padded to the
> length of the longest member) must exactly equal the parity file bytes.

**Why it matters:**
P3 is the mathematical guarantee underlying rebuild. If P3 is violated,
reconstructing a failed extent from parity will produce corrupted data, even if
the reconstructed extent passes its own checksum check (it would not, but it is
important that this invariant is independent).

**Crash behaviour:**
A crash between writing the data file and writing the parity file leaves the
parity stale (reflecting the previous generation of data). The journal records
the write as `data-written`. Recovery rolls forward by recomputing and
rewriting the parity before committing. Therefore P3 can only be false during
an in-flight transaction, never at a stable point.

**Can parity become inconsistent without a journal entry?**
Not in the write path — every data write is preceded by a journal record
(`prepared`) and parity is written before the commit record. However, if a
data extent is corrupted in-place after commit (hardware bitrot), P3 will fire
at the next scrub because the live data no longer XORs to parity. Scrub with
repair rewrites the parity from the (now-corrupted) extents; the separate E1
check will detect and heal the corrupt extent first.

**Detection:** `CheckIntegrityInvariants` reads every member extent file and
parity file and recomputes the XOR. O(group_width × extent_size).

**Repair:** Rerun `Coordinator.Scrub(repair=true)` after healing extents first
(E1 check order matters).

---

### P4 — No two extents in the same parity group are on the same data disk

> Within any single parity group, all member extents must reside on distinct
> data disks.

**Why it matters:**
The XOR rebuild scheme can recover from the loss of exactly one disk per group.
If two extents share a disk, losing that disk destroys two unknowns but the
parity provides only one equation — unrecoverable.

**Detection:** `CheckStateInvariants` — in-memory scan of parity group members.

**Repair:** Move one of the two co-located extents to a different disk and
recompute parity.

---

## Group J — Journal Consistency

### J1 — Journal records have valid checksums

> Every record written to the journal must have a `record_checksum` that matches
> the SHA-256 of the record envelope (all fields except `record_checksum`
> itself). And a `payload_checksum` that matches the SHA-256 of the
> transaction-payload fields.

**Why it matters:**
J1 makes the journal tamper-evident. A corrupted journal record cannot be
silently replayed — the replay engine will reject it and the operator must
intervene.

**Detection:** `CheckJournalInvariants` — re-derives both checksums for every
loaded record.

**Repair:** Truncate or skip the corrupt tail of the journal. The preceding
committed state remains valid.

---

### J2 — Journal state transitions are valid

> For each transaction in the journal, the sequence of state values must follow
> the allowed transitions defined in the state machine:
>
> ```
> prepared → data-written | aborted | replay-required
> data-written → parity-written | aborted | replay-required
> parity-written → metadata-written | replay-required
> metadata-written → committed | replay-required
> replay-required → committed | aborted
> ```

**Why it matters:**
An invalid transition indicates either a software bug or journal corruption.
Replaying an invalid sequence could apply partial state.

**Detection:** `CheckJournalInvariants` — validates each per-transaction
sequence using `ValidateSequence`.

**Repair:** Abort the transaction. Data written in partial stages is
rolled back; any extent files written before the abort are orphaned until the
next scrub.

---

### J3 — No committed transaction has replay-required flag set

> After crash recovery completes, no entry in `state.Transactions` may have
> `replay_required == true`. This invariant is checked at stable points
> post-recovery, not mid-transaction.

**Why it matters:**
The `replay_required` flag is set during crash recovery to mark transactions
that need forward-rolling. It must be cleared before the system is considered
healthy. A lingering flag indicates recovery did not complete successfully.

**Detection:** `CheckStateInvariants` scans `state.Transactions`.

**Repair:** Re-run crash recovery (`Coordinator.Recover`).

---

## Invariant verification ordering

When running a full integrity check, the invariants should be evaluated in this
order to avoid masking root causes with cascading failures:

1. **J1** — Validate journal tamper-evidence first.
2. **J2** — Validate journal state machine.
3. **M3** — Ensure unique extent IDs (prevents confusion in subsequent checks).
4. **M1**, **M2** — Ensure extent cross-references are valid.
5. **E2**, **E3** — Validate extent structural properties.
6. **P1** — Validate parity group cross-references.
7. **P4** — Validate parity group disk-separation guarantee.
8. **J3** — Check no replay-required flag lingers.
9. **E1** — Check on-disk extent data integrity (requires IO).
10. **P2** — Check on-disk parity checksum (requires IO).
11. **P3** — Verify parity XOR correctness (requires IO, most expensive).

The Go checker follows this order in `CheckIntegrityInvariants`.

---

## Summary table

| Code | Layer    | Requires IO | Crash-safe? | Repair action                     |
|------|----------|-------------|-------------|-----------------------------------|
| E1   | Extent   | Yes         | Yes         | Rebuild from parity               |
| E2   | Extent   | No          | Yes         | Software fix                      |
| E3   | Extent   | No          | Yes         | Remove/reallocate extent          |
| M1   | Metadata | No          | Yes         | Remove orphaned extent            |
| M2   | Metadata | No          | Yes         | Mark disk failed, rebuild         |
| M3   | Metadata | No          | Yes         | Remove duplicate                  |
| P1   | Parity   | No          | Yes         | Regenerate parity group entry     |
| P2   | Parity   | Yes         | Yes         | Recompute parity file             |
| P3   | Parity   | Yes         | Yes         | Recompute parity file             |
| P4   | Parity   | No          | Yes         | Move extent, recompute parity     |
| J1   | Journal  | No          | Yes         | Truncate corrupt tail             |
| J2   | Journal  | No          | Yes         | Abort transaction                 |
| J3   | State    | No          | Yes         | Re-run recovery                   |
