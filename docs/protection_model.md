# rtparityd Protection Model Design

## Overview

rtparityd supports a **gradual protection model** that begins with corruption detection on day 1 and upgrades to corruption repair as disks are added.

| Disks | Mode | Detection | Repair |
|-------|------|-----------|--------|
| 1 | Integrity-Only | ✅ Checksums | ❌ External backup required |
| 2 | Mirror-Ready | ✅ Checksums | ⚠️ From mirror (if configured) |
| 3+ | Parity-Protected | ✅ Checksums | ✅ From parity |

---

## Core Concepts

### 1. Pool Protection State

```go
type PoolProtectionState int

const (
    ProtectionIntegrityOnly PoolProtectionState = iota  // 1 disk, checksums only
    ProtectionMirrored                                  // 2+ disks, mirrored extents
    ProtectionParity                                     // 3+ disks, parity-protected
)
```

### 2. Extent Protection Class

Each extent has a protection class that determines how it's protected:

```go
type ExtentProtectionClass int

const (
    ExtentChecksumOnly ExtentProtectionClass = iota  // Checksum only, no redundancy
    ExtentMirrored                                  // Stored on 2 disks
    ExtentParity                                    // Protected by XOR parity
)
```

### 3. Protection Policy

```go
type ProtectionPolicy struct {
    DefaultClass     ExtentProtectionClass
    CriticalFiles   map[string]ExtentProtectionClass  // Override for specific paths
    MinDisksForParity int                            // Default: 3
}
```

---

## Phase 1: Single-Disk Integrity Mode

### What Works

| Feature | Status | Implementation |
|---------|--------|---------------|
| Extent checksums | ✅ | BLAKE3-256 per extent |
| Metadata checksums | ✅ | BLAKE3-256 for metadata snapshot |
| Journal checksums | ✅ | BLAKE3-256 per journal record |
| Scrub | ✅ | Verify all checksums, mark corrupt |
| Read verification | ✅ | Verify on every read |
| Corruption alarm | ✅ | Fail-fast, don't return bad data |
| Self-heal | ❌ | No redundancy to heal from |

### Behavior in 1-Disk Mode

**Write:**
```
1. Write extent data to disk
2. Compute BLAKE3-256 checksum
3. Store checksum in metadata
4. Fsync + journal append
```

**Read:**
```
1. Read extent data
2. Compute checksum
3. Compare with stored checksum
4. If mismatch → return error (do NOT return corrupt data)
5. If corrupt → log alarm, mark extent as corrupted
```

**Scrub:**
```
1. Read each extent
2. Verify checksum
3. If mismatch → log corruption, mark extent, continue scrub
4. Report summary of corrupt extents
```

### What Happens When Corruption Is Detected

```go
type CorruptionEvent struct {
    ExtentID      string
    DiskID        string
    DetectedAt   time.Time
   ChecksumExpected string
    ChecksumActual   string
    Status        string  // "detected" | "quarantined"
}

// On detection:
// 1. Log ERROR with details
// 2. Mark extent as corrupted in metadata
// 3. Alert via status endpoint
// 4. DO NOT serve corrupt data to caller
// 5. DO NOT overwrite corrupt data during scrub
```

### Limitation Acknowledgment

With 1 disk, rtparityd **cannot automatically repair** corruption. The user must:
- Restore from external backup
- Manually replace corrupted extent
- Accept data loss if no backup exists

This is explicitly documented and is the honest trade-off.

---

## Phase 2: Two-Disk Mirror Mode

### Transition to Mirror Mode

When a second disk is added:

```go
func (c *Coordinator) EnableMirroring(diskID string, mirrorPolicy MirrorPolicy) error {
    // 1. Mark new disk as mirror-capable
    // 2. For each existing extent, optionally mirror critical ones
    // 3. New writes can now use ExtentMirrored class
}
```

### Mirror Policy Options

```go
type MirrorPolicy int

const (
    MirrorAll MirrorPolicy = iota   // Mirror every new extent
    MirrorCritical                // Only mirror files matching critical path pattern
    MirrorManual                  // User specifies which files to mirror
)
```

### Mirror Behavior

**Write (mirrored extent):**
```
1. Write primary copy to Disk A
2. Write mirror copy to Disk B
3. Compute checksum for each
4. Store both checksums in metadata
```

**Read (mirrored extent):**
```
1. Read primary copy
2. Verify checksum
3. If corrupt, read mirror copy
4. Verify mirror checksum
5. If mirror OK → heal primary, return data
6. If both corrupt → return error
```

**Scrub (mirrored extent):**
```
1. Read both copies
2. Verify both checksums
3. If one corrupt, heal from the other
4. If both corrupt → mark as unrecoverable
```

---

## Phase 3: Three-Disk Parity Mode

### Transition to Parity Mode

When a third disk is added and user enables parity:

```go
func (c *Coordinator) EnableParityProtection(minDisksForParity int) error {
    // 1. Compute initial parity for existing extents
    // 2. New extents default to ExtentParity class
    // 3. Rebuild process for existing extents (optional)
}
```

### Parity Behavior

**Write (parity extent):**
```
1. Write extent to data disk
2. Compute XOR with other members in parity group
3. Write parity to parity disk
4. Fsync all
```

**Read (parity extent):**
```
1. Read data extent
2. Verify checksum
3. If corrupt → read other members + parity
4. Reconstruct missing data
5. Heal and return
```

**Scrub (parity extent):**
```
1. Verify all members
2. Verify parity
3. If member corrupt → reconstruct from others
4. Write healed member
5. Verify healed checksum
```

---

## Protection Class Transitions

### Per-Extent Upgrade Path

```go
// Extent can be upgraded from CHECKSUM_ONLY to MIRRORED or PARITY
func (c *Coordinator) UpgradeExtentProtection(extentID string, targetClass ExtentProtectionClass) error {
    switch targetClass {
    case ExtentMirrored:
        return c.mirrorExtent(extentID)
    case ExtentParity:
        return c.computeExtentParity(extentID)
    }
}

// Example lifecycle:
// 1. Create file when pool has 1 disk → ExtentChecksumOnly
// 2. Add second disk → optionally mirror critical files
// 3. Add third disk → enable parity, migrate important extents
```

### Whole-Pool Upgrade

```go
type PoolUpgradePath struct {
    From PoolProtectionState
    To   PoolProtectionState
    Steps []string
}

var UpgradePaths = []PoolUpgradePath{
    {
        From: ProtectionIntegrityOnly,
        To:   ProtectionMirrored,
        Steps: []string{
            "1. Add second disk to pool",
            "2. Set mirror policy (all/critical/manual)",
            "3. rtparityd will mirror new writes",
            "4. Optionally: rtparityd migrate critical existing extents",
        },
    },
    {
        From: ProtectionMirrored,
        To:   ProtectionParity,
        Steps: []string{
            "1. Add third disk to pool",
            "2. Enable parity protection",
            "3. rtparityd computes parity for existing extents",
            "4. New writes use parity",
        },
    },
}
```

---

## Metadata Changes

### Extended Extent Record

```go
type Extent struct {
    ExtentID            string
    FileID              string
    DataDiskID          string  // Primary disk
    MirrorDiskID        string  // For mirrored extents (empty if not mirrored)
    ParityGroupID       string  // For parity extents
    LogicalOffset       int64
    Length             int64
    Checksum           string
    CompressionAlg     CompressionAlgorithm
    CompressedSize     int64
    ChecksumAlg        string
    
    // New fields
    ProtectionClass    ExtentProtectionClass
    MirrorChecksum     string  // For mirrored extents
    CorruptionStatus  string  // "ok" | "corrupt" | "healing" | "quarantined"
}
```

### Disk State Extension

```go
type Disk struct {
    DiskID         string
    Role           string  // "data" | "parity" | "mirror"
    ProtectionClass string  // "checksum" | "mirror" | "parity"
    HealthStatus   string  // "online" | "degraded" | "offline"
    
    // For mirror/disk scenarios
    MirrorOfDisk   string  // If this disk is a mirror of another
}
```

---

## Scrub Behavior by Protection State

### 1-Disk (Integrity-Only)

```
Scrub Phase 1: Verify all extents
  - Read each extent
  - Compute checksum
  - Compare with stored checksum
  - If mismatch → log ERROR, mark extent as corrupt
  - Continue scrubbing remaining extents

Scrub Phase 2: Report
  - Summary of corrupt extents
  - Action required: restore from external backup
```

### 2-Disks (Mirrored)

```
Scrub Phase 1: Verify primary copies
  - Same as 1-disk mode

Scrub Phase 2: Verify mirrors (for corrupted primary)
  - Read mirror copy
  - If mirror OK → schedule heal
  - If mirror also corrupt → mark as unrecoverable

Scrub Phase 3: Heal
  - Copy mirror to primary
  - Verify healed extent
```

### 3+ Disks (Parity)

```
Scrub Phase 1: Verify data members
  - Read each member
  - Verify checksum

Scrub Phase 2: Verify parity
  - Read parity extent
  - Verify checksum

Scrub Phase 3: Detect corruption
  - XOR all members + parity = should equal 0
  - If not zero → identify corrupt member

Scrub Phase 4: Reconstruct
  - For each corrupt member:
    - Read other members
    - Read parity
    - Reconstruct corrupt data
    - Write healed data
    - Verify healed checksum
```

---

## API Changes

### New Coordinator Methods

```go
// Protection state queries
func (c *Coordinator) ProtectionState() PoolProtectionState
func (c *Coordinator) ExtentProtectionClass(extentID string) ExtentProtectionClass
func (c *Coordinator) SetExtentProtection(extentID string, class ExtentProtectionClass) error

// Disk management
func (c *Coordinator) AddDisk(diskID string, role string) error
func (c *Coordinator) SetDiskRole(diskID string, role string) error  // "data", "mirror", "parity"

// Mirror operations
func (c *Coordinator) EnableMirroring(policy MirrorPolicy) error
func (c *Coordinator) MirrorExtent(extentID string) error
func (c *Coordinator) MirrorFile(logicalPath string) error

// Parity operations
func (c *Coordinator) EnableParity(minDisks int) error
func (c *Coordinator) ComputeParityForExtent(extentID string) error

// Status
type PoolProtectionStatus struct {
    State            PoolProtectionState
    DiskCount       int
    ExtentCounts    map[ExtentProtectionClass]int
    CorruptExtents  int
    Unrecoverable  int
}
func (c *Coordinator) ProtectionStatus() PoolProtectionStatus
```

### CLI Extensions

```bash
# View protection status
rtpctl protection-status

# Add disk and configure role
rtpctl add-disk -disk-id disk-02 -role mirror
rtpctl add-disk -disk-id disk-03 -role parity

# Set protection for file
rtpctl set-protection -path /important/file.bin -class mirrored

# Migrate existing file to new protection
rtpctl migrate-protection -path /file.bin -target parity

# Enable parity for pool
rtpctl enable-parity -min-disks 3
```

---

## Corrupt Extent Handling

### Detection Flow

```
1. Read-time verification fails
   OR Scrub detects checksum mismatch
   
2. Classify extent:
   - CHECKSUM_ONLY: Mark corrupt, alert, do NOT serve data
   - MIRRORED: Try mirror copy
   - PARITY: Try reconstruction
   
3. Recovery attempt:
   - Success → heal extent, log healing event
   - Failure → mark unrecoverable, alert user
   
4. User action required for unrecoverable:
   - Restore from backup
   - Accept data loss
```

### Quarantine Behavior

```go
type QuarantinePolicy int

const (
    QuarantineImmediate QuarantinePolicy = iota  // Move corrupt extent aside immediately
    QuarantineOnRead                             // Only quarantine if read attempted
)

func (c *Coordinator) SetQuarantinePolicy(policy QuarantinePolicy)
```

---

## Configuration

### Pool Configuration Extension

```yaml
# rtparityd.yaml
pool:
  name: mypool
  
protection:
  # Default protection class for new extents
  default_class: checksum_only
  
  # When to automatically upgrade protection
  auto_upgrade:
    enabled: true
    min_disks: 2
    policy: critical_only  # or "all"
    
  # Critical path patterns (for auto_upgrade)
  critical_paths:
    - /important/*
    - /vm-images/*
    
  # Scrub settings
  scrub:
    interval_hours: 168  # Weekly
    verify_parity: true
    heal_on_scrub: true
    
  # Quarantine policy
  quarantine: immediate  # or "on_read"
```

---

## Invariants

### IP1: Extent Protection Class

```go
// An extent's protection class determines its repair capability:
// - CHECKSUM_ONLY: Can detect corruption, cannot repair
// - MIRRORED: Can detect and repair from mirror
// - PARITY: Can detect and repair from parity
```

### IP2: Protection Class Compatibility

```go
// An extent cannot have MirrorDiskID without being MIRRORED
// An extent cannot have ParityGroupID without being PARITY
// These are set atomically during allocation
```

### IP3: Repair Requires Redundancy

```go
// Repair (as opposed to detection) requires:
// - For CHECKSUM_ONLY: external backup or manual intervention
// - For MIRRORED: at least one good mirror copy
// - For PARITY: at least (N-1) good data members + parity
```

### IP4: Scrub Does Not Block Reads

```go
// Scrub runs in background and:
// - Does not block concurrent reads
// - Does not modify extent during verification
// - Only heals after verification complete
// - Reports heal events to user
```

---

## User Experience

### Starting with 1 Disk

```
$ rtparityd init --disks /dev/disk1
Pool created with 1 disk in integrity-only mode.
Corruption detection enabled. Repair requires external backup.

$ rtparityd write /important/file.bin
Writing file.extent-001 (checksum-protected only)
Write complete.

$ rtparityd scrub
Scrubbing pool...
Found 0 corrupt extents.
Protection status: 1 disk, integrity-only mode
```

### Detecting Corruption

```
$ rtparityd scrub
Scrubbing pool...
[ERROR] Extent extent-001 checksum mismatch
  Expected: abc123...
  Actual:   def456...
  Status: CORRUPT
Corrupt extent quarantined.
Action required: Restore /important/file.bin from backup.

Total: 1 corrupt, 0 recoverable, 1 unrecoverable
```

### Adding Second Disk

```
$ rtparityd add-disk /dev/disk2 --role mirror
Disk disk-02 added as mirror disk.
Pool now in mirrored-ready mode.

$ rtparityd migrate-protection --path /important/* --target mirrored
Migrating /important/file.bin to mirrored protection...
Mirroring extent-001...
Done.
```

### Adding Third Disk for Parity

```
$ rtparityd add-disk /dev/disk3 --role parity
Disk disk-03 added as parity disk.
Pool now in parity-protected mode.

$ rtparityd enable-parity --min-disks 3
Computing parity for existing extents...
Parity enabled.
Corruption repair now available.
```

---

## Comparison with Unraid

| Feature | Unraid (1 disk) | rtparityd (1 disk) |
|---------|-----------------|-------------------|
| Filesystem | ext4 | custom |
| Checksums | ❌ | ✅ |
| Metadata checksums | ❌ | ✅ |
| Journal | ❌ | ✅ |
| Scrub | ❌ | ✅ |
| Corruption detection | ❌ | ✅ |
| Corruption repair | ❌ | ❌ (needs 2+ disks) |

With 2+ disks, rtparityd gains repair capability.

---

## Summary

| Phase | Disks | Mode | Detect | Repair |
|-------|-------|------|--------|--------|
| 1 | 1 | Integrity-Only | ✅ | ❌ |
| 2 | 2 | Mirror-Ready | ✅ | ⚠️ (from mirror) |
| 3 | 3+ | Parity-Protected | ✅ | ✅ |

**Key insight**: rtparityd is valuable from day 1 with 1 disk for corruption detection. The repair capability is a future investment that unlocks as more disks are added.
