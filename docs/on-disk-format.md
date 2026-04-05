# On-disk Metadata Format (v1 - Production-Ready)

## Overview

All persistence uses binary formats with explicit checksums:
- **Magic identifiers**: `RTPJ` (journal), `RTPM` (metadata), `RBLD`/`SCRB` (rebuild/scrub progress)
- **Checksum**: BLAKE3-256 everywhere (consistent across journal, metadata, parity, extents)
- **Encoding**: Big-endian for all integer fields
- **Atomic operations**: All writes use atomic replace (write tmp → fsync → rename → syncDir)

## Journal format

Magic: `RTPJ`, Version: 1

Header (72 bytes):
```
[0:4]   Magic       [4]byte   "RTPJ"
[4:6]   Version     uint16    1
[6]     StateCode   uint8     see stateToCode/codeToState tables
[7]     Flags       uint8     0 (reserved)
[8:16]  Timestamp   int64     UnixNano, UTC
[16:24] OldGen      int64
[24:32] NewGen      int64
[32:36] PayloadLen  uint32    byte length of variable payload section
[36:40] Reserved    uint32    0
[40:72] RecordHash  [32]byte  BLAKE3-256 of header[0:40] + payload
```

Payload (variable):
- `TxID`: uint16 length-prefixed byte slice
- `PoolName`: uint16 length-prefixed byte slice
- `LogicalPath`: uint16 length-prefixed byte slice
- `File`: uint8 present flag + optional FileRecord binary encoding
- `Extents`: uint16 count + repeated Extent binary encodings
- `AffectedExtentIDs`: uint16 count + repeated uint16-length-prefixed strings

Torn-write detection: if fewer than PayloadLen bytes can be read after the length prefix, the record is silently dropped.

## Metadata snapshot format

Magic: `RTPM`, Version: 1

Header (72 bytes):
```
[0:4]   Magic       [4]byte   "RTPM"
[4:6]   Version     uint16    1
[6:8]   Reserved    uint16    0
[8:16]  SavedAt     int64     UnixNano, UTC
[16:24] Generation  uint64    0 (reserved for future WAL integration)
[24:28] PayloadLen  uint32    byte length of encoded SampleState
[28:32] Reserved    uint32    0
[32:40] Reserved    [8]byte   0
[40:72] StateHash   [32]byte  BLAKE3-256(payload)
```

Payload (variable):
Binary encoding of SampleState — see `metadata.encodeState()` / `decodeState()`.

Atomic replace: Write to `<path>.tmp` → `fsync` → `rename` → `syncDir()`.

## Extent file format

Location: `data/<hash2>/<hash1>/extent-<id>.bin`

Content: Raw bytes with BLAKE3-256 checksum stored in metadata (not in file).

## Parity file format

Location: `parity/group-<id>.bin`

Content: Raw XOR parity bytes for the parity group.

## Rebuild progress format

Magic: `RBLD`, Header: 48 bytes

```
[0:4]   Magic       [4]byte   "RBLD"
[4:6]   Version     uint16    1
[6:8]   Reserved    uint16    0
[8:16]  Timestamp   int64     UnixNano, UTC
[16:24] DiskID      uint16-length-prefixed string
[24:32] ExtentCount uint32
[32:48] PayloadLen  uint32    byte length of encoded RebuildProgress
[48:96] PayloadChecksum [32]byte  BLAKE3-256 of payload
```

Atomic replace: Same pattern as metadata snapshot.

## Scrub progress format

Magic: `SCRB`, Header: 53 bytes

```
[0:4]   Magic       [4]byte   "SCRB"
[4:6]   Version     uint16    1
[6:8]   Reserved    uint16    0
[8:16]  Timestamp   int64     UnixNano, UTC
[16:24] DiskID      uint16-length-prefixed string (optional)
[24:32] RunID       uint16-length-prefixed string
[32:40] Status      uint8     0=not started, 1=in progress, 2=complete, 3=failed
[40:53] PayloadLen  uint32    byte length of encoded ScrubProgress
[53:85] PayloadChecksum [32]byte  BLAKE3-256 of payload
```

Atomic replace: Same pattern as metadata snapshot.
