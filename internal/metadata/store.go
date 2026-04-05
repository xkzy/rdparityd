package metadata

// store.go — binary metadata snapshot store.
//
// On-disk binary format (all integers big-endian):
//
//	── header (72 bytes) ──────────────────────────────────────────────────
//	[0:4]   Magic       [4]byte   "RTPM"
//	[4:6]   Version     uint16    1
//	[6:8]   Reserved    uint16    0
//	[8:16]  SavedAt     int64     UnixNano, UTC
//	[16:24] Generation  uint64    0 (reserved for future WAL integration)
//	[24:28] PayloadLen  uint32    byte length of encoded SampleState
//	[28:32] Reserved    uint32    0
//	[32:40] Reserved    [8]byte   0
//	[40:72] StateHash   [32]byte  BLAKE3-256(payload)
//	── payload (variable) ──────────────────────────────────────────────────
//	Binary encoding of SampleState — see encodeState / decodeState.
//
// Checksum algorithm: BLAKE3-256 everywhere, consistent with the journal store.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"lukechampine.com/blake3"
)

const (
	snapshotMagic   = "RTPM"
	SnapshotVersion = 1
	snapHeaderSize  = 72 // 40-byte prefix + 32-byte BLAKE3 hash
)

// SnapshotEnvelope wraps the decoded SampleState with metadata used by the
// store and by callers that inspect or validate snapshots.
type SnapshotEnvelope struct {
	Version int
	SavedAt time.Time
	State   SampleState
	// StateChecksum is the 64-character BLAKE3-256 hex digest of the binary-
	// encoded payload (binaryEncodeState(State)), computed at save time.
	StateChecksum string
}

type Store struct {
	path string
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Save(state SampleState) (SnapshotEnvelope, error) {
	payload := encodeState(state)
	hashBytes := blake3.Sum256(payload)

	envelope := SnapshotEnvelope{
		Version:       SnapshotVersion,
		SavedAt:       time.Now().UTC(),
		State:         state,
		StateChecksum: hex.EncodeToString(hashBytes[:]),
	}

	hdrPrefix := make([]byte, 40)
	copy(hdrPrefix[0:4], snapshotMagic)
	binary.BigEndian.PutUint16(hdrPrefix[4:6], uint16(SnapshotVersion))
	binary.BigEndian.PutUint16(hdrPrefix[6:8], 0) // reserved
	binary.BigEndian.PutUint64(hdrPrefix[8:16], uint64(envelope.SavedAt.UnixNano()))
	binary.BigEndian.PutUint64(hdrPrefix[16:24], 0) // generation (reserved)
	binary.BigEndian.PutUint32(hdrPrefix[24:28], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdrPrefix[28:32], 0) // reserved
	// bytes [32:40] reserved — already zero

	hdr := make([]byte, snapHeaderSize)
	copy(hdr[:40], hdrPrefix)
	copy(hdr[40:72], hashBytes[:])

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("create metadata directory: %w", err)
	}

	tmpPath := s.path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("open metadata temp file: %w", err)
	}

	if _, err := file.Write(hdr); err != nil {
		file.Close()
		return SnapshotEnvelope{}, fmt.Errorf("write metadata header: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return SnapshotEnvelope{}, fmt.Errorf("write metadata payload: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return SnapshotEnvelope{}, fmt.Errorf("sync metadata snapshot: %w", err)
	}
	if err := file.Close(); err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("close metadata snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("replace metadata snapshot: %w", err)
	}

	return envelope, nil
}

func (s *Store) Load() (SampleState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return SampleState{}, fmt.Errorf("read metadata snapshot: %w", err)
	}
	if len(data) < snapHeaderSize {
		return SampleState{}, fmt.Errorf("metadata snapshot too short (%d bytes)", len(data))
	}

	hdrPrefix := data[:40]
	storedHash := data[40:72]
	payload := data[snapHeaderSize:]

	// Verify magic.
	if string(hdrPrefix[0:4]) != snapshotMagic {
		return SampleState{}, fmt.Errorf("invalid metadata magic %q", string(hdrPrefix[0:4]))
	}
	version := binary.BigEndian.Uint16(hdrPrefix[4:6])
	if int(version) != SnapshotVersion {
		return SampleState{}, fmt.Errorf("unexpected metadata version %d", version)
	}
	savedAtNano := int64(binary.BigEndian.Uint64(hdrPrefix[8:16]))
	if savedAtNano == 0 {
		return SampleState{}, fmt.Errorf("missing metadata snapshot timestamp")
	}
	payloadLen := binary.BigEndian.Uint32(hdrPrefix[24:28])
	if uint32(len(payload)) != payloadLen {
		return SampleState{}, fmt.Errorf("metadata payload length mismatch: header=%d actual=%d", payloadLen, len(payload))
	}

	// Verify BLAKE3 hash.
	computedHash := blake3.Sum256(payload)
	if !bytes.Equal(computedHash[:], storedHash) {
		return SampleState{}, fmt.Errorf("metadata snapshot checksum mismatch")
	}

	state, err := decodeState(payload)
	if err != nil {
		return SampleState{}, fmt.Errorf("decode metadata snapshot: %w", err)
	}
	return state, nil
}

func (s *Store) LoadOrCreate(defaultState SampleState) (SampleState, error) {
	state, err := s.Load()
	if err == nil {
		return state, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return SampleState{}, err
	}
	if _, saveErr := s.Save(defaultState); saveErr != nil {
		return SampleState{}, saveErr
	}
	return defaultState, nil
}

// checksumState returns the BLAKE3-256 hex digest of the binary-encoded state.
// Used by validateEnvelope and by external callers that need a canonical state
// fingerprint (e.g. tests, scrub history).
func checksumState(state SampleState) string {
	h := blake3.Sum256(encodeState(state))
	return hex.EncodeToString(h[:])
}

// ── binary encoding of SampleState ───────────────────────────────────────────

func encodeState(state SampleState) []byte {
	var buf bytes.Buffer
	mwritePool(&buf, state.Pool)
	mwriteUint16(&buf, uint16(len(state.Disks)))
	for _, d := range state.Disks {
		mwriteDisk(&buf, d)
	}
	mwriteUint16(&buf, uint16(len(state.Files)))
	for _, f := range state.Files {
		mwriteFileRecord(&buf, f)
	}
	mwriteUint16(&buf, uint16(len(state.Extents)))
	for _, e := range state.Extents {
		mwriteExtent(&buf, e)
	}
	mwriteUint16(&buf, uint16(len(state.ParityGroups)))
	for _, pg := range state.ParityGroups {
		mwriteParityGroup(&buf, pg)
	}
	mwriteUint16(&buf, uint16(len(state.Transactions)))
	for _, tx := range state.Transactions {
		mwriteTransaction(&buf, tx)
	}
	mwriteUint16(&buf, uint16(len(state.ScrubHistory)))
	for _, sr := range state.ScrubHistory {
		mwriteScrubRun(&buf, sr)
	}
	return buf.Bytes()
}

func decodeState(data []byte) (SampleState, error) {
	r := bytes.NewReader(data)
	var state SampleState
	var err error

	if state.Pool, err = mreadPool(r); err != nil {
		return SampleState{}, fmt.Errorf("pool: %w", err)
	}

	numDisks, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_disks: %w", err)
	}
	state.Disks = make([]Disk, numDisks)
	for i := range state.Disks {
		if state.Disks[i], err = mreadDisk(r); err != nil {
			return SampleState{}, fmt.Errorf("disk[%d]: %w", i, err)
		}
	}

	numFiles, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_files: %w", err)
	}
	state.Files = make([]FileRecord, numFiles)
	for i := range state.Files {
		if state.Files[i], err = mreadFileRecord(r); err != nil {
			return SampleState{}, fmt.Errorf("file[%d]: %w", i, err)
		}
	}

	numExtents, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_extents: %w", err)
	}
	state.Extents = make([]Extent, numExtents)
	for i := range state.Extents {
		if state.Extents[i], err = mreadExtent(r); err != nil {
			return SampleState{}, fmt.Errorf("extent[%d]: %w", i, err)
		}
	}

	numGroups, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_parity_groups: %w", err)
	}
	state.ParityGroups = make([]ParityGroup, numGroups)
	for i := range state.ParityGroups {
		if state.ParityGroups[i], err = mreadParityGroup(r); err != nil {
			return SampleState{}, fmt.Errorf("parity_group[%d]: %w", i, err)
		}
	}

	numTx, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_transactions: %w", err)
	}
	state.Transactions = make([]Transaction, numTx)
	for i := range state.Transactions {
		if state.Transactions[i], err = mreadTransaction(r); err != nil {
			return SampleState{}, fmt.Errorf("transaction[%d]: %w", i, err)
		}
	}

	numScrub, err := mreadUint16(r)
	if err != nil {
		return SampleState{}, fmt.Errorf("num_scrub_runs: %w", err)
	}
	state.ScrubHistory = make([]ScrubRun, numScrub)
	for i := range state.ScrubHistory {
		if state.ScrubHistory[i], err = mreadScrubRun(r); err != nil {
			return SampleState{}, fmt.Errorf("scrub_run[%d]: %w", i, err)
		}
	}

	return state, nil
}

// ── per-type encoders ─────────────────────────────────────────────────────────

func mwritePool(buf *bytes.Buffer, p Pool) {
	mwriteStr(buf, p.PoolID)
	mwriteStr(buf, p.Name)
	mwriteStr(buf, p.Version)
	mwriteInt64(buf, p.ExtentSizeBytes)
	mwriteStr(buf, p.ParityMode)
	mwriteInt64(buf, p.CreatedAt.UnixNano())
}

func mreadPool(r *bytes.Reader) (Pool, error) {
	var p Pool
	var err error
	if p.PoolID, err = mreadStr(r); err != nil {
		return p, err
	}
	if p.Name, err = mreadStr(r); err != nil {
		return p, err
	}
	if p.Version, err = mreadStr(r); err != nil {
		return p, err
	}
	if p.ExtentSizeBytes, err = mreadInt64(r); err != nil {
		return p, err
	}
	if p.ParityMode, err = mreadStr(r); err != nil {
		return p, err
	}
	nano, err := mreadInt64(r)
	if err != nil {
		return p, err
	}
	p.CreatedAt = time.Unix(0, nano).UTC()
	return p, nil
}

// DiskRole codes for binary encoding.
const (
	diskRoleData     uint8 = 0x01
	diskRoleParity   uint8 = 0x02
	diskRoleMetadata uint8 = 0x03
)

func diskRoleCode(r DiskRole) uint8 {
	switch r {
	case DiskRoleParity:
		return diskRoleParity
	case DiskRoleMetadata:
		return diskRoleMetadata
	default:
		return diskRoleData
	}
}

func diskRoleFromCode(code uint8) DiskRole {
	switch code {
	case diskRoleParity:
		return DiskRoleParity
	case diskRoleMetadata:
		return DiskRoleMetadata
	default:
		return DiskRoleData
	}
}

func mwriteDisk(buf *bytes.Buffer, d Disk) {
	mwriteStr(buf, d.DiskID)
	mwriteStr(buf, d.UUID)
	buf.WriteByte(diskRoleCode(d.Role))
	mwriteStr(buf, d.FilesystemType)
	mwriteStr(buf, d.Mountpoint)
	mwriteInt64(buf, d.CapacityBytes)
	mwriteInt64(buf, d.FreeBytes)
	mwriteStr(buf, d.HealthStatus)
	mwriteInt64(buf, d.Generation)
}

func mreadDisk(r *bytes.Reader) (Disk, error) {
	var d Disk
	var err error
	if d.DiskID, err = mreadStr(r); err != nil {
		return d, err
	}
	if d.UUID, err = mreadStr(r); err != nil {
		return d, err
	}
	roleCode, err := r.ReadByte()
	if err != nil {
		return d, err
	}
	d.Role = diskRoleFromCode(roleCode)
	if d.FilesystemType, err = mreadStr(r); err != nil {
		return d, err
	}
	if d.Mountpoint, err = mreadStr(r); err != nil {
		return d, err
	}
	if d.CapacityBytes, err = mreadInt64(r); err != nil {
		return d, err
	}
	if d.FreeBytes, err = mreadInt64(r); err != nil {
		return d, err
	}
	if d.HealthStatus, err = mreadStr(r); err != nil {
		return d, err
	}
	if d.Generation, err = mreadInt64(r); err != nil {
		return d, err
	}
	return d, nil
}

func mwriteFileRecord(buf *bytes.Buffer, f FileRecord) {
	mwriteStr(buf, f.FileID)
	mwriteStr(buf, f.Path)
	mwriteInt64(buf, f.SizeBytes)
	mwriteInt64(buf, f.MTime.UnixNano())
	mwriteInt64(buf, f.CTime.UnixNano())
	mwriteStr(buf, f.Policy)
	mwriteStr(buf, string(f.State))
}

func mreadFileRecord(r *bytes.Reader) (FileRecord, error) {
	var f FileRecord
	var err error
	if f.FileID, err = mreadStr(r); err != nil {
		return f, err
	}
	if f.Path, err = mreadStr(r); err != nil {
		return f, err
	}
	if f.SizeBytes, err = mreadInt64(r); err != nil {
		return f, err
	}
	mNano, err := mreadInt64(r)
	if err != nil {
		return f, err
	}
	cNano, err := mreadInt64(r)
	if err != nil {
		return f, err
	}
	f.MTime = time.Unix(0, mNano).UTC()
	f.CTime = time.Unix(0, cNano).UTC()
	if f.Policy, err = mreadStr(r); err != nil {
		return f, err
	}
	stateStr, err := mreadStr(r)
	if err != nil {
		return f, err
	}
	f.State = FileState(stateStr)
	return f, nil
}

func mwriteExtent(buf *bytes.Buffer, e Extent) {
	mwriteStr(buf, e.ExtentID)
	mwriteStr(buf, e.FileID)
	mwriteInt64(buf, e.LogicalOffset)
	mwriteInt64(buf, e.Length)
	mwriteStr(buf, e.DataDiskID)
	mwriteStr(buf, e.PhysicalLocator.RelativePath)
	mwriteInt64(buf, e.PhysicalLocator.OffsetBytes)
	mwriteInt64(buf, e.PhysicalLocator.LengthBytes)
	mwriteStr(buf, e.Checksum)
	mwriteStr(buf, e.ChecksumAlg)
	mwriteInt64(buf, e.Generation)
	mwriteStr(buf, e.ParityGroupID)
	mwriteStr(buf, string(e.State))
}

func mreadExtent(r *bytes.Reader) (Extent, error) {
	var e Extent
	var err error
	if e.ExtentID, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.FileID, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.LogicalOffset, err = mreadInt64(r); err != nil {
		return e, err
	}
	if e.Length, err = mreadInt64(r); err != nil {
		return e, err
	}
	if e.DataDiskID, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.RelativePath, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.OffsetBytes, err = mreadInt64(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.LengthBytes, err = mreadInt64(r); err != nil {
		return e, err
	}
	if e.Checksum, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.ChecksumAlg, err = mreadStr(r); err != nil {
		return e, err
	}
	if e.Generation, err = mreadInt64(r); err != nil {
		return e, err
	}
	if e.ParityGroupID, err = mreadStr(r); err != nil {
		return e, err
	}
	stateStr, err := mreadStr(r)
	if err != nil {
		return e, err
	}
	e.State = ExtentState(stateStr)
	return e, nil
}

func mwriteParityGroup(buf *bytes.Buffer, pg ParityGroup) {
	mwriteStr(buf, pg.ParityGroupID)
	mwriteStr(buf, pg.ParityDiskID)
	mwriteUint16(buf, uint16(len(pg.MemberExtentIDs)))
	for _, id := range pg.MemberExtentIDs {
		mwriteStr(buf, id)
	}
	mwriteStr(buf, pg.ParityChecksum)
	mwriteInt64(buf, pg.Generation)
}

func mreadParityGroup(r *bytes.Reader) (ParityGroup, error) {
	var pg ParityGroup
	var err error
	if pg.ParityGroupID, err = mreadStr(r); err != nil {
		return pg, err
	}
	if pg.ParityDiskID, err = mreadStr(r); err != nil {
		return pg, err
	}
	numMembers, err := mreadUint16(r)
	if err != nil {
		return pg, err
	}
	pg.MemberExtentIDs = make([]string, numMembers)
	for i := range pg.MemberExtentIDs {
		if pg.MemberExtentIDs[i], err = mreadStr(r); err != nil {
			return pg, err
		}
	}
	if pg.ParityChecksum, err = mreadStr(r); err != nil {
		return pg, err
	}
	if pg.Generation, err = mreadInt64(r); err != nil {
		return pg, err
	}
	return pg, nil
}

func mwriteTransaction(buf *bytes.Buffer, tx Transaction) {
	mwriteStr(buf, tx.TxID)
	mwriteStr(buf, tx.State)
	mwriteInt64(buf, tx.StartedAt.UnixNano())
	if tx.CommittedAt != nil {
		buf.WriteByte(1)
		mwriteInt64(buf, tx.CommittedAt.UnixNano())
	} else {
		buf.WriteByte(0)
	}
	mwriteUint16(buf, uint16(len(tx.AffectedExtentIDs)))
	for _, id := range tx.AffectedExtentIDs {
		mwriteStr(buf, id)
	}
	mwriteInt64(buf, tx.OldGeneration)
	mwriteInt64(buf, tx.NewGeneration)
	if tx.ReplayRequired {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func mreadTransaction(r *bytes.Reader) (Transaction, error) {
	var tx Transaction
	var err error
	if tx.TxID, err = mreadStr(r); err != nil {
		return tx, err
	}
	if tx.State, err = mreadStr(r); err != nil {
		return tx, err
	}
	startNano, err := mreadInt64(r)
	if err != nil {
		return tx, err
	}
	tx.StartedAt = time.Unix(0, startNano).UTC()
	hasCommit, err := r.ReadByte()
	if err != nil {
		return tx, err
	}
	if hasCommit == 1 {
		commitNano, err := mreadInt64(r)
		if err != nil {
			return tx, err
		}
		t := time.Unix(0, commitNano).UTC()
		tx.CommittedAt = &t
	}
	numExtents, err := mreadUint16(r)
	if err != nil {
		return tx, err
	}
	tx.AffectedExtentIDs = make([]string, numExtents)
	for i := range tx.AffectedExtentIDs {
		if tx.AffectedExtentIDs[i], err = mreadStr(r); err != nil {
			return tx, err
		}
	}
	if tx.OldGeneration, err = mreadInt64(r); err != nil {
		return tx, err
	}
	if tx.NewGeneration, err = mreadInt64(r); err != nil {
		return tx, err
	}
	replay, err := r.ReadByte()
	if err != nil {
		return tx, err
	}
	tx.ReplayRequired = replay == 1
	return tx, nil
}

func mwriteScrubRun(buf *bytes.Buffer, sr ScrubRun) {
	mwriteStr(buf, sr.RunID)
	mwriteInt64(buf, sr.StartedAt.UnixNano())
	mwriteInt64(buf, sr.CompletedAt.UnixNano())
	if sr.Repair {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	if sr.Healthy {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	mwriteInt32(buf, int32(sr.FilesChecked))
	mwriteInt32(buf, int32(sr.ExtentsChecked))
	mwriteInt32(buf, int32(sr.ParityGroupsChecked))
	mwriteInt32(buf, int32(sr.HealedCount))
	mwriteInt32(buf, int32(sr.FailedCount))
	mwriteInt32(buf, int32(sr.IssueCount))
	mwriteUint16(buf, uint16(len(sr.HealedExtentIDs)))
	for _, id := range sr.HealedExtentIDs {
		mwriteStr(buf, id)
	}
	mwriteUint16(buf, uint16(len(sr.HealedParityGroupIDs)))
	for _, id := range sr.HealedParityGroupIDs {
		mwriteStr(buf, id)
	}
}

func mreadScrubRun(r *bytes.Reader) (ScrubRun, error) {
	var sr ScrubRun
	var err error
	if sr.RunID, err = mreadStr(r); err != nil {
		return sr, err
	}
	startNano, err := mreadInt64(r)
	if err != nil {
		return sr, err
	}
	completedNano, err := mreadInt64(r)
	if err != nil {
		return sr, err
	}
	sr.StartedAt = time.Unix(0, startNano).UTC()
	sr.CompletedAt = time.Unix(0, completedNano).UTC()
	repair, err := r.ReadByte()
	if err != nil {
		return sr, err
	}
	sr.Repair = repair == 1
	healthy, err := r.ReadByte()
	if err != nil {
		return sr, err
	}
	sr.Healthy = healthy == 1
	fc, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.FilesChecked = int(fc)
	ec, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.ExtentsChecked = int(ec)
	pgc, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.ParityGroupsChecked = int(pgc)
	hc, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.HealedCount = int(hc)
	failc, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.FailedCount = int(failc)
	ic, err := mreadInt32(r)
	if err != nil {
		return sr, err
	}
	sr.IssueCount = int(ic)
	numHealed, err := mreadUint16(r)
	if err != nil {
		return sr, err
	}
	sr.HealedExtentIDs = make([]string, numHealed)
	for i := range sr.HealedExtentIDs {
		if sr.HealedExtentIDs[i], err = mreadStr(r); err != nil {
			return sr, err
		}
	}
	numHealedPG, err := mreadUint16(r)
	if err != nil {
		return sr, err
	}
	sr.HealedParityGroupIDs = make([]string, numHealedPG)
	for i := range sr.HealedParityGroupIDs {
		if sr.HealedParityGroupIDs[i], err = mreadStr(r); err != nil {
			return sr, err
		}
	}
	return sr, nil
}

// ── binary helpers (metadata-internal) ───────────────────────────────────────

func mwriteStr(buf *bytes.Buffer, s string) {
	b := []byte(s)
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(b)))
	buf.Write(n[:])
	buf.Write(b)
}

func mwriteUint16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func mwriteInt32(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	buf.Write(b[:])
}

func mwriteInt64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

func mreadStr(r *bytes.Reader) (string, error) {
	n, err := mreadUint16(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func mreadUint16(r *bytes.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

func mreadInt32(r *bytes.Reader) (int32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}

func mreadInt64(r *bytes.Reader) (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

// ── allocator (unchanged from original) ──────────────────────────────────────

type Allocator struct {
	state *SampleState
}

func NewAllocator(state *SampleState) *Allocator {
	return &Allocator{state: state}
}

func (a *Allocator) AllocateFile(path string, sizeBytes int64) (FileRecord, []Extent, error) {
	if a == nil || a.state == nil {
		return FileRecord{}, nil, fmt.Errorf("allocator requires a non-nil state")
	}
	if path == "" {
		return FileRecord{}, nil, fmt.Errorf("path is required")
	}
	if sizeBytes < 0 {
		return FileRecord{}, nil, fmt.Errorf("size must be non-negative")
	}

	now := time.Now().UTC()
	fileID := fmt.Sprintf("file-%06d", len(a.state.Files)+1)
	file := FileRecord{
		FileID:    fileID,
		Path:      path,
		SizeBytes: sizeBytes,
		MTime:     now,
		CTime:     now,
		Policy:    "default",
		State:     FileStateAllocated,
	}

	if sizeBytes == 0 {
		a.state.Files = append(a.state.Files, file)
		return file, nil, nil
	}

	extentSize := a.state.Pool.ExtentSizeBytes
	if extentSize <= 0 {
		extentSize = 1 << 20
	}

	remaining := sizeBytes
	logicalOffset := int64(0)
	extents := make([]Extent, 0)
	groupWidth := a.parityGroupWidth()
	for remaining > 0 {
		allocationLength := extentSize
		if remaining < allocationLength {
			allocationLength = remaining
		}

		extentNumber := len(a.state.Extents) + len(extents) + 1
		parityGroupID := fmt.Sprintf("pg-%06d", ((extentNumber-1)/groupWidth)+1)
		diskIndex, err := a.chooseDisk(extents, parityGroupID)
		if err != nil {
			return FileRecord{}, nil, err
		}

		extentID := fmt.Sprintf("extent-%06d", extentNumber)
		relativePath := extentRelativePath(path, extentNumber)
		extent := Extent{
			ExtentID:      extentID,
			FileID:        fileID,
			LogicalOffset: logicalOffset,
			Length:        allocationLength,
			DataDiskID:    a.state.Disks[diskIndex].DiskID,
			PhysicalLocator: Locator{
				RelativePath: relativePath,
				OffsetBytes:  0,
				LengthBytes:  allocationLength,
			},
			Checksum:      "",
			ChecksumAlg:   ChecksumAlgorithm,
			Generation:    1,
			ParityGroupID: parityGroupID,
			State:         ExtentStateAllocated,
		}
		if a.state.Disks[diskIndex].FreeBytes < allocationLength {
			return FileRecord{}, nil, fmt.Errorf("disk %s lacks free space for allocation", a.state.Disks[diskIndex].DiskID)
		}
		a.state.Disks[diskIndex].FreeBytes -= allocationLength
		extents = append(extents, extent)
		remaining -= allocationLength
		logicalOffset += allocationLength
	}

	a.state.Files = append(a.state.Files, file)
	a.state.Extents = append(a.state.Extents, extents...)
	return file, extents, nil
}

func (a *Allocator) chooseDisk(pending []Extent, parityGroupID string) (int, error) {
	eligible := make([]int, 0)
	for i, disk := range a.state.Disks {
		if disk.Role != DiskRoleData {
			continue
		}
		if strings.ToLower(disk.HealthStatus) != "online" {
			continue
		}
		if disk.FreeBytes > 0 {
			eligible = append(eligible, i)
		}
	}
	if len(eligible) == 0 {
		return -1, fmt.Errorf("no eligible data disks available")
	}

	counts := make(map[string]int)
	usedInGroup := make(map[string]bool)
	for _, extent := range a.state.Extents {
		counts[extent.DataDiskID]++
		if extent.ParityGroupID == parityGroupID {
			usedInGroup[extent.DataDiskID] = true
		}
	}
	for _, extent := range pending {
		counts[extent.DataDiskID]++
		if extent.ParityGroupID == parityGroupID {
			usedInGroup[extent.DataDiskID] = true
		}
	}

	preferred := make([]int, 0, len(eligible))
	fallback := make([]int, 0, len(eligible))
	for _, idx := range eligible {
		if usedInGroup[a.state.Disks[idx].DiskID] {
			fallback = append(fallback, idx)
			continue
		}
		preferred = append(preferred, idx)
	}
	candidates := preferred
	if len(candidates) == 0 {
		candidates = fallback
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := a.state.Disks[candidates[i]]
		right := a.state.Disks[candidates[j]]
		if counts[left.DiskID] == counts[right.DiskID] {
			if left.FreeBytes == right.FreeBytes {
				return left.DiskID < right.DiskID
			}
			return left.FreeBytes > right.FreeBytes
		}
		return counts[left.DiskID] < counts[right.DiskID]
	})

	return candidates[0], nil
}

func (a *Allocator) parityGroupWidth() int {
	if a == nil || a.state == nil {
		return 1
	}

	count := 0
	for _, disk := range a.state.Disks {
		if disk.Role == DiskRoleData && strings.ToLower(disk.HealthStatus) == "online" && disk.FreeBytes > 0 {
			count++
		}
	}
	if count <= 0 {
		return 1
	}
	if count > 8 {
		return 8
	}
	return count
}

func extentRelativePath(path string, extentNumber int) string {
	h := blake3.Sum256([]byte(path))
	prefix := hex.EncodeToString(h[:2])
	return fmt.Sprintf("data/%s/%s/extent-%06d.bin", prefix[:2], prefix[2:], extentNumber)
}
