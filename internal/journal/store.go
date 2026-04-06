package journal

// store.go — binary journal store.
//
// On-disk binary format (all integers big-endian):
//
//	[RecordLen uint32]             total bytes that follow (header + payload)
//	── header (72 bytes) ──────────────────────────────────────────────────
//	[0:4]   Magic       [4]byte   "RTPJ"
//	[4:6]   Version     uint16    1
//	[6]     StateCode   uint8     see stateToCode / codeToState tables
//	[7]     Flags       uint8     0 (reserved)
//	[8:16]  Timestamp   int64     UnixNano, UTC
//	[16:24] OldGen      int64
//	[24:32] NewGen      int64
//	[32:36] PayloadLen  uint32    byte length of variable payload section
//	[36:40] Reserved    uint32    0
//	[40:72] RecordHash  [32]byte  BLAKE3-256 of header[0:40] + payload
//	── payload (variable) ──────────────────────────────────────────────────
//	TxID, PoolName, LogicalPath: each as uint16-length-prefixed byte slice
//	AffectedExtentIDs:           uint16 count + repeated uint16-len strings
//	File:                        uint8 present flag + optional FileRecord encoding
//	Extents:                     uint16 count + repeated Extent encodings
//
// Checksum algorithm: BLAKE3-256 everywhere — for record integrity, extent
// checksums, parity checksums, and metadata snapshots.
//
// Torn-write detection: if fewer than RecordLen bytes can be read after the
// length prefix, the record is silently dropped — it was never committed to
// stable storage.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
	"lukechampine.com/blake3"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	RecordMagic      = "RTPJ"
	RecordVersion    = 1
	recordHeaderSize = 72               // 40-byte prefix + 32-byte BLAKE3 hash
	MaxRecordSize    = 64 * 1024 * 1024 // 64 MiB max record size
	MaxPayloadSize   = MaxRecordSize - recordHeaderSize
)

var (
	ErrInvalidMagic       = errors.New("invalid record magic")
	ErrChecksumMismatch   = errors.New("checksum mismatch")
	ErrUnsupportedVersion = errors.New("unsupported record version")
	ErrTruncatedRecord    = errors.New("truncated record")
	ErrMalformedPayload   = errors.New("malformed payload")
	ErrUnknownStateCode   = errors.New("unknown state code")
	ErrRecordSizeExceeded = errors.New("record size exceeds maximum")
)

// State code constants for the on-disk binary representation.
const (
	stateCodePrepared        uint8 = 0x01
	stateCodeDataWritten     uint8 = 0x02
	stateCodeParityWritten   uint8 = 0x03
	stateCodeMetadataWritten uint8 = 0x04
	stateCodeCommitted       uint8 = 0x05
	stateCodeAborted         uint8 = 0x06
	stateCodeReplayRequired  uint8 = 0x07
)

var stateToCode = map[State]uint8{
	StatePrepared:        stateCodePrepared,
	StateDataWritten:     stateCodeDataWritten,
	StateParityWritten:   stateCodeParityWritten,
	StateMetadataWritten: stateCodeMetadataWritten,
	StateCommitted:       stateCodeCommitted,
	StateAborted:         stateCodeAborted,
	StateReplayRequired:  stateCodeReplayRequired,
}

var codeToState = map[uint8]State{
	stateCodePrepared:        StatePrepared,
	stateCodeDataWritten:     StateDataWritten,
	stateCodeParityWritten:   StateParityWritten,
	stateCodeMetadataWritten: StateMetadataWritten,
	stateCodeCommitted:       StateCommitted,
	stateCodeAborted:         StateAborted,
	stateCodeReplayRequired:  StateReplayRequired,
}

// Record is the in-memory representation of a journal entry. The Magic,
// Version, PayloadChecksum, and RecordChecksum fields carry the same semantics
// as before but are now derived from a compact binary encoding rather than JSON.
type Record struct {
	Magic             string
	Version           int
	TxID              string
	State             State
	Timestamp         time.Time
	PoolName          string
	LogicalPath       string
	File              *metadata.FileRecord
	Extents           []metadata.Extent
	OldGeneration     int64
	NewGeneration     int64
	AffectedExtentIDs []string
	// PayloadChecksum is the 64-character lowercase BLAKE3-256 hex digest of
	// the binary-encoded payload section.
	PayloadChecksum string
	// RecordChecksum is the 64-character lowercase BLAKE3-256 hex digest of
	// canonicalHeaderPrefix + payload, covering every mutable field in one hash.
	RecordChecksum string
}

type ReplayAction struct {
	TxID           string
	LastState      State
	Outcome        State
	Recommendation string
}

type ReplaySummary struct {
	TotalRecords    int
	RequiresReplay  bool
	IncompleteTxIDs []string
	Actions         []ReplayAction
	LastCommittedTx string
	LastAbortedTx   string
}

type Store struct {
	path string
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Append(record Record) (Record, error) {
	sealed, err := sealRecord(record)
	if err != nil {
		return Record{}, err
	}

	payload := encodedPayload(sealed)
	if len(payload) > MaxPayloadSize {
		return Record{}, fmt.Errorf("journal payload too large: %d bytes exceeds maximum allowed %d", len(payload), MaxPayloadSize)
	}

	if err := ensureDir(filepath.Dir(s.path), 0o755); err != nil {
		return Record{}, fmt.Errorf("create journal directory: %w", err)
	}

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return Record{}, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()

	data, err := encodeRecord(sealed)
	if err != nil {
		return Record{}, fmt.Errorf("encode journal record: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return Record{}, fmt.Errorf("write journal record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Record{}, fmt.Errorf("sync journal: %w", err)
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return Record{}, fmt.Errorf("sync journal directory: %w", err)
	}

	return sealed, nil
}

func (s *Store) Load() ([]Record, error) {
	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()

	var records []Record
	for {
		record, err := readRecord(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A torn write produces io.ErrUnexpectedEOF: the record length
			// prefix was read but fewer than RecordLen bytes followed. This
			// indicates the crash happened mid-write; the partial record is
			// silently discarded, matching WAL convention.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("decode journal record: %w", err)
		}
		if err := validateRecord(record); err != nil {
			return nil, fmt.Errorf("validate journal record: %w", err)
		}
		records = append(records, record)
	}

	return records, nil
}

// CompactIfClean removes all journal records when every transaction in the
// journal has reached a terminal state (committed or aborted). This is safe
// because terminal transactions are fully reflected in the metadata snapshot;
// no recovery action is needed for them. The compaction is atomic: the journal
// is written to a temp file, synced, and renamed over the old file.
//
// CompactIfClean is a no-op when the journal is already empty, cannot be read,
// or contains any incomplete transaction.
func (s *Store) CompactIfClean() error {
	records, err := s.Load()
	if err != nil || len(records) == 0 {
		return nil // empty or unreadable — nothing to compact
	}

	// Check that every transaction is in a terminal state.
	txLastState := make(map[string]State)
	for _, rec := range records {
		txLastState[rec.TxID] = rec.State
	}
	for _, state := range txLastState {
		if state != StateCommitted && state != StateAborted {
			return nil // incomplete transaction present; do not compact
		}
	}

	// All transactions terminal: safe to zero the journal.
	// Use write-then-rename for crash-safety.
	tmpPath := s.path + ".compact"
	if err := ensureDir(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("compaction: ensure journal dir: %w", err)
	}
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("compaction: open temp journal: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("compaction: sync empty journal: %w", err)
	}
	f.Close()
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("compaction: rename compact journal: %w", err)
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("compaction: sync journal directory: %w", err)
	}
	return nil
}

func (s *Store) Replay() (ReplaySummary, error) {
	records, err := s.Load()
	if err != nil {
		return ReplaySummary{}, err
	}
	return ReplayRecords(records)
}

func ReplayRecords(records []Record) (ReplaySummary, error) {
	summary := ReplaySummary{TotalRecords: len(records)}
	if len(records) == 0 {
		return summary, nil
	}

	grouped := make(map[string][]Record)
	order := make([]string, 0)
	for _, record := range records {
		if _, exists := grouped[record.TxID]; !exists {
			order = append(order, record.TxID)
		}
		grouped[record.TxID] = append(grouped[record.TxID], record)
	}

	for _, txID := range order {
		txRecords := grouped[txID]
		states := make([]State, 0, len(txRecords))
		for _, record := range txRecords {
			states = append(states, record.State)
		}

		lastState := states[len(states)-1]
		lastRecord := txRecords[len(txRecords)-1]
		if err := ValidateRecordSequence(txRecords); err != nil {
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateAborted,
				Recommendation: fmt.Sprintf("invalid journal sequence detected: %v; abort the transaction and inspect the journal tail", err),
			})
			continue
		}

		if isRepairRecord(lastRecord) && lastState != StateCommitted && lastState != StateAborted {
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "repair transaction is incomplete; replay must verify or finish the healed extent/parity write before continuing",
			})
			continue
		}

		switch lastState {
		case StateCommitted:
			summary.LastCommittedTx = txID
		case StateAborted:
			summary.LastAbortedTx = txID
		case StatePrepared:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateAborted,
				Recommendation: "discard the pending write and keep the previous generation visible",
			})
		case StateDataWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "verify the new data extent and recompute or roll forward parity before committing",
			})
		case StateParityWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "parity is durable; verify checksum and complete the metadata update",
			})
		case StateMetadataWritten:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "metadata is durable; verify checksums and write the final commit marker",
			})
		case StateReplayRequired:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "resume startup replay for this transaction and only clear the dirty flag after reconciliation",
			})
		default:
			summary.RequiresReplay = true
			summary.IncompleteTxIDs = append(summary.IncompleteTxIDs, txID)
			summary.Actions = append(summary.Actions, ReplayAction{
				TxID:           txID,
				LastState:      lastState,
				Outcome:        StateReplayRequired,
				Recommendation: "unknown journal state encountered; inspect manually before proceeding",
			})
		}
	}

	return summary, nil
}

// ── checksum helpers ──────────────────────────────────────────────────────────

// blake3Hex returns the 64-character lowercase BLAKE3-256 hex digest of data.
func blake3Hex(data []byte) string {
	h := blake3.Sum256(data)
	return hex.EncodeToString(h[:])
}

// ── record integrity ──────────────────────────────────────────────────────────

// canonicalHeaderPrefix builds the 40-byte header prefix used when computing
// RecordChecksum. BLAKE3(prefix + payload) is the 32-byte hash stored in the
// on-disk header and recovered into RecordChecksum on load.
func canonicalHeaderPrefix(record Record, payloadLen uint32) []byte {
	hdr := make([]byte, 40)
	copy(hdr[0:4], RecordMagic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(RecordVersion))
	hdr[6] = stateToCode[record.State]
	hdr[7] = 0 // flags
	binary.BigEndian.PutUint64(hdr[8:16], uint64(record.Timestamp.UnixNano()))
	binary.BigEndian.PutUint64(hdr[16:24], uint64(record.OldGeneration))
	binary.BigEndian.PutUint64(hdr[24:32], uint64(record.NewGeneration))
	binary.BigEndian.PutUint32(hdr[32:36], payloadLen)
	binary.BigEndian.PutUint32(hdr[36:40], 0) // reserved
	return hdr
}

func sealRecord(record Record) (Record, error) {
	record = withDefaults(record)
	if err := validateBasic(record); err != nil {
		return Record{}, err
	}

	payload := encodedPayload(record)
	record.PayloadChecksum = blake3Hex(payload)
	hdrPrefix := canonicalHeaderPrefix(record, uint32(len(payload)))
	record.RecordChecksum = blake3Hex(append(hdrPrefix, payload...))
	return record, nil
}

func validateRecord(record Record) error {
	if err := validateBasic(record); err != nil {
		return err
	}
	if record.PayloadChecksum == "" || record.RecordChecksum == "" {
		return fmt.Errorf("missing journal checksum")
	}
	payload := encodedPayload(record)
	if got := blake3Hex(payload); got != record.PayloadChecksum {
		return fmt.Errorf("payload checksum mismatch")
	}
	hdrPrefix := canonicalHeaderPrefix(record, uint32(len(payload)))
	if got := blake3Hex(append(hdrPrefix, payload...)); got != record.RecordChecksum {
		return fmt.Errorf("record checksum mismatch")
	}
	return nil
}

func validateBasic(record Record) error {
	if record.Magic != RecordMagic {
		return fmt.Errorf("unexpected record magic %q", record.Magic)
	}
	if record.Version != RecordVersion {
		return fmt.Errorf("unexpected record version %d", record.Version)
	}
	if record.TxID == "" {
		return fmt.Errorf("missing tx_id")
	}
	if record.State == "" {
		return fmt.Errorf("missing state")
	}
	if record.Timestamp.IsZero() {
		return fmt.Errorf("missing timestamp")
	}
	return nil
}

func withDefaults(record Record) Record {
	if record.Magic == "" {
		record.Magic = RecordMagic
	}
	if record.Version == 0 {
		record.Version = RecordVersion
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}
	return record
}

// ── binary encoding ───────────────────────────────────────────────────────────

// encodeRecord serialises a sealed Record to the on-disk binary format:
//
//	[RecordLen uint32][header 72 bytes][payload PayloadLen bytes]
//
// Header layout (72 bytes):
//
//	[0:40]  prefix (magic, version, state, flags, timestamps, payloadLen, reserved)
//	[40:72] BLAKE3-256(prefix + payload)
func encodeRecord(record Record) ([]byte, error) {
	if err := validateRecordEncodingLimits(record); err != nil {
		return nil, err
	}
	payload := encodedPayload(record)
	if len(payload) > int(^uint32(0))-recordHeaderSize {
		return nil, fmt.Errorf("journal payload too large: %d bytes exceeds uint32 record limit", len(payload))
	}

	code, ok := stateToCode[record.State]
	if !ok {
		return nil, fmt.Errorf("unknown state %q", record.State)
	}

	hdrPrefix := make([]byte, 40)
	copy(hdrPrefix[0:4], RecordMagic)
	binary.BigEndian.PutUint16(hdrPrefix[4:6], uint16(RecordVersion))
	hdrPrefix[6] = code
	hdrPrefix[7] = 0 // flags
	binary.BigEndian.PutUint64(hdrPrefix[8:16], uint64(record.Timestamp.UnixNano()))
	binary.BigEndian.PutUint64(hdrPrefix[16:24], uint64(record.OldGeneration))
	binary.BigEndian.PutUint64(hdrPrefix[24:32], uint64(record.NewGeneration))
	binary.BigEndian.PutUint32(hdrPrefix[32:36], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdrPrefix[36:40], 0) // reserved

	hashInput := make([]byte, 40+len(payload))
	copy(hashInput[:40], hdrPrefix)
	copy(hashInput[40:], payload)
	recordHash := blake3.Sum256(hashInput)

	hdr := make([]byte, recordHeaderSize) // 72 bytes
	copy(hdr[:40], hdrPrefix)
	copy(hdr[40:72], recordHash[:])

	totalLen := uint32(recordHeaderSize + len(payload))
	out := make([]byte, 0, 4+int(totalLen))
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], totalLen)
	out = append(out, lenBuf[:]...)
	out = append(out, hdr...)
	out = append(out, payload...)
	return out, nil
}

// readRecord reads one binary record from r.
// Returns io.EOF when the stream is cleanly exhausted.
// Returns io.ErrUnexpectedEOF for a torn write (partial record at the tail).
func readRecord(r io.Reader) (Record, error) {
	var totalLen uint32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		// 1-3 bytes were available — torn write on the length prefix itself.
		// Propagate io.ErrUnexpectedEOF directly so Load() can identify it as
		// a safe tail truncation, and return any other I/O error unchanged.
		return Record{}, err
	}
	if totalLen < recordHeaderSize {
		return Record{}, io.ErrUnexpectedEOF
	}
	if totalLen > MaxRecordSize {
		return Record{}, fmt.Errorf("%w: size %d exceeds maximum %d", ErrRecordSizeExceeded, totalLen, MaxRecordSize)
	}

	blob := make([]byte, totalLen)
	if _, err := io.ReadFull(r, blob); err != nil {
		// io.ErrUnexpectedEOF means a torn write (partial record at the tail).
		// Any other error is a genuine I/O failure and is returned unchanged.
		return Record{}, err
	}

	hdrPrefix := blob[:40]
	// Copy the stored hash before building hashInput — hdrPrefix shares the
	// same backing array as blob, so append(hdrPrefix, payload...) would
	// overwrite blob[40:72] in place rather than allocating.
	storedHash := make([]byte, 32)
	copy(storedHash, blob[40:72])
	payload := blob[recordHeaderSize:]

	// Verify magic.
	if string(hdrPrefix[0:4]) != RecordMagic {
		return Record{}, fmt.Errorf("%w: got %q", ErrInvalidMagic, string(hdrPrefix[0:4]))
	}

	// Verify BLAKE3 hash: BLAKE3(hdrPrefix + payload) must equal storedHash.
	hashInput := make([]byte, 40+len(payload))
	copy(hashInput[:40], hdrPrefix)
	copy(hashInput[40:], payload)
	computedHash := blake3.Sum256(hashInput)
	if !bytes.Equal(computedHash[:], storedHash) {
		return Record{}, ErrChecksumMismatch
	}

	version := binary.BigEndian.Uint16(hdrPrefix[4:6])
	stateCode := hdrPrefix[6]
	tsNano := int64(binary.BigEndian.Uint64(hdrPrefix[8:16]))
	oldGen := int64(binary.BigEndian.Uint64(hdrPrefix[16:24]))
	newGen := int64(binary.BigEndian.Uint64(hdrPrefix[24:32]))
	payloadLen := binary.BigEndian.Uint32(hdrPrefix[32:36])

	if uint32(recordHeaderSize)+payloadLen != totalLen {
		return Record{}, ErrTruncatedRecord
	}

	state, ok := codeToState[stateCode]
	if !ok {
		return Record{}, fmt.Errorf("%w: %02x", ErrUnknownStateCode, stateCode)
	}

	record, err := decodePayload(payload)
	if err != nil {
		return Record{}, fmt.Errorf("decode journal payload: %w", err)
	}

	record.Magic = RecordMagic
	record.Version = int(version)
	record.State = state
	record.Timestamp = time.Unix(0, tsNano).UTC()
	record.OldGeneration = oldGen
	record.NewGeneration = newGen
	// Set checksum fields so validateRecord can re-verify without re-reading.
	record.PayloadChecksum = blake3Hex(payload)
	record.RecordChecksum = hex.EncodeToString(storedHash)
	return record, nil
}

// encodedPayload serialises the variable-length fields of a Record to a compact
// binary representation. The fixed-width fields (Timestamp, OldGeneration,
// NewGeneration, State) are stored in the 72-byte header and are NOT repeated
// here.
func encodedPayload(record Record) []byte {
	var buf bytes.Buffer
	jwriteStr(&buf, record.TxID)
	jwriteStr(&buf, record.PoolName)
	jwriteStr(&buf, record.LogicalPath)
	jwriteStrSlice(&buf, record.AffectedExtentIDs)
	if record.File != nil {
		buf.WriteByte(1)
		jwriteFileRecord(&buf, *record.File)
	} else {
		buf.WriteByte(0)
	}
	jwriteUint16(&buf, uint16(len(record.Extents)))
	for _, e := range record.Extents {
		jwriteExtent(&buf, e)
	}
	return buf.Bytes()
}

// decodePayload deserialises the variable payload bytes back into the
// non-header fields of a Record.
func decodePayload(data []byte) (Record, error) {
	r := bytes.NewReader(data)
	var rec Record
	var err error

	if rec.TxID, err = jreadStr(r); err != nil {
		return Record{}, fmt.Errorf("tx_id: %w", err)
	}
	if rec.PoolName, err = jreadStr(r); err != nil {
		return Record{}, fmt.Errorf("pool_name: %w", err)
	}
	if rec.LogicalPath, err = jreadStr(r); err != nil {
		return Record{}, fmt.Errorf("logical_path: %w", err)
	}
	if rec.AffectedExtentIDs, err = jreadStrSlice(r); err != nil {
		return Record{}, fmt.Errorf("affected_extent_ids: %w", err)
	}
	filePresent, err := r.ReadByte()
	if err != nil {
		return Record{}, fmt.Errorf("file present flag: %w", err)
	}
	if filePresent == 1 {
		f, err := jreadFileRecord(r)
		if err != nil {
			return Record{}, fmt.Errorf("file: %w", err)
		}
		rec.File = &f
	}
	numExtents, err := jreadUint16(r)
	if err != nil {
		return Record{}, fmt.Errorf("num_extents: %w", err)
	}
	rec.Extents = make([]metadata.Extent, numExtents)
	for i := range rec.Extents {
		if rec.Extents[i], err = jreadExtent(r); err != nil {
			return Record{}, fmt.Errorf("extent[%d]: %w", i, err)
		}
	}
	return rec, nil
}

// ── binary helpers (journal-internal) ────────────────────────────────────────

func validateRecordEncodingLimits(record Record) error {
	checkStr := func(field, s string) error {
		if len([]byte(s)) > 1<<16-1 {
			return fmt.Errorf("%s too long for journal binary encoding: %d bytes > %d", field, len([]byte(s)), 1<<16-1)
		}
		return nil
	}
	checkCount := func(field string, n int) error {
		if n > 1<<16-1 {
			return fmt.Errorf("%s count too large for journal binary encoding: %d > %d", field, n, 1<<16-1)
		}
		return nil
	}
	if err := checkStr("tx_id", record.TxID); err != nil {
		return err
	}
	if err := checkStr("pool_name", record.PoolName); err != nil {
		return err
	}
	if err := checkStr("logical_path", record.LogicalPath); err != nil {
		return err
	}
	if err := checkCount("affected_extent_ids", len(record.AffectedExtentIDs)); err != nil {
		return err
	}
	for i, id := range record.AffectedExtentIDs {
		if err := checkStr(fmt.Sprintf("affected_extent_ids[%d]", i), id); err != nil {
			return err
		}
	}
	if record.File != nil {
		f := *record.File
		if err := checkStr("file.file_id", f.FileID); err != nil {
			return err
		}
		if err := checkStr("file.path", f.Path); err != nil {
			return err
		}
		if err := checkStr("file.policy", f.Policy); err != nil {
			return err
		}
		if err := checkStr("file.state", string(f.State)); err != nil {
			return err
		}
	}
	if err := checkCount("extents", len(record.Extents)); err != nil {
		return err
	}
	for i, e := range record.Extents {
		if err := checkStr(fmt.Sprintf("extents[%d].extent_id", i), e.ExtentID); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].file_id", i), e.FileID); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].data_disk_id", i), e.DataDiskID); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].relative_path", i), e.PhysicalLocator.RelativePath); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].checksum", i), e.Checksum); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].checksum_alg", i), e.ChecksumAlg); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].parity_group_id", i), e.ParityGroupID); err != nil {
			return err
		}
		if err := checkStr(fmt.Sprintf("extents[%d].state", i), string(e.State)); err != nil {
			return err
		}
	}
	return nil
}

func jwriteStr(buf *bytes.Buffer, s string) {
	b := []byte(s)
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(b)))
	buf.Write(n[:])
	buf.Write(b)
}

func jwriteUint16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func jwriteInt64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

func jwriteStrSlice(buf *bytes.Buffer, ss []string) {
	jwriteUint16(buf, uint16(len(ss)))
	for _, s := range ss {
		jwriteStr(buf, s)
	}
}

func jwriteFileRecord(buf *bytes.Buffer, f metadata.FileRecord) {
	jwriteStr(buf, f.FileID)
	jwriteStr(buf, f.Path)
	jwriteInt64(buf, f.SizeBytes)
	jwriteInt64(buf, f.MTime.UnixNano())
	jwriteInt64(buf, f.CTime.UnixNano())
	jwriteStr(buf, f.Policy)
	jwriteStr(buf, string(f.State))
}

func jwriteExtent(buf *bytes.Buffer, e metadata.Extent) {
	jwriteStr(buf, e.ExtentID)
	jwriteStr(buf, e.FileID)
	jwriteInt64(buf, e.LogicalOffset)
	jwriteInt64(buf, e.Length)
	jwriteStr(buf, e.DataDiskID)
	jwriteStr(buf, e.PhysicalLocator.RelativePath)
	jwriteInt64(buf, e.PhysicalLocator.OffsetBytes)
	jwriteInt64(buf, e.PhysicalLocator.LengthBytes)
	jwriteStr(buf, e.Checksum)
	jwriteStr(buf, e.ChecksumAlg)
	jwriteInt64(buf, e.Generation)
	jwriteStr(buf, e.ParityGroupID)
	jwriteStr(buf, string(e.State))
}

func jreadStr(r *bytes.Reader) (string, error) {
	n, err := jreadUint16(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func jreadUint16(r *bytes.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

func jreadInt64(r *bytes.Reader) (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

func jreadStrSlice(r *bytes.Reader) ([]string, error) {
	n, err := jreadUint16(r)
	if err != nil {
		return nil, err
	}
	ss := make([]string, n)
	for i := range ss {
		if ss[i], err = jreadStr(r); err != nil {
			return nil, err
		}
	}
	return ss, nil
}

func jreadFileRecord(r *bytes.Reader) (metadata.FileRecord, error) {
	var f metadata.FileRecord
	var err error
	if f.FileID, err = jreadStr(r); err != nil {
		return f, err
	}
	if f.Path, err = jreadStr(r); err != nil {
		return f, err
	}
	if f.SizeBytes, err = jreadInt64(r); err != nil {
		return f, err
	}
	mNano, err := jreadInt64(r)
	if err != nil {
		return f, err
	}
	cNano, err := jreadInt64(r)
	if err != nil {
		return f, err
	}
	f.MTime = time.Unix(0, mNano).UTC()
	f.CTime = time.Unix(0, cNano).UTC()
	if f.Policy, err = jreadStr(r); err != nil {
		return f, err
	}
	stateStr, err := jreadStr(r)
	if err != nil {
		return f, err
	}
	f.State = metadata.FileState(stateStr)
	return f, nil
}

func jreadExtent(r *bytes.Reader) (metadata.Extent, error) {
	var e metadata.Extent
	var err error
	if e.ExtentID, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.FileID, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.LogicalOffset, err = jreadInt64(r); err != nil {
		return e, err
	}
	if e.Length, err = jreadInt64(r); err != nil {
		return e, err
	}
	if e.DataDiskID, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.RelativePath, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.OffsetBytes, err = jreadInt64(r); err != nil {
		return e, err
	}
	if e.PhysicalLocator.LengthBytes, err = jreadInt64(r); err != nil {
		return e, err
	}
	if e.Checksum, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.ChecksumAlg, err = jreadStr(r); err != nil {
		return e, err
	}
	if e.Generation, err = jreadInt64(r); err != nil {
		return e, err
	}
	if e.ParityGroupID, err = jreadStr(r); err != nil {
		return e, err
	}
	stateStr, err := jreadStr(r)
	if err != nil {
		return e, err
	}
	e.State = metadata.ExtentState(stateStr)
	return e, nil
}
