package journal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// FormatValidationResult collects all format validation findings.
// A non-empty Errors slice means the pool cannot be safely opened.
type FormatValidationResult struct {
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

func (r *FormatValidationResult) ok() bool { return len(r.Errors) == 0 }

func (r *FormatValidationResult) errorf(format string, args ...any) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

func (r *FormatValidationResult) warnf(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// ValidateOnDiskFormats performs an end-to-end format validation of every
// binary file in the pool rooted at rootDir.  It checks:
//
//   - Metadata snapshot: magic, version, BLAKE3 checksum (via metadata.Store.Load)
//   - Journal: magic, version, per-record BLAKE3 checksums (via journal.Store.Load)
//   - Extent files: file present for every metadata-registered extent; checksum matches
//   - Parity files: file present for every parity group; checksum matches
//   - Rebuild progress files: magic "RBLD", length, BLAKE3 checksum
//
// This function is intended to be called on startup before any write path is
// opened, so that silent corruption is caught early.
func ValidateOnDiskFormats(rootDir string, metadataPath string, journalPath string) FormatValidationResult {
	r := FormatValidationResult{
		Errors:   []string{},
		Warnings: []string{},
	}

	// ── 1. Metadata snapshot ────────────────────────────────────────────────
	state, err := validateMetadataFormat(metadataPath, &r)
	if err != nil {
		// No usable state — cannot validate extents or parity.
		return r
	}

	// ── 2. Journal ──────────────────────────────────────────────────────────
	validateJournalFormat(journalPath, &r)

	// ── 3. Extent files ─────────────────────────────────────────────────────
	validateExtentFiles(rootDir, state, &r)

	// ── 4. Parity files ─────────────────────────────────────────────────────
	validateParityFiles(rootDir, state, &r)

	// ── 5. Rebuild progress files ───────────────────────────────────────────
	validateRebuildProgressFiles(rootDir, metadataPath, state, &r)

	return r
}

func validateMetadataFormat(metadataPath string, r *FormatValidationResult) (metadata.SampleState, error) {
	f, err := os.Open(metadataPath)
	if os.IsNotExist(err) {
		r.warnf("metadata file does not exist yet at %s; pool may be uninitialized", metadataPath)
		return metadata.SampleState{}, err
	}
	if err != nil {
		r.errorf("cannot open metadata file %s: %v", metadataPath, err)
		return metadata.SampleState{}, err
	}
	defer f.Close()

	// Probe magic and version from raw bytes before delegating to the store.
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		r.errorf("metadata file %s too short to read header: %v", metadataPath, err)
		return metadata.SampleState{}, err
	}
	if string(hdr[0:4]) != "RTPM" {
		r.errorf("metadata file %s has wrong magic %q (expected \"RTPM\")", metadataPath, string(hdr[0:4]))
		return metadata.SampleState{}, fmt.Errorf("wrong magic")
	}
	version := binary.BigEndian.Uint16(hdr[4:6])
	if version != metadata.SnapshotVersion {
		r.errorf("metadata file %s has unsupported version %d (expected %d)", metadataPath, version, metadata.SnapshotVersion)
		return metadata.SampleState{}, fmt.Errorf("unsupported version")
	}

	// Full load with checksum verification.
	state, err := metadata.NewStore(metadataPath).Load()
	if err != nil {
		r.errorf("metadata checksum/decode failure in %s: %v", metadataPath, err)
		return metadata.SampleState{}, err
	}
	return state, nil
}

func validateJournalFormat(journalPath string, r *FormatValidationResult) {
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		r.warnf("journal file does not exist at %s; will be created on first write", journalPath)
		return
	}

	f, err := os.Open(journalPath)
	if err != nil {
		r.errorf("cannot open journal file %s: %v", journalPath, err)
		return
	}
	defer f.Close()

	// Peek at the first record's magic.
	var recLen [4]byte
	if _, err := io.ReadFull(f, recLen[:]); err != nil {
		if err == io.EOF {
			r.warnf("journal file %s is empty (no records)", journalPath)
			return
		}
		r.errorf("journal file %s: cannot read first record length: %v", journalPath, err)
		return
	}
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		r.errorf("journal file %s: cannot read magic bytes: %v", journalPath, err)
		return
	}
	if string(magic[:]) != RecordMagic {
		r.errorf("journal file %s has wrong magic %q (expected %q)", journalPath, string(magic[:]), RecordMagic)
		return
	}

	// Full load with per-record BLAKE3 verification.
	records, err := NewStore(journalPath).Load()
	if err != nil {
		r.errorf("journal record checksum/decode failure in %s: %v", journalPath, err)
		return
	}

	// Check for lingering replay-required records — these are not fatal but
	// indicate a crashed recovery that was never completed.
	for _, rec := range records {
		if rec.State == StateReplayRequired {
			r.warnf("journal %s contains replay-required record for tx %s — run Recover() before opening for writes", journalPath, rec.TxID)
		}
	}
}

func validateExtentFiles(rootDir string, state metadata.SampleState, r *FormatValidationResult) {
	for _, extent := range state.Extents {
		if extent.Checksum == "" {
			continue // not yet committed; skip
		}
		path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(path)
		if err != nil {
			r.errorf("extent %s: file missing or unreadable at %s: %v", extent.ExtentID, path, err)
			continue
		}
		normalized := normalizeExtentLength(data, extent.Length)
		got := digestBytes(normalized)
		if got != extent.Checksum {
			r.errorf("extent %s: checksum mismatch at %s: stored=%s disk=%s",
				extent.ExtentID, path, extent.Checksum, got)
		}
	}
}

func validateParityFiles(rootDir string, state metadata.SampleState, r *FormatValidationResult) {
	for _, group := range state.ParityGroups {
		if group.ParityChecksum == "" {
			continue
		}
		path := filepath.Join(rootDir, "parity", group.ParityGroupID+".bin")
		data, err := os.ReadFile(path)
		if err != nil {
			r.errorf("parity group %s: file missing or unreadable at %s: %v", group.ParityGroupID, path, err)
			continue
		}
		got := digestBytes(data)
		if got != group.ParityChecksum {
			r.errorf("parity group %s: checksum mismatch at %s: stored=%s disk=%s",
				group.ParityGroupID, path, group.ParityChecksum, got)
		}
	}
}

func validateRebuildProgressFiles(rootDir string, metadataPath string, state metadata.SampleState, r *FormatValidationResult) {
	// Discover any rebuild progress files by scanning for RBLD-magic files
	// in the directory that holds metadata. Progress files are named
	// rebuild-progress-<sanitized-disk-id>.bin.
	metaDir := filepath.Dir(metadataPath)
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		r.warnf("cannot list metadata directory %s to check rebuild progress files: %v", metaDir, err)
		return
	}

	diskIDs := make(map[string]bool)
	for _, disk := range state.Disks {
		diskIDs[disk.DiskID] = true
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "rebuild-progress-") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		path := filepath.Join(metaDir, entry.Name())
		if err := validateRebuildProgressFile(path); err != nil {
			r.errorf("rebuild progress file %s: %v", path, err)
		}
	}
}

func validateRebuildProgressFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read file: %v", err)
	}
	if len(data) < rebuildProgressHdr {
		return fmt.Errorf("file too short: %d bytes (need at least %d)", len(data), rebuildProgressHdr)
	}
	if string(data[0:4]) != rebuildProgressMagic {
		return fmt.Errorf("wrong magic %q (expected %q)", string(data[0:4]), rebuildProgressMagic)
	}
	count := binary.BigEndian.Uint32(data[4:8])
	payload := data[rebuildProgressHdr:]

	// Verify BLAKE3 checksum stored in [16:48].
	var storedHash [32]byte
	copy(storedHash[:], data[16:48])

	gotHash := digestBytes(payload)
	storedHex := fmt.Sprintf("%x", storedHash)
	if gotHash != storedHex {
		return fmt.Errorf("checksum mismatch: stored=%s disk=%s", storedHex, gotHash)
	}

	// Rough sanity: can we decode `count` extent IDs from the payload?
	buf := payload
	for i := uint32(0); i < count; i++ {
		if len(buf) < 2 {
			return fmt.Errorf("payload truncated at extent ID %d of %d", i, count)
		}
		idLen := binary.BigEndian.Uint16(buf[0:2])
		buf = buf[2:]
		if int(idLen) > len(buf) {
			return fmt.Errorf("extent ID %d claims length %d but only %d bytes remain", i, idLen, len(buf))
		}
		buf = buf[idLen:]
	}
	return nil
}
