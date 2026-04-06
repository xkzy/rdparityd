package journal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"lukechampine.com/blake3"
)

// ScrubProgress persists scrub checkpoint state so a later scrub can resume
// after interruption without reprocessing already-checked extents/groups.
// The file is binary, checksummed, and atomically replaced on each update.
type ScrubProgress struct {
	Repair                bool
	CompletedExtents      []string
	CompletedParityGroups []string
	LastUpdated           time.Time
}

const (
	scrubProgressMagic = "SCRB"
	scrubProgressHdr   = 53 // magic(4) + flags(1) + extentCount(4) + groupCount(4) + ts(8) + hash(32)
)

func scrubProgressPath(metadataPath string) string {
	return filepath.Join(filepath.Dir(metadataPath), "scrub.progress")
}

func saveScrubProgress(metadataPath string, progress ScrubProgress) error {
	if len(progress.CompletedExtents) > int(^uint32(0)) || len(progress.CompletedParityGroups) > int(^uint32(0)) {
		return fmt.Errorf("scrub progress count exceeds uint32 format limit")
	}
	for i, id := range progress.CompletedExtents {
		if len([]byte(id)) > 1<<16-1 {
			return fmt.Errorf("completed_extents[%d] too long for scrub progress encoding: %d bytes > %d", i, len([]byte(id)), 1<<16-1)
		}
	}
	for i, id := range progress.CompletedParityGroups {
		if len([]byte(id)) > 1<<16-1 {
			return fmt.Errorf("completed_parity_groups[%d] too long for scrub progress encoding: %d bytes > %d", i, len([]byte(id)), 1<<16-1)
		}
	}
	var payload []byte
	for _, id := range progress.CompletedExtents {
		b := []byte(id)
		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(b)))
		payload = append(payload, lenBuf[:]...)
		payload = append(payload, b...)
	}
	for _, id := range progress.CompletedParityGroups {
		b := []byte(id)
		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(b)))
		payload = append(payload, lenBuf[:]...)
		payload = append(payload, b...)
	}

	hdr := make([]byte, scrubProgressHdr)
	copy(hdr[0:4], scrubProgressMagic)
	if progress.Repair {
		hdr[4] = 0x01
	}
	binary.BigEndian.PutUint32(hdr[5:9], uint32(len(progress.CompletedExtents)))
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(progress.CompletedParityGroups)))
	ns := progress.LastUpdated.UnixNano()
	if ns < 0 {
		ns = 0
	}
	binary.BigEndian.PutUint64(hdr[13:21], uint64(ns))
	hash := blake3.Sum256(payload)
	copy(hdr[21:53], hash[:])

	return replaceSyncFile(scrubProgressPath(metadataPath), append(hdr, payload...), 0o600)
}

func loadScrubProgress(metadataPath string) (ScrubProgress, error) {
	path := scrubProgressPath(metadataPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ScrubProgress{}, nil
		}
		return ScrubProgress{}, fmt.Errorf("read scrub progress: %w", err)
	}
	if len(data) < scrubProgressHdr {
		return ScrubProgress{}, fmt.Errorf("scrub progress file too short")
	}
	if string(data[0:4]) != scrubProgressMagic {
		return ScrubProgress{}, fmt.Errorf("scrub progress: invalid magic")
	}
	repair := data[4]&0x01 == 0x01
	extentCount := int(binary.BigEndian.Uint32(data[5:9]))
	groupCount := int(binary.BigEndian.Uint32(data[9:13]))
	ts := int64(binary.BigEndian.Uint64(data[13:21]))
	storedHash := data[21:53]
	payload := data[53:]
	computed := blake3.Sum256(payload)
	if !bytes.Equal(computed[:], storedHash) {
		return ScrubProgress{}, fmt.Errorf("scrub progress: checksum mismatch")
	}

	r := bytes.NewReader(payload)
	progress := ScrubProgress{Repair: repair}
	if ts > 0 {
		progress.LastUpdated = time.Unix(0, ts).UTC()
	}
	progress.CompletedExtents = make([]string, 0, extentCount)
	for i := 0; i < extentCount; i++ {
		var l uint16
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return ScrubProgress{}, fmt.Errorf("scrub progress: truncated extent length")
		}
		b := make([]byte, int(l))
		if _, err := io.ReadFull(r, b); err != nil {
			return ScrubProgress{}, fmt.Errorf("scrub progress: truncated extent id")
		}
		progress.CompletedExtents = append(progress.CompletedExtents, string(b))
	}
	progress.CompletedParityGroups = make([]string, 0, groupCount)
	for i := 0; i < groupCount; i++ {
		var l uint16
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return ScrubProgress{}, fmt.Errorf("scrub progress: truncated parity length")
		}
		b := make([]byte, int(l))
		if _, err := io.ReadFull(r, b); err != nil {
			return ScrubProgress{}, fmt.Errorf("scrub progress: truncated parity id")
		}
		progress.CompletedParityGroups = append(progress.CompletedParityGroups, string(b))
	}
	return progress, nil
}

func deleteScrubProgress(metadataPath string) {
	_ = removeSync(scrubProgressPath(metadataPath))
}
