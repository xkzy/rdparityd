package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const SnapshotVersion = 1

type SnapshotEnvelope struct {
	Version       int         `json:"version"`
	SavedAt       time.Time   `json:"saved_at"`
	State         SampleState `json:"state"`
	StateChecksum string      `json:"state_checksum"`
}

type Store struct {
	path string
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Save(state SampleState) (SnapshotEnvelope, error) {
	envelope := SnapshotEnvelope{
		Version: SnapshotVersion,
		SavedAt: time.Now().UTC(),
		State:   state,
	}
	envelope.StateChecksum = checksumState(envelope.State)

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("create metadata directory: %w", err)
	}

	tmpPath := s.path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotEnvelope{}, fmt.Errorf("open metadata temp file: %w", err)
	}

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(envelope); err != nil {
		file.Close()
		return SnapshotEnvelope{}, fmt.Errorf("encode metadata snapshot: %w", err)
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

	var envelope SnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return SampleState{}, fmt.Errorf("decode metadata snapshot: %w", err)
	}
	if err := validateEnvelope(envelope); err != nil {
		return SampleState{}, err
	}
	return envelope.State, nil
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

func validateEnvelope(envelope SnapshotEnvelope) error {
	if envelope.Version != SnapshotVersion {
		return fmt.Errorf("unexpected metadata snapshot version %d", envelope.Version)
	}
	if envelope.SavedAt.IsZero() {
		return fmt.Errorf("missing metadata snapshot timestamp")
	}
	if envelope.StateChecksum == "" {
		return fmt.Errorf("missing metadata state checksum")
	}
	if got := checksumState(envelope.State); got != envelope.StateChecksum {
		return fmt.Errorf("metadata snapshot checksum mismatch")
	}
	return nil
}

func checksumState(state SampleState) string {
	data, _ := json.Marshal(state)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

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
	for remaining > 0 {
		allocationLength := extentSize
		if remaining < allocationLength {
			allocationLength = remaining
		}

		diskIndex, err := a.chooseDisk(extents)
		if err != nil {
			return FileRecord{}, nil, err
		}

		extentNumber := len(a.state.Extents) + len(extents) + 1
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
			ChecksumAlg:   "blake3",
			Generation:    1,
			ParityGroupID: fmt.Sprintf("pg-%06d", ((extentNumber-1)/8)+1),
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

func (a *Allocator) chooseDisk(pending []Extent) (int, error) {
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
	for _, extent := range a.state.Extents {
		counts[extent.DataDiskID]++
	}
	for _, extent := range pending {
		counts[extent.DataDiskID]++
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		left := a.state.Disks[eligible[i]]
		right := a.state.Disks[eligible[j]]
		if counts[left.DiskID] == counts[right.DiskID] {
			if left.FreeBytes == right.FreeBytes {
				return left.DiskID < right.DiskID
			}
			return left.FreeBytes > right.FreeBytes
		}
		return counts[left.DiskID] < counts[right.DiskID]
	})

	return eligible[0], nil
}

func extentRelativePath(path string, extentNumber int) string {
	sum := sha256.Sum256([]byte(path))
	prefix := hex.EncodeToString(sum[:2])
	return fmt.Sprintf("data/%s/%s/extent-%06d.bin", prefix[:2], prefix[2:], extentNumber)
}
