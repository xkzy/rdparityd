package journal

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestTrimBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	if _, err := coord.Trim(); err == nil || !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("expected unsupported trim, got %v", err)
	}
}

func TestDefragBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	if _, err := coord.Defrag(); err == nil || !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("expected unsupported defrag, got %v", err)
	}
}

func TestSnapshotBasic(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	if _, err := coord.Snapshot("test-snapshot"); err == nil || !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("expected unsupported snapshot, got %v", err)
	}
}

func TestSnapshotAutoName(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	if _, err := coord.Snapshot(""); err == nil || !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("expected unsupported snapshot, got %v", err)
	}
}

func TestTrimGenericReturnsCommandFailure(t *testing.T) {
	orig := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		return exec.Command("sh", "-c", "exit 17")
	}
	defer func() { execCommand = orig }()

	mountpoint := t.TempDir()
	_, _, err := trimGeneric([]metadata.Disk{{
		Role:       metadata.DiskRoleData,
		Mountpoint: mountpoint,
	}}, t.TempDir())
	if err == nil {
		t.Fatal("expected trimGeneric to return command failure")
	}
	if !strings.Contains(err.Error(), "fstrim") {
		t.Fatalf("expected fstrim failure, got %v", err)
	}
}

func TestDefragNtfsReturnsCommandFailure(t *testing.T) {
	orig := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		return exec.Command("sh", "-c", "exit 23")
	}
	defer func() { execCommand = orig }()

	rootDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	_, _, err := defragNtfs(metadata.SampleState{
		Disks: []metadata.Disk{{
			Mountpoint: t.TempDir(),
		}},
	}, rootDir)
	if err == nil {
		t.Fatal("expected defragNtfs to return command failure")
	}
	if !strings.Contains(err.Error(), "defrag") {
		t.Fatalf("expected defrag failure, got %v", err)
	}
}

func TestWakeDiskRejectsNonDeviceControlPath(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	state := metadata.PrototypeState("demo")
	state.Disks[0].Mountpoint = t.TempDir()
	if _, err := metadata.NewStore(metaPath).Save(state); err != nil {
		t.Fatalf("save metadata: %v", err)
	}

	_, err := coord.WakeDisk(state.Disks[0].DiskID)
	if err == nil {
		t.Fatal("expected WakeDisk to be unsupported")
	}
	if !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("unexpected WakeDisk error: %v", err)
	}
}

func TestGetSleepStatusRejectsNonDeviceControlPath(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	state := metadata.PrototypeState("demo")
	state.Disks[0].Mountpoint = t.TempDir()
	if _, err := metadata.NewStore(metaPath).Save(state); err != nil {
		t.Fatalf("save metadata: %v", err)
	}

	_, err := coord.GetSleepStatus()
	if err == nil {
		t.Fatal("expected GetSleepStatus to be unsupported")
	}
	if !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
		t.Fatalf("unexpected GetSleepStatus error: %v", err)
	}
}

func TestEnableSleepRejectsInvalidValues(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	for _, tc := range []struct {
		timeout   int
		minActive int
	}{
		{timeout: 0, minActive: 0},
		{timeout: -1, minActive: 0},
		{timeout: 10, minActive: -1},
		{timeout: 10, minActive: 11},
	} {
		if err := coord.EnableSleep(tc.timeout, tc.minActive); err == nil || !strings.Contains(err.Error(), ErrUnsupportedAdminOperation.Error()) {
			t.Fatalf("expected unsupported enable sleep timeout=%d minActive=%d, got %v", tc.timeout, tc.minActive, err)
		}
	}
}
