package journal

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestJournalAppendRejectsOverlongStringField(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "journal.log"))
	tooLong := strings.Repeat("x", (1<<16))
	_, err := store.Append(Record{
		TxID:        tooLong,
		State:       StatePrepared,
		Timestamp:   time.Now().UTC(),
		PoolName:    "demo",
		LogicalPath: "/test/file.bin",
	})
	if err == nil {
		t.Fatal("expected Append to reject overlong tx_id")
	}
	if !containsAny(err.Error(), "too long", "tx_id") {
		t.Fatalf("expected tx_id length error, got: %v", err)
	}
}

func TestSaveScrubProgressRejectsOverlongID(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "metadata.bin")
	err := saveScrubProgress(metaPath, ScrubProgress{
		CompletedExtents: []string{strings.Repeat("e", 1<<16)},
		LastUpdated:      time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected saveScrubProgress to reject overlong extent id")
	}
	if !containsAny(err.Error(), "too long", "completed_extents") {
		t.Fatalf("expected scrub progress length error, got: %v", err)
	}
}

func TestSaveRebuildProgressRejectsOverlongID(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "metadata.bin")
	err := saveRebuildProgress(metaPath, RebuildProgress{
		DiskID:           "disk-01",
		CompletedExtents: []string{strings.Repeat("e", 1<<16)},
		LastUpdated:      time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected saveRebuildProgress to reject overlong extent id")
	}
	if !containsAny(err.Error(), "too long", "completed_extents") {
		t.Fatalf("expected rebuild progress length error, got: %v", err)
	}
}

func TestMetadataSaveRejectsOverlongStringField(t *testing.T) {
	store := metadata.NewStore(filepath.Join(t.TempDir(), "metadata.bin"))
	state := metadata.PrototypeState("demo")
	state.Pool.Name = strings.Repeat("p", 1<<16)
	_, err := store.Save(state)
	if err == nil {
		t.Fatal("expected metadata Save to reject overlong pool.name")
	}
	if !strings.Contains(err.Error(), "pool.name") || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("expected pool.name length error, got: %v", err)
	}
}
