package journal

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestIsMetadataTransaction(t *testing.T) {
	cases := []struct {
		record Record
		want   bool
	}{
		{
			record: Record{LogicalPath: metadataPathPrefixScrub + "run-1"},
			want:   true,
		},
		{
			record: Record{LogicalPath: metadataPathPrefixRebuild + "disk-01"},
			want:   true,
		},
		{
			record: Record{
				LogicalPath: metadataPathPrefixScrub + "run-1",
				File:        &metadata.FileRecord{FileID: "f1"},
			},
			want: false,
		},
		{
			record: Record{
				LogicalPath: metadataPathPrefixScrub + "run-1",
				Extents:     []metadata.Extent{{ExtentID: "e1"}},
			},
			want: false,
		},
		{
			record: Record{LogicalPath: "/data/file.txt"},
			want:   false,
		},
	}

	for _, tc := range cases {
		got := isMetadataTransaction(tc.record)
		if got != tc.want {
			t.Errorf("isMetadataTransaction(%q) = %v, want %v", tc.record.LogicalPath, got, tc.want)
		}
	}
}

func TestMetadataKind(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{metadataPathPrefixScrub + "run-1", "scrub"},
		{metadataPathPrefixRebuild + "disk-01", "rebuild"},
		{"/data/file.txt", ""},
	}
	for _, tc := range cases {
		record := Record{LogicalPath: tc.path}
		got := metadataKind(record)
		if got != tc.want {
			t.Errorf("metadataKind(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestScrubTransactionPath(t *testing.T) {
	got := scrubTransactionPath("run-99")
	if !strings.HasPrefix(got, metadataPathPrefixScrub) {
		t.Fatalf("expected prefix %q, got %q", metadataPathPrefixScrub, got)
	}
	if !strings.HasSuffix(got, "run-99") {
		t.Fatalf("expected suffix run-99, got %q", got)
	}
}

func TestRebuildTransactionPath(t *testing.T) {
	got := rebuildTransactionPath("disk-02")
	if !strings.HasPrefix(got, metadataPathPrefixRebuild) {
		t.Fatalf("expected prefix %q, got %q", metadataPathPrefixRebuild, got)
	}
	if !strings.HasSuffix(got, "disk-02") {
		t.Fatalf("expected suffix disk-02, got %q", got)
	}
}

func TestLogScrubStartAndComplete(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.bin")
	store := NewStore(journalPath)

	if err := logScrubStart(store, "demo", "run-1"); err != nil {
		t.Fatalf("logScrubStart returned error: %v", err)
	}
	if err := logScrubComplete(store, "demo", "run-1"); err != nil {
		t.Fatalf("logScrubComplete returned error: %v", err)
	}

	records, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if !strings.Contains(records[0].LogicalPath, "run-1") {
		t.Fatalf("unexpected path in start record: %q", records[0].LogicalPath)
	}
	if records[0].State != StatePrepared {
		t.Fatalf("expected prepared state, got %q", records[0].State)
	}
	if records[1].State != StateCommitted {
		t.Fatalf("expected committed state, got %q", records[1].State)
	}
}

func TestLogRebuildStartAndComplete(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.bin")
	store := NewStore(journalPath)

	if err := logRebuildStart(store, "demo", "disk-01"); err != nil {
		t.Fatalf("logRebuildStart returned error: %v", err)
	}
	if err := logRebuildComplete(store, "demo", "disk-01"); err != nil {
		t.Fatalf("logRebuildComplete returned error: %v", err)
	}

	records, err := store.Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if !strings.Contains(records[0].LogicalPath, "disk-01") {
		t.Fatalf("unexpected path in start record: %q", records[0].LogicalPath)
	}
}

func TestRollForwardMetadataTransactionScrubComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bin")
	state := metadata.PrototypeState("demo")
	now := time.Now().UTC()
	records := []Record{
		{
			TxID:        "tx-scrub-1",
			Timestamp:   now,
			LogicalPath: scrubTransactionPath("run-1"),
			State:       StatePrepared,
		},
		{
			TxID:        "tx-scrub-1-complete",
			Timestamp:   now,
			LogicalPath: scrubTransactionPath("run-1"),
			State:       StateCommitted,
		},
	}

	if err := rollForwardMetadataTransaction(path, &state, records); err != nil {
		t.Fatalf("rollForwardMetadataTransaction returned error: %v", err)
	}
}

func TestRollForwardMetadataTransactionScrubIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bin")
	state := metadata.PrototypeState("demo")
	now := time.Now().UTC()
	records := []Record{
		{
			TxID:        "tx-scrub-2",
			Timestamp:   now,
			LogicalPath: scrubTransactionPath("run-2"),
			State:       StatePrepared,
		},
	}

	err := rollForwardMetadataTransaction(path, &state, records)
	if err == nil {
		t.Fatal("expected error for incomplete scrub transaction")
	}
}

func TestRollForwardMetadataTransactionRebuildComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bin")
	state := metadata.PrototypeState("demo")
	now := time.Now().UTC()
	records := []Record{
		{
			TxID:        "tx-rebuild-1",
			Timestamp:   now,
			LogicalPath: rebuildTransactionPath("disk-01"),
			State:       StatePrepared,
		},
		{
			TxID:        "tx-rebuild-1-done",
			Timestamp:   now,
			LogicalPath: rebuildTransactionPath("disk-01"),
			State:       StateCommitted,
		},
	}

	if err := rollForwardMetadataTransaction(path, &state, records); err != nil {
		t.Fatalf("rollForwardMetadataTransaction returned error: %v", err)
	}
}

func TestRollForwardMetadataTransactionRebuildIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bin")
	state := metadata.PrototypeState("demo")
	now := time.Now().UTC()
	records := []Record{
		{
			TxID:        "tx-rebuild-2",
			Timestamp:   now,
			LogicalPath: rebuildTransactionPath("disk-02"),
			State:       StatePrepared,
		},
	}

	err := rollForwardMetadataTransaction(path, &state, records)
	if err == nil {
		t.Fatal("expected error for incomplete rebuild transaction")
	}
}

func TestRollForwardMetadataTransactionNilState(t *testing.T) {
	err := rollForwardMetadataTransaction("/tmp/meta.bin", nil, []Record{{}})
	if err == nil {
		t.Fatal("expected error for nil state")
	}
}

func TestRollForwardMetadataTransactionEmptyRecords(t *testing.T) {
	state := metadata.PrototypeState("demo")
	err := rollForwardMetadataTransaction("/tmp/meta.bin", &state, nil)
	if err == nil {
		t.Fatal("expected error for empty records")
	}
}

func TestRollForwardMetadataTransactionUnknownKind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bin")
	state := metadata.PrototypeState("demo")
	records := []Record{
		{
			TxID:        "tx-unknown",
			LogicalPath: "/data/file.txt", // not a metadata transaction
			State:       StateCommitted,
			File:        nil,
			Extents:     nil,
		},
	}

	err := rollForwardMetadataTransaction(path, &state, records)
	if err == nil {
		t.Fatal("expected error for non-metadata transaction record")
	}
}
