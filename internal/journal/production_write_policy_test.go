package journal

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWriteFileRejectsImplicitSyntheticPayload(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	_, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/prod/reject.bin",
		SizeBytes:   4096,
	})
	if err == nil {
		t.Fatal("expected implicit synthetic write to be rejected")
	}
	if got := err.Error(); got == "" || !bytes.Contains([]byte(got), []byte("AllowSynthetic")) {
		t.Fatalf("expected AllowSynthetic guidance in error, got: %v", err)
	}
}

func TestWriteFileAllowsExplicitSyntheticOptIn(t *testing.T) {
	dir := t.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))

	result, err := coord.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/prod/explicit-synth.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("expected explicit synthetic write to succeed, got: %v", err)
	}
	if result.FinalState != StateCommitted {
		t.Fatalf("expected committed state, got %q", result.FinalState)
	}
}
