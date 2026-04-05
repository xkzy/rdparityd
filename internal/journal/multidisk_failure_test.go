package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestValidateRecoverabilityInvariantsHealthyPool(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("recoverability-"), 65536)
	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/data/test.bin",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	violations := ValidateRecoverabilityInvariants(dir, state)
	if len(violations) != 0 {
		t.Fatalf("expected no violations for healthy pool, got: %v", violations)
	}
}

func TestValidateRecoverabilityInvariantsUnrecoverableDualFailure(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("dual-failure-"), 65536)
	writeResult, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/data/test.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(writeResult.Extents) == 0 {
		t.Skip("no extents to test dual failure")
	}

	// Remove extent file AND parity file to simulate unrecoverable dual failure.
	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("Remove extent returned error: %v", err)
	}
	if len(state.ParityGroups) > 0 {
		pg := state.ParityGroups[0]
		parityPath := filepath.Join(dir, "parity", pg.ParityGroupID+".bin")
		if err := os.Remove(parityPath); err != nil {
			t.Fatalf("Remove parity returned error: %v", err)
		}
	}

	violations := ValidateRecoverabilityInvariants(dir, state)
	if len(violations) == 0 {
		t.Fatal("expected violations for unrecoverable dual failure")
	}
}

func TestAnalyzeMultiDiskFailuresNoFailures(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("no-failure-"), 65536)
	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/data/test.bin",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(dir, state)
	if len(analysis.FailedDisks) != 0 {
		t.Fatalf("expected no failed disks, got: %v", analysis.FailedDisks)
	}
	if !analysis.RecoveryIsPossible {
		t.Fatal("expected RecoveryIsPossible=true for healthy pool")
	}
}

func TestAnalyzeMultiDiskFailuresSingleDiskFailure(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "metadata.bin")
	journalPath := filepath.Join(dir, "journal.bin")
	coord := NewCoordinator(metaPath, journalPath)

	payload := bytes.Repeat([]byte("single-disk-failure-"), 65536)
	writeResult, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/data/test.bin",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if len(writeResult.Extents) == 0 {
		t.Skip("no extents to test single disk failure")
	}

	// Remove only extent files (leave parity intact).
	extentPath := filepath.Join(dir, writeResult.Extents[0].PhysicalLocator.RelativePath)
	if err := os.Remove(extentPath); err != nil {
		t.Fatalf("Remove extent returned error: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	analysis := AnalyzeMultiDiskFailures(dir, state)
	if len(analysis.FailedDisks) != 1 {
		t.Fatalf("expected 1 failed disk, got: %v", analysis.FailedDisks)
	}
	if !analysis.RecoveryIsPossible {
		t.Fatal("expected RecoveryIsPossible=true for single disk failure")
	}
	if analysis.FailureMode != "single" {
		t.Fatalf("expected failure mode %q, got %q", "single", analysis.FailureMode)
	}
}
