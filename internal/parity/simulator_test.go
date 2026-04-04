package parity

import "testing"

func TestRunHealthySimulation(t *testing.T) {
	summary, err := Run(Config{
		DataDisks:       3,
		ExtentCount:     4,
		ExtentSizeBytes: 1024,
		Seed:            11,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !summary.AllExtentsHealthy {
		t.Fatalf("expected healthy extents, got %#v", summary)
	}
	if summary.MismatchesDetected != 0 {
		t.Fatalf("expected 0 mismatches, got %d", summary.MismatchesDetected)
	}
	if !summary.ParityVerified {
		t.Fatal("expected parity verification to succeed")
	}
}

func TestRunRecoversSingleCorruption(t *testing.T) {
	summary, err := Run(Config{
		DataDisks:        3,
		ExtentCount:      4,
		ExtentSizeBytes:  1024,
		Seed:             11,
		InjectCorruption: true,
		CorruptDisk:      1,
		CorruptExtent:    2,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.MismatchesDetected != 1 {
		t.Fatalf("expected 1 mismatch, got %d", summary.MismatchesDetected)
	}
	if summary.RecoveriesPerformed != 1 {
		t.Fatalf("expected 1 recovery, got %d", summary.RecoveriesPerformed)
	}
	if !summary.AllExtentsHealthy {
		t.Fatal("expected all extents healthy after recovery")
	}
	if !summary.ParityVerified {
		t.Fatal("expected parity verification to succeed after recovery")
	}
}

func TestRejectsInvalidConfig(t *testing.T) {
	_, err := Run(Config{DataDisks: 1, ExtentCount: 0, ExtentSizeBytes: 0})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
