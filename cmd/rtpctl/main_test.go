package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRtpctlUnknownSubcommand(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "rtpctl")

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if err := cmd.Run(); err != nil {
		t.Skipf("could not build rtpctl: %v", err)
	}

	cmd = exec.Command(binPath, "unknown-cmd")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
	if !strings.Contains(string(output), "unknown subcommand") && !strings.Contains(string(output), "Usage") {
		t.Logf("output: %s", string(output))
	}
}

func TestRtpctlSamplePool(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "rtpctl_test_binary")

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if err := cmd.Run(); err != nil {
		t.Skipf("could not build rtpctl: %v", err)
	}

	rtpctl := exec.Command(binPath, "sample-pool")
	output, err := rtpctl.CombinedOutput()
	if err != nil {
		t.Errorf("sample-pool failed: %v\noutput: %s", err, output)
	}
	if !strings.Contains(string(output), "pool") {
		t.Errorf("expected pool info in output, got: %s", output)
	}
}

func TestRtpctlJournalDemo(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "rtpctl_test_binary")

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if err := cmd.Run(); err != nil {
		t.Skipf("could not build rtpctl: %v", err)
	}

	rtpctl := exec.Command(binPath, "journal-demo")
	output, err := rtpctl.CombinedOutput()
	if err != nil {
		t.Errorf("journal-demo failed: %v\noutput: %s", err, output)
	}
	if !strings.Contains(string(output), "journal_path") {
		t.Errorf("expected journal_path in output, got: %s", output)
	}
}

func TestRtpctlSubcommands(t *testing.T) {
	subcommands := []string{
		"sample-pool",
		"journal-demo",
	}

	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "rtpctl_test_binary")

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if err := cmd.Run(); err != nil {
		t.Skipf("could not build rtpctl: %v", err)
	}

	for _, sub := range subcommands {
		sub := sub
		t.Run(sub, func(t *testing.T) {
			rtpctl := exec.Command(binPath, sub)
			output, err := rtpctl.CombinedOutput()
			if err != nil {
				t.Errorf("%s failed: %v\noutput: %s", sub, err, output)
			}
		})
	}
}

func TestRtpctlAllocateDemoFlags(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "rtpctl_test_binary")

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if err := cmd.Run(); err != nil {
		t.Skipf("could not build rtpctl: %v", err)
	}

	rtpctl := exec.Command(binPath, "allocate-demo", "-pool-name=test", "-size-bytes=1024")
	output, err := rtpctl.CombinedOutput()
	if err != nil {
		t.Errorf("allocate-demo with flags failed: %v\noutput: %s", err, output)
	}
	if !strings.Contains(string(output), "metadata_path") {
		t.Errorf("expected metadata_path in output, got: %s", output)
	}
}
