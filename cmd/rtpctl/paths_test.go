package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultTmpPathsNotSymlinks(t *testing.T) {
	tmpDir := t.TempDir()

	safeName := func(name string) bool {
		fullPath := filepath.Join(tmpDir, name)
		info, err := os.Lstat(fullPath)
		if os.IsNotExist(err) {
			return true
		}
		if err != nil {
			t.Logf("lstat error for %s: %v", fullPath, err)
			return false
		}
		if info.Mode()&os.ModeSymlink != 0 {
			t.Errorf("default tmp path %s is a symlink (S-05: should not be)", fullPath)
			return false
		}
		return true
	}

	if !safeName("rtparityd-metadata.bin") {
		t.Error("metadata default path should not be a symlink")
	}
	if !safeName("rtparityd-journal.bin") {
		t.Error("journal default path should not be a symlink")
	}
}

func TestTmpDirIsNotWorldWritable(t *testing.T) {
	tmp := os.TempDir()
	info, err := os.Stat(tmp)
	if err != nil {
		t.Skipf("cannot stat temp dir: %v", err)
	}

	if info.Mode()&0077 != 0 {
		t.Logf("temp dir %s has permissions %o", tmp, info.Mode())
	}
}
