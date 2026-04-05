package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ensureDir creates path if needed and fsyncs every newly created directory and
// its parent so directory creation itself survives power loss.
func ensureDir(path string, perm os.FileMode) error {
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return nil
	}

	info, err := os.Stat(clean)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("path exists but is not a directory: %s", clean)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("stat directory %s: %w", clean, err)
	}

	missing := make([]string, 0)
	probe := clean
	for {
		info, statErr := os.Stat(probe)
		if statErr == nil {
			if !info.IsDir() {
				return fmt.Errorf("path component exists but is not a directory: %s", probe)
			}
			break
		}
		if !os.IsNotExist(statErr) {
			return fmt.Errorf("stat directory component %s: %w", probe, statErr)
		}
		missing = append(missing, probe)
		parent := filepath.Dir(probe)
		if parent == probe {
			break
		}
		probe = parent
	}

	if err := os.MkdirAll(clean, perm); err != nil {
		return fmt.Errorf("mkdir all %s: %w", clean, err)
	}

	for i := len(missing) - 1; i >= 0; i-- {
		dir := missing[i]
		parent := filepath.Dir(dir)
		if parent != dir {
			if err := syncDir(parent); err != nil {
				return fmt.Errorf("sync parent directory %s: %w", parent, err)
			}
		}
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync created directory %s: %w", dir, err)
		}
	}
	return nil
}

// replaceSyncFile atomically replaces path with data using write-to-temp,
// fsync(temp), rename, and fsync(parent directory). It is safe for both new
// files and overwrites of existing files.
func replaceSyncFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := ensureDir(dir, 0o755); err != nil {
		return fmt.Errorf("ensure parent directory for %s: %w", path, err)
	}

	tmpPath := filepath.Join(dir, fmt.Sprintf(".%s.tmp-%d-%d", filepath.Base(path), os.Getpid(), time.Now().UnixNano()))
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return fmt.Errorf("open temp file %s: %w", tmpPath, err)
	}

	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := f.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write temp file %s: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temp file %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp file into place %s: %w", path, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync parent directory for %s: %w", path, err)
	}
	return nil
}

// removeSync removes path and fsyncs the parent directory so deletion is
// durable. Missing files are treated as already removed.
func removeSync(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync parent directory after remove %s: %w", path, err)
	}
	return nil
}
