/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package metadata

import (
	"fmt"
	"os"
	"path/filepath"
)

// ensureDir creates path if needed and fsyncs every newly created directory and
// its parent so directory creation itself is durable across power loss.
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

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("sync directory: %w", err)
	}
	return d.Close()
}
