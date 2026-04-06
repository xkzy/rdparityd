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

package journal

import (
	"os"
	"testing"
)

func FuzzLoadScrubProgress(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("SCRB"))
	f.Add([]byte("SCRB\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("XXXX"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := t.TempDir() + "/scrub-progress.bin"
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		_, err := loadScrubProgress(path)
		if err != nil {
			t.Logf("loadScrubProgress rejected: %v", err)
		}
	})
}
