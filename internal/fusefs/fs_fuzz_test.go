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

package fusefs

import (
	"strings"
	"testing"
)

func FuzzChildName(f *testing.F) {
	f.Add("valid_name")
	f.Add("name/with/slash")
	f.Add("name\x00with\x00nul")
	f.Add("")
	f.Add(".")
	f.Add("..")
	f.Add("/absolute")
	f.Add("name/..")
	f.Add("..//name")

	f.Fuzz(func(t *testing.T, name string) {
		valid := fuzzIsValidName(name)
		if valid {
			if name == "" || name == "." || name == ".." {
				t.Errorf("fuzzIsValidName(%q) = true but name is reserved", name)
			}
			if strings.ContainsRune(name, '/') {
				t.Errorf("fuzzIsValidName(%q) = true but name contains slash", name)
			}
			if strings.Contains(name, "\x00") {
				t.Errorf("fuzzIsValidName(%q) = true but name contains NUL", name)
			}
		}
	})
}

func fuzzIsValidName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsRune(name, '/') {
		return false
	}
	if strings.Contains(name, "\x00") {
		return false
	}
	return true
}
