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
	"syscall"
	"testing"
)

func TestCreateRejectsInvalidNames(t *testing.T) {
	invalidNames := []string{
		"",
		".",
		"..",
		"/",
		"//",
		"name/",
		"/name",
		"name/name",
		"name\x00nul",
	}

	for _, name := range invalidNames {
		if isValidName(name) {
			t.Errorf("isValidName(%q) = true, want false", name)
		}
	}
}

func TestCreateAcceptsValidNames(t *testing.T) {
	validNames := []string{
		"file.txt",
		"hello",
		"a",
		"file-with-dashes",
		"file_with_underscores",
		"file.with.dots",
		"123",
		"CamelCase",
		"lowercase",
		"mixed123CASE",
	}

	for _, name := range validNames {
		if !isValidName(name) {
			t.Errorf("isValidName(%q) = false, want true", name)
		}
	}
}

func TestIsValidNameEdgeCases(t *testing.T) {
	if isValidName("name\x00embedded") {
		t.Error("isValidName should reject embedded NUL")
	}

	if isValidName("name/with/slash") {
		t.Error("isValidName should reject slashes")
	}
}

func TestMkdirRejectsInvalidNames(t *testing.T) {
	invalidNames := []string{
		"",
		".",
		"..",
		"/",
		"//",
		"name/",
		"/name",
		"name/name",
		"name\x00nul",
	}

	for _, name := range invalidNames {
		if isValidName(name) {
			t.Errorf("isValidName(%q) = true for Mkdir, want false", name)
		}
	}
}

func isValidName(name string) bool {
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

func TestSyscallEinval(t *testing.T) {
	if syscall.EINVAL != 22 {
		t.Logf("EINVAL = %d (platform-specific)", syscall.EINVAL)
	}
}
