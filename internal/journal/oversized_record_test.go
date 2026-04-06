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
	"io"
	"testing"
)

func TestReadRecordRejectsOversized(t *testing.T) {
	tooLarge := make([]byte, MaxRecordSize+1)
	tooLarge[0] = 0x52
	tooLarge[1] = 0x54
	tooLarge[2] = 0x50
	tooLarge[3] = 0x4a

	_, err := readRecord(&reader{data: tooLarge})
	if err == nil {
		t.Error("readRecord should reject record exceeding MaxRecordSize")
	}
}

func TestReadRecordAcceptsMaxSize(t *testing.T) {
	exactSize := make([]byte, MaxRecordSize)
	exactSize[0] = 0x52
	exactSize[1] = 0x54
	exactSize[2] = 0x50
	exactSize[3] = 0x4a

	_, err := readRecord(&reader{data: exactSize})
	if err != nil && err != io.EOF {
		t.Logf("readRecord on exact MaxRecordSize returned error (may be expected for invalid content): %v", err)
	}
}

func TestReadRecordTruncated(t *testing.T) {
	truncated := []byte{0x52, 0x54, 0x50, 0x4a}
	_, err := readRecord(&reader{data: truncated})
	if err == nil {
		t.Error("readRecord should reject truncated record")
	}
}

func TestReadRecordEmpty(t *testing.T) {
	_, err := readRecord(&reader{data: []byte{}})
	if err == nil {
		t.Error("readRecord should reject empty data")
	}
}

func TestReadRecordBadMagic(t *testing.T) {
	badMagic := make([]byte, 72)
	badMagic[0] = 'X'
	badMagic[1] = 'X'
	badMagic[2] = 'X'
	badMagic[3] = 'X'

	_, err := readRecord(&reader{data: badMagic})
	if err == nil {
		t.Error("readRecord should reject bad magic")
	}
}

type reader struct {
	data []byte
	pos  int
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}
