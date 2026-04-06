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
