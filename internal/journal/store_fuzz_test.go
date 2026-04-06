package journal

import (
	"io"
	"testing"
)

func FuzzReadRecord(f *testing.F) {
	testcases := [][]byte{
		{},
		{0, 0, 0, 0},
		{0, 0, 0, 1},
		{0x52, 0x54, 0x50, 0x4a, 0x01, 0x00},
		[]byte("RTPJ\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
	}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, err := readRecord(&fuzzReader{data: data})
		if err != nil {
			t.Logf("readRecord rejected: %v", err)
		}
	})
}

type fuzzReader struct {
	data []byte
	pos  int
}

func (r *fuzzReader) Read(p []byte) (n int, err error) {
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
