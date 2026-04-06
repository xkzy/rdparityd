package journal

import (
	"bytes"
	"io"
	"testing"
)

func TestDecompressBombRejection(t *testing.T) {
	t.Run("zstd", func(t *testing.T) { testDecompressBomb(t, CompressionZstd) })
	t.Run("lz4", func(t *testing.T) { testDecompressBomb(t, CompressionLZ4) })
	t.Run("snappy", func(t *testing.T) { testDecompressBomb(t, CompressionSnappy) })
	t.Run("gzip", func(t *testing.T) { testDecompressBomb(t, CompressionGZip) })
	t.Run("xz", func(t *testing.T) { testDecompressBomb(t, CompressionXZ) })
}

func testDecompressBomb(t *testing.T, alg CompressionAlg) {
	tiny := []byte("x")
	compressed, err := compress(tiny, alg)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	decompressed, err := decompress(compressed, alg)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if len(decompressed) > MaxPayloadSize {
		t.Errorf("decompressed size %d exceeds MaxPayloadSize %d", len(decompressed), MaxPayloadSize)
	}
}

func TestDecompressRoundTrip(t *testing.T) {
	algorithms := []CompressionAlg{CompressionZstd, CompressionLZ4, CompressionSnappy, CompressionGZip, CompressionXZ}
	for _, alg := range algorithms {
		alg := alg
		t.Run(string(alg), func(t *testing.T) {
			data := bytes.Repeat([]byte("A"), 1024)
			compressed, err := compress(data, alg)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			decompressed, err := decompress(compressed, alg)
			if err != nil {
				t.Fatalf("decompress failed: %v", err)
			}

			if !bytes.Equal(data, decompressed) {
				t.Errorf("round-trip mismatch")
			}
		})
	}
}

func TestDecompressEmptyData(t *testing.T) {
	result, err := decompress(nil, CompressionNone)
	if err != nil {
		t.Fatalf("decompress(nil, none) failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("decompress(nil, none) returned %d bytes, want 0", len(result))
	}

	result, err = decompress(nil, CompressionZstd)
	if err != nil {
		t.Fatalf("decompress(nil, zstd) failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("decompress(nil, zstd) returned %d bytes, want 0", len(result))
	}
}

func TestCompressEmptyData(t *testing.T) {
	algorithms := []CompressionAlg{CompressionZstd, CompressionLZ4, CompressionGZip, CompressionXZ}
	for _, alg := range algorithms {
		alg := alg
		t.Run(string(alg), func(t *testing.T) {
			result, err := compress(nil, alg)
			if err != nil {
				t.Fatalf("compress(nil) failed: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("compress(nil) returned %d bytes, want 0", len(result))
			}
		})
	}
}

func TestDecompressUnknownAlgorithm(t *testing.T) {
	_, err := decompress([]byte{1, 2, 3, 4}, CompressionAlg("unknown"))
	if err == nil {
		t.Error("decompress with unknown algorithm should fail")
	}
}

func TestDecompressFromLimitedReader(t *testing.T) {
	data := bytes.Repeat([]byte("X"), 1024*1024)
	compressed, err := compress(data, CompressionZstd)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	limited := io.LimitReader(bytes.NewReader(compressed), MaxPayloadSize/2)
	buf := new(bytes.Buffer)
	io.Copy(buf, limited)
	result, err := decompress(buf.Bytes(), CompressionZstd)
	if err != nil {
		t.Fatalf("decompress from limited reader failed: %v", err)
	}

	if len(result) > int(MaxPayloadSize/2) {
		t.Errorf("decompressed from limited reader returned %d bytes, exceeds limit %d", len(result), MaxPayloadSize/2)
	}
}
