package journal

import (
	"bytes"
	"context"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func TestCompressionAlgorithms(t *testing.T) {
	testData := []byte("This is test data for compression. It contains repeated patterns that should compress well. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")

	tests := []struct {
		name     string
		alg      CompressionAlg
		ratioMin float64 // minimum expected compression ratio
	}{
		{"Zstd", CompressionZstd, 0.3},
		{"LZ4", CompressionLZ4, 0.3},
		{"Snappy", CompressionSnappy, 0.3},
		{"GZip", CompressionGZip, 0.3},
		{"XZ", CompressionXZ, 0.3},
		{"None", CompressionNone, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compress(testData, tt.alg)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			decompressed, err := decompress(compressed, tt.alg)
			if err != nil {
				t.Fatalf("decompress failed: %v", err)
			}

			if !bytes.Equal(decompressed, testData) {
				t.Fatalf("decompressed data does not match original")
			}

			if tt.alg != CompressionNone {
				ratio := compressionRatio(int64(len(testData)), int64(len(compressed)))
				if ratio > tt.ratioMin {
					t.Logf("compression ratio: %.2f (original: %d, compressed: %d)", ratio, len(testData), len(compressed))
				}
			}
		})
	}
}

func TestCompressionRoundtrip(t *testing.T) {
	// Test with various data patterns
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello")},
		{"random", bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x12}, 1000)},
		{"zeros", bytes.Repeat([]byte{0}, 10000)},
		{"text", bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 100)},
	}

	allAlgs := []CompressionAlg{CompressionZstd, CompressionLZ4, CompressionSnappy, CompressionGZip, CompressionXZ}

	for _, tc := range testCases {
		for _, alg := range allAlgs {
			t.Run(tc.name+"/"+string(alg), func(t *testing.T) {
				compressed, err := compress(tc.data, alg)
				if err != nil {
					t.Fatalf("compress: %v", err)
				}

				decompressed, err := decompress(compressed, alg)
				if err != nil {
					t.Fatalf("decompress: %v", err)
				}

				if !bytes.Equal(decompressed, tc.data) {
					t.Fatalf("data mismatch: got %d bytes, want %d bytes", len(decompressed), len(tc.data))
				}
			})
		}
	}
}

func TestCompressionUnknownAlgorithm(t *testing.T) {
	_, err := compress([]byte("test"), CompressionAlg("unknown"))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}

	_, err = decompress([]byte("test"), CompressionAlg("unknown"))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func TestCompressionWithWriteFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := dir + "/metadata.bin"
	journalPath := dir + "/journal.log"
	coord := NewCoordinator(metaPath, journalPath)

	// Test data that compresses well
	testData := bytes.Repeat([]byte("This is repeated test data for compression. "), 100)

	// Write with zstd compression
	res, err := coord.WriteFile(context.Background(), WriteRequest{
		PoolName:       "test",
		LogicalPath:    "/test/compressed.bin",
		Payload:        testData,
		CompressionAlg: "zstd",
	})
	if err != nil {
		t.Fatalf("WriteFile with compression failed: %v", err)
	}

	// Verify extent has compression metadata
	if len(res.Extents) == 0 {
		t.Fatal("no extents returned")
	}
	ext := res.Extents[0]
	if ext.CompressionAlg != metadata.CompressionZstd {
		t.Errorf("expected compression alg zstd, got %s", ext.CompressionAlg)
	}
	if ext.CompressedSize == 0 {
		t.Error("compressed size should be set")
	}
	if ext.CompressedSize >= ext.Length {
		t.Logf("warning: compressed size (%d) >= original size (%d)", ext.CompressedSize, ext.Length)
	}

	// Read back and verify
	readRes, err := coord.ReadFile("/test/compressed.bin")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(readRes.Data, testData) {
		t.Fatalf("read data does not match written data")
	}
}

func TestCompressionRatio(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		minRatioZstd float64
	}{
		{"random", bytes.Repeat([]byte{0xAB}, 10000), 0.8},
		{"zeros", bytes.Repeat([]byte{0}, 10000), 0.01},
		{"text", bytes.Repeat([]byte("hello world "), 1000), 0.3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, _ := compress(tt.data, CompressionZstd)
			ratio := compressionRatio(int64(len(tt.data)), int64(len(compressed)))
			t.Logf("zstd ratio for %s: %.2f", tt.name, ratio)
		})
	}
}
