package journal

import (
	"bytes"
	"testing"
)

func FuzzDecompressRoundTrip(f *testing.F) {
	seedData := []byte("hello world hello world hello world")
	f.Add(seedData, uint8(1))
	f.Add(seedData, uint8(2))
	f.Add(seedData, uint8(3))
	f.Add(seedData, uint8(4))
	f.Add(seedData, uint8(5))

	f.Fuzz(func(t *testing.T, data []byte, alg uint8) {
		compressionAlg := CompressionAlg("")
		switch alg {
		case 1:
			compressionAlg = CompressionZstd
		case 2:
			compressionAlg = CompressionLZ4
		case 3:
			compressionAlg = CompressionSnappy
		case 4:
			compressionAlg = CompressionGZip
		case 5:
			compressionAlg = CompressionXZ
		default:
			compressionAlg = CompressionNone
		}

		compressed, err := compress(data, compressionAlg)
		if err != nil {
			t.Skip()
		}

		decompressed, err := decompress(compressed, compressionAlg)
		if err != nil {
			t.Fatalf("decompress failed: %v", err)
		}

		if !bytes.Equal(data, decompressed) {
			t.Fatalf("round-trip mismatch: original=%d bytes, decompressed=%d bytes", len(data), len(decompressed))
		}
	})
}

func FuzzDecompressUnknownAlgo(f *testing.F) {
	f.Add([]byte{99, 0, 0, 4, 1, 2, 3, 4}, uint8(99))
	f.Add([]byte{0, 0, 0, 4, 1, 2, 3, 4}, uint8(0))

	f.Fuzz(func(t *testing.T, data []byte, alg uint8) {
		compressionAlg := CompressionAlg("")
		switch alg {
		case 1:
			compressionAlg = CompressionZstd
		case 2:
			compressionAlg = CompressionLZ4
		case 3:
			compressionAlg = CompressionSnappy
		case 4:
			compressionAlg = CompressionGZip
		case 5:
			compressionAlg = CompressionXZ
		default:
			compressionAlg = CompressionAlg("unknown")
		}

		_, err := decompress(data, compressionAlg)
		if err != nil {
			t.Logf("decompress rejected: %v", err)
		}
	})
}

func FuzzAlgorithmMismatch(f *testing.F) {
	seedData := []byte("hello world test data for mismatch detection")
	f.Add(seedData, uint8(1), uint8(2))
	f.Add(seedData, uint8(2), uint8(1))
	f.Add(seedData, uint8(3), uint8(4))
	f.Add(seedData, uint8(4), uint8(3))

	f.Fuzz(func(t *testing.T, data []byte, compressAlg, decompressAlg uint8) {
		cAlg := toCompressionAlg(compressAlg)
		dAlg := toCompressionAlg(decompressAlg)

		compressed, err := compress(data, cAlg)
		if err != nil {
			t.Skip()
		}

		_, err = decompress(compressed, dAlg)
		if cAlg != dAlg && err == nil {
			t.Errorf("decompress should fail when algorithm mismatch: compress=%s decompress=%s", cAlg, dAlg)
		}
	})
}

func toCompressionAlg(alg uint8) CompressionAlg {
	switch alg {
	case 1:
		return CompressionZstd
	case 2:
		return CompressionLZ4
	case 3:
		return CompressionSnappy
	case 4:
		return CompressionGZip
	case 5:
		return CompressionXZ
	default:
		return CompressionNone
	}
}
