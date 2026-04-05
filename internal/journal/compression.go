package journal

// compression.go — Transparent compression support for extent data.
//
// Compression is applied at the extent level, after the extent data is generated
// but before it's written to disk. The compressed data is stored on disk,
// and decompression happens automatically on read.
//
// Compression is optional and controlled by the CompressionAlg field in WriteRequest.
// If CompressionAlg is empty, no compression is applied.
//
// Supported algorithms:
// - zstd: Fast, good compression ratio (default recommended)
// - lz4: Very fast, moderate compression
// - snappy: Fast, moderate compression, designed for speed
// - gzip: Standard POSIX compression, wide compatibility
// - xz: Highest compression, slower
//
// Design decisions:
// - Checksums are computed on COMPRESSED data, not original data
// - Parity is computed from uncompressed data (so compression doesn't affect parity)
// - CompressedSize records the on-disk size for space accounting

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

var (
	ErrUnknownCompression = errors.New("unknown compression algorithm")
	ErrDecompressFailed   = errors.New("decompression failed")
	ErrCompressFailed     = errors.New("compression failed")
)

type CompressionAlg string

const (
	CompressionNone   CompressionAlg = ""
	CompressionZstd   CompressionAlg = "zstd"
	CompressionLZ4    CompressionAlg = "lz4"
	CompressionSnappy CompressionAlg = "snappy"
	CompressionGZip   CompressionAlg = "gzip"
	CompressionXZ     CompressionAlg = "xz"
)

var zstdEncoderPool = sync.Pool{
	New: func() interface{} {
		enc, _ := zstd.NewWriter(nil)
		return enc
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		dec, _ := zstd.NewReader(nil)
		return dec
	},
}

func compress(data []byte, alg CompressionAlg) ([]byte, error) {
	if alg == CompressionNone || data == nil {
		return data, nil
	}

	switch alg {
	case CompressionZstd:
		return compressZstd(data)
	case CompressionLZ4:
		return compressLZ4(data)
	case CompressionSnappy:
		return compressSnappy(data)
	case CompressionGZip:
		return compressGZip(data)
	case CompressionXZ:
		return compressXZ(data)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownCompression, alg)
	}
}

func decompress(data []byte, alg CompressionAlg) ([]byte, error) {
	if alg == CompressionNone || data == nil {
		return data, nil
	}

	switch alg {
	case CompressionZstd:
		return decompressZstd(data)
	case CompressionLZ4:
		return decompressLZ4(data)
	case CompressionSnappy:
		return decompressSnappy(data)
	case CompressionGZip:
		return decompressGZip(data)
	case CompressionXZ:
		return decompressXZ(data)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownCompression, alg)
	}
}

func compressZstd(data []byte) ([]byte, error) {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	var buf bytes.Buffer
	enc.Reset(&buf)
	if _, err := enc.Write(data); err != nil {
		return nil, fmt.Errorf("zstd write: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("zstd close: %w", err)
	}
	return buf.Bytes(), nil
}

func decompressZstd(data []byte) ([]byte, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)

	if err := dec.Reset(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("zstd reset: %w", err)
	}
	out, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return out, nil
}

func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	wr := lz4.NewWriter(&buf)
	if _, err := wr.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}
	return buf.Bytes(), nil
}

func decompressLZ4(data []byte) ([]byte, error) {
	rd := lz4.NewReader(bytes.NewReader(data))
	out, err := io.ReadAll(rd)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	return out, nil
}

func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func decompressSnappy(data []byte) ([]byte, error) {
	out, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress: %w", err)
	}
	return out, nil
}

func compressionRatio(uncompressed, compressed int64) float64 {
	if uncompressed == 0 || compressed == 0 {
		return 1.0
	}
	return float64(compressed) / float64(uncompressed)
}

func compressGZip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	wr, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip writer: %w", err)
	}
	if _, err := wr.Write(data); err != nil {
		wr.Close()
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func decompressGZip(data []byte) ([]byte, error) {
	rd, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	out, err := io.ReadAll(rd)
	if err != nil {
		rd.Close()
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	if err := rd.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return out, nil
}

func compressXZ(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	wr, err := xz.NewWriter(&buf)
	if err != nil {
		return nil, fmt.Errorf("xz writer: %w", err)
	}
	if _, err := wr.Write(data); err != nil {
		wr.Close()
		return nil, fmt.Errorf("xz write: %w", err)
	}
	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("xz close: %w", err)
	}
	return buf.Bytes(), nil
}

func decompressXZ(data []byte) ([]byte, error) {
	rd, err := xz.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("xz reader: %w", err)
	}
	out, err := io.ReadAll(rd)
	if err != nil {
		return nil, fmt.Errorf("xz decompress: %w", err)
	}
	return out, nil
}
