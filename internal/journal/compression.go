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
	"encoding/binary"
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
	ErrAlgorithmMismatch  = errors.New("compression algorithm mismatch")

	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
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

const compressedFrameHeaderSize = 4

type compressedFrameHeader struct {
	Algorithm  uint8
	Reserved   uint8
	PayloadLen uint16
}

func (h compressedFrameHeader) Valid() bool {
	switch h.Algorithm {
	case algNone, algZstd, algLZ4, algSnappy, algGZip, algXZ:
		return true
	default:
		return false
	}
}

const (
	algNone   = 0
	algZstd   = 1
	algLZ4    = 2
	algSnappy = 3
	algGZip   = 4
	algXZ     = 5
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
	if data == nil {
		return nil, nil
	}
	if alg == CompressionNone {
		return data, nil
	}

	compressed, err := compressInner(data, alg)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, compressedFrameHeaderSize+len(compressed))
	frame[0] = compressionAlgToID(alg)
	frame[1] = 0
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(compressed)))
	copy(frame[compressedFrameHeaderSize:], compressed)
	return frame, nil
}

func decompress(data []byte, alg CompressionAlg) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	if alg == CompressionNone {
		return data, nil
	}
	if len(data) < compressedFrameHeaderSize {
		return nil, fmt.Errorf("compressed data too short for frame header: %d bytes", len(data))
	}

	header := compressedFrameHeader{
		Algorithm:  data[0],
		Reserved:   data[1],
		PayloadLen: binary.BigEndian.Uint16(data[2:4]),
	}

	if !header.Valid() {
		return nil, fmt.Errorf("%w: invalid algorithm id %d", ErrUnknownCompression, header.Algorithm)
	}

	if header.Algorithm != compressionAlgToID(alg) {
		return nil, fmt.Errorf("%w: stored=%s requested=%s", ErrAlgorithmMismatch, compressionIDToAlg(header.Algorithm), alg)
	}

	payloadStart := compressedFrameHeaderSize
	payloadLen := int(header.PayloadLen)
	if len(data) < payloadStart+payloadLen {
		return nil, fmt.Errorf("compressed data shorter than declared payload length: header=%d actual=%d", payloadLen, len(data)-payloadStart)
	}

	payload := data[payloadStart : payloadStart+payloadLen]
	return decompressInner(payload, alg)
}

func compressionAlgToID(alg CompressionAlg) uint8 {
	switch alg {
	case CompressionNone:
		return algNone
	case CompressionZstd:
		return algZstd
	case CompressionLZ4:
		return algLZ4
	case CompressionSnappy:
		return algSnappy
	case CompressionGZip:
		return algGZip
	case CompressionXZ:
		return algXZ
	default:
		return 0
	}
}

func compressionIDToAlg(id uint8) CompressionAlg {
	switch id {
	case algNone:
		return CompressionNone
	case algZstd:
		return CompressionZstd
	case algLZ4:
		return CompressionLZ4
	case algSnappy:
		return CompressionSnappy
	case algGZip:
		return CompressionGZip
	case algXZ:
		return CompressionXZ
	default:
		return CompressionAlg("")
	}
}

func compressInner(data []byte, alg CompressionAlg) ([]byte, error) {
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

func decompressInner(data []byte, alg CompressionAlg) ([]byte, error) {
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
	out, err := io.ReadAll(io.LimitReader(dec, MaxPayloadSize))
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return out, nil
}

func compressLZ4(data []byte) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	wr := lz4.NewWriter(buf)
	if _, err := wr.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

func decompressLZ4(data []byte) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	rd := lz4.NewReader(bytes.NewReader(data))
	out, err := io.ReadAll(io.LimitReader(rd, MaxPayloadSize))
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
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	wr, err := gzip.NewWriterLevel(buf, gzip.DefaultCompression)
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
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

func decompressGZip(data []byte) ([]byte, error) {
	rd, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	out, err := io.ReadAll(io.LimitReader(rd, MaxPayloadSize))
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
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	wr, err := xz.NewWriter(buf)
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
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

func decompressXZ(data []byte) ([]byte, error) {
	rd, err := xz.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("xz reader: %w", err)
	}
	out, err := io.ReadAll(io.LimitReader(rd, MaxPayloadSize))
	if err != nil {
		return nil, fmt.Errorf("xz decompress: %w", err)
	}
	return out, nil
}
