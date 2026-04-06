package journal

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkWriteFile(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := "/test/bench_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}
}

func BenchmarkWriteFileSmall(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := []byte("small payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := "/test/small_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}
}

func BenchmarkReadFile(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	path := "/test/readbench.bin"
	if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
		b.Fatalf("WriteFile failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coord.ReadFile(path); err != nil {
			b.Fatalf("ReadFile failed: %v", err)
		}
	}
}

func BenchmarkRecovery(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := []byte("recovery test data")

	for i := 0; i < 100; i++ {
		path := "/test/recovery_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		coord2 := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
		if _, err := coord2.Recover(); err != nil {
			b.Fatalf("Recovery failed: %v", err)
		}
	}
}

func BenchmarkScrub(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := []byte("scrub test data")

	for i := 0; i < 50; i++ {
		path := "/test/scrub_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coord.Scrub(false); err != nil {
			b.Fatalf("Scrub failed: %v", err)
		}
	}
}

func BenchmarkChecksumComputation(b *testing.B) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = digestBytes(data)
	}
}

func BenchmarkCompressionZstd(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping compression benchmark in short mode")
	}

	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compress(data, CompressionZstd)
	}
}

func BenchmarkCompressionGZip(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping compression benchmark in short mode")
	}

	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compress(data, CompressionGZip)
	}
}

func BenchmarkMetadataLoad(b *testing.B) {
	dir := b.TempDir()
	coord := NewCoordinator(filepath.Join(dir, "metadata.bin"), filepath.Join(dir, "journal.bin"))
	payload := []byte("metadata test")

	for i := 0; i < 100; i++ {
		path := "/test/meta_" + string(rune(i)) + ".bin"
		if _, err := coord.WriteFile(context.Background(), WriteRequest{PoolName: "demo", LogicalPath: path, Payload: payload}); err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coord.ReadMeta(); err != nil {
			b.Fatalf("ReadMeta failed: %v", err)
		}
	}
}
