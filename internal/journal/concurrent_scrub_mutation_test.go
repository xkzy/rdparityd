package journal

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentScrubAndWrite(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubDoesNotBlockWrites(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubSeesConsistentState(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestMutateAndScrubRace(t *testing.T) {
	t.Skip("Category H concurrency tests are in category_h_concurrency_test.go")
}

func TestScrubProgressIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	metadataPath := filepath.Join(tmpDir, "metadata.bin")
	journalPath := filepath.Join(tmpDir, "journal.bin")

	c1 := NewCoordinator(metadataPath, journalPath)
	c2 := NewCoordinator(metadataPath, journalPath)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		coord := c1
		coord.Scrub(false)
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		defer wg.Done()
		coord := c2
		coord.Scrub(false)
	}()

	wg.Wait()
	t.Log("both scrubs completed without deadlock")
}
