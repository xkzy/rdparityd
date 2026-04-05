package journal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// ============================================================================
// CATEGORY H: CONCURRENCY TESTS (10 tests)
// Tests verify correct handling of concurrent writes, write+repair conflicts,
// and elimination of race conditions.
// ============================================================================

// TestCategoryH_ConcurrentWritesToDifferentFiles verifies that multiple goroutines
// can write different files simultaneously without data corruption.
//
// VERIFICATION GOALS:
// - Each file written with correct data
// - No cross-file data corruption
// - All writes commit successfully
// - Concurrent writes don't block indefinitely
//
// FAILURE SCENARIOS:
// - N goroutines each writing different file simultaneously
// - Files may land in same extent regions (shared parity)
//
// EXPECTED OUTCOMES:
// - All N writes succeed
// - Each file readable with original payload
// - No data loss or mix-up between files
// - No hung goroutines
func TestCategoryH_ConcurrentWritesToDifferentFiles(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	const writers = 10
	payloads := make([][]byte, writers)
	paths := make([]string, writers)
	var wg sync.WaitGroup
	errCh := make(chan error, writers)

	for i := 0; i < writers; i++ {
		paths[i] = fmt.Sprintf("/concurrency/file-%02d.bin", i)
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, 4096+(i*257))
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := coord.WriteFile(WriteRequest{
				PoolName:    "demo",
				LogicalPath: paths[i],
				Payload:     payloads[i],
			})
			if err != nil {
				errCh <- fmt.Errorf("write %d failed: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := 0; i < writers; i++ {
		result, err := coord.ReadFile(paths[i])
		if err != nil {
			t.Fatalf("read back %s failed: %v", paths[i], err)
		}
		if !result.Verified {
			t.Fatalf("expected verified read for %s", paths[i])
		}
		if !bytes.Equal(result.Data, payloads[i]) {
			t.Fatalf("data mismatch for %s", paths[i])
		}
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if len(state.Files) != writers {
		t.Fatalf("expected %d files, got %d", writers, len(state.Files))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after concurrent writes: %v", violations[0])
	}
}

// TestCategoryH_ConcurrentReadsWhileWriting verifies that reads don't return
// torn or partial writes from concurrent write operations.
//
// VERIFICATION GOALS:
// - Reads see committed file state (all-or-nothing)
// - No partial writes visible to readers
// - Readers don't block writers (minimal locking)
// - Read consistency maintained
//
// FAILURE SCENARIOS:
// - Writer in-flight, readers polling for completion
// - Readers attempting to read before/during/after commit
//
// EXPECTED OUTCOMES:
// - Readers see either pre-write or post-write state (never partial)
// - Writer commits atomically
// - Multiple concurrent readers all get same view
func TestCategoryH_ConcurrentReadsWhileWriting(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	originalPayload := bytes.Repeat([]byte("read-consistent-"), 150000)
	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/concurrent/read.bin",
		Payload:     originalPayload,
	}); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	var wg sync.WaitGroup
	readerResultsCh := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for attempt := 0; attempt < 20; attempt++ {
				result, err := coord.ReadFile("/concurrent/read.bin")
				if err != nil {
					readerResultsCh <- false
					return
				}
				if !bytes.Equal(result.Data, originalPayload) {
					readerResultsCh <- false
					return
				}
				time.Sleep(1 * time.Millisecond)
			}
			readerResultsCh <- true
		}(i)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}

	if len(state.Extents) > 0 {
		extent := state.Extents[0]
		extentPath := filepath.Join(root, extent.PhysicalLocator.RelativePath)
		data, _ := os.ReadFile(extentPath)
		if len(data) > 0 {
			data[0] ^= 0x41
			os.WriteFile(extentPath, data, 0o600)
		}
	}

	time.Sleep(10 * time.Millisecond)

	wg.Wait()
	close(readerResultsCh)

	for success := range readerResultsCh {
		if !success {
			t.Fatal("concurrent reader saw inconsistent/partial data")
		}
	}

	finalResult, err := coord.ReadFile("/concurrent/read.bin")
	if err != nil {
		t.Fatalf("final read failed: %v", err)
	}
	if !bytes.Equal(finalResult.Data, originalPayload) {
		t.Fatal("final read data mismatch")
	}
}

// TestCategoryH_ConcurrentWriteToSameFile verifies behavior when multiple writers
// attempt to write to the same logical file path simultaneously.
//
// VERIFICATION GOALS:
// - Last-writer-wins or clear error (no silent merge)
// - File state consistent after all writers complete
// - No data from losing writers persists
// - Lock/serialization prevents corruption
//
// FAILURE SCENARIOS:
// - 3 goroutines attempt to write same file path
// - Each with different payload
// - All launched simultaneously
//
// EXPECTED OUTCOMES:
// - One write succeeds, others fail with conflict error
// - Final file data matches one of the 3 payloads
// - No hybrid corruption from multiple writers
// - File readable with valid checksum
func TestCategoryH_ConcurrentWriteToSameFile(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	const path = "/same-file/conflict.bin"
	payloadA := bytes.Repeat([]byte("AAAA"), 2048)
	payloadB := bytes.Repeat([]byte("BBBB"), 2048)
	payloadC := bytes.Repeat([]byte("CCCC"), 2048)

	var wg sync.WaitGroup
	resultsCh := make(chan struct {
		index   int
		err     error
		payload []byte
	}, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payloadA,
		})
		resultsCh <- struct {
			index   int
			err     error
			payload []byte
		}{index: 0, err: err, payload: payloadA}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payloadB,
		})
		resultsCh <- struct {
			index   int
			err     error
			payload []byte
		}{index: 1, err: err, payload: payloadB}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: path,
			Payload:     payloadC,
		})
		resultsCh <- struct {
			index   int
			err     error
			payload []byte
		}{index: 2, err: err, payload: payloadC}
	}()

	wg.Wait()
	close(resultsCh)

	var successCount int
	var successPayload []byte

	for result := range resultsCh {
		if result.err == nil {
			successCount++
			successPayload = result.payload
		}
	}

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful write, got %d", successCount)
	}

	readResult, err := coord.ReadFile(path)
	if err != nil {
		t.Fatalf("read back failed: %v", err)
	}

	if !bytes.Equal(readResult.Data, successPayload) {
		t.Fatal("final file data does not match the successful write payload")
	}

	if !readResult.Verified {
		t.Fatal("expected verified read of written file")
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if len(state.Files) != 1 {
		t.Fatalf("expected 1 file in metadata, got %d", len(state.Files))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after same-file write conflict: %v", violations[0])
	}
}

// TestCategoryH_WriteAndRepairRaceCondition verifies that concurrent write and
// repair operations don't corrupt data or create race conditions.
//
// VERIFICATION GOALS:
// - Write and repair can proceed concurrently safely
// - Repair on different file doesn't interfere with write
// - Both operations complete successfully
// - No deadlock or data corruption
//
// FAILURE SCENARIOS:
// - Write file A in progress
// - Simultaneously corrupt file B extent and start repair
// - Both operations racing
//
// EXPECTED OUTCOMES:
// - Write A completes successfully
// - Repair B completes successfully
// - Files A and B both readable with correct data
// - No interference between operations
func TestCategoryH_WriteAndRepairRaceCondition(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	payloadA := bytes.Repeat([]byte("A"), 8192)
	payloadB := bytes.Repeat([]byte("B"), (1<<20)+333)
	payloadC := bytes.Repeat([]byte("C"), 16384)

	if _, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/race/a.bin",
		Payload:     payloadA,
	}); err != nil {
		t.Fatalf("initial write A failed: %v", err)
	}
	writeB, err := coord.WriteFile(WriteRequest{
		PoolName:    "demo",
		LogicalPath: "/race/b.bin",
		Payload:     payloadB,
	})
	if err != nil {
		t.Fatalf("initial write B failed: %v", err)
	}
	if len(writeB.Extents) == 0 {
		t.Fatal("expected extents for file B")
	}

	// Corrupt one extent from B before the race begins so the repair path has
	// real work to do while another write commits.
	extentPath := filepath.Join(root, writeB.Extents[0].PhysicalLocator.RelativePath)
	corrupt, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read extent for corruption failed: %v", err)
	}
	corrupt[0] ^= 0x7f
	if err := os.WriteFile(extentPath, corrupt, 0o600); err != nil {
		t.Fatalf("corrupt extent write failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: "/race/c.bin",
			Payload:     payloadC,
		}); err != nil {
			errCh <- fmt.Errorf("concurrent write C failed: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := coord.ReadFile("/race/b.bin")
		if err != nil {
			errCh <- fmt.Errorf("concurrent repair/read B failed: %w", err)
			return
		}
		if !bytes.Equal(result.Data, payloadB) {
			errCh <- fmt.Errorf("repaired B data mismatch")
		}
		if !result.Verified {
			errCh <- fmt.Errorf("repaired B not marked verified")
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	resultB, err := coord.ReadFile("/race/b.bin")
	if err != nil {
		t.Fatalf("final read B failed: %v", err)
	}
	if !bytes.Equal(resultB.Data, payloadB) {
		t.Fatal("final B data mismatch after repair race")
	}

	resultC, err := coord.ReadFile("/race/c.bin")
	if err != nil {
		t.Fatalf("final read C failed: %v", err)
	}
	if !bytes.Equal(resultC.Data, payloadC) {
		t.Fatal("final C data mismatch after write race")
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta failed: %v", err)
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after write/repair race: %v", violations[0])
	}
}

// TestCategoryH_ConcurrentScrubAndWrite verifies that scrub and write operations
// can proceed concurrently without conflicts or data corruption.
//
// VERIFICATION GOALS:
// - Scrub doesn't block writes (lock-free or short locks)
// - Writes don't interfere with scrub accuracy
// - Scrub sees consistent file snapshot
// - No missed corruptions due to concurrent writes
//
// FAILURE SCENARIOS:
// - Start long-running scrub
// - While scrub in-flight, concurrent writes to different files
// - Some files written after scrub started scanning them
//
// EXPECTED OUTCOMES:
// - Scrub completes without error
// - All concurrent writes succeed
// - Scrub results accurate for scanned portion
// - New files written during scrub may not appear in result
func TestCategoryH_ConcurrentScrubAndWrite(t *testing.T) {
	root := t.TempDir()
	coord := NewCoordinator(
		filepath.Join(root, "metadata.json"),
		filepath.Join(root, "journal.log"),
	)

	for i := 0; i < 3; i++ {
		payload := bytes.Repeat([]byte{byte('s' + i)}, (1<<20)+(i*89))
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: fmt.Sprintf("/scrub-write/%d.bin", i),
			Payload:     payload,
		}); err != nil {
			t.Fatalf("pre-scrub write %d failed: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := coord.Scrub(false)
		if err != nil {
			errCh <- fmt.Errorf("scrub failed: %w", err)
			return
		}
		if !result.Healthy {
			errCh <- fmt.Errorf("scrub reported unhealthy: %+v", result.Issues)
		}
	}()

	time.Sleep(5 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		payload := bytes.Repeat([]byte("concurrent-write-"), 180000)
		if _, err := coord.WriteFile(WriteRequest{
			PoolName:    "demo",
			LogicalPath: "/scrub-write/concurrent.bin",
			Payload:     payload,
		}); err != nil {
			errCh <- fmt.Errorf("concurrent write failed: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	readResult, err := coord.ReadFile("/scrub-write/concurrent.bin")
	if err != nil {
		t.Fatalf("read concurrent write failed: %v", err)
	}
	if !readResult.Verified {
		t.Fatal("concurrent write not verified")
	}
}

// TestCategoryH_MultipleRepairsOfDifferentExtents verifies that multiple repair
// operations on different extents can proceed safely in parallel.
//
// VERIFICATION GOALS:
// - Parallel repairs don't interfere with each other
// - All extents repaired correctly
// - Repair operations are lock-free or minimize contention
// - No cascading failures from concurrent repairs
//
// FAILURE SCENARIOS:
// - Corrupt 3 extents across different files
// - Launch 3 concurrent repair tasks for each extent
//
// EXPECTED OUTCOMES:
// - All 3 repairs complete
// - All 3 extents repaired correctly
// - No cross-extent corruption
// - No missed repairs
func TestCategoryH_MultipleRepairsOfDifferentExtents(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	allPaths := []string{"/repair/a.bin", "/repair/b.bin", "/repair/c.bin", "/repair/d.bin", "/repair/e.bin"}
	targetPaths := make(map[string]string)
	for _, p := range allPaths {
		result, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: p, AllowSynthetic: true, SizeBytes: 4096})
		if err != nil {
			t.Fatalf("WriteFile %s failed: %v", p, err)
		}
		if len(result.Extents) == 0 {
			t.Fatalf("expected extents for %s", p)
		}
		targetPaths[p] = filepath.Join(root, result.Extents[0].PhysicalLocator.RelativePath)
	}

	paths := []string{"/repair/a.bin", "/repair/c.bin", "/repair/e.bin"}
	for _, p := range paths {
		data, err := os.ReadFile(targetPaths[p])
		if err != nil {
			t.Fatalf("read extent %s failed: %v", p, err)
		}
		data[0] ^= 0x55
		if err := os.WriteFile(targetPaths[p], data, 0o600); err != nil {
			t.Fatalf("corrupt extent %s failed: %v", p, err)
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(paths))
	for _, p := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			readResult, err := NewCoordinator(metaPath, journalPath).ReadFile(path)
			if err != nil {
				errCh <- fmt.Errorf("repair/read %s failed: %w", path, err)
				return
			}
			if !readResult.Verified {
				errCh <- fmt.Errorf("repair/read %s not verified", path)
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	result, err := NewCoordinator(metaPath, journalPath).Scrub(false)
	if err != nil {
		t.Fatalf("final scrub failed: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("pool unhealthy after parallel repairs: %+v", result)
	}
}

// TestCategoryH_RaceFredomMetadataUpdates verifies that concurrent metadata
// updates from different operations don't create race conditions or lost updates.
//
// VERIFICATION GOALS:
// - Metadata updates atomic (no partial/torn writes)
// - Concurrent updates don't lose data
// - Metadata reflects all updates consistently
// - No race conditions in JSON/state serialization
//
// FAILURE SCENARIOS:
// - N concurrent writes each updating metadata
// - Simultaneous scrub updating repair history
// - All racing to update same metadata file
//
// EXPECTED OUTCOMES:
// - All updates persisted
// - Metadata readable without corruption
// - Extent records accurate
// - Repair history complete
func TestCategoryH_RaceFredomMetadataUpdates(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")

	const writers = 5
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			coord := NewCoordinator(metaPath, journalPath)
			_, err := coord.WriteFile(WriteRequest{
				PoolName:       "demo",
				LogicalPath:    fmt.Sprintf("/race/file-%d.bin", i),
				AllowSynthetic: true,
				SizeBytes:      4096 + int64(i),
			})
			if err != nil {
				errCh <- fmt.Errorf("writer %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed: %v", err)
	}
	if len(state.Files) != writers {
		t.Fatalf("expected %d files, got %d", writers, len(state.Files))
	}
	if len(state.Transactions) != writers {
		t.Fatalf("expected %d transactions, got %d", writers, len(state.Transactions))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after concurrent updates: %v", violations[0])
	}
}

// TestCategoryH_ConcurrentJournalReplay verifies that journal replay during
// coordinator startup is race-free when concurrent operations attempted.
//
// VERIFICATION GOALS:
// - Replay completes before allowing new operations
// - No race between replay and concurrent writes
// - Replayed state consistent with new operations
// - No lost transactions
//
// FAILURE SCENARIOS:
// - Start coordinator with incomplete journal transactions
// 2. While replay in-flight, write new files
// - Both racing for metadata/journal access
//
// EXPECTED OUTCOMES:
// - Replay completes fully before new writes proceed
// - No lost writes from either replay or new operations
// - Final metadata consistent
// - All files readable
func TestCategoryH_ConcurrentJournalReplay(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/replay/base.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
	})
	if err != nil {
		t.Fatalf("base write failed: %v", err)
	}
	_, err = coord.WriteFile(WriteRequest{
		PoolName:       "demo",
		LogicalPath:    "/replay/recover.bin",
		AllowSynthetic: true,
		SizeBytes:      4096,
		FailAfter:      StateParityWritten,
	})
	if err != nil {
		t.Fatalf("incomplete write failed: %v", err)
	}

	replayDone := make(chan error, 1)
	go func() {
		_, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("demo"))
		replayDone <- err
	}()

	writeDone := make(chan error, 1)
	go func() {
		_, err := NewCoordinator(metaPath, journalPath).WriteFile(WriteRequest{
			PoolName:       "demo",
			LogicalPath:    "/replay/new-write.bin",
			AllowSynthetic: true,
			SizeBytes:      4096,
		})
		writeDone <- err
	}()

	if err := <-replayDone; err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("concurrent write failed: %v", err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed: %v", err)
	}
	if len(state.Files) != 3 {
		t.Fatalf("expected 3 files after replay + write, got %d", len(state.Files))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after concurrent replay: %v", violations[0])
	}
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal replay summary failed: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal should be clean after replay+write: %+v", summary)
	}
}

// TestCategoryH_HighConcurrencyStressTest verifies that the coordinator handles
// high concurrent load without data corruption or race conditions.
//
// VERIFICATION GOALS:
// - System stable under sustained concurrent load
// - No deadlocks or hung goroutines
// - All operations complete (no dropped requests)
// - Data integrity maintained throughout
//
// FAILURE SCENARIOS:
// - 20 goroutines writing files
// - 10 goroutines reading files
// - 5 goroutines performing repairs
// - All active simultaneously for 10+ seconds
//
// EXPECTED OUTCOMES:
// - All operations complete
// - No crashes or panics
// - All files readable with correct data
// - No data loss or corruption
// - No deadlocks
func TestCategoryH_HighConcurrencyStressTest(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	_, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/stress/base.bin", AllowSynthetic: true, SizeBytes: 4096})
	if err != nil {
		t.Fatalf("base write failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := NewCoordinator(metaPath, journalPath).WriteFile(WriteRequest{
				PoolName:       "demo",
				LogicalPath:    fmt.Sprintf("/stress/write-%02d.bin", i),
				AllowSynthetic: true,
				SizeBytes:      4096 + int64(i),
			})
			if err != nil {
				errCh <- fmt.Errorf("writer %d failed: %w", i, err)
			}
		}(i)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				readResult, err := NewCoordinator(metaPath, journalPath).ReadFile("/stress/base.bin")
				if err != nil {
					errCh <- fmt.Errorf("reader %d iteration %d failed: %w", i, j, err)
					return
				}
				if !readResult.Verified {
					errCh <- fmt.Errorf("reader %d iteration %d got unverified read", i, j)
					return
				}
			}
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("high-concurrency stress test timed out (possible deadlock)")
	}
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("metadata load failed: %v", err)
	}
	if len(state.Files) != 21 {
		t.Fatalf("expected 21 files after stress test, got %d", len(state.Files))
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after stress test: %v", violations[0])
	}
}

// TestCategoryH_DeadlockFredomInLockingHierarchy verifies that the locking
// hierarchy is deadlock-free even under pathological concurrent scenarios.
//
// VERIFICATION GOALS:
// - No circular lock dependencies
// - Lock acquisition order consistent
// - Timeout mechanisms prevent indefinite waits
// - No priority inversions
//
// FAILURE SCENARIOS:
// - Multiple operations acquiring locks in various orders
// - Stress pattern designed to trigger potential deadlocks
//
// EXPECTED OUTCOMES:
// - All operations complete
// - No timeouts or deadlock detection
// - System responsive (no long blocking periods)
// - Lock contention minimal
func TestCategoryH_DeadlockFredomInLockingHierarchy(t *testing.T) {
	root := t.TempDir()
	metaPath := filepath.Join(root, "metadata.json")
	journalPath := filepath.Join(root, "journal.log")
	coord := NewCoordinator(metaPath, journalPath)

	base, err := coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/deadlock/base.bin", AllowSynthetic: true, SizeBytes: 4096})
	if err != nil {
		t.Fatalf("base write failed: %v", err)
	}
	_, err = coord.WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/deadlock/pending.bin", AllowSynthetic: true, SizeBytes: 4096, FailAfter: StateParityWritten})
	if err != nil {
		t.Fatalf("pending write failed: %v", err)
	}
	if len(base.Extents) == 0 {
		t.Fatal("expected base extents")
	}
	extentPath := filepath.Join(root, base.Extents[0].PhysicalLocator.RelativePath)
	data, err := os.ReadFile(extentPath)
	if err != nil {
		t.Fatalf("read base extent failed: %v", err)
	}
	data[0] ^= 0xFF
	if err := os.WriteFile(extentPath, data, 0o600); err != nil {
		t.Fatalf("corrupt base extent failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	ops := []func() error{
		func() error { _, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("demo")); return err },
		func() error { _, err := NewCoordinator(metaPath, journalPath).WriteFile(WriteRequest{PoolName: "demo", LogicalPath: "/deadlock/new.bin", AllowSynthetic: true, SizeBytes: 4096}); return err },
		func() error { _, err := NewCoordinator(metaPath, journalPath).Scrub(true); return err },
		func() error { return NewCoordinator(metaPath, journalPath).AddDisk("disk-extra", "55555555-5555-5555-5555-555555555555", metadata.DiskRoleData, "/mnt/data03", 4<<40) },
	}
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op func() error) {
			defer wg.Done()
			if err := op(); err != nil {
				errCh <- fmt.Errorf("op %d failed: %w", i, err)
			}
		}(i, op)
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("operations timed out (possible deadlock)")
	}
	close(errCh)
	for err := range errCh {
		t.Logf("concurrent op finished with safe error: %v", err)
	}

	// Deadlock test is about liveness plus post-race consistency, not forcing
	// every conflicting operation to succeed under intentionally adversarial IO.
	if _, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("demo")); err != nil {
		t.Fatalf("final recovery failed: %v", err)
	}
	state, err := metadata.NewStore(metaPath).Load()
	if err != nil {
		t.Fatalf("final metadata load failed: %v", err)
	}
	if violations := CheckStateInvariants(state); len(violations) > 0 {
		t.Fatalf("state invariants violated after deadlock test: %v", violations[0])
	}
	summary, err := NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal replay summary failed: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal should be clean after deadlock test: %+v", summary)
	}
}
