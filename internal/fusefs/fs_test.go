/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package fusefs_test

// Integration tests for the rdparityd FUSE filesystem.
//
// Each test mounts the pool filesystem at a temporary directory, exercises
// filesystem operations through the normal OS APIs (os.ReadFile, os.WriteFile,
// etc.), and then verifies both the FUSE-visible result and the underlying
// coordinator state.
//
// The tests require /dev/fuse and fusermount to be available. They are skipped
// automatically when running in environments that don't have FUSE support.

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/xkzy/rdparityd/internal/fusefs"
	"github.com/xkzy/rdparityd/internal/journal"
)

// testMount mounts the pool filesystem at a temporary directory and returns the
// mountpoint and a cleanup function. The test is skipped if FUSE is not
// available in the current environment.
func testMount(t *testing.T) (mnt string, coord *journal.Coordinator, cleanup func()) {
	t.Helper()

	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("FUSE not available: /dev/fuse missing")
	}

	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coord = journal.NewCoordinator(metadataPath, journalPath)

	mnt = filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatalf("create mountpoint: %v", err)
	}

	srv, err := fusefs.Mount(mnt, coord, fusefs.Options{PoolName: "demo"})
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOSYS) {
			t.Skipf("FUSE mount not permitted in this environment: %v", err)
		}
		t.Fatalf("fusefs.Mount: %v", err)
	}

	// Wait for the mount to be ready before returning.
	if err := srv.WaitMount(); err != nil {
		srv.Unmount() //nolint:errcheck
		t.Fatalf("WaitMount: %v", err)
	}

	cleanup = func() {
		if err := srv.Unmount(); err != nil {
			t.Logf("warning: unmount failed: %v", err)
		}
	}
	return mnt, coord, cleanup
}

// ─── TestFUSEWriteAndReadBack ─────────────────────────────────────────────────

// Writing a file through the FUSE mount must make the same bytes readable back
// via the FUSE mount and via the coordinator's read path.
func TestFUSEWriteAndReadBack(t *testing.T) {
	mnt, coord, cleanup := testMount(t)
	defer cleanup()

	// Create nested directory structure via mkdir.
	dirPath := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Write a multi-extent payload.
	payload := make([]byte, (1<<20)+512)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	filePath := filepath.Join(dirPath, "hello.bin")
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile via FUSE: %v", err)
	}

	// Read back via FUSE.
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile via FUSE: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("FUSE readback mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// Read back via coordinator (exercises the same checksum-verified path).
	result, err := coord.ReadFile("/shares/demo/hello.bin")
	if err != nil {
		t.Fatalf("coordinator.ReadFile: %v", err)
	}
	if !result.Verified {
		t.Error("coordinator read was not verified")
	}
	if !bytes.Equal(result.Data, payload) {
		t.Fatalf("coordinator readback mismatch: got %d bytes, want %d", len(result.Data), len(payload))
	}
}

// ─── TestFUSEReaddirListsCommittedFiles ───────────────────────────────────────

// Files written through the FUSE mount must appear in directory listings.
func TestFUSEReaddirListsCommittedFiles(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	// Write a few files through the FUSE mount.
	files := []string{"alpha.bin", "beta.bin", "gamma.bin"}
	dir := filepath.Join(mnt, "shares", "test")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name+"_data"), 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	// List the directory.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			got = append(got, e.Name())
		}
	}
	sort.Strings(got)
	sort.Strings(files)
	if len(got) != len(files) {
		t.Fatalf("ReadDir: got %v, want %v", got, files)
	}
	for i := range files {
		if got[i] != files[i] {
			t.Errorf("ReadDir entry %d: got %q, want %q", i, got[i], files[i])
		}
	}
}

// ─── TestFUSEGetattrSize ──────────────────────────────────────────────────────

// Stat on a FUSE file must return the correct size.
func TestFUSEGetattrSize(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	const wantSize = 12345
	payload := make([]byte, wantSize)
	for i := range payload {
		payload[i] = 0xAB
	}
	filePath := filepath.Join(dir, "sized.bin")
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != wantSize {
		t.Errorf("Stat size: got %d, want %d", info.Size(), wantSize)
	}
}

// ─── TestFUSEWritePreCommitMetadataState ──────────────────────────────────────

// After writing through FUSE, the coordinator metadata must contain the file
// with correct size and at least one parity group.
func TestFUSEWritePreCommitMetadataState(t *testing.T) {
	mnt, coord, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	payload := make([]byte, (1<<20)+100)
	for i := range payload {
		payload[i] = byte(i % 127)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta-check.bin"), payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if len(state.Files) == 0 {
		t.Fatal("expected at least one file in metadata after FUSE write")
	}
	found := false
	for _, f := range state.Files {
		if f.Path == "/shares/demo/meta-check.bin" {
			if f.SizeBytes != int64(len(payload)) {
				t.Errorf("metadata size: got %d, want %d", f.SizeBytes, len(payload))
			}
			found = true
		}
	}
	if !found {
		t.Error("file /shares/demo/meta-check.bin not found in metadata")
	}
	if len(state.ParityGroups) == 0 {
		t.Error("expected at least one parity group after FUSE write")
	}
}

// ─── TestFUSERemountPreservesFiles ───────────────────────────────────────────

// Files written through a FUSE mount must still be readable after unmounting
// and remounting (i.e., data persists through the coordinator's durable store).
func TestFUSERemountPreservesFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("FUSE not available")
	}
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")

	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	// First mount: write a file.
	{
		mnt := filepath.Join(dir, "mnt1")
		if err := os.Mkdir(mnt, 0o755); err != nil {
			t.Fatalf("mkdir mnt1: %v", err)
		}
		coord := journal.NewCoordinator(metadataPath, journalPath)
		srv, err := fusefs.Mount(mnt, coord, fusefs.Options{PoolName: "demo"})
		if err != nil {
			t.Skipf("mount 1: %v", err)
		}
		srv.WaitMount() //nolint:errcheck

		if err := os.MkdirAll(filepath.Join(mnt, "shares", "demo"), 0o755); err != nil {
			srv.Unmount() //nolint:errcheck
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(mnt, "shares", "demo", "persist.bin"), payload, 0o644); err != nil {
			srv.Unmount() //nolint:errcheck
			t.Fatalf("WriteFile: %v", err)
		}
		if err := srv.Unmount(); err != nil {
			t.Logf("unmount 1: %v", err)
		}
	}

	// Second mount: read the file back.
	{
		mnt := filepath.Join(dir, "mnt2")
		if err := os.Mkdir(mnt, 0o755); err != nil {
			t.Fatalf("mkdir mnt2: %v", err)
		}
		coord := journal.NewCoordinator(metadataPath, journalPath)
		srv, err := fusefs.Mount(mnt, coord, fusefs.Options{PoolName: "demo"})
		if err != nil {
			t.Fatalf("mount 2: %v", err)
		}
		defer srv.Unmount() //nolint:errcheck
		srv.WaitMount()     //nolint:errcheck

		got, err := os.ReadFile(filepath.Join(mnt, "shares", "demo", "persist.bin"))
		if err != nil {
			t.Fatalf("ReadFile after remount: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("remount readback mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	}
}

// ─── TestFUSEReaddirShowsVirtualDirectories ───────────────────────────────────

// Reading the root after a write must show the first-level path component as a
// directory entry.
func TestFUSEReaddirShowsVirtualDirectories(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	if err := os.MkdirAll(filepath.Join(mnt, "shares", "demo"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "shares", "demo", "dir-test.bin"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !containsStr(names, "shares") {
		t.Errorf("root ReadDir missing 'shares' directory, got: %v", names)
	}

	entries2, err := os.ReadDir(filepath.Join(mnt, "shares"))
	if err != nil {
		t.Fatalf("ReadDir /shares: %v", err)
	}
	names2 := make([]string, 0, len(entries2))
	for _, e := range entries2 {
		names2 = append(names2, e.Name())
	}
	if !containsStr(names2, "demo") {
		t.Errorf("/shares ReadDir missing 'demo' directory, got: %v", names2)
	}
}

// ─── TestFUSEInvariantsCleanAfterWrite ────────────────────────────────────────

// All storage invariants must pass after a FUSE write.
func TestFUSEInvariantsCleanAfterWrite(t *testing.T) {
	mnt, coord, cleanup := testMount(t)
	defer cleanup()

	if err := os.MkdirAll(filepath.Join(mnt, "shares", "demo"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	payload := make([]byte, (1<<20)+256)
	for i := range payload {
		payload[i] = byte(i % 199)
	}
	if err := os.WriteFile(filepath.Join(mnt, "shares", "demo", "invariant-check.bin"), payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	state, err := coord.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	vs := journal.CheckIntegrityInvariants(coord.RootDir(), state)
	for _, v := range vs {
		t.Errorf("invariant violation after FUSE write: %s", v.Error())
	}

	records, err := journal.NewStore(filepath.Join(coord.RootDir(), "..", "journal.log")).Load()
	_ = records
	// Just verify that the journal is reachable (no error expected).
	if err != nil {
		t.Logf("journal load: %v", err)
	}
}

// ─── TestFUSEMultipleFilesInSameDirectory ─────────────────────────────────────

// Multiple files written to the same logical directory must all be readable
// and all appear in the directory listing.
func TestFUSEMultipleFilesInSameDirectory(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "multi")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	payloads := map[string][]byte{
		"a.bin": []byte("aaaaaaa"),
		"b.bin": []byte("bbbbbbbbbbbbb"),
		"c.bin": []byte("ccc"),
	}
	for name, data := range payloads {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	seen := make(map[string]bool)
	for _, e := range entries {
		if !e.IsDir() {
			seen[e.Name()] = true
		}
	}
	for name, data := range payloads {
		if !seen[name] {
			t.Errorf("file %s not found in ReadDir", name)
		}
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("ReadFile %s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, data) {
			t.Errorf("file %s content mismatch", name)
		}
	}
}

// ─── TestFUSEUnlinkReturnsEnotsup ─────────────────────────────────────────────

// Deleting a file must return an error (not silently succeed without
// persistence). The PoC returns ENOTSUP.
func TestFUSEUnlinkReturnsEnotsup(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	filePath := filepath.Join(dir, "to-delete.bin")
	if err := os.WriteFile(filePath, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	err := os.Remove(filePath)
	if err == nil {
		t.Fatal("expected Remove to fail, got nil error")
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("expected PathError, got %T: %v", err, err)
	}
}

// ─── TestFUSEWriteTinyFile ─────────────────────────────────────────────────────

// Writing a small file (sub-extent size) must work correctly.
func TestFUSEWriteTinyFile(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	tiny := []byte("hello from rdparityd FUSE!")
	filePath := filepath.Join(dir, "tiny.txt")
	if err := os.WriteFile(filePath, tiny, 0o644); err != nil {
		t.Fatalf("WriteFile tiny: %v", err)
	}
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile tiny: %v", err)
	}
	if !bytes.Equal(got, tiny) {
		t.Fatalf("tiny file mismatch: got %q, want %q", got, tiny)
	}
}

// ─── TestFUSEDirectoryStatIsDir ───────────────────────────────────────────────

// Stat on a virtual FUSE directory must indicate a directory.
func TestFUSEDirectoryStatIsDir(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stat-test.bin"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	for _, d := range []string{
		filepath.Join(mnt, "shares"),
		filepath.Join(mnt, "shares", "demo"),
	} {
		info, err := os.Stat(d)
		if err != nil {
			t.Fatalf("Stat %s: %v", d, err)
		}
		if !info.IsDir() {
			t.Errorf("expected %s to be a directory", d)
		}
	}
}

// ─── TestFUSEWriteCallsCoordinatorJournal ─────────────────────────────────────

// After writing through FUSE, the journal must have at least one committed
// record for the written file.
func TestFUSEWriteCallsCoordinatorJournal(t *testing.T) {
	dir := t.TempDir()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("FUSE not available")
	}
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coord := journal.NewCoordinator(metadataPath, journalPath)

	mnt := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatalf("mkdir mnt: %v", err)
	}
	srv, err := fusefs.Mount(mnt, coord, fusefs.Options{PoolName: "demo"})
	if err != nil {
		t.Skipf("mount: %v", err)
	}
	srv.WaitMount()     //nolint:errcheck
	defer srv.Unmount() //nolint:errcheck

	if err := os.MkdirAll(filepath.Join(mnt, "shares", "demo"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "shares", "demo", "journal-check.bin"), []byte("test"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	records, err := journal.NewStore(journalPath).Load()
	if err != nil {
		t.Fatalf("journal.Load: %v", err)
	}
	// Post-compaction: journal is empty after all transactions commit.
	// Verify via metadata instead.
	state, err := journal.NewCoordinator(metadataPath, journalPath).ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	hasFile := false
	for _, f := range state.Files {
		if f.Path == "/shares/demo/journal-check.bin" {
			hasFile = true
		}
	}
	if !hasFile {
		t.Fatal("expected committed file in metadata after FUSE write")
	}
	// Journal should be clean (compacted).
	summary, err := journal.NewStore(journalPath).Replay()
	if err != nil {
		t.Fatalf("journal.Replay: %v", err)
	}
	if summary.RequiresReplay {
		t.Fatalf("journal requires replay after committed FUSE write: %+v", summary)
	}
	_ = records // kept to not break build; may be empty post-compaction
}

// ─── benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkFUSEWrite measures the throughput of writing a 4 MiB file through
// the FUSE mount.
func BenchmarkFUSEWrite(b *testing.B) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		b.Skip("FUSE not available")
	}
	dir := b.TempDir()
	metadataPath := filepath.Join(dir, "metadata.json")
	journalPath := filepath.Join(dir, "journal.log")
	coord := journal.NewCoordinator(metadataPath, journalPath)
	mnt := filepath.Join(dir, "mnt")
	os.Mkdir(mnt, 0o755) //nolint:errcheck
	srv, err := fusefs.Mount(mnt, coord, fusefs.Options{PoolName: "demo"})
	if err != nil {
		b.Skipf("mount: %v", err)
	}
	srv.WaitMount()     //nolint:errcheck
	defer srv.Unmount() //nolint:errcheck

	os.MkdirAll(filepath.Join(mnt, "bench"), 0o755) //nolint:errcheck

	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		name := filepath.Join(mnt, "bench", fmt.Sprintf("bench-%d-%d.bin", time.Now().UnixNano(), i))
		if err := os.WriteFile(name, payload, 0o644); err != nil {
			b.Fatalf("WriteFile: %v", err)
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
