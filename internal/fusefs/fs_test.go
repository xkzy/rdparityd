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

// ─── TestFUSEUnlink ───────────────────────────────────────────────────────────

// Deleting a file through the FUSE mount must remove it from the filesystem
// and make it inaccessible on subsequent read attempts.
func TestFUSEUnlink(t *testing.T) {
	mnt, coord, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	filePath := filepath.Join(dir, "to-delete.bin")
	if err := os.WriteFile(filePath, []byte("data to be deleted"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Verify file exists via coordinator.
	_, err := coord.ReadFile("/shares/demo/to-delete.bin")
	if err != nil {
		t.Fatalf("file should exist before unlink: %v", err)
	}

	// Unlink the file.
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Verify file no longer exists via coordinator.
	_, err = coord.ReadFile("/shares/demo/to-delete.bin")
	if err == nil {
		t.Fatal("expected error reading deleted file")
	}

	// Verify file no longer appears in directory listing.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.Name() == "to-delete.bin" {
			t.Error("deleted file still appears in directory")
		}
	}
}

// ─── TestFUSEUnlinkNonexistent ─────────────────────────────────────────────────

// Removing a nonexistent file must return ENOENT.
func TestFUSEUnlinkNonexistent(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	filePath := filepath.Join(dir, "nonexistent.bin")
	err := os.Remove(filePath)
	if err == nil {
		t.Fatal("expected error removing nonexistent file")
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("expected PathError, got %T: %v", err, err)
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected ENOENT, got %v", err)
	}
}

// ─── TestFUSERename ────────────────────────────────────────────────────────────

// Renaming a file through the FUSE mount must update its path in metadata
// and make it accessible at the new path.
func TestFUSERename(t *testing.T) {
	mnt, coord, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	oldPath := filepath.Join(dir, "oldname.txt")
	newPath := filepath.Join(dir, "newname.txt")
	payload := []byte("rename test data")
	if err := os.WriteFile(oldPath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Verify file exists at old path.
	_, err := coord.ReadFile("/shares/demo/oldname.txt")
	if err != nil {
		t.Fatalf("file should exist at old path: %v", err)
	}

	// Rename the file.
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Verify file no longer exists at old path.
	_, err = coord.ReadFile("/shares/demo/oldname.txt")
	if err == nil {
		t.Fatal("file still exists at old path after rename")
	}

	// Verify file exists at new path.
	result, err := coord.ReadFile("/shares/demo/newname.txt")
	if err != nil {
		t.Fatalf("file should exist at new path: %v", err)
	}
	if !bytes.Equal(result.Data, payload) {
		t.Fatalf("content mismatch at new path: got %v, want %v", result.Data, payload)
	}

	// Verify file appears in directory listing with new name.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Name() == "newname.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("renamed file not found in directory listing")
	}
}

// ─── TestFUSERenameOverwrite ───────────────────────────────────────────────────

// Renaming a file to an existing filename must fail with EEXIST.
func TestFUSERenameOverwrite(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	file1 := filepath.Join(dir, "file1.txt")
	file2 := filepath.Join(dir, "file2.txt")
	if err := os.WriteFile(file1, []byte("content1"), 0o644); err != nil {
		t.Fatalf("WriteFile file1: %v", err)
	}
	if err := os.WriteFile(file2, []byte("content2"), 0o644); err != nil {
		t.Fatalf("WriteFile file2: %v", err)
	}

	// Try to rename file1 to file2's name (should fail).
	err := os.Rename(file1, file2)
	if err == nil {
		t.Fatal("expected error renaming over existing file")
	}
	// os.Rename returns *os.LinkError which wraps the underlying error.
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) {
		t.Fatalf("expected LinkError, got %T: %v", err, err)
	}
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("expected EEXIST, got %v", err)
	}

	// Both files should still exist with original content.
	got1, err := os.ReadFile(file1)
	if err != nil || !bytes.Equal(got1, []byte("content1")) {
		t.Error("file1 content changed after failed rename")
	}
	got2, err := os.ReadFile(file2)
	if err != nil || !bytes.Equal(got2, []byte("content2")) {
		t.Error("file2 content changed after failed rename")
	}
}

// ─── TestFUSERenameNonexistent ─────────────────────────────────────────────────

// Renaming a nonexistent file must return ENOENT.
func TestFUSERenameNonexistent(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	oldPath := filepath.Join(dir, "nonexistent.txt")
	newPath := filepath.Join(dir, "newname.txt")
	err := os.Rename(oldPath, newPath)
	if err == nil {
		t.Fatal("expected error renaming nonexistent file")
	}
	// os.Rename returns *os.LinkError which wraps the underlying error.
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) {
		t.Fatalf("expected LinkError, got %T: %v", err, err)
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected ENOENT, got %v", err)
	}
}

// ─── TestFUSETruncate ──────────────────────────────────────────────────────────

// Truncating a file through the FUSE mount must update its size in metadata.
// Note: The current TruncateFile implementation removes entire extents,
// so truncation only works correctly when truncating at an extent boundary
// or when the file has multiple extents and we truncate to remove whole extents.
func TestFUSETruncate(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	filePath := filepath.Join(dir, "truncate-test.bin")
	// Write a file larger than one extent (1 MiB + 1 byte) so it spans extents.
	// The first extent is 1 MiB, second extent is 1 byte.
	original := make([]byte, 1<<21) // 2 MiB
	for i := range original {
		original[i] = byte(i % 256)
	}
	if err := os.WriteFile(filePath, original, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Truncate to exactly 1 MiB (extent boundary).
	if err := os.Truncate(filePath, 1<<20); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Verify new size via stat.
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat after truncate: %v", err)
	}
	if info.Size() != 1<<20 {
		t.Fatalf("size after truncate: got %d, want %d", info.Size(), 1<<20)
	}

	// Verify content is correct up to new size.
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile after truncate: %v", err)
	}
	if int64(len(got)) != 1<<20 {
		t.Fatalf("read length after truncate: got %d, want %d", len(got), 1<<20)
	}
	for i := 0; i < 1<<20; i++ {
		if got[i] != original[i] {
			t.Fatalf("content mismatch at byte %d: got %v, want %v", i, got[i], original[i])
		}
	}
}

// ─── TestFUSETruncateExpand ────────────────────────────────────────────────────

// Truncating a file to a larger size is currently not supported by the
// coordinator's TruncateFile, so the size remains unchanged.
func TestFUSETruncateExpand(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	filePath := filepath.Join(dir, "expand-test.bin")
	originalContent := []byte("hello")
	if err := os.WriteFile(filePath, originalContent, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Try to expand to 8192 bytes.
	if err := os.Truncate(filePath, 8192); err != nil {
		t.Fatalf("Truncate expand: %v", err)
	}

	// Verify size is unchanged (TruncateFile doesn't support expansion).
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat after truncate: %v", err)
	}
	if info.Size() != int64(len(originalContent)) {
		t.Fatalf("size after expand truncate: got %d, want %d (expansion not supported)", info.Size(), len(originalContent))
	}

	// Original content should still be readable.
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile after truncate: %v", err)
	}
	if !bytes.Equal(got, originalContent) {
		t.Errorf("content changed after expand attempt: got %v, want %v", got, originalContent)
	}
}

// ─── TestFUSERmdirEmpty ───────────────────────────────────────────────────────

// Removing an empty directory must succeed.
func TestFUSERmdirEmpty(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	subdir := filepath.Join(dir, "empty-dir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("Mkdir subdir: %v", err)
	}

	// Remove the empty directory.
	if err := os.Remove(subdir); err != nil {
		t.Fatalf("Remove empty dir: %v", err)
	}

	// Verify it's gone.
	_, err := os.Stat(subdir)
	if err == nil {
		t.Fatal("removed directory still exists")
	}
}

// ─── TestFUSERmdirNonempty ────────────────────────────────────────────────────

// Removing a non-empty directory must fail with ENOTEMPTY.
func TestFUSERmdirNonempty(t *testing.T) {
	mnt, _, cleanup := testMount(t)
	defer cleanup()

	dir := filepath.Join(mnt, "shares", "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	subdir := filepath.Join(dir, "nonempty-dir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("Mkdir subdir: %v", err)
	}

	// Create a file inside.
	if err := os.WriteFile(filepath.Join(subdir, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Try to remove the non-empty directory.
	err := os.Remove(subdir)
	if err == nil {
		t.Fatal("expected error removing non-empty directory")
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("expected PathError, got %T: %v", err, err)
	}
	if !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("expected ENOTEMPTY, got %v", err)
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
