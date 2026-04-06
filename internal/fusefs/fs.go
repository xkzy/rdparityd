// Package fusefs provides a FUSE filesystem that presents the rdparityd pool's
// logical file namespace as a standard POSIX directory tree.
//
// All reads are routed through the coordinator's checksum-verified read path
// (with automatic healing from parity when an extent is corrupted). All writes
// are committed through the coordinator's journaled write path, so every file
// creation survives a crash after Flush (close(2)).
//
// # Layout
//
// The pool stores files at logical paths like "/shares/demo/file.bin". When
// mounted at "/mnt/pool", the same file is visible at
// "/mnt/pool/shares/demo/file.bin". Virtual directories ("shares", "demo") are
// synthesised on-the-fly from the metadata store; no directory entries are
// persisted separately.
//
// # Write model
//
// FUSE sends writes in small, potentially out-of-order chunks. This package
// buffers all writes in memory and commits the complete buffer to the
// coordinator on Flush (triggered by close(2) or explicit fsync). This means
// a file write is atomic from the coordinator's perspective: either the whole
// file is committed with a journal record, or nothing is.
package fusefs

import (
	"context"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
)

// ─── compile-time interface checks ───────────────────────────────────────────

var _ = (fs.NodeGetattrer)((*dirNode)(nil))
var _ = (fs.NodeLookuper)((*dirNode)(nil))
var _ = (fs.NodeReaddirer)((*dirNode)(nil))
var _ = (fs.NodeCreater)((*dirNode)(nil))
var _ = (fs.NodeUnlinker)((*dirNode)(nil))
var _ = (fs.NodeStatfser)((*dirNode)(nil))
var _ = (fs.NodeMkdirer)((*dirNode)(nil))

var _ = (fs.NodeGetattrer)((*fileNode)(nil))
var _ = (fs.NodeOpener)((*fileNode)(nil))
var _ = (fs.NodeSetattrer)((*fileNode)(nil))

var _ = (fs.FileFlusher)((*fileHandle)(nil))
var _ = (fs.FileReader)((*fileHandle)(nil))
var _ = (fs.FileWriter)((*fileHandle)(nil))
var _ = (fs.FileReleaser)((*fileHandle)(nil))
var _ = (fs.FileGetattrer)((*fileHandle)(nil))

// ─── dirNode ─────────────────────────────────────────────────────────────────

// dirNode is a virtual directory. Its children are derived from the pool
// metadata at query time, not stored in the node itself.
//
// prefix is the logical path of this directory (empty string for root, or a
// path like "/shares/demo").
type dirNode struct {
	fs.Inode
	coord    *journal.Coordinator
	prefix   string
	poolName string
}

func (d *dirNode) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	state, err := d.coord.ReadMeta()
	if err != nil {
		return syscall.EIO
	}
	totalExtents := int64(len(state.Extents))
	const blockSize = 4096
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.Blocks = uint64(totalExtents * (1 << 20) / blockSize)
	out.Bfree = out.Blocks / 2
	out.Bavail = out.Bfree
	out.Files = uint64(len(state.Files)) + 1024
	out.Ffree = 1024
	out.NameLen = 255
	return fs.OK
}

func (d *dirNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o755
	out.Nlink = 2
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	now := uint64(time.Now().Unix())
	out.Atime = now
	out.Mtime = now
	out.Ctime = now
	return fs.OK
}

func (d *dirNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	state, err := d.coord.ReadMeta()
	if err != nil {
		return nil, syscall.EIO
	}
	return newDirStream(d.prefix, state.Files), fs.OK
}

func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	state, err := d.coord.ReadMeta()
	if err != nil {
		return nil, syscall.EIO
	}

	childPrefix := d.prefix + "/" + name
	if d.prefix == "" {
		childPrefix = "/" + name
	}

	// Check for an exact file match first.
	for _, file := range state.Files {
		if file.Path == childPrefix {
			fillFileAttr(&out.Attr, file)
			n := &fileNode{
				coord:    d.coord,
				path:     childPrefix,
				poolName: d.poolName,
				file:     file,
			}
			child := d.NewInode(ctx, n, fs.StableAttr{Mode: syscall.S_IFREG})
			return child, fs.OK
		}
	}

	// Check for a virtual directory (any file whose path starts with this prefix + /).
	dirPrefix := childPrefix + "/"
	for _, file := range state.Files {
		if strings.HasPrefix(file.Path, dirPrefix) {
			now := uint64(time.Now().Unix())
			out.Attr.Mode = syscall.S_IFDIR | 0o755
			out.Attr.Nlink = 2
			out.Attr.Uid = uint32(os.Getuid())
			out.Attr.Gid = uint32(os.Getgid())
			out.Attr.Atime = now
			out.Attr.Mtime = now
			out.Attr.Ctime = now
			child := d.NewInode(ctx, &dirNode{
				coord:    d.coord,
				prefix:   childPrefix,
				poolName: d.poolName,
			}, fs.StableAttr{Mode: syscall.S_IFDIR})
			return child, fs.OK
		}
	}

	return nil, syscall.ENOENT
}

func (d *dirNode) Create(
	ctx context.Context,
	name string,
	flags uint32,
	mode uint32,
	out *fuse.EntryOut,
) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if strings.Contains(name, "\x00") {
		return nil, nil, 0, syscall.EINVAL
	}
	if name == ".." || name == "." || name == "" {
		return nil, nil, 0, syscall.EINVAL
	}
	if strings.ContainsRune(name, '/') {
		return nil, nil, 0, syscall.EINVAL
	}
	logicalPath := d.prefix + "/" + name
	if d.prefix == "" {
		logicalPath = "/" + name
	}

	now := time.Now()
	fakeFile := metadata.FileRecord{
		Path:      logicalPath,
		SizeBytes: 0,
		MTime:     now,
		CTime:     now,
	}
	fillFileAttr(&out.Attr, fakeFile)

	n := &fileNode{
		coord:    d.coord,
		path:     logicalPath,
		poolName: d.poolName,
		file:     fakeFile,
	}
	child := d.NewInode(ctx, n, fs.StableAttr{Mode: syscall.S_IFREG})

	fh := &fileHandle{
		coord:    d.coord,
		path:     logicalPath,
		poolName: d.poolName,
		writable: true,
	}
	return child, fh, fuse.FOPEN_DIRECT_IO, fs.OK
}

// Mkdir creates a virtual directory. No persistent state is written for the
// directory itself — the directory becomes visible in listings only once a file
// is created beneath it (since our directory structure is fully derived from
// file paths at query time). This is intentional: it means there are no
// "empty" directories to clean up, and the directory namespace is always
// consistent with the file namespace.
func (d *dirNode) Mkdir(
	ctx context.Context,
	name string,
	_ uint32,
	out *fuse.EntryOut,
) (*fs.Inode, syscall.Errno) {
	if strings.Contains(name, "\x00") {
		return nil, syscall.EINVAL
	}
	if name == ".." || name == "." || name == "" {
		return nil, syscall.EINVAL
	}
	if strings.ContainsRune(name, '/') {
		return nil, syscall.EINVAL
	}
	childPrefix := d.prefix + "/" + name
	if d.prefix == "" {
		childPrefix = "/" + name
	}
	now := uint64(time.Now().Unix())
	out.Attr.Mode = syscall.S_IFDIR | 0o755
	out.Attr.Nlink = 2
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Atime = now
	out.Attr.Mtime = now
	out.Attr.Ctime = now
	child := d.NewInode(ctx, &dirNode{
		coord:    d.coord,
		prefix:   childPrefix,
		poolName: d.poolName,
	}, fs.StableAttr{Mode: syscall.S_IFDIR})
	return child, fs.OK
}

// Unlink removes a file from the pool. This requires:
// 1. Remove file from metadata
// 2. Remove all extents from metadata
// 3. Update parity groups (remove extent references)
// 4. Update disk free space
// 5. Delete extent files from disk
// 6. Update and fsync parity files
// Currently returns ENOTSUP - requires DeleteFile in journal.Coordinator.
func (d *dirNode) Unlink(_ context.Context, _ string) syscall.Errno {
	return syscall.ENOTSUP
}

// ─── fileNode ─────────────────────────────────────────────────────────────────

// fileNode is a file stored in the pool. Its content is always fetched through
// the coordinator's read path, which verifies checksums and heals from parity.
type fileNode struct {
	fs.Inode
	coord    *journal.Coordinator
	path     string
	poolName string
	file     metadata.FileRecord
}

func (f *fileNode) Getattr(_ context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// If a file handle is open, it may have a more up-to-date size (e.g. while
	// a write is in progress). Use its view if available.
	if fh != nil {
		if hg, ok := fh.(fs.FileGetattrer); ok {
			return hg.Getattr(nil, out)
		}
	}

	// Reload to pick up changes committed by other processes.
	state, err := f.coord.ReadMeta()
	if err == nil {
		for _, file := range state.Files {
			if file.Path == f.path {
				fillFileAttr(&out.Attr, file)
				return fs.OK
			}
		}
	}
	// Fall back to the cached record.
	fillFileAttr(&out.Attr, f.file)
	return fs.OK
}

// Setattr accepts size truncation so that tools like echo/dd that truncate
// before writing don't get ENOTSUP. Other attribute changes are silently
// ignored for the PoC.
func (f *fileNode) Setattr(_ context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		if hg, ok := fh.(fs.FileGetattrer); ok {
			hg.Getattr(nil, out)
		}
	}
	if size, ok := in.GetSize(); ok {
		if wh, ok := fh.(*fileHandle); ok {
			wh.mu.Lock()
			if uint64(len(wh.data)) > size {
				wh.data = wh.data[:size]
			} else {
				extended := make([]byte, size)
				copy(extended, wh.data)
				wh.data = extended
			}
			wh.size = int64(size)
			wh.dirty = true
			wh.mu.Unlock()
			out.Size = size
		}
	}
	return fs.OK
}

func (f *fileNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	writable := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	truncate := flags&syscall.O_TRUNC != 0

	if writable {
		var initData []byte
		if !truncate && flags&syscall.O_RDWR != 0 {
			// Read-write open without truncate: load existing content so that
			// partial overwrites don't lose data.
			result, err := f.coord.ReadFile(f.path)
			if err == nil {
				initData = result.Data
			}
		}
		fh := &fileHandle{
			coord:    f.coord,
			path:     f.path,
			poolName: f.poolName,
			data:     initData,
			size:     int64(len(initData)),
			writable: true,
		}
		return fh, fuse.FOPEN_DIRECT_IO, fs.OK
	}

	// Read-only: load the verified content now. FOPEN_DIRECT_IO is used so that
	// the kernel doesn't cache the data (the coordinator already manages
	// checksums and healing, and this avoids stale-cache bugs in the PoC).
	result, err := f.coord.ReadFile(f.path)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	fh := &fileHandle{
		coord:    f.coord,
		path:     f.path,
		poolName: f.poolName,
		data:     result.Data,
		size:     result.BytesRead,
	}
	return fh, fuse.FOPEN_DIRECT_IO, fs.OK
}

// ─── fileHandle ───────────────────────────────────────────────────────────────

// fileHandle is the per-open-fd state. It is safe for concurrent use because
// the FUSE kernel layer can issue multiple Read/Write operations in parallel
// for the same file descriptor.
type fileHandle struct {
	mu       sync.Mutex
	coord    *journal.Coordinator
	path     string
	poolName string
	data     []byte
	size     int64
	writable bool
	dirty    bool
	flushed  bool // true once we have committed; makes Flush idempotent
}

func (h *fileHandle) Getattr(_ context.Context, out *fuse.AttrOut) syscall.Errno {
	h.mu.Lock()
	sz := int64(len(h.data))
	if h.size > sz {
		sz = h.size
	}
	h.mu.Unlock()
	out.Mode = syscall.S_IFREG | 0o644
	out.Nlink = 1
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(sz)
	now := uint64(time.Now().Unix())
	out.Atime = now
	out.Mtime = now
	out.Ctime = now
	return fs.OK
}

func (h *fileHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	data := h.data
	h.mu.Unlock()

	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

func (h *fileHandle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if !h.writable {
		return 0, syscall.EBADF
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	end := off + int64(len(data))
	if end > int64(len(h.data)) {
		extended := make([]byte, end)
		copy(extended, h.data)
		h.data = extended
		h.size = end
	}
	copy(h.data[off:], data)
	h.dirty = true
	return uint32(len(data)), fs.OK
}

// Flush is called on every close(2). It commits the write buffer to the
// coordinator when the file was opened for writing and at least one Write was
// issued. Flush is idempotent: the second call for the same logical
// modification is a no-op.
func (h *fileHandle) Flush(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.writable || !h.dirty || h.flushed {
		return fs.OK
	}

	payload := append([]byte(nil), h.data...)
	_, err := h.coord.WriteFile(context.Background(), journal.WriteRequest{
		PoolName:    h.poolName,
		LogicalPath: h.path,
		Payload:     payload,
	})
	if err != nil {
		return syscall.EIO
	}
	h.flushed = true
	return fs.OK
}

func (h *fileHandle) Release(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = nil
	return fs.OK
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func fillFileAttr(out *fuse.Attr, file metadata.FileRecord) {
	out.Mode = syscall.S_IFREG | 0o644
	out.Nlink = 1
	out.Size = uint64(file.SizeBytes)
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	mtime := uint64(file.MTime.Unix())
	if mtime == 0 {
		mtime = uint64(time.Now().Unix())
	}
	out.Atime = mtime
	out.Mtime = mtime
	out.Ctime = uint64(file.CTime.Unix())
	if out.Ctime == 0 {
		out.Ctime = mtime
	}
}

// ─── dirStream ───────────────────────────────────────────────────────────────

// dirStream returns the DirStream for listing children of prefix.
func newDirStream(prefix string, files []metadata.FileRecord) fs.DirStream {
	seen := make(map[string]bool)
	entries := []fuse.DirEntry{
		{Name: ".", Mode: syscall.S_IFDIR},
		{Name: "..", Mode: syscall.S_IFDIR},
	}

	for _, file := range files {
		child, isDir := childName(prefix, file.Path)
		if child == "" || seen[child] {
			continue
		}
		seen[child] = true
		mode := uint32(syscall.S_IFREG)
		if isDir {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{
			Name: child,
			Mode: mode,
		})
	}

	return &sliceDirStream{entries: entries}
}

// childName returns the name of the direct child of prefix within the given
// path, plus whether that child is itself a directory. Returns ("", false) if
// path is not a descendant of prefix.
func childName(prefix, filePath string) (name string, isDir bool) {
	// Normalise: if prefix is empty, all files are direct or nested children.
	base := filePath
	if prefix != "" {
		if !strings.HasPrefix(filePath, prefix+"/") {
			return "", false
		}
		base = filePath[len(prefix)+1:]
	} else {
		// prefix="" means root; strip the leading slash from absolute paths.
		base = strings.TrimPrefix(filePath, "/")
	}

	if base == "" {
		return "", false
	}

	slash := strings.IndexByte(base, '/')
	if slash == -1 {
		return base, false
	}
	return base[:slash], true
}

// sliceDirStream is a simple in-memory DirStream.
type sliceDirStream struct {
	mu      sync.Mutex
	entries []fuse.DirEntry
	pos     int
}

func (s *sliceDirStream) HasNext() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pos < len(s.entries)
}

func (s *sliceDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.entries) {
		return fuse.DirEntry{}, syscall.ENOENT
	}
	e := s.entries[s.pos]
	s.pos++
	return e, fs.OK
}

func (s *sliceDirStream) Close() {
	s.mu.Lock()
	s.pos = len(s.entries)
	s.mu.Unlock()
}
