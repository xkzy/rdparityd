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
	"fmt"
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

// compile-time interface checks ensure we implement all required FUSE interfaces

var (
	_ = (fs.NodeGetattrer)((*dirNode)(nil))
	_ = (fs.NodeLookuper)((*dirNode)(nil))
	_ = (fs.NodeReaddirer)((*dirNode)(nil))
	_ = (fs.NodeCreater)((*dirNode)(nil))
	_ = (fs.NodeUnlinker)((*dirNode)(nil))
	_ = (fs.NodeStatfser)((*dirNode)(nil))
	_ = (fs.NodeMkdirer)((*dirNode)(nil))
	_ = (fs.NodeRmdirer)((*dirNode)(nil))
	_ = (fs.NodeRenamer)((*dirNode)(nil))
	_ = (fs.NodeSymlinker)((*dirNode)(nil))
	_ = (fs.NodeMknoder)((*dirNode)(nil))
	_ = (fs.NodeLinker)((*dirNode)(nil))
	_ = (fs.NodeSetattrer)((*dirNode)(nil))
	_ = (fs.NodeAccesser)((*dirNode)(nil))
	_ = (fs.NodeReadlinker)((*dirNode)(nil))

	_ = (fs.NodeGetattrer)((*fileNode)(nil))
	_ = (fs.NodeOpener)((*fileNode)(nil))
	_ = (fs.NodeSetattrer)((*fileNode)(nil))
	_ = (fs.NodeReadlinker)((*fileNode)(nil))

	_ = (fs.FileFlusher)((*fileHandle)(nil))
	_ = (fs.FileReader)((*fileHandle)(nil))
	_ = (fs.FileWriter)((*fileHandle)(nil))
	_ = (fs.FileReleaser)((*fileHandle)(nil))
	_ = (fs.FileGetattrer)((*fileHandle)(nil))
	_ = (fs.FileFsyncer)((*fileHandle)(nil))
	_ = (fs.FileSetattrer)((*fileHandle)(nil))
)

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

// fullPath returns the absolute logical path for a child name within this directory.
func (d *dirNode) fullPath(name string) string {
	if d.prefix == "" {
		return "/" + name
	}
	return d.prefix + "/" + name
}

// parentPath returns the logical path of the parent directory.
func (d *dirNode) parentPath() string {
	if d.prefix == "" {
		return ""
	}
	slash := strings.LastIndexByte(d.prefix, '/')
	if slash <= 0 {
		return ""
	}
	return d.prefix[:slash]
}

// baseName returns the name of this directory within its parent.
func (d *dirNode) baseName() string {
	if d.prefix == "" {
		return ""
	}
	slash := strings.LastIndexByte(d.prefix, '/')
	if slash < 0 {
		return d.prefix
	}
	return d.prefix[slash+1:]
}

// Statfs returns filesystem statistics.
func (d *dirNode) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	state, err := d.coord.ReadMeta()
	if err != nil {
		return syscall.EIO
	}

	var totalBytes, freeBytes int64
	for _, disk := range state.Disks {
		totalBytes += disk.CapacityBytes
		freeBytes += disk.FreeBytes
	}

	const blockSize = 4096
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.Blocks = uint64(totalBytes / blockSize)
	out.Bfree = uint64(freeBytes / blockSize)
	out.Bavail = out.Bfree
	out.Files = uint64(len(state.Files))
	out.Ffree = 1024
	out.NameLen = 255
	return fs.OK
}

// Getattr returns attributes for this directory.
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

// Readdir returns all children of this directory.
func (d *dirNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	state, err := d.coord.ReadMeta()
	if err != nil {
		return nil, syscall.EIO
	}
	return newDirStream(d.prefix, state.Files), fs.OK
}

// Lookup looks up a child by name.
func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." {
		out.Attr.Mode = syscall.S_IFDIR | 0o755
		out.Attr.Nlink = 2
		out.Attr.Uid = uint32(os.Getuid())
		out.Attr.Gid = uint32(os.Getgid())
		now := uint64(time.Now().Unix())
		out.Attr.Atime = now
		out.Attr.Mtime = now
		out.Attr.Ctime = now
		return d.NewInode(ctx, d, fs.StableAttr{Mode: syscall.S_IFDIR}), fs.OK
	}

	if name == ".." {
		parentPrefix := d.parentPath()
		out.Attr.Mode = syscall.S_IFDIR | 0o755
		out.Attr.Nlink = 2
		out.Attr.Uid = uint32(os.Getuid())
		out.Attr.Gid = uint32(os.Getgid())
		now := uint64(time.Now().Unix())
		out.Attr.Atime = now
		out.Attr.Mtime = now
		out.Attr.Ctime = now

		if parentPrefix == "" {
			return d.NewInode(ctx, d, fs.StableAttr{Mode: syscall.S_IFDIR}), fs.OK
		}
		parent := &dirNode{
			coord:    d.coord,
			prefix:   parentPrefix,
			poolName: d.poolName,
		}
		return d.NewInode(ctx, parent, fs.StableAttr{Mode: syscall.S_IFDIR}), fs.OK
	}

	state, err := d.coord.ReadMeta()
	if err != nil {
		return nil, syscall.EIO
	}

	childPath := d.fullPath(name)

	// Check for an exact file match.
	for _, file := range state.Files {
		if file.Path == childPath {
			fillFileAttr(&out.Attr, file)
			if file.FileType == metadata.FileTypeDirectory {
				child := d.NewInode(ctx, &dirNode{
					coord:    d.coord,
					prefix:   childPath,
					poolName: d.poolName,
				}, fs.StableAttr{Mode: modeFromFile(file)})
				return child, fs.OK
			}
			n := &fileNode{
				coord:    d.coord,
				path:     childPath,
				poolName: d.poolName,
				file:     file,
			}
			child := d.NewInode(ctx, n, fs.StableAttr{Mode: modeFromFile(file)})
			return child, fs.OK
		}
	}

	// Check for a virtual directory (any file whose path starts with this prefix + /).
	dirPrefix := childPath + "/"
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
				prefix:   childPath,
				poolName: d.poolName,
			}, fs.StableAttr{Mode: syscall.S_IFDIR})
			return child, fs.OK
		}
	}

	return nil, syscall.ENOENT
}

// Create creates a new file.
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

	logicalPath := d.fullPath(name)

	now := time.Now()
	fakeFile := metadata.FileRecord{
		Path:      logicalPath,
		SizeBytes: 0,
		MTime:     now,
		CTime:     now,
		Mode:      mode,
	}
	fillFileAttr(&out.Attr, fakeFile)

	n := &fileNode{
		coord:    d.coord,
		path:     logicalPath,
		poolName: d.poolName,
		file:     fakeFile,
	}
	child := d.NewInode(ctx, n, fs.StableAttr{Mode: modeFromFile(fakeFile)})

	fh := &fileHandle{
		coord:    d.coord,
		path:     logicalPath,
		poolName: d.poolName,
		writable: true,
	}
	return child, fh, fuse.FOPEN_DIRECT_IO, fs.OK
}

// Mkdir creates a directory.
func (d *dirNode) Mkdir(
	ctx context.Context,
	name string,
	mode uint32,
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
	childPrefix := d.fullPath(name)

	if err := d.coord.CreateDirectory(childPrefix, mode); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, syscall.EEXIST
		}
		return nil, syscall.EIO
	}

	now := uint64(time.Now().Unix())
	out.Attr.Mode = syscall.S_IFDIR | (mode & 0o7777)
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
	}, fs.StableAttr{Mode: syscall.S_IFDIR | (mode & 0o7777)})
	return child, fs.OK
}

// Unlink removes a regular file.
func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	targetPath := d.fullPath(name)

	// Check if it's a directory
	state, err := d.coord.ReadMeta()
	if err != nil {
		return syscall.EIO
	}

	for _, file := range state.Files {
		if file.Path == targetPath {
			if file.FileType == metadata.FileTypeDirectory {
				return syscall.EISDIR
			}
			break
		}
	}

	_, err = d.coord.DeleteFile(targetPath)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	return fs.OK
}

// Rmdir removes an empty directory.
func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	targetPath := d.fullPath(name)

	state, err := d.coord.ReadMeta()
	if err != nil {
		return syscall.EIO
	}

	var isDir bool
	for _, file := range state.Files {
		if file.Path == targetPath {
			if file.FileType != metadata.FileTypeDirectory {
				return syscall.ENOTDIR
			}
			isDir = true
			break
		}
	}
	if !isDir {
		return syscall.ENOENT
	}

	dirPrefix := targetPath + "/"
	for _, file := range state.Files {
		if strings.HasPrefix(file.Path, dirPrefix) {
			return syscall.ENOTEMPTY
		}
	}

	if err := d.coord.DeleteDirectory(targetPath); err != nil {
		if strings.Contains(err.Error(), "not empty") {
			return syscall.ENOTEMPTY
		}
		if strings.Contains(err.Error(), "not found") {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	return fs.OK
}

// Rename renames a file or directory.
func (d *dirNode) Rename(ctx context.Context, oldName string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := d.fullPath(oldName)
	newDir := newParent.(*dirNode)
	newPath := newDir.fullPath(newName)

	if oldPath == newPath {
		return fs.OK
	}

	// Check that source exists
	state, err := d.coord.ReadMeta()
	if err != nil {
		return syscall.EIO
	}

	found := false
	for _, file := range state.Files {
		if file.Path == oldPath {
			found = true
			break
		}
	}
	if !found {
		return syscall.ENOENT
	}

	// Check destination doesn't exist
	for _, file := range state.Files {
		if file.Path == newPath {
			return syscall.EEXIST
		}
	}

	_, err = d.coord.RenameFile(oldPath, newPath)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return syscall.ENOENT
		}
		if strings.Contains(err.Error(), "already exists") {
			return syscall.EEXIST
		}
		return syscall.EIO
	}

	return fs.OK
}

// Symlink creates a symbolic link.
func (d *dirNode) Symlink(ctx context.Context, target string, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	symlinkPath := d.fullPath(name)

	if err := d.coord.CreateSymlink(symlinkPath, target, uint32(syscall.S_IFLNK|0o777)); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, syscall.EEXIST
		}
		return nil, syscall.EIO
	}

	now := time.Now()
	fakeFile := metadata.FileRecord{
		Path:       symlinkPath,
		SizeBytes:  int64(len(target)),
		MTime:      now,
		CTime:      now,
		FileType:   metadata.FileTypeSymlink,
		Mode:       uint32(syscall.S_IFLNK | 0o777),
		LinkTarget: target,
	}
	fillFileAttr(&out.Attr, fakeFile)

	n := &fileNode{
		coord:    d.coord,
		path:     symlinkPath,
		poolName: d.poolName,
		file:     fakeFile,
	}
	child := d.NewInode(ctx, n, fs.StableAttr{Mode: syscall.S_IFLNK})
	return child, fs.OK
}

// Mknod creates a special file (FIFO, socket, block device, char device).
func (d *dirNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	nodePath := d.fullPath(name)

	fileType := metadata.FileTypeFromMode(mode)
	devMajor := dev >> 8
	devMinor := dev & 0xff

	if err := d.coord.CreateSpecialFile(nodePath, mode, devMajor, devMinor); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, syscall.EEXIST
		}
		return nil, syscall.EIO
	}

	now := time.Now()
	fakeFile := metadata.FileRecord{
		Path:      nodePath,
		SizeBytes: 0,
		MTime:     now,
		CTime:     now,
		FileType:  fileType,
		Mode:      mode,
		DevMajor:  devMajor,
		DevMinor:  devMinor,
	}

	switch fileType {
	case metadata.FileTypeFIFO:
		fakeFile.Mode = mode | syscall.S_IFIFO
	case metadata.FileTypeSocket:
		fakeFile.Mode = mode | syscall.S_IFSOCK
	case metadata.FileTypeBlockDevice:
		fakeFile.Mode = mode | syscall.S_IFBLK
	case metadata.FileTypeCharDevice:
		fakeFile.Mode = mode | syscall.S_IFCHR
	}

	fillFileAttr(&out.Attr, fakeFile)

	n := &fileNode{
		coord:    d.coord,
		path:     nodePath,
		poolName: d.poolName,
		file:     fakeFile,
	}

	child := d.NewInode(ctx, n, fs.StableAttr{Mode: fakeFile.Mode})
	return child, fs.OK
}

// Link creates a hard link.
func (d *dirNode) Link(ctx context.Context, node fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	existing := node.(*fileNode)
	hardLinkPath := d.fullPath(name)

	if err := d.coord.CreateHardLink(existing.path, hardLinkPath); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, syscall.EEXIST
		}
		if strings.Contains(err.Error(), "not found") {
			return nil, syscall.ENOENT
		}
		if strings.Contains(err.Error(), "cannot hard link") {
			return nil, syscall.EPERM
		}
		return nil, syscall.EIO
	}

	fillFileAttr(&out.Attr, existing.file)

	n := &fileNode{
		coord:    d.coord,
		path:     hardLinkPath,
		poolName: d.poolName,
		file:     existing.file,
	}
	child := d.NewInode(ctx, n, fs.StableAttr{Mode: modeFromFile(existing.file)})
	return child, fs.OK
}

// Setattr sets directory attributes.
func (d *dirNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		if err := d.coord.SetFileMode(d.prefix, mode); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return syscall.ENOENT
			}
			return syscall.EIO
		}
	}

	if atime, ok := in.GetATime(); ok {
		if mtime, ok2 := in.GetMTime(); ok2 {
			if err := d.coord.SetFileTimes(d.prefix, atime, mtime); err != nil {
				if strings.Contains(err.Error(), "not found") {
					return syscall.ENOENT
				}
				return syscall.EIO
			}
		} else {
			if err := d.coord.SetFileTimes(d.prefix, atime, atime); err != nil {
				if strings.Contains(err.Error(), "not found") {
					return syscall.ENOENT
				}
				return syscall.EIO
			}
		}
	} else if mtime, ok := in.GetMTime(); ok {
		if err := d.coord.SetFileTimes(d.prefix, mtime, mtime); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return syscall.ENOENT
			}
			return syscall.EIO
		}
	}

	return d.Getattr(ctx, fh, out)
}

// Access checks file access permissions.
func (d *dirNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	return fs.OK
}

// Readlink reads the target of a symbolic link.
func (d *dirNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return nil, syscall.ENOLINK
}

// fileNode is a file stored in the pool. Its content is always fetched through
// the coordinator's read path, which verifies checksums and heals from parity.
type fileNode struct {
	fs.Inode
	coord    *journal.Coordinator
	path     string
	poolName string
	file     metadata.FileRecord
}

// Getattr returns attributes for this file.
func (f *fileNode) Getattr(_ context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
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

// Setattr modifies file attributes (size, mode, times).
func (f *fileNode) Setattr(_ context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		if hg, ok := fh.(fs.FileGetattrer); ok {
			return hg.Getattr(nil, out)
		}
	}

	// Handle size truncation
	if size, ok := in.GetSize(); ok {
		// If file is open with a handle, update via the handle
		if wh, ok := fh.(*fileHandle); ok {
			wh.mu.Lock()
			if uint64(len(wh.data)) > size {
				wh.data = wh.data[:size]
			} else if uint64(len(wh.data)) < size {
				extended := make([]byte, size)
				copy(extended, wh.data)
				wh.data = extended
			}
			wh.size = int64(size)
			wh.dirty = true
			wh.mu.Unlock()
			out.Size = size
		} else {
			// Truncate via coordinator
			_, err := f.coord.TruncateFile(f.path, int64(size))
			if err != nil {
				return syscall.EIO
			}
			out.Size = size
		}
	}

	// Handle mode changes
	if mode, ok := in.GetMode(); ok {
		if err := f.coord.SetFileMode(f.path, mode); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return syscall.ENOENT
			}
			return syscall.EIO
		}
	}

	// Handle time changes
	if atime, ok := in.GetATime(); ok {
		if mtime, ok2 := in.GetMTime(); ok2 {
			if err := f.coord.SetFileTimes(f.path, atime, mtime); err != nil {
				if strings.Contains(err.Error(), "not found") {
					return syscall.ENOENT
				}
				return syscall.EIO
			}
		} else {
			if err := f.coord.SetFileTimes(f.path, atime, atime); err != nil {
				if strings.Contains(err.Error(), "not found") {
					return syscall.ENOENT
				}
				return syscall.EIO
			}
		}
	} else if mtime, ok := in.GetMTime(); ok {
		if err := f.coord.SetFileTimes(f.path, mtime, mtime); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return syscall.ENOENT
			}
			return syscall.EIO
		}
	}

	f.Getattr(nil, fh, out)
	return fs.OK
}

// Open opens the file for reading or writing.
func (f *fileNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	writable := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	truncate := flags&syscall.O_TRUNC != 0

	if writable {
		var initData []byte
		if !truncate && flags&syscall.O_RDWR != 0 {
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

	// Read-only
	result, err := f.coord.ReadFile(f.path)
	if err != nil {
		fmt.Printf("Open ReadFile error: path=%s error=%v\n", f.path, err)
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

// Readlink reads the symbolic link target.
func (f *fileNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	state, err := f.coord.ReadMeta()
	if err != nil {
		return nil, syscall.EIO
	}

	for _, file := range state.Files {
		if file.Path == f.path {
			if file.FileType == metadata.FileTypeSymlink && file.LinkTarget != "" {
				return []byte(file.LinkTarget), fs.OK
			}
			return nil, syscall.EINVAL
		}
	}

	if f.file.FileType == metadata.FileTypeSymlink && f.file.LinkTarget != "" {
		return []byte(f.file.LinkTarget), fs.OK
	}

	return nil, syscall.ENOLINK
}

// fileHandle is the per-open-fd state.
type fileHandle struct {
	mu       sync.Mutex
	coord    *journal.Coordinator
	path     string
	poolName string
	data     []byte
	size     int64
	writable bool
	dirty    bool
	flushed  bool
}

// Getattr returns file attributes from the handle.
func (h *fileHandle) Getattr(_ context.Context, out *fuse.AttrOut) syscall.Errno {
	h.mu.Lock()
	sz := h.size
	if sz < int64(len(h.data)) {
		sz = int64(len(h.data))
	}
	h.mu.Unlock()
	out.Size = uint64(sz)
	out.Mode = syscall.S_IFREG | 0o644
	out.Nlink = 1
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	now := uint64(time.Now().Unix())
	out.Atime = now
	out.Mtime = now
	out.Ctime = now
	return fs.OK
}

// Setattr modifies file attributes through the handle.
func (h *fileHandle) Setattr(_ context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		h.mu.Lock()
		if uint64(len(h.data)) > size {
			h.data = h.data[:size]
		} else if uint64(len(h.data)) < size {
			extended := make([]byte, size)
			copy(extended, h.data)
			h.data = extended
		}
		h.size = int64(size)
		h.dirty = true
		h.mu.Unlock()
		out.Size = size
	}
	return fs.OK
}

// Read reads data from the file.
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

// Write writes data to the file.
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

// Flush commits the write buffer to the coordinator.
func (h *fileHandle) Flush(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.writable || !h.dirty || h.flushed {
		return fs.OK
	}

	payload := make([]byte, len(h.data))
	copy(payload, h.data)

	if err := h.coord.SaveFileData(h.path, payload); err != nil {
		return syscall.EIO
	}
	h.flushed = true
	return fs.OK
}

// Fsync synchronizes file state to storage.
func (h *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.Flush(ctx)
}

// Release releases the file handle.
func (h *fileHandle) Release(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = nil
	return fs.OK
}

// helpers

// modeFromFile returns the appropriate syscall mode for a file based on its metadata.
func modeFromFile(file metadata.FileRecord) uint32 {
	switch file.FileType {
	case metadata.FileTypeSymlink:
		return syscall.S_IFLNK | 0o777
	case metadata.FileTypeDirectory:
		return syscall.S_IFDIR | 0o755
	case metadata.FileTypeFIFO:
		return syscall.S_IFIFO | 0o644
	case metadata.FileTypeSocket:
		return syscall.S_IFSOCK | 0o644
	case metadata.FileTypeBlockDevice:
		return syscall.S_IFBLK | 0o644
	case metadata.FileTypeCharDevice:
		return syscall.S_IFCHR | 0o644
	default:
		mode := uint32(syscall.S_IFREG | 0o644)
		if file.Mode != 0 {
			mode = file.Mode & 0o777
			if file.Mode&syscall.S_IFMT == 0 {
				mode |= syscall.S_IFREG
			} else {
				mode |= file.Mode & syscall.S_IFMT
			}
		}
		return mode
	}
}

// fillFileAttr populates a fuse.Attr from a FileRecord.
func fillFileAttr(out *fuse.Attr, file metadata.FileRecord) {
	out.Size = uint64(file.SizeBytes)
	out.Mode = modeFromFile(file)
	out.Nlink = 1
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	mtime := uint64(file.MTime.Unix())
	if mtime == 0 {
		mtime = uint64(time.Now().Unix())
	}
	out.Atime = mtime
	out.Mtime = mtime
	ctime := uint64(file.CTime.Unix())
	if ctime == 0 {
		ctime = mtime
	}
	out.Ctime = ctime
}

// sliceDirStream is a simple in-memory DirStream.
type sliceDirStream struct {
	mu      sync.Mutex
	entries []fuse.DirEntry
	pos     int
}

// newDirStream creates a directory stream for listing children of prefix.
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
		mode := modeFromFile(file)
		if isDir {
			mode = syscall.S_IFDIR | 0o755
		}
		entries = append(entries, fuse.DirEntry{
			Name: child,
			Mode: mode,
		})
	}

	return &sliceDirStream{entries: entries}
}

// childName returns the name of the direct child of prefix within the given
// path, plus whether that child is itself a directory.
func childName(prefix, filePath string) (name string, isDir bool) {
	base := filePath
	if prefix != "" {
		if !strings.HasPrefix(filePath, prefix+"/") {
			return "", false
		}
		base = filePath[len(prefix)+1:]
	} else {
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
	defer s.mu.Unlock()
	s.pos = len(s.entries)
}
