package fusefs

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/xkzy/rdparityd/internal/journal"
)

// Options configures the FUSE mount.
type Options struct {
	// PoolName is the rdparityd pool name used when writing new files.
	// Defaults to "demo" if empty.
	PoolName string

	// Debug enables verbose FUSE operation logging.
	Debug bool
}

// Mount mounts the pool at mountpoint and returns a running FUSE server. The
// server serves the filesystem in background goroutines. Call server.Wait() to
// block until the filesystem is unmounted, or server.Unmount() to dismount it
// programmatically.
//
// The caller must ensure that mountpoint is an existing empty directory.
func Mount(mountpoint string, coord *journal.Coordinator, opts Options) (*fuse.Server, error) {
	if opts.PoolName == "" {
		opts.PoolName = coord.PoolName()
	}

	root := &dirNode{
		coord:    coord,
		prefix:   "",
		poolName: opts.PoolName,
	}

	sec := time.Second
	return fs.Mount(mountpoint, root, &fs.Options{
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
		MountOptions: fuse.MountOptions{
			Debug:   opts.Debug,
			FsName:  "rdparityd",
			Name:    "rdparityd",
			Options: []string{"default_permissions"},
		},
	})
}
