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
		poolName, err := coord.PoolName()
		if err != nil {
			return nil, err
		}
		opts.PoolName = poolName
	}

	if err := coord.EnsureInitialized(opts.PoolName); err != nil {
		return nil, err
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
