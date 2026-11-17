//go:build linux

package main

import (
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"os"
)

// Published files are regular and immutable. Avoid blocking or following a
// replaced final component before checking its type.
func openCacheFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
}
func readCachedManifest(path string) ([]byte, error) {
	return readBoundedCacheFile(path, maxManifest)
}
func readBoundedCacheFile(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("invalid cached manifest file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(data)) > limit {
		return nil, errors.New("cached metadata exceeds limit")
	}
	return data, err
}
