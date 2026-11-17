//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Cache identity is the immutable origin key, independent of the requested set.
// A valid but mismatched manifest fails without fetching the key again.
func (c *cache) manifest(ctx context.Context, req request) (*Manifest, error) {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(req.ManifestKey)))
	key := "m:" + digest
	release := c.protect([]string{key})
	defer release()
	for attempt := 0; attempt < 2; attempt++ {
		err := c.ensure(ctx, key, func(ctx context.Context) (string, error) {
			m, err := LoadManifest(ctx, c.source, req.ManifestKey, req.TestcaseSetID)
			if err != nil {
				return "", err
			}
			data, err := json.Marshal(m)
			if err != nil {
				return "", err
			}
			path := filepath.Join(c.root, "manifests", digest+".json")
			if err := c.reserve(int64(len(data)) + 8192); err != nil {
				return "", err
			}
			f, err := os.CreateTemp(filepath.Join(c.root, "tmp"), "manifest-")
			if err != nil {
				return "", err
			}
			defer os.Remove(f.Name())
			_, err = f.Write(data)
			err = errors.Join(err, f.Close())
			if err != nil {
				return "", err
			}
			return path, os.Rename(f.Name(), path)
		})
		if err != nil {
			return nil, err
		}
		entries, unlock, err := c.lockFiles([]string{key})
		if err != nil {
			continue
		}
		entry := entries[key]
		f, err := os.Open(entry.path)
		var data []byte
		if err == nil {
			data, err = io.ReadAll(io.LimitReader(f, maxManifest+1))
			err = errors.Join(err, f.Close())
		}
		unlock()
		var m *Manifest
		if err == nil {
			if len(data) > maxManifest {
				err = errors.New("cached manifest exceeds limit")
			} else {
				m, err = ParseManifest(data)
			}
		}
		if err == nil {
			if m.TestcaseSetID != req.TestcaseSetID {
				return nil, fmt.Errorf("%w: manifest version mismatch", errInvalid)
			}
			return m, nil
		}
		// Invalidate only the incarnation we read. Readers of a concurrently repaired
		// entry are never detached by this failed reader.
		entry.filesMu.Lock()
		c.mu.Lock()
		if c.entries[key] != entry {
			c.mu.Unlock()
			entry.filesMu.Unlock()
			continue
		}
		tomb, err := os.MkdirTemp(filepath.Join(c.root, "tmp"), "invalid-manifest-")
		if err == nil {
			err = os.Rename(entry.path, filepath.Join(tomb, "manifest"))
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
		}
		if err == nil {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		entry.filesMu.Unlock()
		if tomb != "" {
			err = errors.Join(err, os.RemoveAll(tomb))
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, errors.New("cached manifest changed during recovery; retry request")
}
