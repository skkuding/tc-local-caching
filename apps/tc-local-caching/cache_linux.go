//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const maxCases = 512
const maxManifest = 256 << 10

var errInvalid = errors.New("invalid request")
var errCapacity = errors.New("cache or FD capacity exhausted")

type cacheEntry struct {
	// Never wait for filesMu while holding cache.mu. When both are needed,
	// acquire filesMu first. Published paths remain immutable.
	filesMu  sync.RWMutex
	path     string
	lastUsed time.Time
}
type loadResult struct {
	done chan struct{}
	err  error
}
type cache struct {
	root          string
	source        BundleSource
	high, low     int64
	ctx           context.Context
	mu            sync.Mutex // entries, refs, loading, downloads and lastUsed
	entries       map[string]*cacheEntry
	refs          map[string]int
	loading       map[string]*loadResult
	downloads     map[string]int
	slot          chan struct{}
	loads         sync.WaitGroup
	removeEvicted func(string) error // defaults to os.RemoveAll; allows removal fault injection
}
type acquired struct {
	cases []caseMetadata
	files []*os.File
}

func (a acquired) close() {
	for _, f := range a.files {
		_ = f.Close()
	}
}

func newCache(ctx context.Context, root string, source BundleSource, high, low int64) (*cache, error) {
	if low <= 0 || high <= low {
		return nil, errors.New("watermarks must satisfy 0 < low < high")
	}
	c := &cache{root: root, source: source, high: high, low: low, ctx: ctx, entries: map[string]*cacheEntry{}, refs: map[string]int{}, loading: map[string]*loadResult{}, downloads: map[string]int{}, slot: make(chan struct{}, 1), removeEvicted: os.RemoveAll}
	for _, d := range []string{"manifests", "sha256", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0700); err != nil {
			return nil, err
		}
	}
	// Only this owner may touch the namespace. No old gzip cache is inventoried.
	if err := os.RemoveAll(filepath.Join(root, "tmp")); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "tmp"), 0700); err != nil {
		return nil, err
	}
	err := filepath.WalkDir(filepath.Join(root, "sha256"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".install-") {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			return filepath.SkipDir
		}
		if len(d.Name()) != 64 {
			return nil
		}
		receipt, err := readReceipt(path)
		if err != nil {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			return filepath.SkipDir
		}
		_ = receipt
		info, err := d.Info()
		if err != nil {
			return err
		}
		c.entries["b:"+d.Name()] = &cacheEntry{path: path, lastUsed: info.ModTime()}
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}
	dirs, err := os.ReadDir(filepath.Join(root, "manifests"))
	if err != nil {
		return nil, err
	}
	for _, d := range dirs {
		path := filepath.Join(root, "manifests", d.Name())
		if d.IsDir() {
			continue
		}
		data, err := os.ReadFile(path)
		_, parseErr := ParseManifest(data)
		if err != nil || parseErr != nil || len(data) > maxManifest || len(d.Name()) != 69 {
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			continue
		}
		info, err := d.Info()
		if err != nil {
			return nil, err
		}
		c.entries["m:"+strings.TrimSuffix(d.Name(), ".json")] = &cacheEntry{path: path, lastUsed: info.ModTime()}
	}
	return c, nil
}
func (c *cache) protect(keys []string) func() {
	c.mu.Lock()
	for _, k := range keys {
		c.refs[k]++
	}
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, k := range keys {
			c.refs[k]--
			if c.refs[k] == 0 {
				delete(c.refs, k)
			}
		}
	}
}

// A waiter cancelling never cancels a load needed by other callers.
func (c *cache) ensure(ctx context.Context, key string, install func(context.Context) (string, error)) error {
	c.mu.Lock()
	if e := c.entries[key]; e != nil {
		e.lastUsed = time.Now()
		c.mu.Unlock()
		return nil
	}
	load := c.loading[key]
	if load == nil {
		if len(c.loading) >= maxConnections*2 {
			c.mu.Unlock()
			return errCapacity
		}
		load = &loadResult{done: make(chan struct{})}
		c.loading[key] = load
		c.loads.Add(1)
		go func() {
			defer c.loads.Done()
			loadCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
			defer cancel()
			var path string
			var err error
			select {
			case c.slot <- struct{}{}:
				path, err = install(loadCtx)
				<-c.slot
			case <-loadCtx.Done():
				err = loadCtx.Err()
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			load.err = err
			if err == nil {
				c.entries[key] = &cacheEntry{path: path, lastUsed: time.Now()}
			}
			delete(c.loading, key)
			close(load.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-load.done:
		return load.err
	}
}
func (c *cache) acquire(ctx context.Context, req request) (acquired, error) {
	if strings.TrimSpace(req.TestcaseSetID) == "" || strings.TrimSpace(req.ManifestKey) == "" || len(req.ManifestKey) > 4096 || len(req.TestcaseSetID) > 512 || (req.Filter != "all" && req.Filter != "public" && req.Filter != "hidden") {
		return acquired{}, errInvalid
	}
	m, err := c.manifest(ctx, req)
	if err != nil {
		return acquired{}, err
	}
	var selected []ManifestTestcase
	needed := map[string]bool{}
	for _, tc := range m.Testcases {
		if (req.Filter == "public" && tc.Hidden) || (req.Filter == "hidden" && !tc.Hidden) {
			continue
		}
		selected = append(selected, tc)
		needed[tc.BundleID] = true
	}
	var bundles []ManifestBundle
	var keys []string
	for _, b := range m.Bundles {
		if needed[b.ID] {
			bundles = append(bundles, b)
			keys = append(keys, "b:"+strings.TrimPrefix(b.Checksum, "sha256:"))
		}
	}
	release := c.protect(keys)
	defer release()
	for _, b := range bundles {
		key := "b:" + strings.TrimPrefix(b.Checksum, "sha256:")
		err := c.ensure(ctx, key, func(ctx context.Context) (string, error) {
			c.mu.Lock()
			c.downloads[b.Checksum]++
			c.mu.Unlock()
			return installBundle(ctx, c.root, c.source, *m, b.ID, c.reserve)
		})
		if err != nil {
			return acquired{}, err
		}
	}
	result := acquired{cases: make([]caseMetadata, 0, len(selected))}
	// Hold all selected bundle read locks until the complete file set is open.
	// Preparation refs also protect bundles while other bundles are still loading.
	entries, unlockFiles, err := c.lockFiles(keys)
	if err != nil {
		return acquired{}, err
	}
	defer unlockFiles()
	paths := map[string]string{}
	for _, b := range bundles {
		paths[b.ID] = entries["b:"+strings.TrimPrefix(b.Checksum, "sha256:")].path
	}
	for _, tc := range selected {
		if err := ctx.Err(); err != nil {
			result.close()
			return acquired{}, err
		}
		root, err := os.OpenRoot(paths[tc.BundleID])
		if err != nil {
			result.close()
			return acquired{}, err
		}
		for _, name := range []string{tc.Input, tc.Output} {
			f, err := root.Open(name)
			if err == nil {
				info, e := f.Stat()
				if e != nil || !info.Mode().IsRegular() {
					f.Close()
					err = errors.Join(e, errors.New("not a regular testcase file"))
				}
			}
			if err != nil {
				root.Close()
				result.close()
				if errors.Is(err, unix.EMFILE) || errors.Is(err, unix.ENFILE) {
					err = errors.Join(errCapacity, err)
				}
				return acquired{}, err
			}
			result.files = append(result.files, f)
		}
		root.Close()
		result.cases = append(result.cases, caseMetadata{tc.ID, tc.Hidden, tc.ScoreWeight})
	}
	return result, nil
}

type fsUsage struct {
	UsedBytes      int64 `json:"usedBytes"`
	AvailableBytes int64 `json:"availableBytes"`
	block          int64
}

func usage(path string) (fsUsage, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return fsUsage{}, err
	}
	block := int64(s.Frsize)
	if block <= 0 {
		block = int64(s.Bsize)
	}
	return fsUsage{int64(s.Blocks-s.Bfree) * block, int64(s.Bavail) * block, block}, nil
}

// lockFiles pins ready entries with read locks. Sort and deduplicate keys so
// overlapping requests use one order and never recursively RLock an entry while
// a writer is pending. Callers protect keys during preparation with refs.
func (c *cache) lockFiles(keys []string) (map[string]*cacheEntry, func(), error) {
	ordered := append([]string(nil), keys...)
	sort.Strings(ordered)
	unique := ordered[:0]
	for _, key := range ordered {
		if len(unique) == 0 || key != unique[len(unique)-1] {
			unique = append(unique, key)
		}
	}
	for {
		c.mu.Lock()
		entries := make(map[string]*cacheEntry, len(unique))
		for _, key := range unique {
			entry := c.entries[key]
			if entry == nil {
				c.mu.Unlock()
				return nil, nil, errors.New("cache entry disappeared before file acquisition")
			}
			entries[key] = entry
		}
		c.mu.Unlock()
		for _, key := range unique {
			entries[key].filesMu.RLock()
		}
		unlock := func() {
			for i := len(unique) - 1; i >= 0; i-- {
				entries[unique[i]].filesMu.RUnlock()
			}
		}
		c.mu.Lock()
		valid := true
		for _, key := range unique {
			if c.entries[key] != entries[key] {
				valid = false
				break
			}
		}
		c.mu.Unlock()
		if valid {
			return entries, unlock, nil
		}
		unlock()
	}
}

// The cold-install slot serializes reservations and writes. Keep filesystem I/O
// and entry-lock waits outside c.mu so unrelated warm requests can progress.
func (c *cache) reserve(needed int64) error {
	u, err := usage(c.root)
	if err != nil {
		return err
	}
	needed = ((needed + u.block - 1) / u.block) * u.block
	if needed > c.high {
		return errCapacity
	}
	if u.UsedBytes+needed <= c.high && u.AvailableBytes >= needed {
		return nil
	}
	type candidate struct {
		key      string
		entry    *cacheEntry
		lastUsed time.Time
	}
	c.mu.Lock()
	var candidates []candidate
	for key, entry := range c.entries {
		if c.refs[key] == 0 && c.loading[key] == nil {
			candidates = append(candidates, candidate{key, entry, entry.lastUsed})
		}
	}
	c.mu.Unlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].lastUsed.Before(candidates[j].lastUsed) })
	for _, candidate := range candidates {
		if u.UsedBytes <= c.low && u.UsedBytes+needed <= c.high && u.AvailableBytes >= needed {
			break
		}
		if _, err := c.evict(candidate.key, candidate.entry); err != nil {
			return err
		}
		u, err = usage(c.root)
		if err != nil {
			return err
		}
	}
	if u.UsedBytes+needed > c.high || u.AvailableBytes < needed {
		return fmt.Errorf("%w: need %d with %d used, %d available", errCapacity, needed, u.UsedBytes, u.AvailableBytes)
	}
	return nil
}

// Recheck identity and preparation refs after acquiring the entry write lock:
// LRU candidates may have been protected or replaced since the snapshot.
func (c *cache) evict(key string, entry *cacheEntry) (bool, error) {
	entry.filesMu.Lock()
	c.mu.Lock()
	if c.entries[key] != entry || c.refs[key] != 0 || c.loading[key] != nil {
		c.mu.Unlock()
		entry.filesMu.Unlock()
		return false, nil
	}
	tombstone, err := os.MkdirTemp(filepath.Join(c.root, "tmp"), ".evict-")
	detached := false
	if err == nil {
		err = os.Rename(entry.path, filepath.Join(tombstone, "artifact"))
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err == nil {
			delete(c.entries, key)
			detached = true
		}
	}
	c.mu.Unlock()
	entry.filesMu.Unlock()
	if tombstone != "" {
		// Remove only the private path: a new installation can already occupy the
		// original path. Existing FDs retain old files; startup cleans any tombstones
		// left by a failed removal or interrupted process.
		err = errors.Join(err, c.removeEvicted(tombstone))
	}
	if detached {
		log.Printf("evict %s", key)
	}
	return detached, err
}

type cacheStats struct {
	fsUsage
	Ready     []string       `json:"ready"`
	Downloads map[string]int `json:"downloads"`
}

func (c *cache) stats() (cacheStats, error) {
	u, err := usage(c.root)
	c.mu.Lock()
	s := cacheStats{fsUsage: u, Ready: []string{}, Downloads: map[string]int{}}
	for k := range c.entries {
		s.Ready = append(s.Ready, k)
	}
	for k, n := range c.downloads {
		s.Downloads[k] = n
	}
	c.mu.Unlock()
	sort.Strings(s.Ready)
	return s, err
}

// Keep compile-time assertion next to the manager source contract.
var _ io.Reader = (*os.File)(nil)
