//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockTestGate struct {
	started, release chan struct{}
	once             sync.Once
}
type lockTestSource struct {
	mu      sync.Mutex
	objects map[string][]byte
	calls   map[string]int
	gates   map[string]*lockTestGate
}

func (s *lockTestSource) OpenObject(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls[key]++
	data, ok := s.objects[key]
	gate := s.gates[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("missing test object %s", key)
	}
	if gate != nil {
		gate.once.Do(func() { close(gate.started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-gate.release:
		}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func requireLockTest(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Large watermarks let synchronization tests run on an ordinary filesystem.
// Capacity/last-close accounting is separately exercised on a dedicated tmpfs.
func newLockTestCache(t *testing.T, warm bool) (*cache, []request, *lockTestSource) {
	t.Helper()
	source := &lockTestSource{objects: map[string][]byte{}, calls: map[string]int{}, gates: map[string]*lockTestGate{}}
	var requests []request
	for _, version := range []string{"a", "b"} {
		objects, req, err := makeArtifacts(version, map[string][]byte{
			"1.in": []byte(version + " input"), "1.out": []byte(version + " output"),
			"2.in": []byte(version + " hidden input"), "2.out": []byte(version + " hidden output"),
		})
		requireLockTest(t, err)
		for key, data := range objects {
			source.objects[key] = data
		}
		requests = append(requests, req)
	}
	ctx, cancel := context.WithCancel(t.Context())
	c, err := newCache(ctx, t.TempDir(), source, 1<<62, 1<<61)
	requireLockTest(t, err)
	t.Cleanup(func() { cancel(); c.loads.Wait() })
	if warm {
		for _, req := range requests {
			result, err := c.acquire(t.Context(), req)
			result.close()
			requireLockTest(t, err)
		}
	}
	return c, requests, source
}
func lockTestBundle(t *testing.T, c *cache, source *lockTestSource, req request) (string, *cacheEntry, string) {
	t.Helper()
	var m Manifest
	requireLockTest(t, json.Unmarshal(source.objects[req.ManifestKey], &m))
	b := m.Bundles[0]
	key := "b:" + strings.TrimPrefix(b.Checksum, "sha256:")
	c.mu.Lock()
	entry := c.entries[key]
	c.mu.Unlock()
	return key, entry, b.ObjectKey
}
func validateLockTestFiles(result acquired, version string) error {
	if len(result.cases) != 2 || len(result.files) != 4 {
		return fmt.Errorf("partial successful acquire: %d cases, %d files", len(result.cases), len(result.files))
	}
	for i, want := range []string{version + " input", version + " output", version + " hidden input", version + " hidden output"} {
		data, err := io.ReadAll(result.files[i])
		if err != nil {
			return err
		}
		if string(data) != want {
			return fmt.Errorf("file %d: got %q want %q", i, data, want)
		}
	}
	if result.cases[0].ID != 1 || result.cases[0].Hidden || result.cases[1].ID != 2 || !result.cases[1].Hidden {
		return fmt.Errorf("incorrect testcase mapping: %+v", result.cases)
	}
	return nil
}
func asyncLockTestAcquire(c *cache, ctx context.Context, req request) <-chan error {
	done := make(chan error, 1)
	go func() {
		result, err := c.acquire(ctx, req)
		defer result.close()
		if err == nil {
			err = validateLockTestFiles(result, req.TestcaseSetID)
		}
		done <- err
	}()
	return done
}
func awaitLockTest(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		requireLockTest(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("cache operation blocked")
	}
}
func lockTestBlocked(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("operation bypassed entry lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
}
func waitLockTestWriter(t *testing.T, entry *cacheEntry) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for entry.filesMu.TryRLock() {
		entry.filesMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("eviction did not queue for entry write lock")
		}
		time.Sleep(time.Millisecond)
	}
}
func asyncLockTestEvict(c *cache, key string, entry *cacheEntry) <-chan error {
	done := make(chan error, 1)
	go func() { _, err := c.evict(key, entry); done <- err }()
	return done
}

func TestAcquireFileLocksArePerBundle(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	_, entry, _ := lockTestBundle(t, c, source, requests[0])
	entry.filesMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(entry.filesMu.Unlock) }
	defer unlock()
	blocked := asyncLockTestAcquire(c, t.Context(), requests[0])
	lockTestBlocked(t, blocked)
	// A blocked file open must not retain c.mu and delay unrelated hits or stats.
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[1]))
	stats := make(chan error, 1)
	go func() { _, err := c.stats(); stats <- err }()
	awaitLockTest(t, stats)
	unlock()
	awaitLockTest(t, blocked)
	// Two opens of the same bundle share read locks.
	entry.filesMu.RLock()
	defer entry.filesMu.RUnlock()
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
}

func TestEvictionWaitsForFileSetWithoutBlockingOtherBundles(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	key, entry, _ := lockTestBundle(t, c, source, requests[0])
	entry.filesMu.RLock()
	var once sync.Once
	unlock := func() { once.Do(entry.filesMu.RUnlock) }
	defer unlock()
	done := asyncLockTestEvict(c, key, entry)
	waitLockTestWriter(t, entry)
	lockTestBlocked(t, done)
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[1]))
	_, err := os.Stat(entry.path)
	requireLockTest(t, err)
	unlock()
	awaitLockTest(t, done)
}

func TestEvictionRechecksPreparationRefs(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	key, entry, _ := lockTestBundle(t, c, source, requests[0])
	entry.filesMu.RLock()
	var once sync.Once
	unlock := func() { once.Do(entry.filesMu.RUnlock) }
	defer unlock()
	done := make(chan error, 1)
	go func() {
		removed, err := c.evict(key, entry)
		if removed {
			err = errors.New("evicted a newly protected entry")
		}
		done <- err
	}()
	waitLockTestWriter(t, entry)
	// Simulate a request protecting a candidate after the GC snapshot.
	release := c.protect([]string{key})
	defer release()
	unlock()
	awaitLockTest(t, done)
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
}

func TestFileLocksRevalidateReplacedEntries(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	key, old, _ := lockTestBundle(t, c, source, requests[0])
	old.filesMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(old.filesMu.Unlock) }
	defer unlock()
	done := make(chan error, 1)
	replacement := &cacheEntry{path: old.path, lastUsed: time.Now()}
	go func() {
		entries, release, err := c.lockFiles([]string{key, key})
		if err == nil {
			defer release()
			if entries[key] != replacement {
				err = errors.New("returned detached entry")
			}
		}
		done <- err
	}()
	lockTestBlocked(t, done)
	c.mu.Lock()
	c.entries[key] = replacement
	c.mu.Unlock()
	unlock()
	awaitLockTest(t, done)
	removed, err := c.evict(key, old)
	requireLockTest(t, err)
	if removed {
		t.Fatal("stale GC removed replacement")
	}
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
}

func TestEvictionRemovalAllowsReloadAndPreservesOpenFDs(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	key, old, _ := lockTestBundle(t, c, source, requests[0])
	held, err := c.acquire(t.Context(), requests[0])
	requireLockTest(t, err)
	defer held.close()
	removing := make(chan string, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	c.removeEvicted = func(path string) error { removing <- path; <-release; return os.RemoveAll(path) }
	done := asyncLockTestEvict(c, key, old)
	select {
	case path := <-removing:
		if filepath.Dir(path) != filepath.Join(c.root, "tmp") || !strings.HasPrefix(filepath.Base(path), ".evict-") {
			t.Fatalf("not a private tombstone: %s", path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("eviction did not reach removal")
	}
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[1]))
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
	removed, err := c.evict(key, old)
	requireLockTest(t, err)
	if removed {
		t.Fatal("stale GC detached replacement")
	}
	c.mu.Lock()
	replacement := c.entries[key]
	c.mu.Unlock()
	if replacement == nil || replacement == old {
		t.Fatal("bundle not reinstalled during deletion")
	}
	unblock()
	awaitLockTest(t, done)
	requireLockTest(t, validateLockTestFiles(held, requests[0].TestcaseSetID))
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
}

func TestCanceledAcquirePreservesSharedBundleLoad(t *testing.T) {
	c, requests, source := newLockTestCache(t, false)
	key, _, objectKey := lockTestBundle(t, c, source, requests[0])
	gate := &lockTestGate{started: make(chan struct{}), release: make(chan struct{})}
	source.gates[objectKey] = gate
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate.release) }) }
	defer unblock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := asyncLockTestAcquire(c, ctx, requests[0])
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("bundle load did not start")
	}
	second := asyncLockTestAcquire(c, t.Context(), requests[0])
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		refs := c.refs[key]
		c.mu.Unlock()
		if refs >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second caller did not join shared load")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation blocked")
	}
	unblock()
	awaitLockTest(t, second)
	// A warm request must not fetch either manifest or bundle again.
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[0]))
	source.mu.Lock()
	defer source.mu.Unlock()
	for key, calls := range source.calls {
		if calls != 1 {
			t.Fatalf("object %s fetched %d times", key, calls)
		}
	}
}

func TestPreparationProtectsEarlierBundles(t *testing.T) {
	c, requests, source := newLockTestCache(t, false)
	var m Manifest
	requireLockTest(t, json.Unmarshal(source.objects[requests[0].ManifestKey], &m))
	gate := &lockTestGate{started: make(chan struct{}), release: make(chan struct{})}
	source.gates[m.Bundles[1].ObjectKey] = gate
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate.release) }) }
	defer unblock()
	done := asyncLockTestAcquire(c, t.Context(), requests[0])
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("second bundle did not start")
	}
	key, entry, _ := lockTestBundle(t, c, source, requests[0])
	if entry == nil {
		t.Fatal("first bundle not installed")
	}
	removed, err := c.evict(key, entry)
	requireLockTest(t, err)
	if removed {
		t.Fatal("GC removed first bundle while second was loading")
	}
	unblock()
	awaitLockTest(t, done)
}

func TestConcurrentAcquireAndEvictReturnsCompleteFileSets(t *testing.T) {
	c, requests, _ := newLockTestCache(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	const workers = 6
	errs := make(chan error, workers+1)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		go func() {
			<-start
			for i := 0; i < 30; i++ {
				req := requests[i%len(requests)]
				result, err := c.acquire(ctx, req)
				if err == nil {
					err = validateLockTestFiles(result, req.TestcaseSetID)
				}
				result.close()
				if err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	go func() {
		<-start
		for i := 0; i < 80; i++ {
			c.mu.Lock()
			entries := make(map[string]*cacheEntry, len(c.entries))
			for key, entry := range c.entries {
				entries[key] = entry
			}
			c.mu.Unlock()
			for key, entry := range entries {
				if _, err := c.evict(key, entry); err != nil {
					errs <- err
					return
				}
			}
		}
		errs <- nil
	}()
	close(start)
	var err error
	for i := 0; i < workers+1; i++ {
		select {
		case next := <-errs:
			err = errors.Join(err, next)
		case <-ctx.Done():
			t.Fatal("acquire/evict deadlock:", ctx.Err())
		}
	}
	requireLockTest(t, err)
}

func TestEvictionRemovalFailureRecoveredOnRestart(t *testing.T) {
	c, requests, source := newLockTestCache(t, true)
	key, entry, _ := lockTestBundle(t, c, source, requests[0])
	c.removeEvicted = func(string) error { return errors.New("injected removal failure") }
	removed, err := c.evict(key, entry)
	if !removed || err == nil {
		t.Fatalf("removed=%v err=%v", removed, err)
	}
	tombstones, err := filepath.Glob(filepath.Join(c.root, "tmp", ".evict-*"))
	requireLockTest(t, err)
	if len(tombstones) != 1 {
		t.Fatalf("missing recoverable tombstone: %v", tombstones)
	}
	_, err = newCache(t.Context(), c.root, source, c.high, c.low)
	requireLockTest(t, err)
	_, err = os.Stat(tombstones[0])
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tombstone survived restart: %v", err)
	}
}

func TestManifestReadDoesNotHoldGlobalLock(t *testing.T) {
	c, requests, _ := newLockTestCache(t, true)
	key := fmt.Sprintf("m:%x", sha256.Sum256([]byte(requests[0].ManifestKey)))
	c.mu.Lock()
	entry := c.entries[key]
	c.mu.Unlock()
	entry.filesMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(entry.filesMu.Unlock) }
	defer unlock()
	done := asyncLockTestAcquire(c, t.Context(), requests[0])
	lockTestBlocked(t, done)
	awaitLockTest(t, asyncLockTestAcquire(c, t.Context(), requests[1]))
	unlock()
	awaitLockTest(t, done)
}
