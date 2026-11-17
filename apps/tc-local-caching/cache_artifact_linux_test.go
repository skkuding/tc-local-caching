//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type objectMap struct {
	mu      sync.Mutex
	objects map[string][]byte
	calls   map[string]int
}

func (s *objectMap) OpenObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[key]++
	data, ok := s.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func artifactCache(t *testing.T, objects map[string][]byte, root string) (*cache, *objectMap) {
	t.Helper()
	source := &objectMap{objects: objects, calls: map[string]int{}}
	ctx, cancel := context.WithCancel(t.Context())
	c, err := newCache(ctx, root, source, 1<<62, 1<<61)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); c.loads.Wait() })
	return c, source
}
func TestPVFilteringSharedBundlesAndWarmOriginReads(t *testing.T) {
	objects, req, err := makeArtifacts("first", map[string][]byte{"1.in": []byte("input"), "1.out": []byte("output"), "2.in": []byte("secret"), "2.out": []byte("answer")})
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(objects[req.ManifestKey], &m); err != nil {
		t.Fatal(err)
	}
	// Different version, identical compressed bytes and relative filenames.
	m.TestcaseSetID = "second"
	data, _ := json.Marshal(m)
	second := request{Op: "acquire", TestcaseSetID: "second", ManifestKey: "second.json", Filter: "all"}
	objects[second.ManifestKey] = data
	c, source := artifactCache(t, objects, t.TempDir())
	public := req
	public.Filter = "public"
	a, err := c.acquire(t.Context(), public)
	if err != nil {
		t.Fatal(err)
	}
	a.close()
	if len(a.cases) != 1 || a.cases[0].Hidden {
		t.Fatal("public filter failed")
	}
	if source.calls[m.Bundles[1].ObjectKey] != 0 {
		t.Fatal("public read downloaded hidden bundle")
	}
	for _, r := range []request{req, req, second} {
		a, err := c.acquire(t.Context(), r)
		if err != nil {
			t.Fatal(err)
		}
		a.close()
	}
	for _, b := range m.Bundles {
		if source.calls[b.ObjectKey] != 1 {
			t.Fatalf("bundle not shared: %s count=%d", b.ObjectKey, source.calls[b.ObjectKey])
		}
	}
	if source.calls[req.ManifestKey] != 1 || source.calls[second.ManifestKey] != 1 {
		t.Fatal("manifest not cached")
	}
	// A mismatch must fail without asking the origin again.
	wrong := req
	wrong.TestcaseSetID = "wrong"
	a, err = c.acquire(t.Context(), wrong)
	a.close()
	if err == nil || source.calls[req.ManifestKey] != 1 {
		t.Fatal("version mismatch fallback")
	}
	source.objects = map[string][]byte{}
	a, err = c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal("warm required origin", err)
	}
	a.close()
	restarted, _ := artifactCache(t, map[string][]byte{}, c.root)
	a, err = restarted.acquire(t.Context(), req)
	if err != nil {
		t.Fatal("restart required origin", err)
	}
	a.close()
}
func TestFailedChecksumCanRetryWithoutPartialPublication(t *testing.T) {
	objects, req, err := makeArtifacts("retry", map[string][]byte{"1.in": []byte("x"), "1.out": []byte("y")})
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	_ = json.Unmarshal(objects[req.ManifestKey], &m)
	key := m.Bundles[0].ObjectKey
	good := objects[key]
	objects[key] = []byte("broken")
	c, source := artifactCache(t, objects, t.TempDir())
	a, err := c.acquire(t.Context(), req)
	a.close()
	if err == nil || len(a.files) != 0 {
		t.Fatal("partial success on invalid checksum")
	}
	if _, err := os.Stat(filepath.Join(c.root, "sha256", strings.TrimPrefix(m.Bundles[0].Checksum, "sha256:")[:2], strings.TrimPrefix(m.Bundles[0].Checksum, "sha256:"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published failed bundle")
	}
	source.mu.Lock()
	source.objects[key] = good
	source.mu.Unlock()
	a, err = c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	a.close()
}
func TestInstallerSpaceAndArchiveLimits(t *testing.T) {
	data := makeBundle(t, []bundleEntry{{"1001.in", "1 2\n", 0x30}, {"1001.out", "3\n", 0x30}})
	source := bundleSourceFunc(func(context.Context, string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil })
	calls := 0
	root := t.TempDir()
	_, err := installBundle(t.Context(), root, source, manifestForBundle(data), "public", func(int64) error {
		calls++
		if calls > 3 {
			return errCapacity
		}
		return nil
	})
	if !errors.Is(err, errCapacity) {
		t.Fatal("space failure ignored", err)
	}
	assertNoBundleStaging(t, root)
	if calls < 4 {
		t.Fatal("no streaming space checks")
	}
	raw := bytes.Repeat([]byte{'x'}, maxManifest+1)
	if _, err := LoadManifest(t.Context(), bundleSourceFunc(func(context.Context, string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(raw)), nil }), "key", "v"); err == nil {
		t.Fatal("unbounded manifest")
	}
}
func TestGCCountsUnlinkedAllocationUntilLastClose(t *testing.T) {
	root := os.Getenv("TC_CAPACITY_ROOT")
	if root == "" {
		t.Skip("set TC_CAPACITY_ROOT to a dedicated tmpfs")
	}
	dir, err := os.MkdirTemp(root, "space-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	objects, req, err := makeArtifacts("large", map[string][]byte{"1.in": bytes.Repeat([]byte{'a'}, 2<<20), "1.out": []byte("ok")})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := artifactCache(t, objects, dir)
	a, err := c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	b, err := c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	c.mu.Lock()
	var key string
	var entry *cacheEntry
	for k, e := range c.entries {
		if strings.HasPrefix(k, "b:") {
			key, entry = k, e
		}
	}
	c.mu.Unlock()
	if _, err := c.evict(key, entry); err != nil {
		t.Fatal(err)
	}
	before, err := usage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(entry.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("GC retained path")
	}
	a.close()
	data, err := io.ReadAll(b.files[0])
	if err != nil || len(data) != 2<<20 {
		t.Fatal("unlink or other close invalidated FD", err)
	}
	c.high = before.UsedBytes + 32768
	c.low = before.UsedBytes
	if err := c.reserve(65536); !errors.Is(err, errCapacity) {
		t.Fatal("counted unlinked bytes as free", err)
	}
	b.close()
	after, err := usage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if before.UsedBytes-after.UsedBytes < 2<<20 {
		t.Fatalf("last close did not reclaim space: %d", before.UsedBytes-after.UsedBytes)
	}
	if err := c.reserve(65536); err != nil {
		t.Fatal("did not use reclaimed space", err)
	}
}

func TestCorruptManifestRecoversWithoutChangingVersion(t *testing.T) {
	objects, req, err := makeArtifacts("repair", map[string][]byte{"1.in": []byte("x"), "1.out": []byte("y")})
	if err != nil {
		t.Fatal(err)
	}
	c, source := artifactCache(t, objects, t.TempDir())
	a, err := c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	a.close()
	manifests, err := os.ReadDir(filepath.Join(c.root, "manifests"))
	if err != nil || len(manifests) != 1 {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.root, "manifests", manifests[0].Name()), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	a, err = c.acquire(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	a.close()
	if source.calls[req.ManifestKey] != 2 {
		t.Fatal("manifest was not recovered")
	}
}
