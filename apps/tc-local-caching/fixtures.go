package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

//go:embed fixtures
var fixtures embed.FS

// TC 1 is public and TC 2 is hidden, using the external PV manifest contract.
func fixtureObjects(version string) (map[string][]byte, request, error) {
	files, err := fixtures.ReadDir("fixtures/" + version)
	if err != nil {
		return nil, request{}, err
	}
	bodies := map[string][]byte{}
	for _, f := range files {
		data, err := fixtures.ReadFile("fixtures/" + version + "/" + f.Name())
		if err != nil {
			return nil, request{}, err
		}
		bodies[f.Name()] = data
	}
	return makeArtifacts(version, bodies)
}
func makeArtifacts(version string, bodies map[string][]byte) (map[string][]byte, request, error) {
	objects := map[string][]byte{}
	m := Manifest{TestcaseSetID: version}
	var ids []int
	for name := range bodies {
		if id, ok := strings.CutSuffix(name, ".in"); ok {
			n, err := strconv.Atoi(id)
			if err != nil || n <= 0 {
				return nil, request{}, fmt.Errorf("invalid fixture id")
			}
			ids = append(ids, n)
		}
	}
	sort.Ints(ids)
	for _, group := range []string{"public", "hidden"} {
		var cases []ManifestTestcase
		for _, id := range ids {
			hidden := id%2 == 0
			if hidden != (group == "hidden") {
				continue
			}
			cases = append(cases, ManifestTestcase{ID: id, BundleID: group, Input: fmt.Sprintf("%d.in", id), Output: fmt.Sprintf("%d.out", id), Hidden: hidden, ScoreWeight: 1})
		}
		if len(cases) == 0 {
			continue
		}
		var buffer bytes.Buffer
		z, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, request{}, err
		}
		archive := tar.NewWriter(z)
		for _, tc := range cases {
			for _, name := range []string{tc.Input, tc.Output} {
				data, ok := bodies[name]
				if !ok {
					return nil, request{}, fmt.Errorf("missing %s", name)
				}
				if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0440, Size: int64(len(data))}); err != nil {
					return nil, request{}, err
				}
				if _, err := archive.Write(data); err != nil {
					return nil, request{}, err
				}
			}
		}
		if err := archive.Close(); err != nil {
			return nil, request{}, err
		}
		if err := z.Close(); err != nil {
			return nil, request{}, err
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(buffer.Bytes()))
		key := "testcases/" + version + "/" + digest + ".tar.zst"
		objects[key] = buffer.Bytes()
		m.Bundles = append(m.Bundles, ManifestBundle{ID: group, ObjectKey: key, Checksum: "sha256:" + digest})
		m.Testcases = append(m.Testcases, cases...)
	}
	if err := m.Validate(); err != nil {
		return nil, request{}, err
	}
	key := "testcases/" + version + "/manifest.json"
	data, err := json.Marshal(m)
	if err != nil {
		return nil, request{}, err
	}
	objects[key] = data
	return objects, request{Op: "acquire", TestcaseSetID: version, ManifestKey: key, Filter: "all"}, nil
}

func seedLocal(root string) error {
	versions, err := fixtures.ReadDir("fixtures")
	if err != nil {
		return err
	}
	for _, v := range versions {
		objects, _, err := fixtureObjects(v.Name())
		if err != nil {
			return err
		}
		for key, data := range objects {
			path := filepath.Join(root, filepath.FromSlash(key))
			if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
				return err
			}
			if err := os.WriteFile(path, data, 0640); err != nil {
				return err
			}
		}
	}
	return nil
}
