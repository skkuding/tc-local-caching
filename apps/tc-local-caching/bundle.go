package main

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type BundleSource interface {
	OpenObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
}

// InstallBundle handles a cache miss for one selected bundle. cacheRoot may be
// a local directory or a mounted PV. The returned directory contains TC files.
// Existing cache entries are not replaced; callers must serialize installations.
// Cache hits, GC, locking and manifest fetching belong to the Manager.
// This wrapper is used by isolated installer tests; production supplies admission.
func InstallBundle(ctx context.Context, cacheRoot string, source BundleSource, manifest Manifest, bundleID string) (string, error) {
	return installBundle(ctx, cacheRoot, source, manifest, bundleID, func(int64) error { return nil })
}
func installBundle(ctx context.Context, cacheRoot string, source BundleSource, manifest Manifest, bundleID string, reserve func(int64) error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(cacheRoot) == "" || source == nil {
		return "", fmt.Errorf("bundle install: cache root and source are required")
	}
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	var bundle *ManifestBundle
	for i := range manifest.Bundles {
		if manifest.Bundles[i].ID == bundleID {
			bundle = &manifest.Bundles[i]
			break
		}
	}
	if bundle == nil {
		return "", fmt.Errorf("bundle install: unknown bundle %q", bundleID)
	}
	digest := strings.TrimPrefix(bundle.Checksum, "sha256:")
	parent := filepath.Join(cacheRoot, "sha256", digest[:2])
	destination := filepath.Join(parent, digest)
	if _, err := os.Lstat(destination); err == nil {
		return "", fmt.Errorf("bundle install: %s: %w", destination, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("inspect cache destination: %w", err)
	}
	if err := os.MkdirAll(parent, 0750); err != nil {
		return "", fmt.Errorf("create cache directory: %w", err)
	}
	// Keep staging on the same filesystem as the destination for atomic rename.
	if err := reserve(16 << 10); err != nil {
		return "", err
	}
	stage, err := os.MkdirTemp(parent, ".install-")
	if err != nil {
		return "", fmt.Errorf("create staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	archive := filepath.Join(stage, "bundle.tar.zst")
	if err := downloadBundle(ctx, source, *bundle, archive, reserve); err != nil {
		return "", err
	}
	payload := filepath.Join(stage, "files")
	if err := os.Mkdir(payload, 0750); err != nil {
		return "", fmt.Errorf("create extraction directory: %w", err)
	}
	if err := extractBundle(ctx, archive, payload, reserve); err != nil {
		return "", fmt.Errorf("extract bundle %q: %w", bundleID, err)
	}
	for _, tc := range manifest.Testcases {
		if tc.BundleID != bundleID {
			continue
		}
		for _, name := range []string{tc.Input, tc.Output} {
			info, err := os.Lstat(filepath.Join(payload, filepath.FromSlash(name)))
			if err != nil {
				return "", fmt.Errorf("bundle %q testcase %d file %q: %w", bundleID, tc.ID, name, err)
			}
			if !info.Mode().IsRegular() {
				return "", fmt.Errorf("bundle %q testcase %d path %q is not a regular file", bundleID, tc.ID, name)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := writeReceipt(payload, reserve); err != nil {
		return "", err
	}
	if err := os.Rename(payload, destination); err != nil {
		return "", fmt.Errorf("publish bundle cache: %w", err)
	}
	return destination, nil
}

func downloadBundle(ctx context.Context, source BundleSource, bundle ManifestBundle, archive string, reserve func(int64) error) error {
	body, err := source.OpenObject(ctx, bundle.ObjectKey)
	if err != nil {
		return fmt.Errorf("download bundle %q: %w", bundle.ID, err)
	}
	file, err := os.OpenFile(archive, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		body.Close()
		return fmt.Errorf("create downloaded bundle: %w", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(&limitedCacheWriter{file: file, reserve: reserve, limit: maxCompressed}, hash), bundleContextReader{ctx, body})
	if err := errors.Join(copyErr, body.Close(), file.Close()); err != nil {
		return fmt.Errorf("save downloaded bundle: %w", err)
	}
	actual := fmt.Sprintf("sha256:%x", hash.Sum(nil))
	if actual != bundle.Checksum {
		return fmt.Errorf("bundle %q checksum mismatch: expected %s, got %s", bundle.ID, bundle.Checksum, actual)
	}
	return nil
}

func extractBundle(ctx context.Context, archive, root string, reserve func(int64) error) error {
	file, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(maxUnpacked)))
	if err != nil {
		return err
	}
	defer decoder.Close()
	stream := bundleContextReader{ctx, decoder}
	reader := tar.NewReader(stream)
	var total int64
	entries := 0
	for {
		header, err := reader.Next()
		if err == io.EOF {
			// Read through the zstd frame trailer to catch truncation/CRC errors.
			_, err = io.Copy(io.Discard, stream)
			return err
		}
		if err != nil {
			return err
		}
		entries++
		if entries > maxArchiveEntries {
			return fmt.Errorf("too many archive entries")
		}
		if header.Size < 0 || header.Size > maxUnpacked-total {
			return fmt.Errorf("unpacked bundle size limit exceeded")
		}
		total += header.Size
		if err := reserve(8192); err != nil {
			return err
		}
		name := strings.TrimPrefix(header.Name, "./")
		if header.Typeflag == tar.TypeDir {
			if header.Name == "." || header.Name == "./" {
				continue
			}
			name = strings.TrimSuffix(name, "/")
		}
		if name == ".receipt.json" || name == "." || !fs.ValidPath(name) || strings.ContainsAny(name, "\\\x00") {
			return fmt.Errorf("invalid archive path %q", header.Name)
		}
		filename := filepath.Join(root, filepath.FromSlash(name))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(filename, 0750); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
				return err
			}
			// Reject duplicate entries; never restore archive permissions or owners.
			out, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0440)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(&limitedCacheWriter{file: out, reserve: reserve, limit: maxUnpacked}, reader)
			if err := errors.Join(copyErr, out.Close()); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported archive entry type %d for %q", header.Typeflag, header.Name)
		}
	}
}

type bundleContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r bundleContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

const maxCompressed int64 = 64 << 20
const maxUnpacked int64 = 128 << 20
const maxArchiveEntries = 4096

// Limit each write so space checks happen during streaming, before allocation.
type limitedCacheWriter struct {
	file           *os.File
	reserve        func(int64) error
	written, limit int64
}

func (w *limitedCacheWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.written {
		return 0, fmt.Errorf("artifact size limit exceeded")
	}
	total := 0
	for len(p) > 0 {
		n := min(len(p), 32<<10)
		if err := w.reserve(int64(n) + 8192); err != nil {
			return total, err
		}
		done, err := w.file.Write(p[:n])
		total += done
		w.written += int64(done)
		p = p[done:]
		if err != nil {
			return total, err
		}
		if done != n {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

type receiptFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func writeReceipt(root string, reserve func(int64) error) error {
	var files []receiptFile
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, err := io.Copy(h, f)
		err = errors.Join(err, f.Close())
		if err != nil {
			return err
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, receiptFile{filepath.ToSlash(name), n, fmt.Sprintf("%x", h.Sum(nil))})
		return nil
	})
	if err != nil {
		return err
	}
	data, err := json.Marshal(files)
	if err != nil {
		return err
	}
	if err := reserve(int64(len(data)) + 8192); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, ".receipt.json"), data, 0600)
}
func readReceipt(root string) ([]receiptFile, error) {
	data, err := readBoundedCacheFile(filepath.Join(root, ".receipt.json"), 1<<20)
	if err != nil {
		return nil, err
	}
	var files []receiptFile
	if err := json.Unmarshal(data, &files); err != nil {
		return nil, err
	}
	if len(files) == 0 || len(files) > maxArchiveEntries {
		return nil, fmt.Errorf("invalid receipt")
	}
	base, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer base.Close()
	for _, saved := range files {
		if !fs.ValidPath(saved.Name) || saved.Size < 0 || saved.Size > maxUnpacked {
			return nil, fmt.Errorf("invalid receipt entry")
		}
		f, err := openCacheFile(base, saved.Name)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			f.Close()
			return nil, fmt.Errorf("invalid installed file")
		}
		hash := sha256.New()
		n, err := io.Copy(hash, io.LimitReader(f, saved.Size+1))
		err = errors.Join(err, f.Close())
		if err != nil || n != saved.Size || fmt.Sprintf("%x", hash.Sum(nil)) != saved.SHA256 {
			return nil, fmt.Errorf("invalid installed file %q", saved.Name)
		}
	}
	return files, nil
}
