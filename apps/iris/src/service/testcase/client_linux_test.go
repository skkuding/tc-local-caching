//go:build linux

package testcase

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func fdCount(t *testing.T) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(fds)
}
func stubManager(t *testing.T, serve func(*net.UnixConn, request)) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "s")
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		var req request
		if err := receiveJSON(conn, &req); err == nil {
			serve(conn, req)
		}
	}()
	t.Cleanup(func() { l.Close(); <-done })
	return socket
}
func TestFDClientMetadataAndSplitResponse(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "case")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	socket := stubManager(t, func(c *net.UnixConn, r request) {
		if r.Op != "acquire" || r.TestcaseSetID != "v" || r.ManifestKey != "key" || r.Filter != "all" {
			t.Error("wrong request", r)
		}
		cases := make([]caseMetadata, 130)
		for idx := range cases {
			cases[idx] = caseMetadata{ID: idx + 1, Hidden: idx%2 == 0, ScoreWeight: 2}
		}
		if err := sendJSON(c, reply{TestcaseSetID: "v", Cases: cases}); err != nil {
			t.Error(err)
			return
		}
		// Separate open descriptions keep offsets independent between clients/cases.
		var files []*os.File
		defer func() {
			for _, f := range files {
				f.Close()
			}
		}()
		for range 260 {
			f, err := os.Open(file.Name())
			if err != nil {
				t.Error(err)
				return
			}
			files = append(files, f)
		}
		for start := 0; start < len(files); start += fdsPerPacket {
			var fds []int
			for _, f := range files[start:min(start+fdsPerPacket, len(files))] {
				fds = append(fds, int(f.Fd()))
			}
			if err := writePacket(c, []byte{1}, fds); err != nil {
				t.Error(err)
				return
			}
		}
	})
	result, err := acquireFiles(t.Context(), socket, "v", "key", "all")
	if err != nil {
		t.Fatal(err)
	}
	defer result.close()
	if len(result.files) != 260 || len(result.cases) != 130 {
		t.Fatal("partial response")
	}
	for _, f := range result.files {
		flag, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
		if err != nil || flag&unix.FD_CLOEXEC == 0 {
			t.Fatal("missing close-on-exec", err)
		}
	}
	data, err := io.ReadAll(result.files[0])
	if err != nil || string(data) != "data" {
		t.Fatal(string(data), err)
	}
}
func TestFDClientRejectsBadResponsesWithoutLeaks(t *testing.T) {
	for _, mode := range []string{"version", "duplicate", "filter", "incomplete", "truncated", "unexpected-json-fd", "nonregular"} {
		t.Run(mode, func(t *testing.T) {
			file, err := os.Open("/dev/null")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { file.Close() })
			socket := stubManager(t, func(c *net.UnixConn, _ request) {
				res := reply{TestcaseSetID: "v", Cases: []caseMetadata{{ID: 1}}}
				switch mode {
				case "version":
					res.TestcaseSetID = "other"
				case "duplicate":
					res.Cases = append(res.Cases, res.Cases[0])
				case "filter":
					res.Cases[0].Hidden = true
				}
				if mode == "unexpected-json-fd" {
					_ = writePacket(c, []byte(`{}`), []int{int(file.Fd())})
					return
				}
				if err := sendJSON(c, res); err != nil {
					return
				}
				switch mode {
				case "incomplete":
					_ = writePacket(c, []byte{1}, []int{int(file.Fd())})
				case "truncated":
					_ = writePacket(c, make([]byte, maxPacket+1), []int{int(file.Fd()), int(file.Fd())})
				case "nonregular":
					_ = writePacket(c, []byte{1}, []int{int(file.Fd()), int(file.Fd())})
				}
			})
			before := fdCount(t)
			_, err = GetVersionedTestcase(t.Context(), socket, "v", "key", PUBLIC_ONLY)
			if err == nil {
				t.Fatal("accepted invalid response")
			}
			// Allow the server's accepted socket to close before comparing FD counts.
			deadline := time.Now().Add(time.Second)
			for fdCount(t) > before && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if after := fdCount(t); after > before {
				t.Fatalf("FD leak: before=%d after=%d", before, after)
			}
		})
	}
}
func TestFDClientCancellation(t *testing.T) {
	acquired := make(chan struct{})
	socket := stubManager(t, func(c *net.UnixConn, _ request) { close(acquired); var b [1]byte; _, _ = c.Read(b[:]) })
	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-acquired; cancel() }()
	started := time.Now()
	_, err := GetVersionedTestcase(ctx, socket, "v", "key", ALL)
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("cancel did not close socket", err)
	}
}
func TestFDClientEmptySelection(t *testing.T) {
	socket := stubManager(t, func(c *net.UnixConn, _ request) { _ = sendJSON(c, reply{TestcaseSetID: "v", Cases: []caseMetadata{}}) })
	got, err := GetVersionedTestcase(t.Context(), socket, "v", "key", PUBLIC_ONLY)
	if err != nil || len(got.Elements) != 0 {
		t.Fatal(got, err)
	}
}

// These tests execute the real Manager binary. There is no shared Go module or
// manifest parser in Iris; only the socket and the existing Testcase API cross.
func TestRealManagerSocket(t *testing.T) {
	binary := os.Getenv("TC_MANAGER_BINARY")
	if binary == "" {
		t.Skip("set TC_MANAGER_BINARY to run cross-process contract tests")
	}
	objects := t.TempDir()
	seed := exec.Command(binary, "seed-local", "-root", objects)
	if out, err := seed.CombinedOutput(); err != nil {
		t.Fatalf("seed: %s %v", out, err)
	}
	root, socket := t.TempDir(), filepath.Join(t.TempDir(), "manager.sock")
	start := func() func() {
		cmd := exec.Command(binary, "serve", "-cache", root, "-socket", socket, "-high", "4611686018427387904", "-low", "2305843009213693952")
		cmd.Env = append(os.Environ(), "TC_OBJECT_ROOT="+objects)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		stop := func() { once.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
		t.Cleanup(stop)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			c, err := net.Dial("unixpacket", socket)
			if err == nil {
				c.Close()
				return stop
			}
			time.Sleep(10 * time.Millisecond)
		}
		stop()
		t.Fatal("manager did not start")
		return nil
	}
	stop := start()
	get := func(version string, filter TestcaseFilterCode) Testcase {
		t.Helper()
		got, err := GetVersionedTestcase(t.Context(), socket, version, "testcases/"+version+"/manifest.json", filter)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	errs := make(chan error, 6)
	for range 6 {
		go func() {
			got, err := GetVersionedTestcase(t.Context(), socket, "sum-v1", "testcases/sum-v1/manifest.json", ALL)
			if err == nil && len(got.Elements) != 2 {
				err = errors.New("partial TC set")
			}
			errs <- err
		}()
	}
	for range 6 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	all := get("sum-v1", ALL)
	public := get("sum-v1", PUBLIC_ONLY)
	hidden := get("sum-v1", HIDDEN_ONLY)
	if len(public.Elements) != 1 || len(hidden.Elements) != 1 || !reflect.DeepEqual(all.Elements, append(public.Elements, hidden.Elements...)) {
		t.Fatal("wrong filters/order", all)
	}
	if all.Elements[0].In != "2 3\n" || all.Elements[0].Out != "5\n" || hidden.Elements[0].Hidden != true {
		t.Fatal("wrong FD mapping", all)
	}
	newer := get("sum-v2", ALL)
	if newer.Elements[0].Out == all.Elements[0].Out {
		t.Fatal("version isolation failed")
	}
	// Removing the origin proves warm reads and restart reuse need no origin GET.
	if err := os.Rename(objects, objects+"-offline"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(objects + "-offline") })
	if got := get("sum-v1", ALL); !reflect.DeepEqual(got, all) {
		t.Fatal("warm mismatch")
	}
	stop()
	start()
	if got := get("sum-v1", ALL); !reflect.DeepEqual(got, all) {
		t.Fatal("restart mismatch")
	}
	if _, err := GetVersionedTestcase(t.Context(), socket, "wrong", "testcases/sum-v1/manifest.json", ALL); err == nil {
		t.Fatal("accepted wrong version")
	}
}

func TestReceiverFDLimit(t *testing.T) {
	if os.Getenv("TC_FD_LIMIT_HELPER") == "1" {
		socket := os.Getenv("TC_FD_LIMIT_SOCKET")
		// Initialize Go's persistent network poller before measuring descriptors.
		prime, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "prime.sock"), Net: "unixpacket"})
		if err != nil {
			t.Fatal(err)
		}
		prime.Close()
		var lim unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
			t.Fatal(err)
		}
		lim.Cur = 24
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
			t.Fatal(err)
		}
		before := fdCount(t)
		a, err := acquireFiles(t.Context(), socket, "v", "key", "all")
		a.close()
		if err == nil || len(a.files) != 0 {
			t.Fatal("FD limit returned partial success")
		}
		if after := fdCount(t); after != before {
			t.Fatalf("FD leak %d -> %d", before, after)
		}
		return
	}
	f, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	socket := stubManager(t, func(c *net.UnixConn, _ request) {
		cases := make([]caseMetadata, 40)
		for i := range cases {
			cases[i].ID = i + 1
		}
		_ = sendJSON(c, reply{TestcaseSetID: "v", Cases: cases})
		fds := make([]int, 64)
		for i := range fds {
			fds[i] = int(f.Fd())
		}
		_ = writePacket(c, []byte{1}, fds)
	})
	cmd := exec.Command(os.Args[0], "-test.run=^TestReceiverFDLimit$")
	cmd.Env = append(os.Environ(), "TC_FD_LIMIT_HELPER=1", "TC_FD_LIMIT_SOCKET="+socket)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
}
