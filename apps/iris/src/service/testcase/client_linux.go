//go:build linux

package testcase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skkuding/codedang/apps/iris/src/loader"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	maxPacket      = 32 << 10
	fdsPerPacket   = 64
	requestTimeout = 40 * time.Second
	maxCases       = 512
)

type request struct {
	Op            string `json:"op"`
	TestcaseSetID string `json:"testcaseSetId,omitempty"`
	ManifestKey   string `json:"manifestKey,omitempty"`
	Filter        string `json:"filter,omitempty"`
}
type caseMetadata struct {
	ID          int     `json:"id"`
	Hidden      bool    `json:"hidden"`
	ScoreWeight float64 `json:"scoreWeight"`
}
type reply struct {
	Error         string         `json:"error,omitempty"`
	Code          string         `json:"code,omitempty"`
	TestcaseSetID string         `json:"testcaseSetId"`
	Cases         []caseMetadata `json:"testcases"`
}

func writePacket(conn *net.UnixConn, data []byte, fds []int) error {
	var control []byte
	if len(fds) != 0 {
		control = unix.UnixRights(fds...)
	}
	n, oobn, err := conn.WriteMsgUnix(data, control, nil)
	if err == nil && (n != len(data) || oobn != len(control)) {
		return io.ErrShortWrite
	}
	return err
}

func sendJSON(conn *net.UnixConn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writePacket(conn, data, nil)
}

func readPacket(conn *net.UnixConn) ([]byte, []int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, nil, err
	}
	data := make([]byte, maxPacket)
	control := make([]byte, unix.CmsgSpace(fdsPerPacket*4))
	var n, oobn, flags int
	var readErr error
	err = raw.Read(func(fd uintptr) bool {
		for {
			n, oobn, flags, _, readErr = unix.Recvmsg(int(fd), data, control, unix.MSG_CMSG_CLOEXEC)
			if readErr != unix.EINTR {
				return readErr != unix.EAGAIN
			}
		}
	})
	if err := errors.Join(err, readErr); err != nil {
		return nil, nil, err
	}
	messages, err := unix.ParseSocketControlMessage(control[:oobn])
	if err != nil {
		return nil, nil, err
	}
	var fds []int
	for _, message := range messages {
		rights, err := unix.ParseUnixRights(&message)
		if err != nil {
			closeFDs(fds)
			return nil, nil, err
		}
		fds = append(fds, rights...)
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		closeFDs(fds)
		return nil, nil, errors.New("truncated socket packet or FD control message")
	}
	if n == 0 {
		closeFDs(fds)
		return nil, nil, io.EOF
	}
	return data[:n], fds, nil
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		unix.Close(fd)
	}
}

func receiveJSON(conn *net.UnixConn, value any) error {
	data, fds, err := readPacket(conn)
	if len(fds) != 0 {
		closeFDs(fds)
		return errors.New("unexpected FDs on JSON packet")
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func exchange(ctx context.Context, socket string, req request) (*net.UnixConn, reply, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	var dialer net.Dialer
	opened, err := dialer.DialContext(ctx, "unixpacket", socket)
	if err != nil {
		cancel()
		return nil, reply{}, nil, err
	}
	conn := opened.(*net.UnixConn)
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	closeConn := func() {
		stop()
		conn.Close()
		cancel()
	}
	var res reply
	if err := sendJSON(conn, req); err != nil {
		closeConn()
		return nil, reply{}, nil, err
	}
	if err := receiveJSON(conn, &res); err != nil {
		closeConn()
		return nil, reply{}, nil, err
	}
	if res.Error != "" {
		closeConn()
		return nil, reply{}, nil, fmt.Errorf("%s: %s", res.Code, res.Error)
	}
	return conn, res, closeConn, nil
}

func acquireFiles(ctx context.Context, socket, setID, key, filter string) (acquired, error) {
	conn, res, cleanup, err := exchange(ctx, socket, request{Op: "acquire", TestcaseSetID: setID, ManifestKey: key, Filter: filter})
	if err != nil {
		return acquired{}, err
	}
	defer cleanup()
	if res.TestcaseSetID != setID || len(res.Cases) > maxCases {
		return acquired{}, errors.New("invalid acquire response metadata")
	}
	seen := map[int]bool{}
	for _, tc := range res.Cases {
		if tc.ID <= 0 || seen[tc.ID] || math.IsNaN(tc.ScoreWeight) || math.IsInf(tc.ScoreWeight, 0) || (filter == "public" && tc.Hidden) || (filter == "hidden" && !tc.Hidden) {
			return acquired{}, errors.New("invalid testcase metadata")
		}
		seen[tc.ID] = true
	}
	result := acquired{cases: res.Cases, files: make([]*os.File, 0, 2*len(res.Cases))}
	for len(result.files) < cap(result.files) {
		expected := min(fdsPerPacket, cap(result.files)-len(result.files))
		data, fds, err := readPacket(conn)
		if err != nil || len(data) != 1 || data[0] != 1 || len(fds) != expected {
			closeFDs(fds)
			result.close()
			return acquired{}, errors.Join(errors.New("incomplete FD response"), err)
		}
		for _, fd := range fds {
			result.files = append(result.files, os.NewFile(uintptr(fd), "testcase"))
		}
	}
	return result, nil
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

// GetVersionedTestcase delegates artifact ownership to the node-local Manager.
// Iris consumes all FDs into its existing in-memory testcase representation.
func GetVersionedTestcase(ctx context.Context, socket, setID, key string, filter TestcaseFilterCode) (Testcase, error) {
	if err := ValidateReference(setID, key); err != nil {
		return Testcase{}, err
	}
	if strings.TrimSpace(socket) == "" || strings.TrimSpace(setID) == "" {
		return Testcase{}, errors.New("manager socket and version reference are required")
	}
	var selection string
	switch filter {
	case ALL:
		selection = "all"
	case PUBLIC_ONLY:
		selection = "public"
	case HIDDEN_ONLY:
		selection = "hidden"
	default:
		return Testcase{}, errors.New("invalid testcase filter")
	}
	result, err := acquireFiles(ctx, socket, setID, key, selection)
	if err != nil {
		return Testcase{}, err
	}
	defer result.close()
	out := Testcase{Elements: make([]loader.ElementOut, 0, len(result.cases))}
	var total int64
	for idx, tc := range result.cases {
		var bodies [2][]byte
		for j := range 2 {
			f := result.files[2*idx+j]
			info, err := f.Stat()
			if err != nil {
				return Testcase{}, err
			}
			if !info.Mode().IsRegular() {
				return Testcase{}, errors.New("manager returned non-regular file")
			}
			if info.Size() < 0 || info.Size() > maxReadBytes-total {
				return Testcase{}, errors.New("testcase data exceeds read limit")
			}
			data, err := io.ReadAll(io.LimitReader(contextFileReader{ctx, f}, maxReadBytes-total+1))
			if err != nil {
				return Testcase{}, err
			}
			total += int64(len(data))
			if total > maxReadBytes {
				return Testcase{}, errors.New("testcase data exceeds read limit")
			}
			bodies[j] = data
		}
		out.Elements = append(out.Elements, loader.ElementOut{Id: tc.ID, Hidden: tc.Hidden, In: string(bodies[0]), Out: string(bodies[1])})
	}
	if err := ctx.Err(); err != nil {
		return Testcase{}, err
	}
	return out, nil
}

const maxReadBytes int64 = 256 << 20

type contextFileReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextFileReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
