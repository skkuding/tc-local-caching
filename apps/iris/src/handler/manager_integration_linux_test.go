//go:build linux

package handler_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/skkuding/codedang/apps/iris/src/common/constants"
	"github.com/skkuding/codedang/apps/iris/src/handler"
	"github.com/skkuding/codedang/apps/iris/src/handler/judge"
	"github.com/skkuding/codedang/apps/iris/src/handler/run"
	"github.com/skkuding/codedang/apps/iris/src/router/response"
	"github.com/skkuding/codedang/apps/iris/src/service/file"
	"github.com/skkuding/codedang/apps/iris/src/service/logger"
	"github.com/skkuding/codedang/apps/iris/src/service/sandbox/judger"
	"github.com/skkuding/codedang/apps/iris/src/service/testcase"
	"go.opentelemetry.io/otel/trace/noop"
)

// No sandbox mock: build and execute C through the existing Judger executable.
func TestManagerWithRealJudgeAndRun(t *testing.T) {
	binary := os.Getenv("TC_MANAGER_BINARY")
	if binary == "" || os.Getenv("LIBJUDGER_PATH") == "" {
		t.Skip("set TC_MANAGER_BINARY and LIBJUDGER_PATH for real judging")
	}
	objects := t.TempDir()
	cmd := exec.Command(binary, "seed-local", "-root", objects)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("seed %v: %s", err, out)
	}
	socket := os.Getenv("TC_MANAGER_SOCKET")
	if socket == "" {
		socket = filepath.Join(t.TempDir(), "s")
		cmd = exec.Command(binary, "serve", "-socket", socket, "-cache", t.TempDir(), "-high", "4611686018427387904", "-low", "2305843009213693952")
		cmd.Env = append(os.Environ(), "TC_OBJECT_ROOT="+objects)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.Dial("unixpacket", socket)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("manager unavailable", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, dir := range []string{constants.RESULT_PATH, "/app/sandbox/logs/run", "/app/sandbox/logs/compile", "logs"} {
		if err := os.MkdirAll(dir, 0770); err != nil {
			t.Fatal(err)
		}
	}
	log := logger.NewLogger(logger.Console, false)
	fm := file.NewFileManager(constants.RESULT_PATH)
	sb := judger.NewJudgerSandboxImpl(fm, log)
	tracer := noop.NewTracerProvider().Tracer("manager-integration")
	reader := testcase.NewTestcaseManager(nil, nil, log, socket)
	code := "#include <stdio.h>\nint main(){long a,b;if(scanf(\"%ld%ld\",&a,&b)!=2)return 1;printf(\"%ld\\n\",a+b);return 0;}"
	for _, tt := range []struct {
		kind, version string
		hidden, wrong bool
		count         int
	}{
		{"judge", "sum-v1", false, false, 2}, {"run", "sum-v1", false, false, 1}, {"judge", "sum-v2", true, false, 1}, {"run", "sum-v2", true, false, 2}, {"judge", "sum-v1", false, true, 2},
	} {
		t.Run(tt.kind+"/"+tt.version+map[bool]string{true: "/wrong", false: ""}[tt.wrong], func(t *testing.T) {
			program := code
			if tt.wrong {
				program = "#include <stdio.h>\nint main(){puts(\"0\");}"
			}
			fields := map[string]any{"code": program, "language": "C", "problemId": 1, "testcaseSetId": tt.version, "manifestKey": "testcases/" + tt.version + "/manifest.json", "timeLimit": 1000, "memoryLimit": 128 << 20, "judgeOnlyHiddenTestcases": tt.hidden, "containHiddenTestcases": tt.hidden}
			data, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			var factory requestFactory
			if tt.kind == "judge" {
				factory = judge.NewFactory(reader, sb, log, tracer)
			} else {
				factory = run.NewFactory(reader, sb, log, tracer)
			}
			task, err := factory.Create(tt.kind, data)
			if err != nil {
				t.Fatal(err)
			}
			units := task.GetBuildUnits()
			for idx, u := range units {
				if err := u.Setup(idx, len(units), fm, sb); err != nil {
					t.Fatal("real build setup", err)
				}
				t.Cleanup(func() { _ = fm.RemoveDir(u.Dir) })
			}
			results := 0
			task.RunAction(context.Background(), "123", func(msg handler.ResultMessage, kinds ...constants.MessageType) {
				if len(kinds) > 0 {
					return
				}
				var result response.JudgeResponse
				if err := json.Unmarshal(msg.EncodedResponse, &result); err != nil {
					t.Error(err)
					return
				}
				want := handler.ACCEPTED
				if tt.wrong {
					want = handler.WRONG_ANSWER
				}
				if result.JudgeResultCode != want {
					t.Errorf("real %s verdict=%+v want=%v", tt.kind, result, want)
				}
				var detail struct {
					Output    string `json:"output"`
					ErrorCode int    `json:"errorCode"`
				}
				if err := json.Unmarshal(result.JudgeResult, &detail); err != nil || detail.Output == "" || detail.ErrorCode != 0 {
					t.Errorf("sandbox did not execute successfully: detail=%s err=%v", result.JudgeResult, err)
				}
				results++
			})
			if results != tt.count {
				t.Fatalf("got %d TC results, want %d", results, tt.count)
			}
		})
	}
}
