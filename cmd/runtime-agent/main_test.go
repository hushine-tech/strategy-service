package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestRunAgentStartsIndicatorSyncLoopWithProcessContext(t *testing.T) {
	invoker := &syncLoopPlatformInvoker{called: make(chan struct{}, 1)}
	agent := runtimeagent.NewAgent(runtimeagent.AgentConfig{
		PlatformInvoker: invoker, IndicatorFlushInterval: time.Millisecond,
	})
	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_IndicatorFrame{IndicatorFrame: &rwv1.IndicatorFrame{
			SessionId: "sess-1", StreamKey: "futures:TESTUSDT:1m", MarketTimeMs: 1000,
			Values: []*rwv1.IndicatorValue{{IndicatorKey: "alpha", Value: 1, HasValue: true}},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("HandleWorkerFrame: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAgentBackgroundLoops(ctx, agent)
	select {
	case <-invoker.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for indicator sync loop")
	}
	cancel()
}

type syncLoopPlatformInvoker struct {
	called chan struct{}
}

func (i *syncLoopPlatformInvoker) InvokePlatformAny(_ context.Context, method string, _ *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	if method == "portfolio.SaveStrategyIndicators" {
		select {
		case i.called <- struct{}{}:
		default:
		}
	}
	return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{})
}

func TestRuntimeIdentityFromConfigBuildsBareIdentity(t *testing.T) {
	cfg := runtimeagent.Config{
		RuntimeID:       "",
		RuntimeName:     "",
		Capabilities:    []string{"strategy"},
		ResourceProfile: "small",
		Version:         "0.1.0",
	}

	identity := runtimeIdentityFromConfig(cfg, 6)

	if identity.Source != "bare" || identity.UserID != 6 {
		t.Fatalf("identity source/user = %q/%d", identity.Source, identity.UserID)
	}
	if !strings.HasPrefix(identity.RuntimeID, "bare-6-") {
		t.Fatalf("runtime_id = %q", identity.RuntimeID)
	}
	if !strings.HasPrefix(identity.Name, "bare-debug-6-") {
		t.Fatalf("name = %q", identity.Name)
	}
}

func TestWorkerPythonExecutablePrefersProjectVenv(t *testing.T) {
	dir := t.TempDir()
	venvBin := filepath.Join(dir, ".venv", "bin")
	if err := os.MkdirAll(venvBin, 0o755); err != nil {
		t.Fatalf("mkdir venv bin: %v", err)
	}
	pythonPath := filepath.Join(venvBin, "python")
	if err := os.WriteFile(pythonPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write venv python: %v", err)
	}
	t.Setenv("HUSHINE_WORKER_PYTHON", "")
	t.Setenv("PYTHON", "")
	t.Setenv("HUSHINE_WORKER_PYTHON_ARGS", "")
	t.Chdir(dir)

	if got := workerPythonExecutable(0); got != pythonPath {
		t.Fatalf("worker python executable = %q, want %q", got, pythonPath)
	}
}
