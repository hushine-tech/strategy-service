package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

func TestParseDebugpyWait(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", value: "", want: false},
		{name: "zero", value: "0", want: false},
		{name: "false", value: " false ", want: false},
		{name: "no", value: "NO", want: false},
		{name: "off", value: "off", want: false},
		{name: "one", value: "1", want: true},
		{name: "true", value: "true", want: true},
		{name: "yes", value: "YES", want: true},
		{name: "on", value: "on", want: true},
		{name: "other-non-empty", value: "wait", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseDebugpyWait(tc.value); got != tc.want {
				t.Fatalf("parseDebugpyWait(%q) = %t, want %t", tc.value, got, tc.want)
			}
		})
	}
}

func TestPrepareRuntimeCoverageRootCreatesLanguageDirectories(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime-coverage")

	got, err := prepareRuntimeCoverageRoot(root)
	if err != nil {
		t.Fatalf("prepareRuntimeCoverageRoot(): %v", err)
	}
	if got != root {
		t.Fatalf("coverage root = %q, want %q", got, root)
	}
	for _, child := range []string{"go", "python"} {
		info, err := os.Stat(filepath.Join(root, child))
		if err != nil {
			t.Fatalf("stat %s coverage directory: %v", child, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s coverage path is not a directory", child)
		}
	}
}

func TestPrepareRuntimeCoverageRootRejectsUntrustedPaths(t *testing.T) {
	separator := string(os.PathSeparator)
	unclean := t.TempDir() + separator + "nested" + separator + ".."
	for _, root := range []string{"relative/coverage", unclean} {
		t.Run(root, func(t *testing.T) {
			if _, err := prepareRuntimeCoverageRoot(root); err == nil {
				t.Fatalf("prepareRuntimeCoverageRoot(%q) error = nil", root)
			}
		})
	}
}

func TestPrepareRuntimeCoverageRootDisabled(t *testing.T) {
	got, err := prepareRuntimeCoverageRoot("")
	if err != nil {
		t.Fatalf("prepareRuntimeCoverageRoot(): %v", err)
	}
	if got != "" {
		t.Fatalf("coverage root = %q, want disabled", got)
	}
}

func TestShutdownStopsActiveWorkersBeforeBoundedGRPCStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan string, 4)
	workers := &shutdownTestWorkerManager{active: true, events: events}
	grpcServer := &shutdownTestGRPCServer{
		workers:      workers,
		events:       events,
		forceStop:    make(chan struct{}),
		gracefulDone: make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		shutdownAgentOnContext(ctx, "", workers, grpcServer, 20*time.Millisecond, 5*time.Millisecond)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bounded runtime-agent shutdown")
	}
	select {
	case <-grpcServer.gracefulDone:
	case <-time.After(time.Second):
		t.Fatal("forced gRPC stop did not release graceful shutdown")
	}

	close(events)
	got := make([]string, 0, 3)
	for event := range events {
		got = append(got, event)
	}
	want := []string{"workers", "grpc-graceful", "grpc-stop"}
	if !slices.Equal(got, want) {
		t.Fatalf("shutdown events = %v, want %v", got, want)
	}
	if workers.active {
		t.Fatal("fake worker remains active after shutdown")
	}
}

type shutdownTestWorkerManager struct {
	active bool
	events chan<- string
}

func (m *shutdownTestWorkerManager) StopAll(_ context.Context, _ time.Duration) error {
	if !m.active {
		return fmt.Errorf("no active worker")
	}
	m.events <- "workers"
	m.active = false
	return nil
}

type shutdownTestGRPCServer struct {
	workers      *shutdownTestWorkerManager
	events       chan<- string
	forceStop    chan struct{}
	gracefulDone chan struct{}
}

func (s *shutdownTestGRPCServer) GracefulStop() {
	if s.workers.active {
		s.events <- "grpc-before-workers"
	} else {
		s.events <- "grpc-graceful"
	}
	<-s.forceStop
	close(s.gracefulDone)
}

func (s *shutdownTestGRPCServer) Stop() {
	s.events <- "grpc-stop"
	close(s.forceStop)
}
