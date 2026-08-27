package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
	"google.golang.org/grpc"
)

func TestHostedDependencyGateFailsBeforeAnyRuntimeReadiness(t *testing.T) {
	calls := make([]string, 0, 4)
	facts := bootstrapTestFacts("hosted")
	ops := runtimeBootstrapOps{
		loadConfig: func(string) (runtimeagent.Config, error) {
			calls = append(calls, "load-config")
			return runtimeagent.Config{RuntimeSource: "hosted", RuntimeID: "rt-1", RuntimeChannelAddr: "127.0.0.1:50055"}, nil
		},
		resolveWorkerLaunchSpec: func(runtimeagent.Config, runtimeagent.RuntimeIdentity, string, []string) (runtimeBootstrapResolution, error) {
			calls = append(calls, "resolve-worker-launch-spec")
			return runtimeBootstrapResolution{launchSpec: bootstrapTestLaunchSpec(), facts: facts}, nil
		},
		verifyProfile: func(context.Context, runtimeagent.WorkerPythonInvocation, runtimeagent.EmbeddedRuntimeFacts) (*strategyv1.RuntimeDependencyProfile, error) {
			calls = append(calls, "verify-profile")
			return nil, &runtimeagent.RuntimeDependencyProfileError{
				Code: "RUNTIME_DEPENDENCY_PROFILE_INVALID", Module: "grpc",
				Message: "runtime dependency startup probe failed",
			}
		},
		emitStartupFailure: func(io.Writer, runtimeagent.RuntimeIdentity, runtimeagent.EmbeddedRuntimeFacts, *runtimeagent.RuntimeDependencyProfileError) {
			calls = append(calls, "emit-startup-failure")
		},
	}
	if code := runWithOps([]string{"--config", "unused.yaml"}, ops); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	want := []string{"load-config", "resolve-worker-launch-spec", "verify-profile", "emit-startup-failure"}
	if !slices.Equal(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func TestSelfHostedDependencyFailureReportsOnceWithoutCreatingReadiness(t *testing.T) {
	calls := make([]string, 0, 8)
	facts := bootstrapTestFacts("self_hosted")
	ops := runtimeBootstrapOps{
		loadConfig: func(string) (runtimeagent.Config, error) {
			calls = append(calls, "load-config")
			return runtimeagent.Config{RuntimeSource: "self_hosted", RuntimeID: "rt-1", RuntimeChannelAddr: "127.0.0.1:50055"}, nil
		},
		resolveWorkerLaunchSpec: func(runtimeagent.Config, runtimeagent.RuntimeIdentity, string, []string) (runtimeBootstrapResolution, error) {
			calls = append(calls, "resolve-worker-launch-spec")
			return runtimeBootstrapResolution{launchSpec: bootstrapTestLaunchSpec(), facts: facts}, nil
		},
		verifyProfile: func(context.Context, runtimeagent.WorkerPythonInvocation, runtimeagent.EmbeddedRuntimeFacts) (*strategyv1.RuntimeDependencyProfile, error) {
			calls = append(calls, "verify-profile")
			return nil, &runtimeagent.RuntimeDependencyProfileError{
				Code: "RUNTIME_DEPENDENCY_PROFILE_INVALID", Module: "grpc",
				Message: "runtime dependency startup probe failed",
			}
		},
		emitStartupFailure: func(io.Writer, runtimeagent.RuntimeIdentity, runtimeagent.EmbeddedRuntimeFacts, *runtimeagent.RuntimeDependencyProfileError) {
			calls = append(calls, "emit-startup-failure")
		},
		loadCredential: func(string) (*runtimeagent.RuntimeCredential, error) {
			calls = append(calls, "load-credential")
			return &runtimeagent.RuntimeCredential{KeyID: "key-1"}, nil
		},
		dialOptions: func(runtimeagent.TLSConfig) ([]grpc.DialOption, error) {
			calls = append(calls, "load-tls")
			return []grpc.DialOption{grpc.WithInsecure()}, nil
		},
		buildStartupFailureRequest: func(runtimeagent.RuntimeIdentity, *runtimeagent.RuntimeCredential, *runtimeagent.RuntimeDependencyProfileError, time.Time, string) (*cpv1.ReportRuntimeStartupFailureRequest, error) {
			calls = append(calls, "build-report")
			return &cpv1.ReportRuntimeStartupFailureRequest{}, nil
		},
		reportStartupFailure: func(context.Context, string, []grpc.DialOption, *cpv1.ReportRuntimeStartupFailureRequest) error {
			calls = append(calls, "report-failure")
			return errors.New("report unavailable")
		},
		now:   func() time.Time { return time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC) },
		nonce: func() (string, error) { return "nonce", nil },
	}
	if code := runWithOps([]string{"--config", "unused.yaml"}, ops); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	want := []string{
		"load-config", "resolve-worker-launch-spec", "verify-profile", "emit-startup-failure",
		"load-credential", "load-tls", "build-report", "report-failure",
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func bootstrapTestLaunchSpec() runtimeagent.WorkerLaunchSpec {
	return runtimeagent.WorkerLaunchSpec{Invocation: runtimeagent.WorkerPythonInvocation{
		Executable: "/app/.venv/bin/python", ArgsPrefix: []string{"-I"}, WorkDir: "/app", Env: []string{"PATH=/app/.venv/bin"},
	}}
}

func bootstrapTestFacts(source string) runtimeagent.EmbeddedRuntimeFacts {
	return runtimeagent.EmbeddedRuntimeFacts{Source: source, Profile: &strategyv1.RuntimeDependencyProfile{
		SchemaVersion: 1, ProfileName: "platform-python-3.13", ProfileVersion: "1.0.0",
		ContractSha256: strings.Repeat("a", 64), HostedPython: "3.13",
		PublicImportRoots:     []string{"dateutil", "google", "grpc"},
		StrategyServiceCommit: strings.Repeat("b", 40), StrategyLibraryCommit: strings.Repeat("c", 40),
		ImageBuildId: strings.Repeat("b", 12) + "-" + strings.Repeat("c", 12) + "-" + strings.Repeat("d", 12) + "-1.0.0-executor",
	}}
}

func TestRunAfterRuntimeAuthenticationDoesNotStartEarly(t *testing.T) {
	authenticated := make(chan struct{})
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runAfterRuntimeAuthentication(
		ctx,
		func(waitCtx context.Context) error {
			select {
			case <-waitCtx.Done():
				return waitCtx.Err()
			case <-authenticated:
				return nil
			}
		},
		func() {
			close(started)
		},
	)

	select {
	case <-started:
		t.Fatal("terminal retry loop started before RuntimeChannel authentication")
	case <-time.After(50 * time.Millisecond):
	}
	close(authenticated)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("terminal retry loop did not start after RuntimeChannel authentication")
	}
}

func TestRunAfterRuntimeAuthenticationHonorsCancellation(t *testing.T) {
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runAfterRuntimeAuthentication(
		ctx,
		func(waitCtx context.Context) error {
			<-waitCtx.Done()
			return waitCtx.Err()
		},
		func() {
			close(started)
		},
	)
	select {
	case <-started:
		t.Fatal("terminal retry loop started after authentication wait was cancelled")
	default:
	}
}

func TestCoordinateRuntimeLifecycleKeepsChannelContextAliveUntilAgentShutdown(
	t *testing.T,
) {
	signalCtx, cancelSignal := context.WithCancel(context.Background())
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	events := make(chan string, 3)
	runtimeStarted := make(chan struct{})
	result := make(chan struct {
		runErr      error
		shutdownErr error
	}, 1)
	go func() {
		runErr, shutdownErr := coordinateRuntimeLifecycle(
			signalCtx,
			serviceCtx,
			cancelService,
			func(runCtx context.Context) error {
				close(runtimeStarted)
				<-runCtx.Done()
				events <- "runtime-channel-stopped"
				return runCtx.Err()
			},
			func() error {
				if serviceCtx.Err() != nil {
					return fmt.Errorf(
						"runtime channel context was cancelled before Agent shutdown",
					)
				}
				events <- "agent-shutdown"
				return nil
			},
		)
		result <- struct {
			runErr      error
			shutdownErr error
		}{runErr: runErr, shutdownErr: shutdownErr}
	}()
	<-runtimeStarted
	cancelSignal()
	outcome := <-result
	if outcome.shutdownErr != nil {
		t.Fatal(outcome.shutdownErr)
	}
	if !errors.Is(outcome.runErr, context.Canceled) {
		t.Fatalf("runtime error = %v", outcome.runErr)
	}
	close(events)
	got := make([]string, 0, 2)
	for event := range events {
		got = append(got, event)
	}
	if !slices.Equal(got, []string{
		"agent-shutdown",
		"runtime-channel-stopped",
	}) {
		t.Fatalf("lifecycle events = %v", got)
	}
}

func TestCoordinateRuntimeLifecycleRetriesUnsafeShutdownBeforeCancellation(
	t *testing.T,
) {
	signalCtx, cancelSignal := context.WithCancel(context.Background())
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	events := make(chan string, 4)
	runtimeStarted := make(chan struct{})
	result := make(chan struct {
		runErr      error
		shutdownErr error
	}, 1)
	shutdownCalls := 0
	go func() {
		runErr, shutdownErr := coordinateRuntimeLifecycle(
			signalCtx,
			serviceCtx,
			cancelService,
			func(runCtx context.Context) error {
				close(runtimeStarted)
				<-runCtx.Done()
				events <- "runtime-channel-stopped"
				return runCtx.Err()
			},
			func() error {
				shutdownCalls++
				if serviceCtx.Err() != nil {
					return fmt.Errorf(
						"runtime channel context cancelled before safe shutdown",
					)
				}
				events <- fmt.Sprintf("agent-shutdown-%d", shutdownCalls)
				if shutdownCalls == 1 {
					return errors.New("terminal state is not durable")
				}
				return nil
			},
		)
		result <- struct {
			runErr      error
			shutdownErr error
		}{runErr: runErr, shutdownErr: shutdownErr}
	}()
	<-runtimeStarted
	cancelSignal()
	select {
	case outcome := <-result:
		if outcome.shutdownErr != nil {
			t.Fatal(outcome.shutdownErr)
		}
		if !errors.Is(outcome.runErr, context.Canceled) {
			t.Fatalf("runtime error = %v", outcome.runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("safe shutdown retry did not complete")
	}
	close(events)
	got := make([]string, 0, 3)
	for event := range events {
		got = append(got, event)
	}
	if !slices.Equal(got, []string{
		"agent-shutdown-1",
		"agent-shutdown-2",
		"runtime-channel-stopped",
	}) {
		t.Fatalf("lifecycle events = %v", got)
	}
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

func TestWorkerIPCListenerUsesLoopbackTCP(t *testing.T) {
	listener, err := listenWorkerIPC()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if listener.Addr().Network() != "tcp" {
		t.Fatalf(
			"worker IPC network = %q, want tcp",
			listener.Addr().Network(),
		)
	}
	host, _, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		t.Fatalf("worker IPC address = %q, want loopback", host)
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

func TestRuntimeAgentStartTimeoutAllowsDebuggerAttach(t *testing.T) {
	if got := runtimeAgentStartTimeout(false); got != 0 {
		t.Fatalf("normal runtime start timeout = %v, want default", got)
	}
	if got := runtimeAgentStartTimeout(true); got != 24*time.Hour {
		t.Fatalf("debug runtime start timeout = %v, want 24h", got)
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

func TestDependencyGateFailureDoesNotPublishCoverageReadinessMarker(t *testing.T) {
	dir := t.TempDir()
	coverageRoot := filepath.Join(dir, "coverage")
	if err := os.MkdirAll(coverageRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := runtimeagent.CoverageFinalization{
		SchemaVersion:  1,
		RuntimeID:      "runtime-early",
		BootID:         "stale-boot",
		State:          runtimeagent.CoverageFinalizationComplete,
		WorkerShutdown: runtimeagent.CoverageFinalizationOK,
		GoSnapshot:     runtimeagent.CoverageFinalizationOK,
		CompletedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := runtimeagent.WriteCoverageFinalization(coverageRoot, stale); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.yaml")
	config := fmt.Sprintf(`dependencies:
  runtime_channel_grpc: "127.0.0.1:50055"
runtime:
  source: "hosted"
  runtime_id: "runtime-early"
  name: "early failure"
  credential_path: %q
log:
  output_dir: %q
`, filepath.Join(dir, "missing.cred"), filepath.Join(dir, "logs"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUSHINE_RUNTIME_COVERAGE_DIR", coverageRoot)
	for _, name := range []string{
		"RUNTIME_CHANNEL_GRPC_ADDR", "DEPENDENCIES_RUNTIME_CHANNEL_GRPC", "RUNTIME_SOURCE",
		"RUNTIME_RUNTIME_ID", "RUNTIME_NAME", "RUNTIME_CREDENTIAL_PATH",
	} {
		t.Setenv(name, "")
	}

	if exitCode := run([]string{"--config", configPath}); exitCode != 1 {
		t.Fatalf("run exit code = %d, want credential failure", exitCode)
	}
	body, err := os.ReadFile(filepath.Join(coverageRoot, runtimeagent.CoverageFinalizationFile))
	if err != nil {
		t.Fatalf("read running marker: %v", err)
	}
	var marker runtimeagent.CoverageFinalization
	if err := json.Unmarshal(body, &marker); err != nil {
		t.Fatalf("decode running marker: %v", err)
	}
	if marker.State != runtimeagent.CoverageFinalizationComplete || marker.BootID != stale.BootID {
		t.Fatalf("marker after dependency gate failure = %+v, want untouched stale marker", marker)
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
		shutdownAgentOnContext(ctx, "", "", "", workers, workers, grpcServer, 20*time.Millisecond, 5*time.Millisecond, defaultCoverageShutdownOps())
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

func TestShutdownKeepsWorkerIPCAvailableWhenTerminalStateIsUnsafe(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make([]string, 0, 3)
	workers := &orderedShutdownWorkers{
		events:  &events,
		stopErr: errors.New("terminal state is not durable"),
	}
	grpcServer := &orderedShutdownGRPC{events: &events}
	ops := coverageShutdownOps{
		writeSnapshot: func(string) error {
			events = append(events, "snapshot")
			return nil
		},
		writeFinalization: func(
			string,
			runtimeagent.CoverageFinalization,
		) error {
			events = append(events, "finalization")
			return nil
		},
		now: time.Now,
	}
	cancel()

	err := shutdownAgentOnContext(
		ctx,
		"/coverage",
		"rt-1",
		"boot-1",
		workers,
		workers,
		grpcServer,
		time.Second,
		time.Second,
		ops,
	)
	if err == nil {
		t.Fatal("shutdown error = nil, want unsafe terminal-state failure")
	}
	if !slices.Equal(events, []string{"workers"}) {
		t.Fatalf(
			"unsafe shutdown events = %v, want IPC and coverage left active",
			events,
		)
	}
}

func TestShutdownFinalizesCoverageAfterWorkersAndSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make([]string, 0, 4)
	workers := &orderedShutdownWorkers{events: &events}
	grpcServer := &orderedShutdownGRPC{events: &events}
	var final runtimeagent.CoverageFinalization
	ops := coverageShutdownOps{
		writeSnapshot: func(string) error {
			events = append(events, "snapshot")
			return nil
		},
		writeFinalization: func(_ string, record runtimeagent.CoverageFinalization) error {
			events = append(events, "finalization")
			final = record
			return nil
		},
		now: func() time.Time { return time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC) },
	}
	done := make(chan struct{})
	go func() {
		shutdownAgentOnContext(ctx, "/coverage", "rt-1", "boot-1", workers, workers, grpcServer, time.Second, time.Second, ops)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
	want := []string{"workers", "snapshot", "finalization", "grpc"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if final.SchemaVersion != 1 || final.RuntimeID != "rt-1" || final.BootID != "boot-1" ||
		final.State != "complete" || final.WorkerShutdown != "ok" || final.GoSnapshot != "ok" ||
		final.ForcedWorkers != 0 || final.CompletedAt != "2026-07-12T01:02:03Z" {
		t.Fatalf("finalization = %+v", final)
	}
}

func TestShutdownMarksCoverageIncompleteForForcedWorkerOrSnapshotFailure(t *testing.T) {
	for _, tc := range []struct {
		name          string
		workers       *orderedShutdownWorkers
		snapshotError error
		workerStatus  string
		forcedWorkers int
		goStatus      string
	}{
		{name: "forced worker", workers: &orderedShutdownWorkers{forced: 1}, workerStatus: "forced", forcedWorkers: 1, goStatus: "ok"},
		{name: "snapshot error", workers: &orderedShutdownWorkers{}, snapshotError: errors.New("snapshot failed"), workerStatus: "ok", goStatus: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			var final runtimeagent.CoverageFinalization
			ops := coverageShutdownOps{
				writeSnapshot: func(string) error { return tc.snapshotError },
				writeFinalization: func(_ string, record runtimeagent.CoverageFinalization) error {
					final = record
					return nil
				},
				now: time.Now,
			}
			done := make(chan struct{})
			go func() {
				shutdownAgentOnContext(ctx, "/coverage", "rt-1", "boot-1", tc.workers, tc.workers, &orderedShutdownGRPC{}, time.Second, time.Second, ops)
				close(done)
			}()
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not finish")
			}
			if final.SchemaVersion != 1 || final.RuntimeID != "rt-1" || final.BootID != "boot-1" ||
				final.State != "incomplete" || final.WorkerShutdown != tc.workerStatus ||
				final.ForcedWorkers != tc.forcedWorkers || final.GoSnapshot != tc.goStatus || final.CompletedAt == "" {
				t.Fatalf("finalization = %+v, want incomplete", final)
			}
		})
	}
}

type orderedShutdownWorkers struct {
	events  *[]string
	stopErr error
	forced  int
}

func (w *orderedShutdownWorkers) Shutdown(context.Context, time.Duration) error {
	if w.events != nil {
		*w.events = append(*w.events, "workers")
	}
	return w.stopErr
}

func (w *orderedShutdownWorkers) ShutdownSummary() runtimeagent.WorkerShutdownSummary {
	return runtimeagent.WorkerShutdownSummary{ForcedStops: w.forced}
}

type orderedShutdownGRPC struct{ events *[]string }

func (g *orderedShutdownGRPC) GracefulStop() {
	if g.events != nil {
		*g.events = append(*g.events, "grpc")
	}
}

func (*orderedShutdownGRPC) Stop() {}

type shutdownTestWorkerManager struct {
	active bool
	events chan<- string
}

func (m *shutdownTestWorkerManager) Shutdown(_ context.Context, _ time.Duration) error {
	if !m.active {
		return fmt.Errorf("no active worker")
	}
	m.events <- "workers"
	m.active = false
	return nil
}

func (*shutdownTestWorkerManager) ShutdownSummary() runtimeagent.WorkerShutdownSummary {
	return runtimeagent.WorkerShutdownSummary{}
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
