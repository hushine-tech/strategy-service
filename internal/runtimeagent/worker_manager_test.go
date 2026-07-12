package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWorkerManagerAllocatesOneTokenPerSession(t *testing.T) {
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "strategy_service.session_worker_entry",
	})

	spec, err := m.PrepareSessionWorker("sess-1")
	if err != nil {
		t.Fatalf("PrepareSessionWorker: %v", err)
	}
	if spec.SessionID != "sess-1" || spec.Token == "" {
		t.Fatalf("spec = %+v, want session and token", spec)
	}
	if spec.AgentAddr == "" {
		t.Fatalf("AgentAddr is empty")
	}
}

func TestWorkerManagerRejectsDuplicateSession(t *testing.T) {
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "strategy_service.session_worker_entry",
	})
	if _, err := m.PrepareSessionWorker("sess-1"); err != nil {
		t.Fatalf("first PrepareSessionWorker: %v", err)
	}
	if _, err := m.PrepareSessionWorker("sess-1"); err == nil {
		t.Fatalf("duplicate session was accepted")
	}
}

func TestWorkerManagerStartsPythonWorkerWithAgentEnv(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "env.txt")
	module := filepath.Join(dir, "worker_stub.py")
	source := fmt.Sprintf(`
import os
from pathlib import Path
Path(%q).write_text("\n".join([
    os.environ.get("HUSHINE_AGENT_ADDR", ""),
    os.environ.get("HUSHINE_SESSION_ID", ""),
    os.environ.get("HUSHINE_WORKER_TOKEN", ""),
    os.environ.get("HUSHINE_RUNTIME_ID", ""),
    "DATABASE_PASSWORD=" + os.environ.get("DATABASE_PASSWORD", ""),
]), encoding="utf-8")
`, out)
	if err := os.WriteFile(module, []byte(source), 0o600); err != nil {
		t.Fatalf("write worker module: %v", err)
	}
	t.Setenv("DATABASE_PASSWORD", "parent-canary-secret")

	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_stub",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker, err := m.StartSessionWorker(ctx, "sess-worker", []string{"HUSHINE_RUNTIME_ID=rt-test"})
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatalf("worker wait: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read worker env output: %v", err)
	}
	lines := strings.Split(string(body), "\n")
	if len(lines) != 5 {
		t.Fatalf("worker env lines = %q", string(body))
	}
	if lines[0] != "127.0.0.1:59000" || lines[1] != "sess-worker" || lines[2] == "" || lines[3] != "rt-test" {
		t.Fatalf("worker env = %q", string(body))
	}
	if lines[4] != "DATABASE_PASSWORD=" {
		t.Fatalf("parent secret leaked to worker: %q", lines[4])
	}
	if err := worker.Wait(); err != nil {
		t.Fatalf("second worker wait should reuse reaped result: %v", err)
	}
	if _, ok := m.Registry().ActiveWorker("sess-worker"); ok {
		t.Fatalf("worker registry still has exited worker")
	}
}

func TestWorkerManagerStartsPythonWorkerWithTypedDebugpyWait(t *testing.T) {
	cases := []struct {
		name string
		wait bool
		want string
	}{
		{name: "disabled", wait: false, want: "false"},
		{name: "enabled", wait: true, want: "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "debug-wait.txt")
			writePythonWorkerModule(t, dir, "worker_debug_wait", fmt.Sprintf(`
import os
from pathlib import Path
Path(%q).write_text(os.environ.get("DEBUG_WAIT", ""), encoding="utf-8")
`, out))
			m := NewWorkerManager(WorkerManagerConfig{
				PythonExecutable: "python3",
				WorkerModule:     "worker_debug_wait",
				AgentAddr:        "127.0.0.1:59000",
				DebugpyWait:      tc.wait,
				WorkDir:          dir,
				StateRoot:        filepath.Join(dir, "state"),
				PythonPath:       []string{dir},
			})
			worker, err := m.StartSessionWorker(context.Background(), "sess-debug-wait", nil)
			if err != nil {
				t.Fatalf("StartSessionWorker: %v", err)
			}
			if err := worker.Wait(); err != nil {
				t.Fatalf("worker wait: %v", err)
			}
			if worker.Spec.DebugpyWait != tc.wait {
				t.Fatalf("worker spec DebugpyWait = %t, want %t", worker.Spec.DebugpyWait, tc.wait)
			}
			body, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("read DEBUG_WAIT output: %v", err)
			}
			if got := string(body); got != tc.want {
				t.Fatalf("worker DEBUG_WAIT = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWorkerManagerStartedWorkerSurvivesCanceledRequestContext(t *testing.T) {
	dir := t.TempDir()
	module := filepath.Join(dir, "worker_sleep.py")
	if err := os.WriteFile(module, []byte(`
import time
time.sleep(10)
`), 0o600); err != nil {
		t.Fatalf("write worker module: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_sleep",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker, err := m.StartSessionWorker(ctx, "sess-survive", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- worker.Wait()
	}()
	select {
	case err := <-done:
		t.Fatalf("worker exited when request context was canceled: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := m.StopSessionWorker(context.Background(), "sess-survive", time.Second); err != nil {
		t.Fatalf("StopSessionWorker: %v", err)
	}
}

func TestWorkerManagerStopSessionWorkerTreatsAlreadyExitedProcessAsStopped(t *testing.T) {
	cmd := exec.Command("python3", "-c", "pass")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short process: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait short process: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{})
	m.active["sess-exited"] = &ManagedWorker{SessionID: "sess-exited", Cmd: cmd}

	err := m.StopSessionWorker(context.Background(), "sess-exited", time.Second)
	if err != nil {
		t.Fatalf("StopSessionWorker: %v", err)
	}
	if worker := m.findWorker("sess-exited"); worker != nil {
		t.Fatalf("exited worker was not forgotten")
	}
}

func TestStopSessionWorkerAlreadyDonePreservesReplacementRegistry(t *testing.T) {
	cmd := exec.Command("python3", "-c", "pass")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short process: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait short process: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{})
	old := &ManagedWorker{SessionID: "sess-replaced", Spec: WorkerStartSpec{Token: "old-token"}, Cmd: cmd}
	m.active[old.SessionID] = old
	if err := m.registry.ExpectWorker(old.SessionID, "replacement-token"); err != nil {
		t.Fatal(err)
	}
	if err := m.registry.AdmitWorker(old.SessionID, "replacement-token", 424242); err != nil {
		t.Fatal(err)
	}

	if err := m.StopSessionWorker(context.Background(), old.SessionID, time.Second); err != nil {
		t.Fatal(err)
	}
	identity, ok := m.registry.ActiveWorker(old.SessionID)
	if !ok || identity.PID != 424242 {
		t.Fatalf("replacement identity=(%+v,%v), want pid 424242", identity, ok)
	}
}

func TestStopSessionWorkerRequestsGracefulStopBeforeKill(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker, marker := startSignalAwareWorker(t)

	err := manager.StopSessionWorker(context.Background(), worker.SessionID, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("SIGTERM marker: %v", err)
	}
}

func TestStopSessionWorkerAndStopAllShareSingleStopOperation(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker, signals, release := startReleaseDrivenSignalCountingWorker(t, "shared-stop")

	stopEntered := make(chan struct{}, 2)
	stopWorker := manager.stopWorker
	manager.stopWorker = func(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
		stopEntered <- struct{}{}
		return stopWorker(ctx, worker, timeout)
	}
	stopSessionDone := make(chan error, 1)
	stopAllDone := make(chan error, 1)
	go func() {
		stopSessionDone <- manager.StopSessionWorker(context.Background(), worker.SessionID, 2*time.Second)
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("StopSessionWorker did not reach worker")
	}
	waitForWorkerFile(t, signals)
	go func() {
		stopAllDone <- manager.StopAll(context.Background(), 2*time.Second)
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("concurrent StopAll did not reach shared worker")
	}
	time.Sleep(100 * time.Millisecond)
	if got := readSignalCount(t, signals); got != 1 {
		t.Fatalf("graceful stop signals = %d, want one shared stop operation", got)
	}
	if err := os.WriteFile(release, []byte("exit"), 0o600); err != nil {
		t.Fatalf("release worker: %v", err)
	}
	if err := <-stopSessionDone; err != nil {
		t.Fatalf("StopSessionWorker: %v", err)
	}
	if err := <-stopAllDone; err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 0 {
		t.Fatalf("forced stops = %d, want shared graceful completion", got)
	}
}

func TestSingleStopOperationFollowerCancellationDoesNotSignalAgain(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker, signals, release := startReleaseDrivenSignalCountingWorker(t, "follower-cancel")

	stopEntered := make(chan struct{}, 2)
	stopWorker := manager.stopWorker
	manager.stopWorker = func(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
		stopEntered <- struct{}{}
		return stopWorker(ctx, worker, timeout)
	}
	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- manager.StopSessionWorker(context.Background(), worker.SessionID, 2*time.Second)
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("stop owner did not reach worker")
	}
	waitForWorkerFile(t, signals)

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	followerDone := make(chan error, 1)
	go func() {
		followerDone <- manager.StopAll(followerCtx, 2*time.Second)
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("stop follower did not reach worker")
	}
	cancelFollower()
	if err := <-followerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("StopAll follower error = %v, want context cancellation", err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := readSignalCount(t, signals); got != 1 {
		t.Fatalf("graceful stop signals = %d, want canceled follower to send none", got)
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 0 {
		t.Fatalf("forced stops = %d, want canceled follower to leave owner in control", got)
	}
	if err := os.WriteFile(release, []byte("exit"), 0o600); err != nil {
		t.Fatalf("release worker: %v", err)
	}
	if err := <-ownerDone; err != nil {
		t.Fatalf("stop owner: %v", err)
	}
}

func TestWaitSessionWorkerAllowsNaturalCoverageExitWithoutSignal(t *testing.T) {
	requirePOSIXSignals(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "unexpected-signal")
	writePythonWorkerModule(t, dir, "worker_natural_exit", fmt.Sprintf(`
import pathlib
import signal
import time
signal.signal(signal.SIGTERM, lambda *_: pathlib.Path(%q).write_text("signal", encoding="utf-8"))
time.sleep(0.15)
`, marker))
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_natural_exit",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	worker, err := manager.StartSessionWorker(context.Background(), "sess-natural-exit", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	if err := manager.WaitSessionWorker(context.Background(), worker.SessionID, time.Second); err != nil {
		t.Fatalf("WaitSessionWorker: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("natural wait signaled worker, marker error = %v", err)
	}
	if got := manager.findWorker(worker.SessionID); got != nil {
		t.Fatalf("naturally exited worker remained active: %+v", got)
	}
}

func TestStopSessionWorkerForceKillsWorkerAfterTimeout(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker := startSignalIgnoringWorker(t)
	timeout := 100 * time.Millisecond
	maxElapsed := timeout + 5*time.Second
	started := time.Now()

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- manager.StopSessionWorker(context.Background(), worker.SessionID, timeout)
	}()
	var err error
	select {
	case err = <-stopDone:
	case <-time.After(maxElapsed):
		t.Fatalf("worker stop did not complete within %v", maxElapsed)
	}
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < timeout {
		t.Fatalf("worker stopped after %v, want force kill after at least %v", elapsed, timeout)
	}
	if worker.Cmd.ProcessState == nil {
		t.Fatal("worker process was not reaped after force kill")
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 1 {
		t.Fatalf("forced stops = %d, want 1", got)
	}
}

func TestDrainingWorkerTimeoutFallsBackToSignalAndForce(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker := startSignalIgnoringWorker(t)
	manager.MarkSessionWorkerDraining(worker.SessionID)
	timeout := 75 * time.Millisecond
	started := time.Now()

	if err := manager.StopAll(context.Background(), timeout); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 2*timeout {
		t.Fatalf("StopAll elapsed = %v, want drain grace then normal stop timeout", elapsed)
	}
	if worker.Cmd.ProcessState == nil {
		t.Fatal("draining worker was not reaped after timeout fallback")
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 1 {
		t.Fatalf("forced stops = %d, want timeout fallback force", got)
	}
}

func TestDrainingWorkerProductionDeadlineReservesForceReapAndCleanup(t *testing.T) {
	requirePOSIXSignals(t)
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	writePythonWorkerModule(t, dir, "worker_draining_deadline", fmt.Sprintf(`
import signal
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(%q).write_text("ready", encoding="utf-8")
while True:
    time.sleep(0.05)
`, ready))
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_draining_deadline",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	cleanupDone := make(chan struct{})
	manager.cleanupSessionRoot = func(path string) error {
		time.Sleep(50 * time.Millisecond)
		err := os.RemoveAll(path)
		close(cleanupDone)
		return err
	}
	worker, err := manager.StartSessionWorker(context.Background(), "draining-production-deadline", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	t.Cleanup(func() {
		if worker.Cmd != nil && worker.Cmd.Process != nil {
			_ = worker.Cmd.Process.Kill()
		}
		_ = worker.Wait()
	})
	waitForWorkerFile(t, ready)
	manager.MarkSessionWorkerDraining(worker.SessionID)

	workerTimeout := 300 * time.Millisecond
	sharedTimeout := 2 * workerTimeout
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	sharedDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("shared shutdown context has no deadline")
	}
	if err := manager.StopAll(ctx, workerTimeout); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if !time.Now().Before(sharedDeadline) {
		t.Fatal("StopAll exhausted the shared shutdown deadline before force/reap/cleanup")
	}
	select {
	case <-cleanupDone:
	default:
		t.Fatal("StopAll returned before managed cleanup completed")
	}
	if worker.Cmd.ProcessState == nil {
		t.Fatal("forced draining worker was not reaped")
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 1 {
		t.Fatalf("forced stops = %d, want one", got)
	}
}

func TestStopSessionWorkerCancellationForceKillsAndReapsWorker(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker := startSignalIgnoringWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()

	err := manager.StopSessionWorker(ctx, worker.SessionID, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StopSessionWorker error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("canceled stop returned after %v, want no more than 5s", elapsed)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- worker.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker was not asynchronously reaped after canceled stop")
	}
	if worker.Cmd.ProcessState == nil {
		t.Fatal("worker process was not asynchronously reaped after canceled stop")
	}
	deadline := time.Now().Add(2 * time.Second)
	for manager.findWorker(worker.SessionID) != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := manager.findWorker(worker.SessionID); got != nil {
		t.Fatalf("worker remained active after canceled stop: %+v", got)
	}
}

func TestCanceledUnmanagedStopEventuallyReleasesOwnership(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	worker := startUnmanagedWorker(t, "sess-unmanaged-cancel")
	worker.Spec = WorkerStartSpec{SessionID: worker.SessionID, Token: "unmanaged-token"}
	if err := manager.registry.ExpectWorker(worker.SessionID, worker.Spec.Token); err != nil {
		t.Fatal(err)
	}
	if err := manager.registry.AdmitWorker(worker.SessionID, worker.Spec.Token, int64(worker.Cmd.Process.Pid)); err != nil {
		t.Fatal(err)
	}
	manager.active[worker.SessionID] = worker
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := manager.StopSessionWorker(ctx, worker.SessionID, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("StopSessionWorker error = %v, want context.Canceled", err)
	}
	_ = worker.Wait()
	deadline := time.Now().Add(time.Second)
	for manager.findWorker(worker.SessionID) != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := manager.findWorker(worker.SessionID); got != nil {
		t.Fatalf("reaped unmanaged worker remained active: %+v", got)
	}
	if identity, ok := manager.registry.ActiveWorker(worker.SessionID); ok {
		t.Fatalf("reaped unmanaged worker remained registered: %+v", identity)
	}
}

func TestStopAllDeduplicatesAliasedWorkers(t *testing.T) {
	requirePOSIXSignals(t)
	manager, worker, counter := startCountingWorker(t)
	if err := manager.Registry().AdmitWorker(worker.SessionID, worker.Spec.Token, int64(worker.Cmd.Process.Pid)); err != nil {
		t.Fatal(err)
	}
	if err := manager.AliasWorkerSession(worker.SessionID, "replacement-session"); err != nil {
		t.Fatal(err)
	}
	stopWorker := manager.stopWorker
	stopAttempts := 0
	manager.stopWorker = func(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
		stopAttempts++
		return stopWorker(ctx, worker, timeout)
	}

	if err := manager.StopAll(context.Background(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if stopAttempts != 1 {
		t.Fatalf("stop attempts=%d want 1", stopAttempts)
	}
	if got := readStopCount(t, counter); got != 1 {
		t.Fatalf("stop count=%d want 1", got)
	}
}

func TestStopAllDoesNotDeduplicateDifferentWorkersWithReusedPID(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	process := &os.Process{Pid: 424242}
	oldWorker := &ManagedWorker{
		SessionID: "sess-old-pid",
		Spec:      WorkerStartSpec{Token: "old-token"},
		Cmd:       &exec.Cmd{Process: process},
	}
	replacement := &ManagedWorker{
		SessionID: "sess-new-pid",
		Spec:      WorkerStartSpec{Token: "new-token"},
		Cmd:       &exec.Cmd{Process: process},
	}
	manager.active[oldWorker.SessionID] = oldWorker
	manager.active[replacement.SessionID] = replacement
	stopped := make(chan *ManagedWorker, 2)
	manager.stopWorker = func(_ context.Context, worker *ManagedWorker, _ time.Duration) error {
		stopped <- worker
		return nil
	}

	if err := manager.StopAll(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got := len(stopped); got != 2 {
		t.Fatalf("stop attempts = %d, want both same-PID generations", got)
	}
	seen := map[*ManagedWorker]bool{}
	seen[<-stopped] = true
	seen[<-stopped] = true
	if !seen[oldWorker] || !seen[replacement] {
		t.Fatalf("stopped workers = %+v, want both same-PID generations", seen)
	}
}

func TestStopAllClosesAdmissionAndDrainsInFlightStart(t *testing.T) {
	requirePOSIXSignals(t)
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_shutdown_admission", "import time\ntime.sleep(30)\n")
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_shutdown_admission",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})

	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	buildEnvironment := manager.buildEnvironment
	manager.buildEnvironment = func(cfg WorkerManagerConfig, spec WorkerStartSpec, extraEnv []string, cleanup workerSessionCleanup) ([]string, string, string, error) {
		close(startEntered)
		<-releaseStart
		return buildEnvironment(cfg, spec, extraEnv, cleanup)
	}

	startDone := make(chan error, 1)
	go func() {
		_, err := manager.StartSessionWorker(context.Background(), "sess-admitted", nil)
		startDone <- err
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("in-flight worker start did not reach the blocking seam")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- manager.StopAll(context.Background(), 2*time.Second)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		stopping := manager.stopping
		manager.mu.Unlock()
		if stopping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("StopAll did not close worker admission")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := manager.StartSessionWorker(context.Background(), "sess-rejected", nil); !errors.Is(err, ErrWorkerManagerStopping) {
		t.Fatalf("worker start error = %v, want ErrWorkerManagerStopping", err)
	}

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("admitted StartSessionWorker: %v", err)
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopAll: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("StopAll did not drain and stop the admitted worker")
	}
	if worker := manager.findWorker("sess-admitted"); worker != nil {
		t.Fatalf("admitted worker remained after StopAll: %+v", worker)
	}
}

func TestStopAllTimeoutPreventsAdmittedStartFromLaunchingLate(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_late_start", "import time\ntime.sleep(30)\n")
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_late_start",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})

	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	buildEnvironment := manager.buildEnvironment
	manager.buildEnvironment = func(cfg WorkerManagerConfig, spec WorkerStartSpec, extraEnv []string, cleanup workerSessionCleanup) ([]string, string, string, error) {
		close(startEntered)
		<-releaseStart
		return buildEnvironment(cfg, spec, extraEnv, cleanup)
	}

	startDone := make(chan error, 1)
	go func() {
		_, err := manager.StartSessionWorker(context.Background(), "sess-late", nil)
		startDone <- err
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("in-flight worker start did not reach blocking seam")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := manager.StopAll(ctx, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopAll error = %v, want context deadline", err)
	}
	close(releaseStart)
	select {
	case err := <-startDone:
		if !errors.Is(err, ErrWorkerManagerStopping) {
			t.Fatalf("late StartSessionWorker error = %v, want ErrWorkerManagerStopping", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late StartSessionWorker did not abort")
	}
	if worker := manager.findWorker("sess-late"); worker != nil {
		t.Fatalf("late worker launched after StopAll returned: %+v", worker)
	}
}

func TestStopAllDeadlineDoesNotWaitBehindCommandStart(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_slow_start", "import time\ntime.sleep(30)\n")
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_slow_start",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	manager.startCommand = func(cmd *exec.Cmd) error {
		close(startEntered)
		<-releaseStart
		return cmd.Start()
	}
	startDone := make(chan error, 1)
	go func() {
		_, err := manager.StartSessionWorker(context.Background(), "sess-slow-start", nil)
		startDone <- err
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter command start seam")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- manager.StopAll(ctx, time.Second) }()
	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopAll error = %v, want context deadline", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseStart)
		t.Fatal("StopAll waited behind command start beyond its shared deadline")
	}
	close(releaseStart)
	select {
	case err := <-startDone:
		if !errors.Is(err, ErrWorkerManagerStopping) {
			t.Fatalf("StartSessionWorker error = %v, want ErrWorkerManagerStopping", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aborted command start did not finish")
	}
	if worker := manager.findWorker("sess-slow-start"); worker != nil {
		t.Fatalf("aborted command start was committed: %+v", worker)
	}
}

func TestShutdownAbortClearsWorkerAdmittedDuringCommandStart(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_admitted_start", "import time\ntime.sleep(30)\n")
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_admitted_start",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	workerAdmitted := make(chan struct{})
	releaseStart := make(chan struct{})
	manager.startCommand = func(cmd *exec.Cmd) error {
		if err := cmd.Start(); err != nil {
			return err
		}
		manager.registry.mu.Lock()
		token := manager.registry.expected["sess-admitted-start"]
		manager.registry.mu.Unlock()
		if err := manager.registry.AdmitWorker(
			"sess-admitted-start",
			token,
			int64(cmd.Process.Pid),
		); err != nil {
			return err
		}
		close(workerAdmitted)
		<-releaseStart
		return nil
	}
	startDone := make(chan error, 1)
	go func() {
		_, err := manager.StartSessionWorker(context.Background(), "sess-admitted-start", nil)
		startDone <- err
	}()
	select {
	case <-workerAdmitted:
	case <-time.After(time.Second):
		t.Fatal("worker did not complete IPC admission in command start seam")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := manager.StopAll(ctx, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopAll error = %v, want context deadline", err)
	}
	close(releaseStart)
	select {
	case err := <-startDone:
		if !errors.Is(err, ErrWorkerManagerStopping) {
			t.Fatalf("StartSessionWorker error = %v, want ErrWorkerManagerStopping", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aborted admitted start did not finish")
	}
	if identity, ok := manager.registry.ActiveWorker("sess-admitted-start"); ok {
		t.Fatalf("aborted admitted worker remained registered: %+v", identity)
	}
}

func TestStopAllUsesOneSharedDeadlineForBlockedCleanup(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	cleanupDone := make([]chan error, 0, 3)
	for i := 0; i < 3; i++ {
		cmd := exec.Command("python3", "-c", "pass")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start short process %d: %v", i, err)
		}
		if err := cmd.Wait(); err != nil {
			t.Fatalf("wait short process %d: %v", i, err)
		}
		processExited := make(chan struct{})
		close(processExited)
		done := make(chan error)
		cleanupDone = append(cleanupDone, done)
		sessionID := fmt.Sprintf("sess-shared-deadline-%d", i)
		manager.active[sessionID] = &ManagedWorker{
			SessionID:     sessionID,
			Spec:          WorkerStartSpec{Token: fmt.Sprintf("token-%d", i)},
			Cmd:           cmd,
			processExited: processExited,
			done:          done,
		}
	}
	defer func() {
		for _, done := range cleanupDone {
			close(done)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := manager.StopAll(ctx, 200*time.Millisecond)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopAll error = %v, want shared context deadline", err)
	}
	if elapsed > 350*time.Millisecond {
		t.Fatalf("StopAll elapsed = %v, want one shared deadline rather than per-worker waits", elapsed)
	}
}

func TestStopSessionWorkerBoundsPostExitCleanup(t *testing.T) {
	requirePOSIXSignals(t)
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_blocked_cleanup", "import time\ntime.sleep(30)\n")
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_blocked_cleanup",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})

	cleanupBlocked := make(chan struct{})
	releaseCleanup := make(chan struct{})
	manager.cleanupSessionRoot = func(path string) error {
		select {
		case <-cleanupBlocked:
		default:
			close(cleanupBlocked)
		}
		<-releaseCleanup
		return os.RemoveAll(path)
	}

	worker, err := manager.StartSessionWorker(context.Background(), "sess-bounded-cleanup", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	stopDone := make(chan error, 1)
	started := time.Now()
	go func() {
		stopDone <- manager.StopSessionWorker(context.Background(), worker.SessionID, 100*time.Millisecond)
	}()

	select {
	case <-cleanupBlocked:
	case <-time.After(time.Second):
		close(releaseCleanup)
		t.Fatal("worker did not reach blocked post-exit cleanup")
	}
	select {
	case err := <-stopDone:
		if !errors.Is(err, ErrWorkerCleanupPending) {
			t.Fatalf("StopSessionWorker error = %v, want ErrWorkerCleanupPending", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("StopSessionWorker returned after %v, want bounded cleanup wait", elapsed)
		}
		assertWorkerSessionReserved(t, manager, worker.SessionID)
		close(releaseCleanup)
	case <-time.After(time.Second):
		close(releaseCleanup)
		t.Fatal("StopSessionWorker blocked beyond its cleanup bound")
	}
}

func TestForgetManagedWorkerPreservesDifferentGenerationWithReusedPID(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	process := &os.Process{Pid: 424242}
	oldWorker := &ManagedWorker{
		SessionID: "sess-old",
		Spec:      WorkerStartSpec{Token: "old-token"},
		Cmd:       &exec.Cmd{Process: process},
	}
	replacement := &ManagedWorker{
		SessionID: "sess-new",
		Spec:      WorkerStartSpec{Token: "new-token"},
		Cmd:       &exec.Cmd{Process: process},
	}
	manager.active[oldWorker.SessionID] = oldWorker
	manager.active[replacement.SessionID] = replacement

	manager.forgetWorker(oldWorker)

	if got := manager.active[replacement.SessionID]; got != replacement {
		t.Fatalf("replacement worker = %+v, want same-PID new generation preserved", got)
	}
}

func TestFindWorkerRejectsSamePIDWithDifferentToken(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	process := &os.Process{Pid: 424242}
	oldWorker := &ManagedWorker{
		SessionID: "sess-old",
		Spec:      WorkerStartSpec{Token: "old-token"},
		Cmd:       &exec.Cmd{Process: process},
	}
	manager.active[oldWorker.SessionID] = oldWorker
	if err := manager.registry.ExpectWorker("sess-new", "new-token"); err != nil {
		t.Fatal(err)
	}
	if err := manager.registry.AdmitWorker("sess-new", "new-token", 424242); err != nil {
		t.Fatal(err)
	}

	if got := manager.findWorker("sess-new"); got != nil {
		t.Fatalf("findWorker returned old generation: %+v", got)
	}
}

func TestStopAllStopsSnapshottedWorkerAfterSessionReplacement(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	original := startUnmanagedWorker(t, "replacement-race")
	replacement := startUnmanagedWorker(t, "replacement-race")
	if err := manager.registry.ExpectWorker(original.SessionID, "original-token"); err != nil {
		t.Fatal(err)
	}
	if err := manager.registry.AdmitWorker(original.SessionID, "original-token", int64(original.Cmd.Process.Pid)); err != nil {
		t.Fatal(err)
	}
	manager.active[original.SessionID] = original

	stopWorker := manager.stopWorker
	manager.stopWorker = func(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
		manager.registry.ForgetWorker(original.SessionID)
		if err := manager.registry.ExpectWorker(replacement.SessionID, "replacement-token"); err != nil {
			t.Fatal(err)
		}
		if err := manager.registry.AdmitWorker(replacement.SessionID, "replacement-token", int64(replacement.Cmd.Process.Pid)); err != nil {
			t.Fatal(err)
		}
		manager.mu.Lock()
		manager.active[original.SessionID] = replacement
		manager.mu.Unlock()
		return stopWorker(ctx, worker, timeout)
	}

	if err := manager.StopAll(context.Background(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if original.Cmd.ProcessState == nil {
		t.Fatal("snapshotted worker was not reaped")
	}
	if replacement.Cmd.ProcessState != nil {
		t.Fatal("replacement worker was stopped")
	}
	if got := manager.findWorker(original.SessionID); got != replacement {
		t.Fatalf("active worker = %+v, want replacement", got)
	}
	identity, ok := manager.registry.ActiveWorker(replacement.SessionID)
	if !ok {
		t.Fatal("replacement worker registry state was removed")
	}
	if identity.PID != int64(replacement.Cmd.Process.Pid) {
		t.Fatalf("replacement registry pid=%d want %d", identity.PID, replacement.Cmd.Process.Pid)
	}
}

func TestStopAllPreservesExpectedReplacementRegistration(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
	original := startUnmanagedWorker(t, "expected-replacement-race")
	original.Spec.Token = "original-token"
	replacement := startUnmanagedWorker(t, "expected-replacement-race")
	replacement.Spec.Token = "replacement-token"
	if err := manager.registry.ExpectWorker(original.SessionID, original.Spec.Token); err != nil {
		t.Fatal(err)
	}
	if err := manager.registry.AdmitWorker(original.SessionID, original.Spec.Token, int64(original.Cmd.Process.Pid)); err != nil {
		t.Fatal(err)
	}
	manager.active[original.SessionID] = original

	stopWorker := manager.stopWorker
	manager.stopWorker = func(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
		manager.registry.ForgetWorker(original.SessionID)
		if err := manager.registry.ExpectWorker(replacement.SessionID, replacement.Spec.Token); err != nil {
			t.Fatal(err)
		}
		manager.mu.Lock()
		manager.active[original.SessionID] = replacement
		manager.mu.Unlock()
		return stopWorker(ctx, worker, timeout)
	}

	if err := manager.StopAll(context.Background(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := manager.registry.AdmitWorker(replacement.SessionID, replacement.Spec.Token, int64(replacement.Cmd.Process.Pid)); err != nil {
		t.Fatalf("replacement expectation was removed: %v", err)
	}
	identity, ok := manager.registry.ActiveWorker(replacement.SessionID)
	if !ok || identity.PID != int64(replacement.Cmd.Process.Pid) {
		t.Fatalf("replacement identity=(%+v,%v), want pid %d", identity, ok, replacement.Cmd.Process.Pid)
	}
}

func TestWorkerManagerStopWaitsForManagedCleanupBeforeReleasingSession(t *testing.T) {
	cmd := exec.Command("python3", "-c", "pass")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short process: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait short process: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{})
	if err := m.registry.ExpectWorker("sess-cleanup", "token"); err != nil {
		t.Fatalf("ExpectWorker: %v", err)
	}
	managedDone := make(chan error)
	m.active["sess-cleanup"] = &ManagedWorker{
		SessionID: "sess-cleanup",
		Spec:      WorkerStartSpec{Token: "token"},
		Cmd:       cmd,
		done:      managedDone,
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.StopSessionWorker(context.Background(), "sess-cleanup", time.Second)
	}()

	select {
	case err := <-stopDone:
		t.Fatalf("StopSessionWorker returned before managed cleanup completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := m.registry.ExpectWorker("sess-cleanup", "other-token"); !errors.Is(err, ErrWorkerAlreadyExists) {
		t.Fatalf("registry released session before managed cleanup: %v", err)
	}

	managedDone <- nil
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopSessionWorker: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopSessionWorker did not return after managed cleanup")
	}
	if err := m.registry.ExpectWorker("sess-cleanup", "replacement-token"); err != nil {
		t.Fatalf("registry retained session after managed cleanup: %v", err)
	}
}

func TestWorkerManagerStopRetainsSessionWhenKillFails(t *testing.T) {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{})
	if err := m.registry.ExpectWorker("sess-kill-error", "token"); err != nil {
		t.Fatalf("ExpectWorker: %v", err)
	}
	m.active["sess-kill-error"] = &ManagedWorker{
		SessionID: "sess-kill-error",
		Cmd:       &exec.Cmd{Process: process},
	}

	err = m.StopSessionWorker(context.Background(), "sess-kill-error", time.Second)
	if err == nil {
		t.Fatal("StopSessionWorker accepted a failed kill")
	}
	if err := m.registry.ExpectWorker("sess-kill-error", "other-token"); !errors.Is(err, ErrWorkerAlreadyExists) {
		t.Fatalf("registry released session after failed kill: %v", err)
	}
}

func TestWorkerManagerStartFailureRetainsSessionWhenCleanupFails(t *testing.T) {
	root := t.TempDir()
	workDir := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(workDir, []byte("file"), 0o600); err != nil {
		t.Fatalf("write workdir blocker: %v", err)
	}
	stateRoot := filepath.Join(root, "state")
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: mustCurrentExecutable(t),
		WorkerModule:     "worker_stub",
		WorkDir:          workDir,
		StateRoot:        stateRoot,
	})
	cleanupFailure := errors.New("start cleanup blocked")
	cleanupCalls := 0
	m.cleanupSessionRoot = func(path string) error {
		cleanupCalls++
		if want := workerSessionRoot(stateRoot, "sess-start-cleanup"); path != want {
			t.Fatalf("cleanup path = %q, want %q", path, want)
		}
		return cleanupFailure
	}

	_, err := m.StartSessionWorker(context.Background(), "sess-start-cleanup", nil)
	if !errors.Is(err, cleanupFailure) || !strings.Contains(err.Error(), "start session worker") {
		t.Fatalf("StartSessionWorker error = %v, want start and cleanup failures", err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleanupCalls)
	}
	if _, err := os.Stat(workerSessionRoot(stateRoot, "sess-start-cleanup")); err != nil {
		t.Fatalf("session root was not retained: %v", err)
	}
	assertWorkerSessionReserved(t, m, "sess-start-cleanup")
	if err := m.StopSessionWorker(context.Background(), "sess-start-cleanup", time.Second); !errors.Is(err, cleanupFailure) {
		t.Fatalf("StopSessionWorker tombstone error = %v, want cleanup failure", err)
	}
	assertWorkerSessionReserved(t, m, "sess-start-cleanup")
}

func TestWorkerManagerNaturalExitRetainsSessionWhenCleanupFails(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit", "pass\n")
	stateRoot := filepath.Join(dir, "state")
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_exit",
		WorkDir:          dir,
		StateRoot:        stateRoot,
		PythonPath:       []string{dir},
	})
	cleanupFailure := errors.New("natural cleanup blocked")
	m.cleanupSessionRoot = func(string) error { return cleanupFailure }

	worker, err := m.StartSessionWorker(context.Background(), "sess-natural-cleanup", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	if err := worker.Wait(); !errors.Is(err, cleanupFailure) {
		t.Fatalf("worker.Wait error = %v, want cleanup failure", err)
	}
	if got := m.findWorker("sess-natural-cleanup"); got != worker {
		t.Fatalf("worker ownership released after cleanup failure: %+v", got)
	}
	assertWorkerSessionReserved(t, m, "sess-natural-cleanup")
}

func TestWorkerManagerStopReturnsCleanupFailureAndRetainsSession(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_stop_cleanup", "import time\ntime.sleep(10)\n")
	stateRoot := filepath.Join(dir, "state")
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_stop_cleanup",
		WorkDir:          dir,
		StateRoot:        stateRoot,
		PythonPath:       []string{dir},
	})
	cleanupFailure := errors.New("stop cleanup blocked")
	m.cleanupSessionRoot = func(string) error { return cleanupFailure }

	worker, err := m.StartSessionWorker(context.Background(), "sess-stop-cleanup", nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	if err := m.StopSessionWorker(context.Background(), "sess-stop-cleanup", time.Second); !errors.Is(err, cleanupFailure) {
		t.Fatalf("StopSessionWorker error = %v, want cleanup failure", err)
	}
	if err := worker.Wait(); !errors.Is(err, cleanupFailure) {
		t.Fatalf("worker.Wait error = %v, want cleanup failure", err)
	}
	if got := m.findWorker("sess-stop-cleanup"); got != worker {
		t.Fatalf("worker ownership released after cleanup failure: %+v", got)
	}
	assertWorkerSessionReserved(t, m, "sess-stop-cleanup")
}

func assertWorkerSessionReserved(t *testing.T, m *WorkerManager, sessionID string) {
	t.Helper()
	if _, err := m.PrepareSessionWorker(sessionID); !errors.Is(err, ErrWorkerAlreadyExists) {
		t.Errorf("PrepareSessionWorker(%q) error = %v, want ErrWorkerAlreadyExists", sessionID, err)
	}
	if _, err := m.StartSessionWorker(context.Background(), sessionID, nil); !errors.Is(err, ErrWorkerAlreadyExists) {
		t.Errorf("StartSessionWorker(%q) error = %v, want ErrWorkerAlreadyExists", sessionID, err)
	}
}

func writePythonWorkerModule(t *testing.T, dir string, name string, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+".py"), []byte(source), 0o600); err != nil {
		t.Fatalf("write worker module: %v", err)
	}
}

func requirePOSIXSignals(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX signal semantics")
	}
}

func startSignalAwareWorker(t *testing.T) (*WorkerManager, *ManagedWorker, string) {
	t.Helper()
	dir := t.TempDir()
	marker := filepath.Join(dir, "sigterm-marker")
	ready := filepath.Join(dir, "ready")
	writePythonWorkerModule(t, dir, "worker_signal_aware", fmt.Sprintf(`
import signal
import sys
import time
from pathlib import Path

marker = Path(%q)
ready = Path(%q)

def stop(_signum, _frame):
    marker.write_text("SIGTERM", encoding="utf-8")
    sys.exit(0)

signal.signal(signal.SIGTERM, stop)
ready.write_text("ready", encoding="utf-8")
while True:
    time.sleep(0.05)
`, marker, ready))
	manager, worker := startWorkerModule(t, dir, "worker_signal_aware", "signal-aware")
	waitForWorkerFile(t, ready)
	return manager, worker, marker
}

func startSignalIgnoringWorker(t *testing.T) (*WorkerManager, *ManagedWorker) {
	t.Helper()
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	writePythonWorkerModule(t, dir, "worker_signal_ignoring", fmt.Sprintf(`
import signal
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(%q).write_text("ready", encoding="utf-8")
while True:
    time.sleep(0.05)
`, ready))
	manager, worker := startWorkerModule(t, dir, "worker_signal_ignoring", "signal-ignoring")
	waitForWorkerFile(t, ready)
	return manager, worker
}

func startCountingWorker(t *testing.T) (*WorkerManager, *ManagedWorker, string) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "stop-count")
	ready := filepath.Join(dir, "ready")
	writePythonWorkerModule(t, dir, "worker_signal_counting", fmt.Sprintf(`
import signal
import sys
import time
from pathlib import Path

counter = Path(%q)
ready = Path(%q)

def stop(_signum, _frame):
    count = int(counter.read_text(encoding="utf-8")) if counter.exists() else 0
    counter.write_text(str(count + 1), encoding="utf-8")
    sys.exit(0)

signal.signal(signal.SIGTERM, stop)
ready.write_text("ready", encoding="utf-8")
while True:
    time.sleep(0.05)
`, counter, ready))
	manager, worker := startWorkerModule(t, dir, "worker_signal_counting", "signal-counting")
	waitForWorkerFile(t, ready)
	return manager, worker, counter
}

func startReleaseDrivenSignalCountingWorker(t *testing.T, sessionID string) (*WorkerManager, *ManagedWorker, string, string) {
	t.Helper()
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	release := filepath.Join(dir, "release")
	signals := filepath.Join(dir, "signals")
	writePythonWorkerModule(t, dir, "worker_release_driven_signal_counting", fmt.Sprintf(`
import signal
import time
from pathlib import Path

ready = Path(%q)
release = Path(%q)
signals = Path(%q)

def stop(_signum, _frame):
    with signals.open("a", encoding="utf-8") as output:
        output.write("SIGTERM\n")

signal.signal(signal.SIGTERM, stop)
ready.write_text("ready", encoding="utf-8")
while not release.exists():
    time.sleep(0.01)
`, ready, release, signals))
	manager, worker := startWorkerModule(t, dir, "worker_release_driven_signal_counting", sessionID)
	waitForWorkerFile(t, ready)
	return manager, worker, signals, release
}

func startWorkerModule(t *testing.T, dir string, module string, sessionID string) (*WorkerManager, *ManagedWorker) {
	t.Helper()
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     module,
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	worker, err := manager.StartSessionWorker(context.Background(), sessionID, nil)
	if err != nil {
		t.Fatalf("StartSessionWorker: %v", err)
	}
	t.Cleanup(func() {
		if worker.Cmd != nil && worker.Cmd.Process != nil {
			_ = worker.Cmd.Process.Kill()
		}
		_ = worker.Wait()
	})
	return manager, worker
}

func startUnmanagedWorker(t *testing.T, sessionID string) *ManagedWorker {
	t.Helper()
	cmd := exec.Command("python3", "-c", "import time; time.sleep(60)")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start unmanaged worker: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	return &ManagedWorker{SessionID: sessionID, Cmd: cmd}
}

func waitForWorkerFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat worker readiness file: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker did not create readiness file %q", path)
}

func readStopCount(t *testing.T, path string) int {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stop count: %v", err)
	}
	var count int
	if _, err := fmt.Sscanf(string(body), "%d", &count); err != nil {
		t.Fatalf("parse stop count %q: %v", string(body), err)
	}
	return count
}

func readSignalCount(t *testing.T, path string) int {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read worker signals: %v", err)
	}
	return strings.Count(string(body), "SIGTERM\n")
}

func TestWorkerManagerAliasWorkerSessionMakesRealSessionStoppable(t *testing.T) {
	cmd := exec.Command("python3", "-c", "import time; time.sleep(60)")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start long process: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	m := NewWorkerManager(WorkerManagerConfig{})
	spec, err := m.PrepareSessionWorker("pending-1")
	if err != nil {
		t.Fatalf("PrepareSessionWorker: %v", err)
	}
	if err := m.Registry().AdmitWorker("pending-1", spec.Token, int64(cmd.Process.Pid)); err != nil {
		t.Fatalf("AdmitWorker: %v", err)
	}
	worker := &ManagedWorker{SessionID: "pending-1", Spec: spec, Cmd: cmd}
	m.active["pending-1"] = worker

	if err := m.AliasWorkerSession("pending-1", "sess-real"); err != nil {
		t.Fatalf("AliasWorkerSession: %v", err)
	}
	if got := m.findWorker("sess-real"); got != worker {
		t.Fatalf("findWorker(real) = %+v, want original worker", got)
	}
	if err := m.StopSessionWorker(context.Background(), "sess-real", time.Second); err != nil {
		t.Fatalf("StopSessionWorker(real): %v", err)
	}
	if got := m.findWorker("sess-real"); got != nil {
		t.Fatalf("real session worker still active: %+v", got)
	}
}
