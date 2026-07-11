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
	if worker.Cmd.ProcessState == nil {
		t.Fatal("worker process was not reaped after canceled stop")
	}
	if got := manager.findWorker(worker.SessionID); got != nil {
		t.Fatalf("worker remained active after canceled stop: %+v", got)
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
