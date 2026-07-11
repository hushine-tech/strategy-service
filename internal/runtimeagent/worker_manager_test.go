package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
