package runtimeagent

import (
	"context"
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
	if err := os.WriteFile(module, []byte(`
import os
from pathlib import Path
Path(os.environ["HUSHINE_TEST_WORKER_ENV_FILE"]).write_text(
    "\n".join([
        os.environ.get("HUSHINE_AGENT_ADDR", ""),
        os.environ.get("HUSHINE_SESSION_ID", ""),
        os.environ.get("HUSHINE_WORKER_TOKEN", ""),
    ]),
    encoding="utf-8",
)
`), 0o600); err != nil {
		t.Fatalf("write worker module: %v", err)
	}

	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_stub",
		AgentAddr:        "127.0.0.1:59000",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker, err := m.StartSessionWorker(ctx, "sess-worker", []string{
		"PYTHONPATH=" + dir,
		"HUSHINE_TEST_WORKER_ENV_FILE=" + out,
	})
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
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 3 {
		t.Fatalf("worker env lines = %q", string(body))
	}
	if lines[0] != "127.0.0.1:59000" || lines[1] != "sess-worker" || lines[2] == "" {
		t.Fatalf("worker env = %q", string(body))
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
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker, err := m.StartSessionWorker(ctx, "sess-survive", []string{"PYTHONPATH=" + dir})
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
