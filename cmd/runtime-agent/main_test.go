package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
)

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
