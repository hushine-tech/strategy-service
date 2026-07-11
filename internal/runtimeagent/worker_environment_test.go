package runtimeagent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildWorkerEnvironmentDoesNotInheritParentSecrets(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "secret-kafka:9092")
	t.Setenv("DATABASE_PASSWORD", "secret-db")
	t.Setenv("CORE_SERVICE_GRPC_ADDR", "secret-core:50051")
	t.Setenv("RUNTIME_CHANNEL_TLS_BUNDLE_JSON", "secret-tls")
	t.Setenv("QUANT_HANDLER_JWT_SECRET", "secret-jwt")

	root := t.TempDir()
	env, sessionRoot, resolvedExecutable, err := buildWorkerEnvironment(WorkerManagerConfig{
		PythonExecutable: mustCurrentExecutable(t),
		WorkDir:          root,
		StateRoot:        filepath.Join(root, "state"),
		PythonPath:       []string{filepath.Join(root, "lib")},
	}, WorkerStartSpec{
		SessionID: "sess-1", Token: "worker-token", AgentAddr: "127.0.0.1:59000",
	}, []string{
		"HUSHINE_RUNTIME_ID=rt-1",
		"HUSHINE_RUNTIME_SOURCE=bare",
		"HUSHINE_RUNTIME_NAME=debug",
	})
	if err != nil {
		t.Fatalf("buildWorkerEnvironment: %v", err)
	}
	if !filepath.IsAbs(resolvedExecutable) {
		t.Fatalf("resolved executable = %q, want absolute path", resolvedExecutable)
	}
	got := envMap(env)
	for _, key := range []string{
		"KAFKA_BROKERS", "DATABASE_PASSWORD", "CORE_SERVICE_GRPC_ADDR",
		"RUNTIME_CHANNEL_TLS_BUNDLE_JSON", "QUANT_HANDLER_JWT_SECRET",
	} {
		if _, ok := got[key]; ok {
			t.Fatalf("parent secret leaked: %s", key)
		}
	}
	if got["HUSHINE_SESSION_ID"] != "sess-1" || got["HUSHINE_RUNTIME_ID"] != "rt-1" {
		t.Fatalf("required worker facts = %+v", got)
	}
	if got["HUSHINE_AGENT_ADDR"] != "127.0.0.1:59000" || got["HUSHINE_WORKER_TOKEN"] != "worker-token" {
		t.Fatalf("typed worker protocol facts = %+v", got)
	}
	if got["HOME"] == os.Getenv("HOME") || !strings.HasPrefix(got["HOME"], sessionRoot) {
		t.Fatalf("HOME = %q, sessionRoot = %q", got["HOME"], sessionRoot)
	}
	if !strings.HasPrefix(got["TMPDIR"], sessionRoot) {
		t.Fatalf("TMPDIR = %q, sessionRoot = %q", got["TMPDIR"], sessionRoot)
	}
}

func TestBuildWorkerEnvironmentGeneratesTypedDebugpyWait(t *testing.T) {
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
			root := t.TempDir()
			env, _, _, err := buildWorkerEnvironment(WorkerManagerConfig{
				PythonExecutable: mustCurrentExecutable(t),
				WorkDir:          root,
				StateRoot:        filepath.Join(root, "state"),
			}, WorkerStartSpec{
				SessionID: "sess", Token: "token", AgentAddr: "127.0.0.1:1", DebugpyWait: tc.wait,
			}, nil)
			if err != nil {
				t.Fatalf("buildWorkerEnvironment: %v", err)
			}
			if got := envMap(env)["DEBUG_WAIT"]; got != tc.want {
				t.Fatalf("DEBUG_WAIT = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildWorkerEnvironmentRejectsUnmodeledExtraEnv(t *testing.T) {
	cases := []string{
		"KAFKA_BROKERS=evil:9092",
		"DATABASE_PASSWORD=secret",
		"PYTHONPATH=/tmp/evil",
		"HUSHINE_WORKER_TOKEN=override",
		"DEBUG_WAIT=true",
		"MY_CUSTOM_VAR=value",
	}
	for _, item := range cases {
		_, _, _, err := buildWorkerEnvironment(WorkerManagerConfig{
			PythonExecutable: mustCurrentExecutable(t),
			WorkDir:          t.TempDir(), StateRoot: t.TempDir(),
		}, WorkerStartSpec{SessionID: "sess", Token: "token", AgentAddr: "127.0.0.1:1"}, []string{item})
		if err == nil || !strings.Contains(err.Error(), "worker extra env key is not allowed") {
			t.Fatalf("extra env %q error = %v", item, err)
		}
	}
}

func TestBuildWorkerEnvironmentPreservesSessionRootWhenPartialCreationCleanupFails(t *testing.T) {
	stateRoot := t.TempDir()
	sessionRoot := workerSessionRoot(stateRoot, "sess-partial")
	if err := os.MkdirAll(sessionRoot, 0o700); err != nil {
		t.Fatalf("create session root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionRoot, "home"), []byte("not-a-directory"), 0o600); err != nil {
		t.Fatalf("write home blocker: %v", err)
	}
	cleanupFailure := errors.New("cleanup blocked")
	cleanupCalls := 0

	_, gotSessionRoot, _, err := buildWorkerEnvironmentWithCleanup(WorkerManagerConfig{
		PythonExecutable: mustCurrentExecutable(t),
		WorkDir:          stateRoot,
		StateRoot:        stateRoot,
	}, WorkerStartSpec{
		SessionID: "sess-partial", Token: "token", AgentAddr: "127.0.0.1:1",
	}, nil, func(path string) error {
		cleanupCalls++
		if path != sessionRoot {
			t.Fatalf("cleanup path = %q, want %q", path, sessionRoot)
		}
		return cleanupFailure
	})
	if !errors.Is(err, cleanupFailure) || !strings.Contains(err.Error(), "create worker session directory") {
		t.Fatalf("build error = %v, want creation and cleanup failures", err)
	}
	if gotSessionRoot != sessionRoot {
		t.Fatalf("sessionRoot = %q, want %q", gotSessionRoot, sessionRoot)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleanupCalls)
	}
}

func envMap(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

func mustCurrentExecutable(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return executable
}
