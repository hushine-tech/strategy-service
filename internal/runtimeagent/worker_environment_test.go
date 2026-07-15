package runtimeagent

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestResolveWorkerLaunchSpecBuildsOneSanitizedImmutableInvocation(t *testing.T) {
	venvPython := makeFakeWorkerVenvPython(t)
	workDir := t.TempDir()
	coverageRoot := filepath.Join(t.TempDir(), "coverage")
	poison := "/tmp/poison-secret"
	processEnv := []string{
		"PATH=" + poison,
		"PYTHONPATH=" + poison,
		"PYTHONHOME=" + poison,
		"VIRTUAL_ENV=" + poison,
		"UV_PROJECT_ENVIRONMENT=" + poison,
		"DATABASE_URL=postgres://secret",
		"KAFKA_BROKERS=secret:9092",
		"CORE_SERVICE_GRPC_ADDR=secret:50051",
		"ORDER_SERVICE_GRPC_ADDR=secret:50052",
		"RUNTIME_CREDENTIAL_JSON=secret-token",
		"HUSHINE_WORKER_PYTHON_ARGS=-Xfrozen_modules=off",
		"HUSHINE_RUNTIME_PROFILE_NAME=platform-python-3.13",
		"HUSHINE_RUNTIME_PROFILE_VERSION=1.0.0",
		"HUSHINE_RUNTIME_CONTRACT_SHA256=" + strings.Repeat("a", 64),
		"HUSHINE_RUNTIME_HOSTED_PYTHON=3.13",
		"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=dateutil,grpc,numpy,pandas,pydantic,requests,yaml",
		"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=local-dev",
		"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=local-dev",
		"HUSHINE_RUNTIME_IMAGE_BUILD_ID=local-dev",
	}

	spec, err := ResolveWorkerLaunchSpec(WorkerManagerConfig{
		PythonExecutable: venvPython,
		PythonArgsPrefix: (CoverageConfig{RootDir: coverageRoot}).PythonArgsPrefix(),
		WorkerModule:     "strategy_service.session_worker_entry",
		AgentAddr:        "127.0.0.1:50000",
		WorkDir:          workDir,
		StateRoot:        filepath.Join(workDir, "state"),
	}, "bare", processEnv)
	if err != nil {
		t.Fatalf("ResolveWorkerLaunchSpec: %v", err)
	}
	wantPrefix := []string{
		"-I", "-Xfrozen_modules=off", "-m", "coverage", "run",
		"--parallel-mode", "--data-file=" + filepath.Join(coverageRoot, "python", ".coverage"),
		"--source=strategy_service",
	}
	if !slices.Equal(spec.Invocation.ArgsPrefix, wantPrefix) {
		t.Fatalf("args prefix = %v, want %v", spec.Invocation.ArgsPrefix, wantPrefix)
	}
	if spec.Invocation.Executable != venvPython {
		t.Fatalf("executable = %q, want preserved venv path %q", spec.Invocation.Executable, venvPython)
	}
	expectedWorkDir, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Invocation.WorkDir != filepath.Clean(expectedWorkDir) {
		t.Fatalf("workdir = %q", spec.Invocation.WorkDir)
	}
	childEnv := envMap(spec.Invocation.Env)
	for _, forbidden := range []string{
		"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT",
		"DATABASE_URL", "KAFKA_BROKERS", "CORE_SERVICE_GRPC_ADDR",
		"ORDER_SERVICE_GRPC_ADDR", "RUNTIME_CREDENTIAL_JSON", "HUSHINE_WORKER_PYTHON_ARGS",
	} {
		if _, ok := childEnv[forbidden]; ok {
			t.Fatalf("forbidden parent env reached child: %s", forbidden)
		}
	}
	if strings.Contains(strings.Join(spec.Invocation.Env, "\n"), poison) {
		t.Fatalf("poisoned parent value reached child: %v", spec.Invocation.Env)
	}
	for _, required := range []string{
		"HUSHINE_RUNTIME_PROFILE_NAME", "HUSHINE_RUNTIME_PROFILE_VERSION",
		"HUSHINE_RUNTIME_CONTRACT_SHA256", "HUSHINE_RUNTIME_HOSTED_PYTHON",
		"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS",
	} {
		if childEnv[required] == "" {
			t.Fatalf("missing embedded profile env %s", required)
		}
	}
	for _, absentLocalDevFact := range []string{
		"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
		"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
		"HUSHINE_RUNTIME_IMAGE_BUILD_ID",
	} {
		if _, ok := childEnv[absentLocalDevFact]; ok {
			t.Fatalf("literal local-dev build fact must be represented by all-missing child facts: %s", absentLocalDevFact)
		}
	}
}

func TestResolveWorkerLaunchSpecRejectsEveryUnapprovedPythonPrefix(t *testing.T) {
	venvPython := makeFakeWorkerVenvPython(t)
	for _, value := range []string{
		"-I", "-c", "-m", "--", "worker.py", "@args", "-X dev",
		" -Xfrozen_modules=off", "-Xfrozen_modules=off ",
		"-Xfrozen_modules=off -I", "-Xfrozen_modules=on",
	} {
		t.Run(strings.ReplaceAll(value, "/", "_"), func(t *testing.T) {
			_, err := ResolveWorkerLaunchSpec(WorkerManagerConfig{
				PythonExecutable: venvPython,
				WorkDir:          t.TempDir(),
			}, "bare", []string{"HUSHINE_WORKER_PYTHON_ARGS=" + value})
			if err == nil {
				t.Fatalf("prefix %q unexpectedly accepted", value)
			}
		})
	}
}

func TestResolveWorkerLaunchSpecRejectsLauncherAndNonVenvPython(t *testing.T) {
	for _, executable := range []string{"uv", "python3", mustCurrentExecutable(t)} {
		_, err := ResolveWorkerLaunchSpec(WorkerManagerConfig{
			PythonExecutable: executable,
			WorkDir:          t.TempDir(),
		}, "hosted", nil)
		if err == nil {
			t.Fatalf("executable %q unexpectedly accepted", executable)
		}
	}
}

func TestResolveWorkerExecutablePreservesSymlinkInvocationPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX symlink semantics")
	}

	binDir := filepath.Join(t.TempDir(), ".venv", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir venv bin: %v", err)
	}
	pythonPath := filepath.Join(binDir, "python")
	if err := os.Symlink(mustCurrentExecutable(t), pythonPath); err != nil {
		t.Fatalf("symlink venv python: %v", err)
	}

	got, err := resolveWorkerExecutable(pythonPath)
	if err != nil {
		t.Fatalf("resolveWorkerExecutable: %v", err)
	}
	want, err := filepath.Abs(pythonPath)
	if err != nil {
		t.Fatalf("absolute venv python: %v", err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("resolved executable = %q, want invocation path %q", got, filepath.Clean(want))
	}
}

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

func makeFakeWorkerVenvPython(t *testing.T) string {
	t.Helper()
	venvRoot := filepath.Join(t.TempDir(), ".venv")
	if err := os.WriteFile(filepath.Join(venvRoot, "pyvenv.cfg"), []byte("home = test\n"), 0o600); err != nil {
		if err := os.MkdirAll(venvRoot, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(venvRoot, "pyvenv.cfg"), []byte("home = test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	binName := "bin"
	pythonName := "python"
	if runtime.GOOS == "windows" {
		binName = "Scripts"
		pythonName = "python.exe"
	}
	binDir := filepath.Join(venvRoot, binName)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pythonPath := filepath.Join(binDir, pythonName)
	if runtime.GOOS == "windows" {
		body, err := os.ReadFile(mustCurrentExecutable(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(pythonPath, body, 0o755); err != nil {
			t.Fatal(err)
		}
	} else if err := os.Symlink(mustCurrentExecutable(t), pythonPath); err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(pythonPath)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(abs)
}
