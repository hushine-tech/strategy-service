package runtimeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

type recordingDependencyProbeRunner struct {
	invocation WorkerPythonInvocation
	args       []string
	result     runtimeProbeResult
}

func (r *recordingDependencyProbeRunner) Run(
	_ context.Context,
	invocation WorkerPythonInvocation,
	args []string,
) runtimeProbeResult {
	r.invocation = cloneWorkerPythonInvocation(invocation)
	r.args = append([]string(nil), args...)
	return r.result
}

func TestVerifyRuntimeDependencyProfileUsesExactWorkerInvocation(t *testing.T) {
	invocation := WorkerPythonInvocation{
		Executable: "/app/strategy-service/.venv/bin/python",
		ArgsPrefix: []string{"-I", "-Xfrozen_modules=off"},
		WorkDir:    "/app/strategy-service",
		Env: []string{
			"GRPC_ENABLE_FORK_SUPPORT=0",
			"PATH=/app/strategy-service/.venv/bin:/usr/bin:/bin",
			"PYTHONDONTWRITEBYTECODE=1",
			"PYTHONUNBUFFERED=1",
		},
	}
	expected := validEmbeddedRuntimeFacts("hosted")
	runner := &recordingDependencyProbeRunner{result: runtimeProbeResult{
		Stdout:   validRuntimeProbeJSON(t, invocation, expected.Profile, "hosted", false),
		ExitCode: 0,
	}}

	got, err := VerifyRuntimeDependencyProfile(
		context.Background(), invocation, expected, runner,
	)
	if err != nil {
		t.Fatalf("VerifyRuntimeDependencyProfile: %v", err)
	}
	wantArgs := []string{
		"-m", "strategy_service.runtime_startup_probe", "verify",
		"--source", "hosted",
		"--expected-invocation-sha256", sha256Text(invocation.Executable),
		"--expected-workdir-sha256", sha256Text(invocation.WorkDir),
		"--json",
	}
	if !slices.Equal(runner.args, wantArgs) {
		t.Fatalf("probe args = %v, want %v", runner.args, wantArgs)
	}
	if runner.invocation.Executable != invocation.Executable ||
		!slices.Equal(runner.invocation.ArgsPrefix, invocation.ArgsPrefix) ||
		runner.invocation.WorkDir != invocation.WorkDir ||
		!slices.Equal(runner.invocation.Env, invocation.Env) {
		t.Fatalf("runner invocation = %+v, want %+v", runner.invocation, invocation)
	}
	if got.GetContractSha256() != expected.Profile.GetContractSha256() ||
		got.GetImageBuildId() != expected.Profile.GetImageBuildId() {
		t.Fatalf("verified profile = %+v", got)
	}

	got.PublicImportRoots[0] = "mutated"
	if expected.Profile.GetPublicImportRoots()[0] == "mutated" {
		t.Fatal("verified profile aliases expected profile")
	}
}

func TestVerifyRuntimeDependencyProfileRejectsUnsafeOrMismatchedResults(t *testing.T) {
	invocation := WorkerPythonInvocation{
		Executable: "/app/strategy-service/.venv/bin/python",
		ArgsPrefix: []string{"-I"},
		WorkDir:    "/app/strategy-service",
		Env: []string{
			"GRPC_ENABLE_FORK_SUPPORT=0",
			"PATH=/app/strategy-service/.venv/bin:/usr/bin:/bin",
			"PYTHONDONTWRITEBYTECODE=1",
			"PYTHONUNBUFFERED=1",
		},
	}
	expected := validEmbeddedRuntimeFacts("hosted")
	valid := func(t *testing.T) []byte {
		return validRuntimeProbeJSON(t, invocation, expected.Profile, "hosted", false)
	}
	tests := []struct {
		name   string
		result func(*testing.T) runtimeProbeResult
	}{
		{name: "nonzero", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: valid(t), ExitCode: 1}
		}},
		{name: "stderr", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: valid(t), Stderr: []byte("secret-canary"), ExitCode: 0}
		}},
		{name: "timeout", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{FailureKind: "timeout"}
		}},
		{name: "malformed", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: []byte("not-json\n"), ExitCode: 0}
		}},
		{name: "duplicate-key", result: func(t *testing.T) runtimeProbeResult {
			body := valid(t)
			return runtimeProbeResult{Stdout: append([]byte(`{"ok":true,`), body[1:]...), ExitCode: 0}
		}},
		{name: "trailing-data", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: append(valid(t), []byte("{}\n")...), ExitCode: 0}
		}},
		{name: "python-3.12", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: mutateRuntimeProbeJSON(t, valid(t), func(value map[string]any) {
				value["python_version"] = "3.12.9"
			}), ExitCode: 0}
		}},
		{name: "sys-prefix-mismatch", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: mutateRuntimeProbeJSON(t, valid(t), func(value map[string]any) {
				value["sys_prefix_sha256"] = strings.Repeat("0", 64)
			}), ExitCode: 0}
		}},
		{name: "profile-mismatch", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: mutateRuntimeProbeJSON(t, valid(t), func(value map[string]any) {
				value["dependency_profile"].(map[string]any)["contract_sha256"] = strings.Repeat("f", 64)
			}), ExitCode: 0}
		}},
		{name: "editable-hosted", result: func(t *testing.T) runtimeProbeResult {
			return runtimeProbeResult{Stdout: validRuntimeProbeJSON(t, invocation, expected.Profile, "hosted", true), ExitCode: 0}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingDependencyProbeRunner{result: tc.result(t)}
			got, err := VerifyRuntimeDependencyProfile(context.Background(), invocation, expected, runner)
			if err == nil || got != nil {
				t.Fatalf("profile = %+v, error = %v; want atomic rejection", got, err)
			}
			dependencyErr, ok := err.(*RuntimeDependencyProfileError)
			if !ok {
				t.Fatalf("error type = %T, want *RuntimeDependencyProfileError", err)
			}
			if dependencyErr.Code != "RUNTIME_DEPENDENCY_PROFILE_INVALID" {
				t.Fatalf("error code = %q", dependencyErr.Code)
			}
			if strings.Contains(err.Error(), "secret-canary") || strings.Contains(err.Error(), "/app/") {
				t.Fatalf("unsafe detail leaked: %q", err)
			}
		})
	}
}

func TestVerifyRuntimeDependencyProfilePreservesOnlySafeReportedFailureModule(t *testing.T) {
	invocation := WorkerPythonInvocation{
		Executable: "/app/strategy-service/.venv/bin/python",
		ArgsPrefix: []string{"-I"},
		WorkDir:    "/app/strategy-service",
		Env: []string{
			"GRPC_ENABLE_FORK_SUPPORT=0",
			"PATH=/app/strategy-service/.venv/bin:/usr/bin:/bin",
			"PYTHONDONTWRITEBYTECODE=1",
			"PYTHONUNBUFFERED=1",
		},
	}
	expected := validEmbeddedRuntimeFacts("hosted")
	body := mutateRuntimeProbeJSON(t, validRuntimeProbeJSON(t, invocation, expected.Profile, "hosted", false), func(value map[string]any) {
		value["ok"] = false
		value["failures"] = []any{map[string]any{
			"code":   "DEPENDENCY_IMPORT_FAILED",
			"module": "dateutil",
			"reason": "required runtime dependency probe failed",
		}}
	})
	runner := &recordingDependencyProbeRunner{result: runtimeProbeResult{Stdout: body, ExitCode: 1}}
	profile, err := VerifyRuntimeDependencyProfile(context.Background(), invocation, expected, runner)
	if err == nil || profile != nil {
		t.Fatalf("profile = %+v, error = %v; want atomic failure", profile, err)
	}
	dependencyErr, ok := err.(*RuntimeDependencyProfileError)
	if !ok || dependencyErr.Module != "dateutil" || strings.Contains(dependencyErr.Error(), "required runtime") {
		t.Fatalf("dependency error = %+v", dependencyErr)
	}
}

func TestLoadEmbeddedRuntimeFactsRequiresEveryExactEnvironmentFact(t *testing.T) {
	expected := validEmbeddedRuntimeFacts("hosted")
	env := embeddedFactsEnvironment(expected.Profile)

	got, err := LoadEmbeddedRuntimeFacts("hosted", env)
	if err != nil {
		t.Fatalf("LoadEmbeddedRuntimeFacts: %v", err)
	}
	if !slices.Equal(got.Profile.GetPublicImportRoots(), expected.Profile.GetPublicImportRoots()) ||
		got.Profile.GetImageBuildId() != expected.Profile.GetImageBuildId() {
		t.Fatalf("facts = %+v", got)
	}

	for _, mutation := range []struct {
		name   string
		key    string
		value  string
		remove bool
	}{
		{name: "missing digest", key: "HUSHINE_RUNTIME_CONTRACT_SHA256", remove: true},
		{name: "poisoned roots", key: "HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS", value: "dateutil,/tmp/secret"},
		{name: "unsorted roots", key: "HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS", value: "grpc,dateutil"},
		{name: "newline", key: "HUSHINE_RUNTIME_PROFILE_NAME", value: "platform\nsecret"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			changed := append([]string(nil), env...)
			for index, item := range changed {
				if strings.HasPrefix(item, mutation.key+"=") {
					if mutation.remove {
						changed = append(changed[:index], changed[index+1:]...)
					} else {
						changed[index] = mutation.key + "=" + mutation.value
					}
					break
				}
			}
			if _, err := LoadEmbeddedRuntimeFacts("hosted", changed); err == nil ||
				(mutation.value != "" && strings.Contains(err.Error(), mutation.value)) {
				t.Fatalf("error = %v, want safe rejection", err)
			}
		})
	}
}

func TestLoadEmbeddedRuntimeFactsAcceptsCurrentFourRepositoryImageBuildID(t *testing.T) {
	expected := validEmbeddedRuntimeFacts("hosted")
	expected.Profile.ImageBuildId = strings.Join([]string{
		expected.Profile.GetStrategyServiceCommit()[:12],
		expected.Profile.GetStrategyLibraryCommit()[:12],
		strings.Repeat("d", 12),
		strings.Repeat("e", 12),
		expected.Profile.GetProfileVersion(),
		"executor-coverage",
	}, "-")

	got, err := LoadEmbeddedRuntimeFacts("hosted", embeddedFactsEnvironment(expected.Profile))
	if err != nil {
		t.Fatalf("LoadEmbeddedRuntimeFacts rejected build_strategy_runtime.sh identity: %v", err)
	}
	if got.Profile.GetImageBuildId() != expected.Profile.GetImageBuildId() {
		t.Fatalf("image build id = %q, want %q", got.Profile.GetImageBuildId(), expected.Profile.GetImageBuildId())
	}
}

func TestLoadEmbeddedRuntimeFactsAllowsOnlyCompleteBareLocalDevIdentity(t *testing.T) {
	profile := validEmbeddedRuntimeFacts("bare").Profile
	profile.StrategyServiceCommit = "local-dev"
	profile.StrategyLibraryCommit = "local-dev"
	profile.ImageBuildId = "local-dev"
	if _, err := LoadEmbeddedRuntimeFacts("bare", embeddedFactsEnvironment(profile)); err != nil {
		t.Fatalf("bare local-dev facts: %v", err)
	}
	if _, err := LoadEmbeddedRuntimeFacts("hosted", embeddedFactsEnvironment(profile)); err == nil {
		t.Fatal("hosted local-dev facts unexpectedly accepted")
	}
}

func TestVerifyRuntimeDependencyProfileWithRealBareVenvPython(t *testing.T) {
	serviceRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(serviceRoot, ".venv", "bin", "python")
	if runtime.GOOS == "windows" {
		python = filepath.Join(serviceRoot, ".venv", "Scripts", "python.exe")
	}
	if _, err := os.Stat(python); err != nil {
		t.Skipf("guarded worker venv unavailable: %v", err)
	}
	environment := []string{
		"HUSHINE_RUNTIME_PROFILE_NAME=platform-python-3.13",
		"HUSHINE_RUNTIME_PROFILE_VERSION=1.0.0",
		"HUSHINE_RUNTIME_CONTRACT_SHA256=8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5",
		"HUSHINE_RUNTIME_HOSTED_PYTHON=3.13",
		"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=dateutil,google,grpc,numpy,pandas,pydantic,requests,yaml",
		"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=local-dev",
		"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=local-dev",
		"HUSHINE_RUNTIME_IMAGE_BUILD_ID=local-dev",
	}
	launchSpec, err := ResolveWorkerLaunchSpec(WorkerManagerConfig{
		PythonExecutable: python,
		WorkDir:          serviceRoot,
	}, "bare", environment)
	if err != nil {
		t.Fatalf("ResolveWorkerLaunchSpec: %v", err)
	}
	facts, err := LoadEmbeddedRuntimeFacts("bare", environment)
	if err != nil {
		t.Fatalf("LoadEmbeddedRuntimeFacts: %v", err)
	}
	got, err := VerifyRuntimeDependencyProfile(
		context.Background(), launchSpec.Invocation, facts, nil,
	)
	if err != nil {
		t.Fatalf("real startup verifier: %v", err)
	}
	if !slices.Equal(got.GetPublicImportRoots(), facts.Profile.GetPublicImportRoots()) ||
		got.GetImageBuildId() != "local-dev" {
		t.Fatalf("real verified profile = %+v", got)
	}
}

func TestRuntimeProbeRunnerBoundsBothPipesAndReapsHungChild(t *testing.T) {
	serviceRoot, python := realWorkerVenvForTest(t)
	invocation := WorkerPythonInvocation{
		Executable: python,
		ArgsPrefix: []string{"-I"},
		WorkDir:    serviceRoot,
		Env:        []string{"PATH=" + filepath.Dir(python)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	result := (execRuntimeProbeRunner{}).Run(ctx, invocation, []string{
		"-c",
		"import sys,time; sys.stdout.write('o'*70000); sys.stdout.flush(); sys.stderr.write('e'*70000); sys.stderr.flush(); time.sleep(30)",
	})
	if result.FailureKind != "overflow" {
		t.Fatalf("failure kind = %q, stdout=%d stderr=%d", result.FailureKind, len(result.Stdout), len(result.Stderr))
	}
	if len(result.Stdout) > runtimeProbeOutputLimit || len(result.Stderr) > runtimeProbeOutputLimit {
		t.Fatalf("bounded output exceeded: stdout=%d stderr=%d", len(result.Stdout), len(result.Stderr))
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("overflowing child was not terminated and reaped promptly")
	}
}

func TestRuntimeProbeRunnerClosesStdinAndEnforcesDeadline(t *testing.T) {
	serviceRoot, python := realWorkerVenvForTest(t)
	invocation := WorkerPythonInvocation{
		Executable: python,
		ArgsPrefix: []string{"-I"},
		WorkDir:    serviceRoot,
		Env:        []string{"PATH=" + filepath.Dir(python)},
	}
	stdinResult := (execRuntimeProbeRunner{}).Run(context.Background(), invocation, []string{
		"-c", "import sys; data=sys.stdin.buffer.read(); print(len(data))",
	})
	if stdinResult.FailureKind != "" || stdinResult.ExitCode != 0 || string(stdinResult.Stdout) != "0\n" {
		t.Fatalf("closed stdin result = %+v", stdinResult)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	timeoutResult := (execRuntimeProbeRunner{}).Run(ctx, invocation, []string{
		"-c", "import time; time.sleep(30)",
	})
	if timeoutResult.FailureKind != "timeout" {
		t.Fatalf("deadline failure kind = %q", timeoutResult.FailureKind)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("timed out child was not killed and reaped promptly")
	}
}

func realWorkerVenvForTest(t *testing.T) (string, string) {
	t.Helper()
	serviceRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(serviceRoot, ".venv", "bin", "python")
	if runtime.GOOS == "windows" {
		python = filepath.Join(serviceRoot, ".venv", "Scripts", "python.exe")
	}
	if _, err := os.Stat(python); err != nil {
		t.Skipf("guarded worker venv unavailable: %v", err)
	}
	physicalRoot, err := filepath.EvalSymlinks(serviceRoot)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(physicalRoot), filepath.Clean(python)
}

func validEmbeddedRuntimeFacts(source string) EmbeddedRuntimeFacts {
	return EmbeddedRuntimeFacts{
		Source: source,
		Profile: &strategyv1.RuntimeDependencyProfile{
			SchemaVersion:         1,
			ProfileName:           "platform-python-3.13",
			ProfileVersion:        "1.0.0",
			ContractSha256:        strings.Repeat("a", 64),
			HostedPython:          "3.13",
			PublicImportRoots:     []string{"dateutil", "grpc", "numpy", "pandas", "pydantic", "requests", "yaml"},
			StrategyServiceCommit: strings.Repeat("b", 40),
			StrategyLibraryCommit: strings.Repeat("c", 40),
			ImageBuildId: strings.Join([]string{
				strings.Repeat("b", 12), strings.Repeat("c", 12),
				strings.Repeat("d", 12), strings.Repeat("e", 12),
				"1.0.0", "executor",
			}, "-"),
		},
	}
}

func validRuntimeProbeJSON(
	t *testing.T,
	invocation WorkerPythonInvocation,
	profile *strategyv1.RuntimeDependencyProfile,
	source string,
	editable bool,
) []byte {
	t.Helper()
	originKind := "venv-site"
	if editable {
		originKind = "editable"
	}
	profileJSON := map[string]any{
		"schema_version":          profile.GetSchemaVersion(),
		"profile_name":            profile.GetProfileName(),
		"profile_version":         profile.GetProfileVersion(),
		"contract_sha256":         profile.GetContractSha256(),
		"hosted_python":           profile.GetHostedPython(),
		"public_import_roots":     profile.GetPublicImportRoots(),
		"strategy_service_commit": profile.GetStrategyServiceCommit(),
		"strategy_library_commit": profile.GetStrategyLibraryCommit(),
		"image_build_id":          profile.GetImageBuildId(),
	}
	packages := []any{}
	for _, distribution := range []string{"hushine-strategy-library", "hushine-strategy-service"} {
		packages = append(packages, map[string]any{
			"distribution":       distribution,
			"version":            "0.1.0",
			"direct_url_present": editable,
			"editable":           editable,
			"origin_kind":        originKind,
			"origin_sha256":      strings.Repeat("e", 64),
		})
	}
	value := map[string]any{
		"schema_version":        1,
		"ok":                    true,
		"source":                source,
		"python_version":        "3.13.5",
		"dependency_profile":    profileJSON,
		"sys_prefix_sha256":     expectedWorkerPrefixSHA256(invocation.Executable),
		"sys_executable_sha256": sha256Text(invocation.Executable),
		"workdir_sha256":        sha256Text(invocation.WorkDir),
		"packages":              packages,
		"failures":              []any{},
	}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(body, '\n')
}

func mutateRuntimeProbeJSON(t *testing.T, body []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func sha256Text(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func embeddedFactsEnvironment(profile *strategyv1.RuntimeDependencyProfile) []string {
	environment := []string{
		"HUSHINE_RUNTIME_PROFILE_NAME=" + profile.GetProfileName(),
		"HUSHINE_RUNTIME_PROFILE_VERSION=" + profile.GetProfileVersion(),
		"HUSHINE_RUNTIME_CONTRACT_SHA256=" + profile.GetContractSha256(),
		"HUSHINE_RUNTIME_HOSTED_PYTHON=" + profile.GetHostedPython(),
		"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=" + strings.Join(profile.GetPublicImportRoots(), ","),
		"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=" + profile.GetStrategyServiceCommit(),
		"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=" + profile.GetStrategyLibraryCommit(),
		"HUSHINE_RUNTIME_IMAGE_BUILD_ID=" + profile.GetImageBuildId(),
	}
	if profile.GetImageBuildId() != "local-dev" {
		environment = append(environment,
			"HUSHINE_RUNTIME_GOLANG_LIB_COMMIT="+strings.Repeat("d", 40),
			"HUSHINE_RUNTIME_CORE_SERVICE_COMMIT="+strings.Repeat("e", 40),
		)
	}
	return environment
}
