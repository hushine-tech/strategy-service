package runtimeagent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

var allowedWorkerExtraEnv = map[string]struct{}{
	"HUSHINE_RUNTIME_ID":     {},
	"HUSHINE_RUNTIME_SOURCE": {},
	"HUSHINE_RUNTIME_NAME":   {},
}

var embeddedRuntimeEnvironmentKeys = map[string]struct{}{
	"HUSHINE_RUNTIME_PROFILE_NAME":            {},
	"HUSHINE_RUNTIME_PROFILE_VERSION":         {},
	"HUSHINE_RUNTIME_CONTRACT_SHA256":         {},
	"HUSHINE_RUNTIME_HOSTED_PYTHON":           {},
	"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS":     {},
	"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT": {},
	"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT": {},
	"HUSHINE_RUNTIME_IMAGE_BUILD_ID":          {},
}

type WorkerLaunchSpec struct {
	Invocation      WorkerPythonInvocation
	WorkerModule    string
	AgentAddr       string
	DebugpyBasePort int
	DebugpyWait     bool
	StateRoot       string
}

func ResolveWorkerLaunchSpec(
	cfg WorkerManagerConfig,
	runtimeSource string,
	processEnv []string,
) (WorkerLaunchSpec, error) {
	var spec WorkerLaunchSpec
	if runtimeSource != "hosted" && runtimeSource != "self_hosted" && runtimeSource != "bare" {
		return spec, fmt.Errorf("invalid runtime source")
	}
	if len(cfg.PythonPath) != 0 {
		return spec, fmt.Errorf("worker source paths are not allowed")
	}
	if cfg.DebugpyBasePort < 0 || cfg.DebugpyBasePort > 65535 {
		return spec, fmt.Errorf("worker debug port is invalid")
	}
	executable, err := resolveWorkerVenvExecutable(cfg.PythonExecutable)
	if err != nil {
		return spec, err
	}
	workDir, err := absoluteWorkerWorkDir(cfg.WorkDir)
	if err != nil {
		return spec, err
	}
	physicalWorkDir, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		return spec, fmt.Errorf("resolve worker work directory identity: %w", err)
	}
	physicalWorkDir, err = filepath.Abs(physicalWorkDir)
	if err != nil {
		return spec, fmt.Errorf("resolve absolute worker work directory identity: %w", err)
	}
	workDir = filepath.Clean(physicalWorkDir)

	parentEnv, err := exactEnvironmentMap(processEnv)
	if err != nil {
		return spec, err
	}
	argsPrefix := []string{"-I"}
	if raw, ok := parentEnv["HUSHINE_WORKER_PYTHON_ARGS"]; ok && raw != "" {
		if raw != "-Xfrozen_modules=off" {
			return spec, fmt.Errorf("HUSHINE_WORKER_PYTHON_ARGS contains an unapproved Python argument")
		}
		argsPrefix = append(argsPrefix, raw)
	}
	if err := validateTrustedCoveragePythonArgs(cfg.PythonArgsPrefix); err != nil {
		return spec, err
	}
	argsPrefix = append(argsPrefix, cfg.PythonArgsPrefix...)

	values := map[string]string{
		"GRPC_ENABLE_FORK_SUPPORT": "0",
		"PYTHONUNBUFFERED":         "1",
		"PYTHONDONTWRITEBYTECODE":  "1",
	}
	platformValues, err := trustedWorkerPlatformEnvironment(executable)
	if err != nil {
		return spec, err
	}
	for key, value := range platformValues {
		values[key] = value
	}
	localDevBuildFacts := runtimeSource == "bare" &&
		parentEnv["HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT"] == "local-dev" &&
		parentEnv["HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT"] == "local-dev" &&
		parentEnv["HUSHINE_RUNTIME_IMAGE_BUILD_ID"] == "local-dev"
	for key := range embeddedRuntimeEnvironmentKeys {
		value, ok := parentEnv[key]
		if !ok {
			continue
		}
		if value == "" || len(value) > 1024 || strings.ContainsAny(value, "\x00\r\n") {
			return spec, fmt.Errorf("embedded runtime environment fact is invalid: %s", key)
		}
		if localDevBuildFacts && (key == "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT" ||
			key == "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT" ||
			key == "HUSHINE_RUNTIME_IMAGE_BUILD_ID") {
			continue
		}
		values[key] = value
	}
	baseEnv := sortedEnvironment(values)

	stateRoot := strings.TrimSpace(cfg.StateRoot)
	if stateRoot == "" {
		stateRoot = filepath.Join(workDir, ".hushine-worker-state")
	} else if !filepath.IsAbs(stateRoot) {
		stateRoot = filepath.Join(workDir, stateRoot)
	}
	stateRoot = filepath.Clean(stateRoot)
	workerModule := strings.TrimSpace(cfg.WorkerModule)
	if workerModule == "" {
		workerModule = "strategy_service.session_worker_entry"
	}
	if !modulePattern.MatchString(workerModule) {
		return spec, fmt.Errorf("worker module is invalid")
	}
	agentAddr := strings.TrimSpace(cfg.AgentAddr)
	if agentAddr == "" {
		agentAddr = "127.0.0.1:0"
	}
	spec = WorkerLaunchSpec{
		Invocation: WorkerPythonInvocation{
			Executable: executable,
			ArgsPrefix: append([]string(nil), argsPrefix...),
			WorkDir:    workDir,
			Env:        baseEnv,
		},
		WorkerModule:    workerModule,
		AgentAddr:       agentAddr,
		DebugpyBasePort: cfg.DebugpyBasePort,
		DebugpyWait:     cfg.DebugpyWait,
		StateRoot:       stateRoot,
	}
	return spec, nil
}

func exactEnvironmentMap(environment []string) (map[string]string, error) {
	values := make(map[string]string, len(environment))
	for _, item := range environment {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" || strings.ContainsAny(key, "\x00\r\n") {
			return nil, fmt.Errorf("process environment contains an invalid entry")
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("process environment contains a duplicate key")
		}
		values[key] = value
	}
	return values, nil
}

func validateTrustedCoveragePythonArgs(args []string) error {
	if len(args) == 0 {
		return nil
	}
	if len(args) != 6 || args[0] != "-m" || args[1] != "coverage" ||
		args[2] != "run" || args[3] != "--parallel-mode" ||
		args[5] != "--include=*/strategy_service/*" ||
		!strings.HasPrefix(args[4], "--data-file=") {
		return fmt.Errorf("trusted coverage Python prefix is invalid")
	}
	dataFile := strings.TrimPrefix(args[4], "--data-file=")
	if len(dataFile) > 1024 || !filepath.IsAbs(dataFile) || filepath.Clean(dataFile) != dataFile ||
		filepath.Base(dataFile) != ".coverage" || filepath.Base(filepath.Dir(dataFile)) != "python" {
		return fmt.Errorf("trusted coverage data file is invalid")
	}
	return nil
}

func sortedEnvironment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func resolveWorkerVenvExecutable(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || !filepath.IsAbs(name) {
		return "", fmt.Errorf("worker Python must be an absolute virtualenv executable path")
	}
	resolved, err := resolveWorkerExecutable(name)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(resolved)
	venvRoot := filepath.Dir(parent)
	base := filepath.Base(resolved)
	validLayout := filepath.Base(parent) == "bin" && base == "python"
	if runtime.GOOS == "windows" {
		validLayout = strings.EqualFold(filepath.Base(parent), "Scripts") && strings.EqualFold(base, "python.exe")
	}
	if !validLayout {
		return "", fmt.Errorf("worker Python must use the guarded virtualenv launcher")
	}
	info, err := os.Stat(filepath.Join(venvRoot, "pyvenv.cfg"))
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("worker Python virtualenv marker is unavailable")
	}
	return resolved, nil
}

type workerSessionCleanup func(string) error

type workerSessionCleanupError struct {
	sessionRoot string
	err         error
}

func (e *workerSessionCleanupError) Error() string {
	return fmt.Sprintf("remove worker session root %s: %v", e.sessionRoot, e.err)
}

func (e *workerSessionCleanupError) Unwrap() error {
	return e.err
}

func buildWorkerEnvironmentFromLaunchSpec(
	launchSpec WorkerLaunchSpec,
	startSpec WorkerStartSpec,
	extraEnv []string,
	cleanup workerSessionCleanup,
) (env []string, sessionRoot string, resolvedExecutable string, err error) {
	baseValues, err := exactEnvironmentMap(launchSpec.Invocation.Env)
	if err != nil {
		return nil, "", "", fmt.Errorf("invalid resolved worker base environment")
	}
	sessionRoot = workerSessionRoot(launchSpec.StateRoot, startSpec.SessionID)
	homeDir := filepath.Join(sessionRoot, "home")
	tmpDir := filepath.Join(sessionRoot, "tmp")
	cacheDir := filepath.Join(sessionRoot, "cache")
	values := make(map[string]string, len(baseValues)+16)
	for key, value := range baseValues {
		values[key] = value
	}
	for key, value := range map[string]string{
		"HOME":                 homeDir,
		"USERPROFILE":          homeDir,
		"TMPDIR":               tmpDir,
		"TMP":                  tmpDir,
		"TEMP":                 tmpDir,
		"XDG_CACHE_HOME":       cacheDir,
		"UV_CACHE_DIR":         cacheDir,
		"HUSHINE_AGENT_ADDR":   strings.TrimSpace(startSpec.AgentAddr),
		"HUSHINE_WORKER_TOKEN": startSpec.Token,
		"HUSHINE_SESSION_ID":   strings.TrimSpace(startSpec.SessionID),
		"HUSHINE_DEBUGPY_PORT": strconv.Itoa(startSpec.DebugpyPort),
		"DEBUG_WAIT":           strconv.FormatBool(startSpec.DebugpyWait),
	} {
		if _, exists := values[key]; exists {
			return nil, "", launchSpec.Invocation.Executable, fmt.Errorf("worker session environment conflicts with immutable base")
		}
		values[key] = value
	}
	for _, item := range extraEnv {
		key, value, parseErr := parseEnvItem(item)
		if parseErr != nil {
			return nil, "", launchSpec.Invocation.Executable, parseErr
		}
		if _, allowed := allowedWorkerExtraEnv[key]; !allowed {
			return nil, "", launchSpec.Invocation.Executable, fmt.Errorf("worker extra env key is not allowed: %s", key)
		}
		if _, exists := values[key]; exists {
			return nil, "", launchSpec.Invocation.Executable, fmt.Errorf("worker extra env conflicts with immutable base: %s", key)
		}
		values[key] = value
	}
	for _, dir := range []string{sessionRoot, homeDir, tmpDir, cacheDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			createErr := fmt.Errorf("create worker session directory %s: %w", dir, err)
			cleanupErr := runWorkerSessionCleanup(cleanup, sessionRoot)
			return nil, sessionRoot, launchSpec.Invocation.Executable, errors.Join(
				createErr,
				cleanupWorkerSessionError(sessionRoot, cleanupErr),
			)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			secureErr := fmt.Errorf("secure worker session directory %s: %w", dir, err)
			cleanupErr := runWorkerSessionCleanup(cleanup, sessionRoot)
			return nil, sessionRoot, launchSpec.Invocation.Executable, errors.Join(
				secureErr,
				cleanupWorkerSessionError(sessionRoot, cleanupErr),
			)
		}
	}
	return sortedEnvironment(values), sessionRoot, launchSpec.Invocation.Executable, nil
}

func runWorkerSessionCleanup(cleanup workerSessionCleanup, sessionRoot string) error {
	if cleanup == nil {
		cleanup = os.RemoveAll
	}
	return cleanup(sessionRoot)
}

func cleanupWorkerSessionError(sessionRoot string, err error) error {
	if err == nil {
		return nil
	}
	return &workerSessionCleanupError{sessionRoot: sessionRoot, err: err}
}

func hasWorkerSessionCleanupError(err error) bool {
	var cleanupErr *workerSessionCleanupError
	return errors.As(err, &cleanupErr)
}

func absoluteWorkerWorkDir(workDir string) (string, error) {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		workDir = "."
	}
	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return "", fmt.Errorf("resolve worker work directory: %w", err)
	}
	return filepath.Clean(absWorkDir), nil
}

func resolveWorkerExecutable(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("python executable is required")
	}
	lookedUp, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("resolve python executable %q: %w", name, err)
	}
	absExecutable, err := filepath.Abs(lookedUp)
	if err != nil {
		return "", fmt.Errorf("resolve absolute python executable %q: %w", lookedUp, err)
	}
	absExecutable = filepath.Clean(absExecutable)
	validatedTarget, err := filepath.EvalSymlinks(absExecutable)
	if err != nil {
		return "", fmt.Errorf("resolve python executable symlinks %q: %w", absExecutable, err)
	}
	validatedTarget, err = filepath.Abs(validatedTarget)
	if err != nil {
		return "", fmt.Errorf("resolve final python executable %q: %w", validatedTarget, err)
	}
	validatedTarget = filepath.Clean(validatedTarget)
	info, err := os.Stat(validatedTarget)
	if err != nil {
		return "", fmt.Errorf("stat python executable %q: %w", validatedTarget, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("python executable is not a regular file: %s", validatedTarget)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("python executable is not executable: %s", validatedTarget)
	}
	return absExecutable, nil
}

func parseEnvItem(item string) (string, string, error) {
	key, value, ok := strings.Cut(item, "=")
	key = strings.TrimSpace(key)
	if !ok || key == "" {
		return "", "", fmt.Errorf("invalid worker env item: %q", item)
	}
	return key, value, nil
}

func workerSessionRoot(stateRoot, sessionID string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(sessionID)))
	return filepath.Join(stateRoot, hex.EncodeToString(digest[:16]))
}
