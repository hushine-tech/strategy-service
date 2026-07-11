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

func buildWorkerEnvironment(
	cfg WorkerManagerConfig,
	spec WorkerStartSpec,
	extraEnv []string,
) (env []string, sessionRoot string, resolvedExecutable string, err error) {
	return buildWorkerEnvironmentWithCleanup(cfg, spec, extraEnv, os.RemoveAll)
}

func buildWorkerEnvironmentWithCleanup(
	cfg WorkerManagerConfig,
	spec WorkerStartSpec,
	extraEnv []string,
	cleanup workerSessionCleanup,
) (env []string, sessionRoot string, resolvedExecutable string, err error) {
	workDir, err := absoluteWorkerWorkDir(cfg.WorkDir)
	if err != nil {
		return nil, "", "", err
	}

	stateRoot := strings.TrimSpace(cfg.StateRoot)
	if stateRoot == "" {
		stateRoot = filepath.Join(workDir, ".hushine-worker-state")
	} else if !filepath.IsAbs(stateRoot) {
		stateRoot = filepath.Join(workDir, stateRoot)
	}
	stateRoot = filepath.Clean(stateRoot)

	pythonPath := make([]string, 0, len(cfg.PythonPath))
	for _, item := range cfg.PythonPath {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if !filepath.IsAbs(item) {
			item = filepath.Join(workDir, item)
		}
		pythonPath = append(pythonPath, filepath.Clean(item))
	}

	resolvedExecutable, err = resolveWorkerExecutable(cfg.PythonExecutable)
	if err != nil {
		return nil, "", "", err
	}

	sessionRoot = workerSessionRoot(stateRoot, spec.SessionID)
	homeDir := filepath.Join(sessionRoot, "home")
	tmpDir := filepath.Join(sessionRoot, "tmp")
	cacheDir := filepath.Join(sessionRoot, "cache")
	values := map[string]string{
		"PYTHONPATH":              strings.Join(pythonPath, string(os.PathListSeparator)),
		"HOME":                    homeDir,
		"USERPROFILE":             homeDir,
		"TMPDIR":                  tmpDir,
		"TMP":                     tmpDir,
		"TEMP":                    tmpDir,
		"XDG_CACHE_HOME":          cacheDir,
		"UV_CACHE_DIR":            cacheDir,
		"PYTHONUNBUFFERED":        "1",
		"PYTHONDONTWRITEBYTECODE": "1",
		"HUSHINE_AGENT_ADDR":      strings.TrimSpace(spec.AgentAddr),
		"HUSHINE_WORKER_TOKEN":    spec.Token,
		"HUSHINE_SESSION_ID":      strings.TrimSpace(spec.SessionID),
		"HUSHINE_DEBUGPY_PORT":    strconv.Itoa(spec.DebugpyPort),
		"DEBUG_WAIT":              strconv.FormatBool(spec.DebugpyWait),
	}

	platformValues, err := trustedWorkerPlatformEnvironment(resolvedExecutable)
	if err != nil {
		return nil, "", "", err
	}
	for key, value := range platformValues {
		if _, exists := values[key]; exists {
			return nil, "", "", fmt.Errorf("trusted worker platform env key conflicts with baseline: %s", key)
		}
		values[key] = value
	}

	for _, item := range extraEnv {
		key, value, parseErr := parseEnvItem(item)
		if parseErr != nil {
			return nil, "", "", parseErr
		}
		if _, allowed := allowedWorkerExtraEnv[key]; !allowed {
			return nil, "", "", fmt.Errorf("worker extra env key is not allowed: %s", key)
		}
		values[key] = value
	}

	for _, dir := range []string{sessionRoot, homeDir, tmpDir, cacheDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			createErr := fmt.Errorf("create worker session directory %s: %w", dir, err)
			cleanupErr := runWorkerSessionCleanup(cleanup, sessionRoot)
			return nil, sessionRoot, resolvedExecutable, errors.Join(
				createErr,
				cleanupWorkerSessionError(sessionRoot, cleanupErr),
			)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			secureErr := fmt.Errorf("secure worker session directory %s: %w", dir, err)
			cleanupErr := runWorkerSessionCleanup(cleanup, sessionRoot)
			return nil, sessionRoot, resolvedExecutable, errors.Join(
				secureErr,
				cleanupWorkerSessionError(sessionRoot, cleanupErr),
			)
		}
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env = make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env, sessionRoot, resolvedExecutable, nil
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
