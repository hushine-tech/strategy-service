package runtimeagent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// newLegacyWorkerManager keeps unit tests for process lifecycle mechanics
// independent from the production startup gate. Production callers can only
// construct a manager from a resolved, verified WorkerLaunchSpec.
func newLegacyWorkerManager(cfg WorkerManagerConfig) *WorkerManager {
	if strings.TrimSpace(cfg.PythonExecutable) == "" {
		cfg.PythonExecutable = "python3"
	}
	if strings.TrimSpace(cfg.WorkerModule) == "" {
		cfg.WorkerModule = "strategy_service.session_worker_entry"
	}
	if strings.TrimSpace(cfg.AgentAddr) == "" {
		cfg.AgentAddr = "127.0.0.1:0"
	}
	if workDir, err := absoluteWorkerWorkDir(cfg.WorkDir); err == nil {
		cfg.WorkDir = workDir
	}
	if strings.TrimSpace(cfg.StateRoot) == "" {
		cfg.StateRoot = filepath.Join(cfg.WorkDir, ".hushine-worker-state")
	}
	return newWorkerManagerState(cfg, nil, buildWorkerEnvironmentWithCleanup)
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
