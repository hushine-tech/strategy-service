package runtimeagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WorkerManagerConfig struct {
	PythonExecutable string
	PythonArgsPrefix []string
	WorkerModule     string
	AgentAddr        string
	DebugpyBasePort  int
	WorkDir          string
	StateRoot        string
	PythonPath       []string
}

type WorkerStartSpec struct {
	SessionID   string
	Token       string
	AgentAddr   string
	DebugpyPort int
}

type WorkerManager struct {
	cfg      WorkerManagerConfig
	registry *SessionRegistry
	mu       sync.Mutex
	active   map[string]*ManagedWorker

	cleanupSessionRoot workerSessionCleanup
	cleanupFailures    map[string]error
}

type ManagedWorker struct {
	SessionID string
	Spec      WorkerStartSpec
	Cmd       *exec.Cmd

	done     <-chan error
	waitOnce sync.Once
	waitErr  error
}

func NewWorkerManager(cfg WorkerManagerConfig) *WorkerManager {
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
	return &WorkerManager{
		cfg:                cfg,
		registry:           NewSessionRegistry(),
		active:             map[string]*ManagedWorker{},
		cleanupSessionRoot: os.RemoveAll,
		cleanupFailures:    map[string]error{},
	}
}

func (m *WorkerManager) PrepareSessionWorker(sessionID string) (WorkerStartSpec, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return WorkerStartSpec{}, fmt.Errorf("session_id is required")
	}
	if m.retainedCleanupFailure(sessionID) != nil {
		return WorkerStartSpec{}, ErrWorkerAlreadyExists
	}
	token, err := randomToken()
	if err != nil {
		return WorkerStartSpec{}, err
	}
	if err := m.registry.ExpectWorker(sessionID, token); err != nil {
		return WorkerStartSpec{}, err
	}
	debugpyPort := 0
	if m.cfg.DebugpyBasePort > 0 {
		debugpyPort = m.cfg.DebugpyBasePort
	}
	spec := WorkerStartSpec{
		SessionID:   sessionID,
		Token:       token,
		AgentAddr:   m.cfg.AgentAddr,
		DebugpyPort: debugpyPort,
	}
	return spec, nil
}

func (m *WorkerManager) StartSessionWorker(ctx context.Context, sessionID string, extraEnv []string) (*ManagedWorker, error) {
	spec, err := m.PrepareSessionWorker(sessionID)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		m.registry.ForgetWorker(sessionID)
		return nil, ctx.Err()
	default:
	}
	args := append([]string{}, m.cfg.PythonArgsPrefix...)
	args = append(args, "-m", m.cfg.WorkerModule)
	env, sessionRoot, resolvedExecutable, err := buildWorkerEnvironmentWithCleanup(m.cfg, spec, extraEnv, m.cleanupSessionRoot)
	if err != nil {
		if hasWorkerSessionCleanupError(err) {
			m.retainCleanupFailure(spec.SessionID, err)
		} else {
			m.registry.ForgetWorker(sessionID)
		}
		return nil, err
	}
	cmd := exec.Command(resolvedExecutable, args...)
	if strings.TrimSpace(m.cfg.WorkDir) != "" {
		cmd.Dir = m.cfg.WorkDir
	}
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		startErr := errors.Join(
			fmt.Errorf("start session worker: %w", err),
			cleanupWorkerSessionError(sessionRoot, cleanupErr),
		)
		if cleanupErr == nil {
			m.registry.ForgetWorker(sessionID)
		} else {
			m.retainCleanupFailure(spec.SessionID, startErr)
		}
		return nil, startErr
	}
	done := make(chan error, 1)
	worker := &ManagedWorker{SessionID: spec.SessionID, Spec: spec, Cmd: cmd, done: done}
	m.mu.Lock()
	m.active[spec.SessionID] = worker
	m.mu.Unlock()
	go func() {
		waitErr := cmd.Wait()
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		waitErr = errors.Join(waitErr, cleanupWorkerSessionError(sessionRoot, cleanupErr))
		if cleanupErr == nil {
			m.clearCleanupFailure(spec.SessionID)
			m.registry.ForgetWorker(spec.SessionID)
			m.forgetWorker(worker)
		} else {
			m.retainCleanupFailure(spec.SessionID, waitErr)
		}
		done <- waitErr
		close(done)
	}()
	return worker, nil
}

func (m *WorkerManager) StopSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if err := m.retainedCleanupFailure(sessionID); err != nil {
		return err
	}
	worker := m.findWorker(sessionID)
	if worker == nil || worker.Cmd == nil || worker.Cmd.Process == nil {
		m.registry.ForgetWorker(sessionID)
		return nil
	}
	if err := worker.Cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) && worker.done == nil {
			m.registry.ForgetWorker(sessionID)
			m.forgetWorker(worker)
			return nil
		}
		if !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("kill session worker %s: %w", sessionID, err)
		}
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- worker.Wait()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("session worker did not exit after kill: %s", sessionID)
	case waitErr := <-waitDone:
		if hasWorkerSessionCleanupError(waitErr) {
			return waitErr
		}
		m.clearCleanupFailure(sessionID)
		m.registry.ForgetWorker(sessionID)
		m.forgetWorker(worker)
		return nil
	}
}

func (m *WorkerManager) retainCleanupFailure(sessionID string, err error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupFailures[sessionID] = err
}

func (m *WorkerManager) retainedCleanupFailure(sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cleanupFailures[sessionID]
}

func (m *WorkerManager) clearCleanupFailure(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cleanupFailures, sessionID)
}

func (m *WorkerManager) AliasWorkerSession(existingSessionID string, sessionID string) error {
	existingSessionID = strings.TrimSpace(existingSessionID)
	sessionID = strings.TrimSpace(sessionID)
	if existingSessionID == "" || sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if err := m.registry.AliasWorkerSession(existingSessionID, sessionID); err != nil {
		return err
	}
	if existingSessionID == sessionID {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if worker := m.active[existingSessionID]; worker != nil {
		m.active[sessionID] = worker
		return nil
	}
	identity, ok := m.registry.ActiveWorker(sessionID)
	if !ok || identity.PID <= 0 {
		return nil
	}
	for _, worker := range m.active {
		if worker != nil && worker.Cmd != nil && worker.Cmd.Process != nil && int64(worker.Cmd.Process.Pid) == identity.PID {
			m.active[sessionID] = worker
			return nil
		}
	}
	return nil
}

func (w *ManagedWorker) Wait() error {
	if w == nil || w.Cmd == nil {
		return nil
	}
	w.waitOnce.Do(func() {
		if w.done != nil {
			w.waitErr = <-w.done
			return
		}
		w.waitErr = w.Cmd.Wait()
	})
	return w.waitErr
}

func (m *WorkerManager) Registry() *SessionRegistry {
	return m.registry
}

func (m *WorkerManager) findWorker(sessionID string) *ManagedWorker {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	if worker := m.active[sessionID]; worker != nil {
		m.mu.Unlock()
		return worker
	}
	m.mu.Unlock()
	identity, ok := m.registry.ActiveWorker(sessionID)
	if !ok || identity.PID <= 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, worker := range m.active {
		if worker != nil && worker.Cmd != nil && worker.Cmd.Process != nil && int64(worker.Cmd.Process.Pid) == identity.PID {
			return worker
		}
	}
	return nil
}

func (m *WorkerManager) forgetWorker(worker *ManagedWorker) {
	if worker == nil {
		return
	}
	pid := int64(0)
	if worker.Cmd != nil && worker.Cmd.Process != nil {
		pid = int64(worker.Cmd.Process.Pid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, active := range m.active {
		if active == worker {
			delete(m.active, key)
			continue
		}
		if pid > 0 && active != nil && active.Cmd != nil && active.Cmd.Process != nil && int64(active.Cmd.Process.Pid) == pid {
			delete(m.active, key)
		}
	}
}

func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate worker token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
