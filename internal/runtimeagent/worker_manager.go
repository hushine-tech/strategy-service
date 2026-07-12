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
	DebugpyWait      bool
	WorkDir          string
	StateRoot        string
	PythonPath       []string
}

type WorkerStartSpec struct {
	SessionID   string
	Token       string
	AgentAddr   string
	DebugpyPort int
	DebugpyWait bool
}

type managedWorkerStop func(context.Context, *ManagedWorker, time.Duration) error
type workerEnvironmentBuilder func(WorkerManagerConfig, WorkerStartSpec, []string, workerSessionCleanup) ([]string, string, string, error)

var (
	ErrWorkerManagerStopping = errors.New("worker manager is stopping")
	ErrWorkerCleanupPending  = errors.New("worker process exited but session cleanup is still pending")
)

type WorkerShutdownSummary struct {
	ForcedStops int
}

type WorkerManager struct {
	cfg      WorkerManagerConfig
	registry *SessionRegistry
	mu       sync.Mutex
	active   map[string]*ManagedWorker
	stopping bool
	starting int
	drained  chan struct{}

	forcedStops int

	cleanupSessionRoot workerSessionCleanup
	buildEnvironment   workerEnvironmentBuilder
	cleanupFailures    map[string]error
	stopWorker         managedWorkerStop
}

type ManagedWorker struct {
	SessionID string
	Spec      WorkerStartSpec
	Cmd       *exec.Cmd

	processExited  <-chan struct{}
	processExitErr error
	done           <-chan error
	waitOnce       sync.Once
	waitErr        error
	forceOnce      sync.Once
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
	manager := &WorkerManager{
		cfg:                cfg,
		registry:           NewSessionRegistry(),
		active:             map[string]*ManagedWorker{},
		cleanupSessionRoot: os.RemoveAll,
		buildEnvironment:   buildWorkerEnvironmentWithCleanup,
		cleanupFailures:    map[string]error{},
	}
	manager.stopWorker = manager.stopManagedWorker
	return manager
}

func (m *WorkerManager) PrepareSessionWorker(sessionID string) (WorkerStartSpec, error) {
	m.mu.Lock()
	stopping := m.stopping
	m.mu.Unlock()
	if stopping {
		return WorkerStartSpec{}, ErrWorkerManagerStopping
	}
	return m.prepareSessionWorker(sessionID)
}

func (m *WorkerManager) prepareSessionWorker(sessionID string) (WorkerStartSpec, error) {
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
		DebugpyWait: m.cfg.DebugpyWait,
	}
	return spec, nil
}

func (m *WorkerManager) StartSessionWorker(ctx context.Context, sessionID string, extraEnv []string) (*ManagedWorker, error) {
	if err := m.beginWorkerStart(); err != nil {
		return nil, err
	}
	defer m.endWorkerStart()
	spec, err := m.prepareSessionWorker(sessionID)
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
	env, sessionRoot, resolvedExecutable, err := m.buildEnvironment(m.cfg, spec, extraEnv, m.cleanupSessionRoot)
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
	processExited := make(chan struct{})
	done := make(chan error, 1)
	worker := &ManagedWorker{
		SessionID:     spec.SessionID,
		Spec:          spec,
		Cmd:           cmd,
		processExited: processExited,
		done:          done,
	}
	m.mu.Lock()
	m.active[spec.SessionID] = worker
	m.mu.Unlock()
	go func() {
		waitErr := cmd.Wait()
		worker.processExitErr = waitErr
		close(processExited)
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		waitErr = errors.Join(waitErr, cleanupWorkerSessionError(sessionRoot, cleanupErr))
		if cleanupErr == nil {
			m.clearCleanupFailureForWorker(spec.SessionID, worker)
			m.registry.ForgetWorkerIdentity(spec.SessionID, managedWorkerPID(worker), spec.Token)
			m.forgetWorker(worker)
		} else {
			m.retainCleanupFailureForWorker(spec.SessionID, worker, waitErr)
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
	return m.stopWorker(ctx, worker, timeout)
}

func (m *WorkerManager) stopManagedWorker(ctx context.Context, worker *ManagedWorker, timeout time.Duration) error {
	if worker == nil || worker.Cmd == nil || worker.Cmd.Process == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	sessionID := worker.SessionID
	if err := m.retainedCleanupFailure(sessionID); err != nil {
		return err
	}
	if err := requestWorkerStop(worker.Cmd.Process); err != nil {
		if errors.Is(err, os.ErrProcessDone) && worker.done == nil {
			m.registry.ForgetWorkerIdentity(sessionID, managedWorkerPID(worker), worker.Spec.Token)
			m.forgetWorker(worker)
			return nil
		}
		if !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("request stop for session worker %s: %w", sessionID, err)
		}
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- worker.Wait()
	}()
	deadline := time.Now().Add(timeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	processExited := worker.processExitedSignal()
	if processExited == nil {
		processExited = make(chan struct{})
	}
	select {
	case <-ctx.Done():
		return errors.Join(ctx.Err(), m.forceStopWorker(sessionID, worker, waitDone))
	case <-timer.C:
		return m.forceStopWorker(sessionID, worker, waitDone)
	case <-processExited:
		return m.waitForWorkerCleanup(sessionID, worker, waitDone, time.Until(deadline))
	case waitErr := <-waitDone:
		return m.finishWorkerStop(sessionID, worker, waitErr)
	}
}

func (m *WorkerManager) forceStopWorker(sessionID string, worker *ManagedWorker, waitDone <-chan error) error {
	m.recordForcedStop(worker)
	if err := worker.Cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill session worker %s: %w", sessionID, err)
	}
	forceWait := 2 * time.Second
	processExited := worker.processExitedSignal()
	if processExited == nil {
		select {
		case waitErr := <-waitDone:
			return m.finishWorkerStop(sessionID, worker, waitErr)
		case <-time.After(forceWait):
			return fmt.Errorf("reap session worker %s after kill: %w", sessionID, context.DeadlineExceeded)
		}
	}
	select {
	case <-processExited:
		return m.waitForWorkerCleanup(sessionID, worker, waitDone, forceWait)
	case <-time.After(forceWait):
		return fmt.Errorf("reap session worker %s after kill: %w", sessionID, context.DeadlineExceeded)
	}
}

func (m *WorkerManager) StopAll(ctx context.Context, timeout time.Duration) error {
	type workerSnapshot struct {
		worker *ManagedWorker
		pid    int
	}

	if err := m.closeAdmissionAndWait(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	workers := make([]workerSnapshot, 0, len(m.active))
	for _, worker := range m.active {
		pid := 0
		if worker != nil && worker.Cmd != nil && worker.Cmd.Process != nil {
			pid = worker.Cmd.Process.Pid
		}
		workers = append(workers, workerSnapshot{worker: worker, pid: pid})
	}
	m.mu.Unlock()

	seenPIDs := make(map[int]struct{}, len(workers))
	var stopErrors []error
	for _, worker := range workers {
		if worker.pid > 0 {
			if _, ok := seenPIDs[worker.pid]; ok {
				continue
			}
			seenPIDs[worker.pid] = struct{}{}
		}
		if err := m.stopWorker(ctx, worker.worker, timeout); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}
	return errors.Join(stopErrors...)
}

func (m *WorkerManager) waitForWorkerCleanup(sessionID string, worker *ManagedWorker, waitDone <-chan error, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case waitErr := <-waitDone:
		return m.finishWorkerStop(sessionID, worker, waitErr)
	case <-timer.C:
		return fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending)
	}
}

func (m *WorkerManager) beginWorkerStart() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		return ErrWorkerManagerStopping
	}
	m.starting++
	return nil
}

func (m *WorkerManager) endWorkerStart() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.starting > 0 {
		m.starting--
	}
	if m.stopping && m.starting == 0 && m.drained != nil {
		close(m.drained)
		m.drained = nil
	}
}

func (m *WorkerManager) closeAdmissionAndWait(ctx context.Context) error {
	m.mu.Lock()
	m.stopping = true
	if m.starting == 0 {
		m.mu.Unlock()
		return nil
	}
	if m.drained == nil {
		m.drained = make(chan struct{})
	}
	drained := m.drained
	m.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *WorkerManager) recordForcedStop(worker *ManagedWorker) {
	if worker == nil {
		return
	}
	worker.forceOnce.Do(func() {
		m.mu.Lock()
		m.forcedStops++
		m.mu.Unlock()
	})
}

func (m *WorkerManager) ShutdownSummary() WorkerShutdownSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	return WorkerShutdownSummary{ForcedStops: m.forcedStops}
}

func (m *WorkerManager) finishWorkerStop(sessionID string, worker *ManagedWorker, waitErr error) error {
	if hasWorkerSessionCleanupError(waitErr) {
		return waitErr
	}
	m.clearCleanupFailureForWorker(sessionID, worker)
	m.registry.ForgetWorkerIdentity(sessionID, managedWorkerPID(worker), worker.Spec.Token)
	m.forgetWorker(worker)
	return nil
}

func managedWorkerPID(worker *ManagedWorker) int64 {
	if worker == nil || worker.Cmd == nil || worker.Cmd.Process == nil {
		return 0
	}
	return int64(worker.Cmd.Process.Pid)
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

func (m *WorkerManager) retainCleanupFailureForWorker(sessionID string, worker *ManagedWorker, err error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active[sessionID] == worker {
		m.cleanupFailures[sessionID] = err
	}
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

func (m *WorkerManager) clearCleanupFailureForWorker(sessionID string, worker *ManagedWorker) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active[sessionID] == worker {
		delete(m.cleanupFailures, sessionID)
	}
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

func (w *ManagedWorker) processExitedSignal() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.processExited
}

func (w *ManagedWorker) processError() error {
	if w == nil || w.processExited == nil {
		return nil
	}
	<-w.processExited
	return w.processExitErr
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
