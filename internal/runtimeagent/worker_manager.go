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
	"sync/atomic"
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
	Generation  uint64
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
	cfg         WorkerManagerConfig
	registry    *SessionRegistry
	mu          sync.Mutex
	active      map[string]*ManagedWorker
	stopping    bool
	abortStarts bool
	starting    int
	drained     chan struct{}

	forcedStops int

	cleanupSessionRoot workerSessionCleanup
	buildEnvironment   workerEnvironmentBuilder
	startCommand       func(*exec.Cmd) error
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
	stopOnce       sync.Once
	stopDone       chan struct{}
	stopErr        error
	forceOnce      sync.Once
	draining       atomic.Bool
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
		startCommand:       func(cmd *exec.Cmd) error { return cmd.Start() },
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
	generation, err := m.registry.ExpectWorkerGeneration(sessionID, token)
	if err != nil {
		return WorkerStartSpec{}, err
	}
	debugpyPort := 0
	if m.cfg.DebugpyBasePort > 0 {
		debugpyPort = m.cfg.DebugpyBasePort
	}
	spec := WorkerStartSpec{
		SessionID:   sessionID,
		Token:       token,
		Generation:  generation,
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
		m.registry.ForgetWorkerIdentity(spec.SessionID, 0, spec.Token)
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
			m.registry.ForgetWorkerIdentity(spec.SessionID, 0, spec.Token)
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
	abortErr := m.workerStartAbortError(ctx)
	m.mu.Unlock()
	var startErr error
	if abortErr == nil {
		startErr = m.startCommand(cmd)
	}
	if abortErr == nil && startErr == nil {
		m.mu.Lock()
		abortErr = m.workerStartAbortError(ctx)
		if abortErr == nil {
			m.active[spec.SessionID] = worker
		}
		m.mu.Unlock()
	}
	if abortErr != nil {
		var terminateErr error
		if cmd.Process != nil {
			m.recordForcedStop(worker)
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				terminateErr = fmt.Errorf("kill aborted session worker %s: %w", spec.SessionID, err)
			} else {
				_ = cmd.Wait()
			}
		}
		if terminateErr != nil {
			m.mu.Lock()
			m.active[spec.SessionID] = worker
			m.mu.Unlock()
			m.watchStartedWorker(worker, sessionRoot, processExited, done)
			return nil, errors.Join(abortErr, terminateErr)
		}
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		err := errors.Join(abortErr, cleanupWorkerSessionError(sessionRoot, cleanupErr))
		if cleanupErr == nil {
			m.registry.ForgetWorkerIdentity(spec.SessionID, managedWorkerPID(worker), spec.Token)
		} else {
			m.retainCleanupFailure(spec.SessionID, err)
		}
		return nil, err
	}
	if startErr != nil {
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		startErr = errors.Join(
			fmt.Errorf("start session worker: %w", startErr),
			cleanupWorkerSessionError(sessionRoot, cleanupErr),
		)
		if cleanupErr == nil {
			m.registry.ForgetWorkerIdentity(spec.SessionID, 0, spec.Token)
		} else {
			m.retainCleanupFailure(spec.SessionID, startErr)
		}
		return nil, startErr
	}
	m.watchStartedWorker(worker, sessionRoot, processExited, done)
	return worker, nil
}

func (m *WorkerManager) watchStartedWorker(worker *ManagedWorker, sessionRoot string, processExited chan struct{}, done chan error) {
	go func() {
		waitErr := worker.Cmd.Wait()
		worker.processExitErr = waitErr
		close(processExited)
		cleanupErr := runWorkerSessionCleanup(m.cleanupSessionRoot, sessionRoot)
		waitErr = errors.Join(waitErr, cleanupWorkerSessionError(sessionRoot, cleanupErr))
		if cleanupErr == nil {
			m.clearCleanupFailureForWorker(worker.Spec.SessionID, worker)
			m.registry.ForgetWorkerIdentity(worker.Spec.SessionID, managedWorkerPID(worker), worker.Spec.Token)
			m.forgetWorker(worker)
		} else {
			m.retainCleanupFailureForWorker(worker.Spec.SessionID, worker, waitErr)
		}
		done <- waitErr
		close(done)
	}()
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

// WaitSessionWorker waits for a worker that has already been asked to stop by
// the session protocol. It never sends a process signal, so the Python wrapper
// can finish its final-status acknowledgement and coverage flush naturally.
func (m *WorkerManager) WaitSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error {
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
	if worker == nil {
		return nil
	}
	waited := make(chan error, 1)
	go func() {
		waitErr := worker.Wait()
		if finishErr := m.finishWorkerStop(sessionID, worker, waitErr); finishErr != nil {
			waited <- finishErr
			return
		}
		waited <- waitErr
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("wait session worker %s: %w", sessionID, context.DeadlineExceeded)
	case err := <-waited:
		return err
	}
}

// MarkSessionWorkerDraining records that the worker is completing its terminal
// protocol handshake and should be allowed a bounded natural exit before any
// process signal is sent.
func (m *WorkerManager) MarkSessionWorkerDraining(sessionID string) {
	if worker := m.findWorker(sessionID); worker != nil {
		worker.draining.Store(true)
	}
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
	owner := false
	worker.stopOnce.Do(func() {
		owner = true
		worker.stopDone = make(chan struct{})
		allowDrainGrace := worker.draining.Load()
		go func() {
			worker.stopErr = m.runManagedWorkerStop(ctx, worker, timeout, allowDrainGrace)
			close(worker.stopDone)
		}()
	})
	if owner {
		<-worker.stopDone
		return worker.stopErr
	}
	return waitForSharedWorkerStop(ctx, sessionID, worker, timeout)
}

func (m *WorkerManager) runManagedWorkerStop(ctx context.Context, worker *ManagedWorker, timeout time.Duration, allowDrainGrace bool) error {
	sessionID := worker.SessionID
	stopTimeout := timeout
	var waitDone <-chan error
	if allowDrainGrace {
		waitDone = m.beginWorkerStopWait(sessionID, worker)
		graceTimer := time.NewTimer(timeout)
		select {
		case waitErr := <-waitDone:
			graceTimer.Stop()
			return waitErr
		case <-ctx.Done():
			graceTimer.Stop()
		case <-graceTimer.C:
		}
		stopTimeout = drainingSignalTimeout(ctx, timeout)
	}
	initialStopForced, err := requestWorkerStop(worker.Cmd.Process)
	if err == nil && initialStopForced {
		m.recordForcedStop(worker)
	}
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) && worker.done == nil {
			m.registry.ForgetWorkerIdentity(sessionID, managedWorkerPID(worker), worker.Spec.Token)
			m.forgetWorker(worker)
			return nil
		}
		if !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("request stop for session worker %s: %w", sessionID, err)
		}
	}
	if waitDone == nil {
		waitDone = m.beginWorkerStopWait(sessionID, worker)
	}
	deadline := time.Now().Add(stopTimeout)
	timer := time.NewTimer(stopTimeout)
	defer timer.Stop()
	processExited := worker.processExitedSignal()
	if processExited == nil {
		processExited = make(chan struct{})
	}
	select {
	case <-ctx.Done():
		return errors.Join(ctx.Err(), m.forceStopWorker(ctx, sessionID, worker, waitDone))
	case <-timer.C:
		forceCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return m.forceStopWorker(forceCtx, sessionID, worker, waitDone)
	case <-processExited:
		return m.waitForWorkerCleanup(ctx, sessionID, worker, waitDone, time.Until(deadline))
	case waitErr := <-waitDone:
		return waitErr
	}
}

// drainingSignalTimeout leaves half of the shared shutdown time for force,
// process reap, and managed cleanup after the graceful signal phase.
func drainingSignalTimeout(ctx context.Context, configured time.Duration) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return configured
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	signalBudget := remaining / 2
	if signalBudget <= 0 {
		signalBudget = time.Nanosecond
	}
	if signalBudget < configured {
		return signalBudget
	}
	return configured
}

func (m *WorkerManager) beginWorkerStopWait(sessionID string, worker *ManagedWorker) <-chan error {
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- m.finishWorkerStop(sessionID, worker, worker.Wait())
	}()
	return waitDone
}

func waitForSharedWorkerStop(ctx context.Context, sessionID string, worker *ManagedWorker, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("wait for shared stop of session worker %s: %w", sessionID, context.DeadlineExceeded)
	case <-worker.stopDone:
		return worker.stopErr
	}
}

func (m *WorkerManager) forceStopWorker(ctx context.Context, sessionID string, worker *ManagedWorker, waitDone <-chan error) error {
	m.recordForcedStop(worker)
	if err := worker.Cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill session worker %s: %w", sessionID, err)
	}
	processExited := worker.processExitedSignal()
	if processExited == nil {
		select {
		case waitErr := <-waitDone:
			return waitErr
		case <-ctx.Done():
			return fmt.Errorf("reap session worker %s after kill: %w", sessionID, ctx.Err())
		}
	}
	select {
	case <-processExited:
		return m.waitForWorkerCleanup(ctx, sessionID, worker, waitDone, -1)
	case <-ctx.Done():
		return fmt.Errorf("reap session worker %s after kill: %w", sessionID, ctx.Err())
	}
}

func (m *WorkerManager) StopAll(ctx context.Context, timeout time.Duration) error {
	drainErr := m.closeAdmissionAndWait(ctx)
	m.mu.Lock()
	if drainErr != nil {
		m.abortStarts = true
	}
	workers := make([]*ManagedWorker, 0, len(m.active))
	seen := make(map[*ManagedWorker]struct{}, len(m.active))
	for _, worker := range m.active {
		if worker == nil {
			continue
		}
		if _, ok := seen[worker]; ok {
			continue
		}
		seen[worker] = struct{}{}
		workers = append(workers, worker)
	}
	m.mu.Unlock()

	stopErrors := make(chan error, len(workers))
	var stopped sync.WaitGroup
	stopped.Add(len(workers))
	for _, worker := range workers {
		go func(worker *ManagedWorker) {
			defer stopped.Done()
			if err := m.stopWorker(ctx, worker, timeout); err != nil {
				stopErrors <- err
			}
		}(worker)
	}
	stopped.Wait()
	close(stopErrors)
	errs := make([]error, 0, len(workers)+1)
	if drainErr != nil {
		errs = append(errs, drainErr)
	}
	for err := range stopErrors {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *WorkerManager) waitForWorkerCleanup(ctx context.Context, sessionID string, worker *ManagedWorker, waitDone <-chan error, timeout time.Duration) error {
	if timeout < 0 {
		select {
		case waitErr := <-waitDone:
			return waitErr
		case <-ctx.Done():
			return errors.Join(ctx.Err(), fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending))
		}
	}
	if timeout <= 0 {
		select {
		case waitErr := <-waitDone:
			return waitErr
		default:
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending))
		}
		return fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case waitErr := <-waitDone:
		return waitErr
	case <-ctx.Done():
		return errors.Join(ctx.Err(), fmt.Errorf("session worker %s cleanup: %w", sessionID, ErrWorkerCleanupPending))
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
		m.mu.Lock()
		m.abortStarts = true
		m.mu.Unlock()
		return ctx.Err()
	}
}

// workerStartAbortError is called only while m.mu is held. A start admitted
// before shutdown may finish inside the drain budget, but it must not commit
// after that budget expires or after its own request is canceled.
func (m *WorkerManager) workerStartAbortError(ctx context.Context) error {
	if m.abortStarts {
		return ErrWorkerManagerStopping
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
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
		if worker != nil && worker.Cmd != nil && worker.Cmd.Process != nil && int64(worker.Cmd.Process.Pid) == identity.PID && worker.Spec.Token == identity.token {
			return worker
		}
	}
	return nil
}

func (m *WorkerManager) forgetWorker(worker *ManagedWorker) {
	if worker == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, active := range m.active {
		if active == worker {
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
