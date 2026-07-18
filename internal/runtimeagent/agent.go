package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type WorkerStarter interface {
	StartSessionWorker(ctx context.Context, sessionID string, extraEnv []string) (*ManagedWorker, error)
}

type WorkerStopper interface {
	StopSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error
}

type WorkerExitWaiter interface {
	WaitSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error
}

type WorkerDrainer interface {
	MarkSessionWorkerDraining(sessionID string)
}

type PlatformInvoker interface {
	InvokePlatformAny(ctx context.Context, method string, request *anypb.Any, timeout time.Duration) (*anypb.Any, error)
}

type WorkerSender interface {
	SendToWorker(sessionID string, frame *rwv1.AgentFrame) error
}

type RuntimeRequestError struct {
	Code            string
	Message         string
	DependencyError *strategyv1.RuntimeDependencyError
}

func (e *RuntimeRequestError) Error() string {
	if e == nil || strings.TrimSpace(e.Message) == "" {
		return "runtime worker request failed"
	}
	return e.Message
}

type AgentConfig struct {
	RuntimeID                string
	RuntimeSource            string
	RuntimeName              string
	UserID                   int64
	WorkerStarter            WorkerStarter
	WorkerStopper            WorkerStopper
	PlatformInvoker          PlatformInvoker
	WorkerSender             WorkerSender
	StartTimeout             time.Duration
	RequestTimeout           time.Duration
	IndicatorLimit           int
	IndicatorFlushInterval   time.Duration
	IndicatorFinalizeTimeout time.Duration
	IndicatorRetryInitial    time.Duration
	IndicatorRetryMax        time.Duration
}

type Agent struct {
	cfg AgentConfig

	mu                sync.Mutex
	nextGeneration    uint64
	generations       map[string]*workerGeneration
	pending           map[string]*pendingSessionStart
	ready             map[string]chan struct{}
	workerCallReply   map[string]chan *rwv1.PlatformCallResult
	workerCallSession map[string]string
	runRequests       map[string]*anypb.Any
	indicatorSync     *IndicatorSyncManager
}

type workerGeneration struct {
	sessionID  string
	generation uint64

	mu                 sync.Mutex
	closing            bool
	inFlight           int
	drained            chan struct{}
	drainOnce          sync.Once
	durablePossible    bool
	runningAccepted    bool
	connected          bool
	terminalAck        bool
	explicitStopAck    bool
	explicitStopStatus string
	cleanupRunning     bool
	cleanupDone        chan struct{}
	cleanupErr         error
	authGeneration     uint64
}

func newWorkerGeneration(sessionID string, generation uint64) *workerGeneration {
	return &workerGeneration{
		sessionID: sessionID, generation: generation, drained: make(chan struct{}),
	}
}

func (g *workerGeneration) admit(method string) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	if strings.TrimSpace(method) == "portfolio.SaveSession" {
		g.durablePossible = true
	}
	g.inFlight++
	return true
}

func (g *workerGeneration) completePlatformCall() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.inFlight > 0 {
		g.inFlight--
	}
	if g.closing && g.inFlight == 0 {
		g.drainOnce.Do(func() { close(g.drained) })
	}
	g.mu.Unlock()
}

func (g *workerGeneration) beginCleanup() (bool, <-chan struct{}) {
	if g == nil {
		done := make(chan struct{})
		close(done)
		return false, done
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closing = true
	if g.inFlight == 0 {
		g.drainOnce.Do(func() { close(g.drained) })
	}
	if g.cleanupRunning {
		return false, g.cleanupDone
	}
	g.cleanupRunning = true
	g.cleanupErr = nil
	g.cleanupDone = make(chan struct{})
	return true, g.cleanupDone
}

func (g *workerGeneration) finishCleanup(err error) {
	g.mu.Lock()
	g.cleanupErr = err
	g.cleanupRunning = false
	done := g.cleanupDone
	g.cleanupDone = nil
	g.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func (g *workerGeneration) cleanupResult() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cleanupErr
}

func (g *workerGeneration) bindAuthenticatedGeneration(generation uint64) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if generation == 0 {
		return false
	}
	if g.authGeneration == 0 {
		g.authGeneration = generation
		return true
	}
	return generation == g.authGeneration
}

func (g *workerGeneration) acceptRunning() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing || g.terminalAck || g.explicitStopAck {
		return false
	}
	g.runningAccepted = true
	return true
}

func (g *workerGeneration) markConnected() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.connected = true
	return true
}

type pendingSessionStart struct {
	started chan string
	failed  chan *RuntimeRequestError
	start   *rwv1.StartSession
}

func NewAgent(cfg AgentConfig) *Agent {
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = 30 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	agent := &Agent{
		cfg:               cfg,
		generations:       map[string]*workerGeneration{},
		pending:           map[string]*pendingSessionStart{},
		ready:             map[string]chan struct{}{},
		workerCallReply:   map[string]chan *rwv1.PlatformCallResult{},
		workerCallSession: map[string]string{},
		runRequests:       map[string]*anypb.Any{},
	}
	agent.indicatorSync = NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: cfg.PlatformInvoker,
		IndicatorLimit:  cfg.IndicatorLimit,
		FlushInterval:   cfg.IndicatorFlushInterval,
		RequestTimeout:  cfg.RequestTimeout,
		FinalizeTimeout: cfg.IndicatorFinalizeTimeout,
		RetryInitial:    cfg.IndicatorRetryInitial,
		RetryMax:        cfg.IndicatorRetryMax,
	})
	return agent
}

func (a *Agent) RunSyncLoop(ctx context.Context) {
	a.indicatorSync.Run(ctx)
}

type RestartSessionOptions struct {
	SessionID       string
	MaxLossClosePct float64
	Leverage        float64
}

type RestartSessionResult struct {
	OldSessionID string `json:"old_session_id"`
	NewSessionID string `json:"new_session_id"`
	RuntimeID    string `json:"runtime_id"`
}

func (a *Agent) SetWorkerSender(sender WorkerSender) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg.WorkerSender = sender
}

func (a *Agent) HandleRuntimeRequest(ctx context.Context, frame *cpv1.RuntimeFrame) *cpv1.RuntimeFrame {
	req := frame.GetRequest()
	if req == nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", "runtime request payload is empty")
	}
	switch strings.TrimSpace(req.GetMethod()) {
	case "RunStrategy":
		return a.handleRunStrategy(ctx, frame, req)
	case "PreviewRunStrategy", "ValidateStrategySource":
		return a.handleOneShotRuntimeUnary(ctx, frame, req)
	case "GetStrategyStatus", "StopStrategy":
		return a.handleSessionRuntimeUnary(ctx, frame, req)
	default:
		return runtimeErrorFrame(frame.GetCorrelationId(), "Unimplemented", "unsupported strategy method: "+req.GetMethod())
	}
}

func (a *Agent) handleRunStrategy(
	ctx context.Context,
	frame *cpv1.RuntimeFrame,
	req *cpv1.StrategyRequest,
) *cpv1.RuntimeFrame {
	var runReq strategyv1.RunStrategyRequest
	if req.GetRequest() == nil || req.GetRequest().UnmarshalTo(&runReq) != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", "invalid RunStrategy request payload")
	}
	if a.cfg.WorkerStarter == nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "FailedPrecondition", "worker starter is not configured")
	}
	sessionID, generation := a.reserveWorkerGeneration()
	runtimeID := strings.TrimSpace(runReq.GetRuntimeId())
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(a.cfg.RuntimeID)
	}
	start := &rwv1.StartSession{
		SessionId:          sessionID,
		UserId:             runReq.GetUserId(),
		RuntimeId:          runtimeID,
		RunStrategyRequest: req.GetRequest(),
	}
	pending := &pendingSessionStart{
		started: make(chan string, 1),
		failed:  make(chan *RuntimeRequestError, 1),
		start:   start,
	}
	a.mu.Lock()
	a.pending[sessionID] = pending
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.pending, sessionID)
		a.mu.Unlock()
	}()

	worker, err := a.cfg.WorkerStarter.StartSessionWorker(ctx, sessionID, a.workerEnv())
	if err != nil {
		a.forgetWorkerGeneration(sessionID, generation)
		return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", err.Error())
	}
	if worker != nil && worker.Spec.Generation > 0 && !generation.bindAuthenticatedGeneration(worker.Spec.Generation) {
		_ = a.cleanupWorkerGeneration(sessionID, generation, "worker generation identity mismatch")
		return runtimeErrorFrame(frame.GetCorrelationId(), "FailedPrecondition", "worker generation identity mismatch")
	}
	a.watchWorkerGeneration(sessionID, generation, worker)

	workerExited := worker.processExitedSignal()
	timer := time.NewTimer(a.cfg.StartTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		_ = a.cleanupWorkerGeneration(sessionID, generation, "runtime request cancelled")
		return runtimeErrorFrame(frame.GetCorrelationId(), "Cancelled", ctx.Err().Error())
	case <-timer.C:
		_ = a.cleanupWorkerGeneration(sessionID, generation, "session worker start timed out")
		return runtimeErrorFrame(frame.GetCorrelationId(), "DeadlineExceeded", "session worker did not report started")
	case <-workerExited:
		select {
		case sessionID := <-pending.started:
			return responseFrame(frame.GetCorrelationId(), &strategyv1.RunStrategyResponse{SessionId: sessionID})
		default:
		}
		select {
		case requestErr := <-pending.failed:
			_ = a.cleanupWorkerGeneration(sessionID, generation, requestErr.Error())
			return runtimeRequestErrorFrame(frame.GetCorrelationId(), requestErr)
		default:
		}
		_ = a.cleanupWorkerGeneration(sessionID, generation, "session worker exited before reporting started")
		return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", managedWorkerExitError("reporting started", worker.processError()).Error())
	case requestErr := <-pending.failed:
		select {
		case startedSessionID := <-pending.started:
			return responseFrame(frame.GetCorrelationId(), &strategyv1.RunStrategyResponse{SessionId: startedSessionID})
		default:
		}
		_ = a.cleanupWorkerGeneration(sessionID, generation, requestErr.Error())
		return runtimeRequestErrorFrame(frame.GetCorrelationId(), requestErr)
	case startedSessionID := <-pending.started:
		return responseFrame(frame.GetCorrelationId(), &strategyv1.RunStrategyResponse{SessionId: startedSessionID})
	}
}

func (a *Agent) watchWorkerGeneration(
	sessionID string,
	generation *workerGeneration,
	worker *ManagedWorker,
) {
	if worker == nil || worker.processExitedSignal() == nil {
		return
	}
	go func() {
		<-worker.processExitedSignal()
		generation.mu.Lock()
		connected := generation.connected
		generation.mu.Unlock()
		if connected {
			return
		}
		_ = a.cleanupWorkerGeneration(sessionID, generation, "session worker process exited")
	}()
}

func (a *Agent) reserveWorkerGeneration() (string, *workerGeneration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		sessionID := mustRandomToken()[:32]
		if _, exists := a.generations[sessionID]; exists {
			continue
		}
		a.nextGeneration++
		generation := newWorkerGeneration(sessionID, a.nextGeneration)
		a.generations[sessionID] = generation
		return sessionID, generation
	}
}

func (a *Agent) forgetWorkerGeneration(sessionID string, generation *workerGeneration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.generations[sessionID] != generation {
		return false
	}
	delete(a.generations, sessionID)
	return true
}

func (a *Agent) RestartSession(ctx context.Context, opts RestartSessionOptions) (RestartSessionResult, error) {
	if a.cfg.PlatformInvoker == nil {
		return RestartSessionResult{}, fmt.Errorf("platform invoker is not configured")
	}
	if a.cfg.WorkerStarter == nil {
		return RestartSessionResult{}, fmt.Errorf("worker starter is not configured")
	}
	runtimeID := strings.TrimSpace(a.cfg.RuntimeID)
	if runtimeID == "" {
		return RestartSessionResult{}, fmt.Errorf("runtime_id is required")
	}
	session, err := a.resolveRestartSession(ctx, opts)
	if err != nil {
		return RestartSessionResult{}, err
	}
	oldSessionID := strings.TrimSpace(session.GetSessionId())
	if oldSessionID == "" {
		return RestartSessionResult{}, fmt.Errorf("session_id is required")
	}
	if session.GetRuntimeId() != "" && session.GetRuntimeId() != runtimeID {
		return RestartSessionResult{}, fmt.Errorf("session %s belongs to runtime %s, not %s", oldSessionID, session.GetRuntimeId(), runtimeID)
	}
	if a.cfg.UserID > 0 && session.GetUserId() > 0 && session.GetUserId() != a.cfg.UserID {
		return RestartSessionResult{}, fmt.Errorf("session %s belongs to user %d, not %d", oldSessionID, session.GetUserId(), a.cfg.UserID)
	}
	userID := session.GetUserId()
	if userID == 0 {
		userID = a.cfg.UserID
	}
	leverage := session.GetLeverage()
	if opts.Leverage > 0 {
		leverage = opts.Leverage
	}
	runReq := a.restartRunRequest(session, runtimeID)
	runReq.UserId = userID
	runReq.RuntimeId = runtimeID
	if opts.MaxLossClosePct > 0 {
		runReq.MaxLossClosePct = opts.MaxLossClosePct
	}
	if leverage > 0 {
		runReq.Leverage = leverage
	}
	if a.cfg.WorkerStopper != nil {
		if err := a.cfg.WorkerStopper.StopSessionWorker(ctx, oldSessionID, 5*time.Second); err != nil {
			return RestartSessionResult{}, err
		}
	}
	a.cleanupSessionState(oldSessionID, "bare debug worker restarted locally")
	if err := a.markSessionRecoverable(ctx, session, runtimeID); err != nil {
		return RestartSessionResult{}, err
	}
	packed, err := anypb.New(runReq)
	if err != nil {
		return RestartSessionResult{}, err
	}
	frame := a.handleRunStrategy(ctx, &cpv1.RuntimeFrame{CorrelationId: "local-restart-" + oldSessionID}, &cpv1.StrategyRequest{
		Method:  "RunStrategy",
		Request: packed,
	})
	if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_ERROR {
		errFrame := frame.GetError()
		if errFrame == nil {
			return RestartSessionResult{}, fmt.Errorf("restart run failed")
		}
		return RestartSessionResult{}, fmt.Errorf("%s: %s", errFrame.GetCode(), errFrame.GetMessage())
	}
	var resp strategyv1.RunStrategyResponse
	if frame.GetResponse() == nil || frame.GetResponse().GetResponse() == nil {
		return RestartSessionResult{}, fmt.Errorf("restart run response payload is empty")
	}
	if err := frame.GetResponse().GetResponse().UnmarshalTo(&resp); err != nil {
		return RestartSessionResult{}, fmt.Errorf("unpack restart response: %w", err)
	}
	if strings.TrimSpace(resp.GetSessionId()) == "" {
		return RestartSessionResult{}, fmt.Errorf("restart run returned empty session_id")
	}
	return RestartSessionResult{
		OldSessionID: oldSessionID,
		NewSessionID: resp.GetSessionId(),
		RuntimeID:    runtimeID,
	}, nil
}

func (a *Agent) resolveRestartSession(ctx context.Context, opts RestartSessionOptions) (*portfoliov1.StrategySessionEntry, error) {
	sessionID := strings.TrimSpace(opts.SessionID)
	if sessionID != "" {
		cached := a.sessionFromCachedRunRequest(sessionID)
		req := &portfoliov1.GetSessionRequest{
			SessionId: sessionID,
			UserId:    a.cfg.UserID,
		}
		var resp portfoliov1.GetSessionResponse
		if err := a.invokePlatformProto(ctx, "portfolio.GetSession", req, &resp); err != nil {
			if cached != nil {
				return cached, nil
			}
			return nil, err
		}
		if resp.GetSession() == nil {
			if cached != nil {
				return cached, nil
			}
			return nil, fmt.Errorf("session not found: %s", sessionID)
		}
		return resp.GetSession(), nil
	}
	for _, statusValue := range []string{"running", "recoverable"} {
		req := &portfoliov1.ListSessionsRequest{
			Limit:     1,
			RuntimeId: strings.TrimSpace(a.cfg.RuntimeID),
			Status:    statusValue,
			UserId:    a.cfg.UserID,
		}
		var resp portfoliov1.ListSessionsResponse
		if err := a.invokePlatformProto(ctx, "portfolio.ListSessions", req, &resp); err != nil {
			return nil, err
		}
		if len(resp.GetSessions()) > 0 && resp.GetSessions()[0] != nil {
			return resp.GetSessions()[0], nil
		}
	}
	return nil, fmt.Errorf("no running or recoverable session found for runtime %s", strings.TrimSpace(a.cfg.RuntimeID))
}

func (a *Agent) markSessionRecoverable(ctx context.Context, session *portfoliov1.StrategySessionEntry, runtimeID string) error {
	req := &portfoliov1.UpdateSessionRequest{
		SessionId:     session.GetSessionId(),
		Status:        "recoverable",
		BarsProcessed: session.GetBarsProcessed(),
		Error:         "bare debug worker restarted locally",
		RuntimeId:     runtimeID,
	}
	var resp portfoliov1.UpdateSessionResponse
	return a.invokePlatformProto(ctx, "portfolio.UpdateSession", req, &resp)
}

func (a *Agent) invokePlatformProto(ctx context.Context, method string, req proto.Message, resp proto.Message) error {
	packed, err := anypb.New(req)
	if err != nil {
		return err
	}
	out, err := a.cfg.PlatformInvoker.InvokePlatformAny(ctx, method, packed, a.cfg.RequestTimeout)
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	if out == nil {
		return fmt.Errorf("platform response payload is empty")
	}
	if err := out.UnmarshalTo(resp); err != nil {
		return fmt.Errorf("unpack %s response: %w", method, err)
	}
	return nil
}

func (a *Agent) cleanupSessionState(sessionID string, reason string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	var replies []chan *rwv1.PlatformCallResult
	a.mu.Lock()
	delete(a.pending, sessionID)
	delete(a.ready, sessionID)
	delete(a.runRequests, sessionID)
	for callID, callSessionID := range a.workerCallSession {
		if callSessionID != sessionID {
			continue
		}
		if reply := a.workerCallReply[callID]; reply != nil {
			replies = append(replies, reply)
		}
		delete(a.workerCallReply, callID)
		delete(a.workerCallSession, callID)
	}
	a.mu.Unlock()
	a.indicatorSync.ForgetSession(context.Background(), sessionID)
	if strings.TrimSpace(reason) == "" {
		reason = "session worker was restarted"
	}
	for _, reply := range replies {
		select {
		case reply <- &rwv1.PlatformCallResult{Ok: false, Error: reason}:
		default:
		}
	}
}

func (a *Agent) HandleWorkerDisconnect(identity WorkerIdentity, cause error) error {
	sessionID := strings.TrimSpace(identity.SessionID)
	if sessionID == "" {
		return nil
	}
	a.mu.Lock()
	generation := a.generations[sessionID]
	a.mu.Unlock()
	if generation == nil {
		return nil
	}
	if !generation.bindAuthenticatedGeneration(identity.Generation) {
		return nil
	}
	generation.mu.Lock()
	generation.connected = false
	generation.mu.Unlock()
	reason := "session worker disconnected"
	if cause == nil {
		reason = "session worker stream closed"
	}
	return a.cleanupWorkerGeneration(sessionID, generation, reason)
}

func (a *Agent) cleanupWorkerGeneration(
	sessionID string,
	generation *workerGeneration,
	reason string,
) error {
	owner, done := generation.beginCleanup()
	if !owner {
		if done != nil {
			<-done
		}
		return generation.cleanupResult()
	}
	timeout := a.cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var cleanupErr error
	select {
	case <-generation.drained:
		cleanupErr = a.reconcileWorkerGeneration(ctx, sessionID, generation, reason)
	case <-ctx.Done():
		cleanupErr = fmt.Errorf("drain worker generation platform calls: %w", ctx.Err())
	}
	if cleanupErr == nil && a.cfg.WorkerStopper != nil {
		cleanupErr = a.cfg.WorkerStopper.StopSessionWorker(ctx, sessionID, timeout)
	}
	if cleanupErr == nil {
		if a.forgetWorkerGeneration(sessionID, generation) {
			a.cleanupSessionState(sessionID, reason)
		}
	}
	generation.finishCleanup(cleanupErr)
	if cleanupErr != nil {
		a.scheduleWorkerGenerationCleanup(sessionID, generation, reason)
	}
	return cleanupErr
}

func (a *Agent) scheduleWorkerGenerationCleanup(
	sessionID string,
	generation *workerGeneration,
	reason string,
) {
	delay := 250 * time.Millisecond
	if a.cfg.RequestTimeout >= 100*time.Millisecond && a.cfg.RequestTimeout < delay {
		delay = a.cfg.RequestTimeout
	}
	time.AfterFunc(delay, func() {
		a.mu.Lock()
		current := a.generations[sessionID]
		a.mu.Unlock()
		if current == generation {
			_ = a.cleanupWorkerGeneration(sessionID, generation, reason)
		}
	})
}

func (a *Agent) reconcileWorkerGeneration(
	ctx context.Context,
	sessionID string,
	generation *workerGeneration,
	reason string,
) error {
	generation.mu.Lock()
	durablePossible := generation.durablePossible
	terminalAcknowledged := generation.terminalAck
	explicitStopAcknowledged := generation.explicitStopAck
	explicitStopStatus := generation.explicitStopStatus
	generation.mu.Unlock()
	if terminalAcknowledged || !durablePossible {
		return nil
	}
	if a.cfg.PlatformInvoker == nil {
		return fmt.Errorf("platform invoker is not configured")
	}
	var response portfoliov1.GetSessionResponse
	err := a.invokePlatformProto(ctx, "portfolio.GetSession", &portfoliov1.GetSessionRequest{
		SessionId: sessionID, UserId: a.cfg.UserID,
	}, &response)
	if err != nil {
		if isExplicitPlatformNotFound(err) {
			return nil
		}
		return err
	}
	session := response.GetSession()
	if session == nil {
		return fmt.Errorf("reconciliation response is missing session")
	}
	if strings.TrimSpace(session.GetSessionId()) != sessionID {
		return fmt.Errorf("reconciliation returned mismatched session_id")
	}
	if runtimeID := strings.TrimSpace(session.GetRuntimeId()); runtimeID != "" && runtimeID != strings.TrimSpace(a.cfg.RuntimeID) {
		return fmt.Errorf("reconciliation returned mismatched runtime_id")
	}
	if a.cfg.UserID > 0 && session.GetUserId() > 0 && session.GetUserId() != a.cfg.UserID {
		return fmt.Errorf("reconciliation returned mismatched user_id")
	}
	statusValue := strings.TrimSpace(strings.ToLower(session.GetStatus()))
	switch statusValue {
	case "finished", "stopped", "failed", "stop_failed", "recoverable":
		return nil
	case "pending", "running", "stopping":
		message := strings.TrimSpace(reason)
		if message == "" {
			message = "session worker disconnected"
		}
		targetStatus := "failed"
		if explicitStopAcknowledged && statusValue != "pending" {
			targetStatus = strings.TrimSpace(strings.ToLower(explicitStopStatus))
			if targetStatus == "" {
				targetStatus = "stopped"
			}
			message = ""
		}
		return a.updateSession(ctx, sessionID, targetStatus, int64(session.GetBarsProcessed()), message)
	default:
		return fmt.Errorf("cannot reconcile session %s from status %q", sessionID, session.GetStatus())
	}
}

func isExplicitPlatformNotFound(err error) bool {
	if err == nil {
		return false
	}
	if grpcstatus.Code(err) == codes.NotFound {
		return true
	}
	code, _, found := strings.Cut(strings.TrimSpace(err.Error()), ":")
	return found && strings.EqualFold(strings.TrimSpace(code), "NotFound")
}

func (a *Agent) handleOneShotRuntimeUnary(
	ctx context.Context,
	frame *cpv1.RuntimeFrame,
	req *cpv1.StrategyRequest,
) *cpv1.RuntimeFrame {
	if req.GetRequest() == nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", "runtime request payload is empty")
	}
	if a.cfg.WorkerStarter == nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "FailedPrecondition", "worker starter is not configured")
	}
	pendingID := "control-" + mustRandomToken()[:16]
	ready := make(chan struct{}, 1)
	a.mu.Lock()
	a.ready[pendingID] = ready
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.ready, pendingID)
		a.mu.Unlock()
	}()

	worker, err := a.cfg.WorkerStarter.StartSessionWorker(ctx, pendingID, a.workerEnv())
	if err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", err.Error())
	}
	cleanupRequired := true
	defer func() {
		if cleanupRequired {
			a.cleanupOneShotWorker(pendingID, a.timeoutForFrame(frame))
		}
	}()
	if err := a.waitWorkerReady(ctx, ready, worker, a.timeoutForFrame(frame)); err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
	}
	resp, err := a.invokeWorkerUnary(ctx, pendingID, req.GetMethod(), req.GetRequest(), a.timeoutForFrame(frame))
	if err != nil {
		return runtimeRequestErrorFrame(frame.GetCorrelationId(), err)
	}
	switch strings.TrimSpace(req.GetMethod()) {
	case "PreviewRunStrategy":
		var previewResp strategyv1.PreviewRunStrategyResponse
		if err := resp.UnmarshalTo(&previewResp); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", "invalid PreviewRunStrategy response payload")
		}
	case "ValidateStrategySource":
		var validateResp strategyv1.ValidateStrategySourceResponse
		if err := resp.UnmarshalTo(&validateResp); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", "invalid ValidateStrategySource response payload")
		}
	}
	if waiter, ok := a.cfg.WorkerStarter.(WorkerExitWaiter); ok {
		if err := waiter.WaitSessionWorker(ctx, pendingID, a.timeoutForFrame(frame)); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
		}
		a.cleanupSessionState(pendingID, "one-shot worker completed")
	} else {
		a.cleanupOneShotWorker(pendingID, a.timeoutForFrame(frame))
	}
	cleanupRequired = false
	return responseAnyFrame(frame.GetCorrelationId(), resp)
}

func (a *Agent) cleanupOneShotWorker(sessionID string, timeout time.Duration) {
	if timeout <= 0 || timeout > a.cfg.RequestTimeout {
		timeout = a.cfg.RequestTimeout
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if a.cfg.WorkerStopper != nil {
		_ = a.cfg.WorkerStopper.StopSessionWorker(ctx, sessionID, timeout)
	}
	if waiter, ok := a.cfg.WorkerStarter.(WorkerExitWaiter); ok {
		_ = waiter.WaitSessionWorker(ctx, sessionID, timeout)
	} else if waiter, ok := a.cfg.WorkerStopper.(WorkerExitWaiter); ok {
		_ = waiter.WaitSessionWorker(ctx, sessionID, timeout)
	}
	a.cleanupSessionState(sessionID, "one-shot worker completed")
}

func (a *Agent) handleSessionRuntimeUnary(
	ctx context.Context,
	frame *cpv1.RuntimeFrame,
	req *cpv1.StrategyRequest,
) *cpv1.RuntimeFrame {
	sessionID, err := runtimeRequestSessionID(req)
	if err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
	}
	a.mu.Lock()
	generation := a.generations[sessionID]
	a.mu.Unlock()
	controlCallAdmitted := false
	if generation != nil {
		if !generation.admit("runtime." + strings.TrimSpace(req.GetMethod())) {
			return runtimeErrorFrame(frame.GetCorrelationId(), "Unavailable", "worker generation is closing")
		}
		controlCallAdmitted = true
		defer func() {
			if controlCallAdmitted {
				generation.completePlatformCall()
			}
		}()
	}
	resp, err := a.invokeWorkerUnary(ctx, sessionID, req.GetMethod(), req.GetRequest(), a.timeoutForFrame(frame))
	if err != nil {
		return runtimeRequestErrorFrame(frame.GetCorrelationId(), err)
	}
	if strings.TrimSpace(req.GetMethod()) == "StopStrategy" {
		var stopResp strategyv1.StopStrategyResponse
		if err := resp.UnmarshalTo(&stopResp); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", "invalid StopStrategy response payload")
		}
		if stopResp.GetStopped() {
			var stopReq strategyv1.StopStrategyRequest
			explicitStopStatus := "stopped"
			if err := req.GetRequest().UnmarshalTo(&stopReq); err == nil &&
				stopReq.GetStopAction() == strategyv1.StopAction_STOP_ACTION_FINISH {
				// FINISH is only durable after the Agent flushes indicators while
				// acknowledging FinalStatus. A worker lost in the response-to-final
				// gap must remain recoverable instead of claiming a complete finish.
				explicitStopStatus = "recoverable"
			}
			if generation != nil {
				generation.mu.Lock()
				generation.explicitStopAck = true
				generation.explicitStopStatus = explicitStopStatus
				generation.mu.Unlock()
			}
			if controlCallAdmitted {
				generation.completePlatformCall()
				controlCallAdmitted = false
			}
			if waiter, ok := a.cfg.WorkerStopper.(WorkerExitWaiter); ok {
				if err := waiter.WaitSessionWorker(ctx, sessionID, a.timeoutForFrame(frame)); err != nil {
					return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
				}
			}
		}
	}
	if controlCallAdmitted {
		generation.completePlatformCall()
		controlCallAdmitted = false
	}
	return responseAnyFrame(frame.GetCorrelationId(), resp)
}

func (a *Agent) HandleWorkerFrame(
	ctx context.Context,
	workerSessionID string,
	frame *rwv1.WorkerFrame,
	send func(*rwv1.AgentFrame) error,
) error {
	if frame == nil {
		return nil
	}
	a.mu.Lock()
	generation := a.generations[strings.TrimSpace(workerSessionID)]
	a.mu.Unlock()
	switch frame.GetPayload().(type) {
	case *rwv1.WorkerFrame_Hello:
		a.mu.Lock()
		ready := a.ready[workerSessionID]
		a.mu.Unlock()
		if ready != nil {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		a.mu.Lock()
		pending := a.pending[workerSessionID]
		a.mu.Unlock()
		if pending != nil && send != nil {
			return send(&rwv1.AgentFrame{
				Payload: &rwv1.AgentFrame_StartSession{StartSession: pending.start},
			})
		}
	case *rwv1.WorkerFrame_Progress:
		progress := frame.GetProgress()
		realSessionID := strings.TrimSpace(progress.GetSessionId())
		if generation != nil && realSessionID != "" && realSessionID != strings.TrimSpace(workerSessionID) {
			if pending := a.pendingGenerationStart(workerSessionID); pending != nil {
				select {
				case pending.failed <- &RuntimeRequestError{
					Code: "FailedPrecondition", Message: "worker returned mismatched canonical session_id",
				}:
				default:
				}
				return nil
			}
			return fmt.Errorf("worker progress session_id does not match authenticated generation")
		}
		statusValue := strings.TrimSpace(strings.ToLower(progress.GetStatus()))
		a.mu.Lock()
		pending := a.pending[workerSessionID]
		a.mu.Unlock()
		if pending != nil && isSessionStartFailureStatus(statusValue) {
			message := strings.TrimSpace(progress.GetError())
			if message == "" {
				message = "session worker failed before start"
			}
			select {
			case pending.failed <- &RuntimeRequestError{
				Code: "FailedPrecondition", Message: message,
				DependencyError: cloneDependencyError(progress.GetDependencyError()),
			}:
			default:
			}
			return nil
		}
		if statusValue == "running" && realSessionID != "" {
			if pending != nil {
				if generation != nil && !generation.acceptRunning() {
					select {
					case pending.failed <- &RuntimeRequestError{
						Code: "FailedPrecondition", Message: "worker generation closed before running acceptance",
					}:
					default:
					}
					return nil
				}
				a.rememberRunRequest(realSessionID, pending.start.GetRunStrategyRequest())
				select {
				case pending.started <- realSessionID:
				default:
				}
			}
		}
	case *rwv1.WorkerFrame_PlatformCall:
		call := frame.GetPlatformCall()
		if send == nil {
			return fmt.Errorf("worker platform call sender is not configured")
		}
		result := a.invokeWorkerPlatformCallForGeneration(ctx, generation, call)
		return send(&rwv1.AgentFrame{
			Payload: &rwv1.AgentFrame_PlatformCallResult{PlatformCallResult: result},
		})
	case *rwv1.WorkerFrame_PlatformCallResult:
		result := frame.GetPlatformCallResult()
		if result == nil {
			return nil
		}
		a.mu.Lock()
		reply := a.workerCallReply[strings.TrimSpace(result.GetCallId())]
		a.mu.Unlock()
		if reply != nil {
			select {
			case reply <- result:
			default:
			}
		}
	case *rwv1.WorkerFrame_IndicatorFrame:
		if generation != nil && strings.TrimSpace(frame.GetIndicatorFrame().GetSessionId()) != strings.TrimSpace(workerSessionID) {
			return fmt.Errorf("indicator frame session_id does not match authenticated generation")
		}
		return a.indicatorSync.ReceiveFrame(frame.GetIndicatorFrame())
	case *rwv1.WorkerFrame_FinalStatus:
		if generation != nil && strings.TrimSpace(frame.GetFinalStatus().GetSessionId()) != strings.TrimSpace(workerSessionID) {
			return fmt.Errorf("final status session_id does not match authenticated generation")
		}
		if err := a.handleWorkerFinalStatus(ctx, frame.GetFrameId(), frame.GetFinalStatus(), send); err != nil {
			return err
		}
		if generation != nil {
			generation.mu.Lock()
			generation.terminalAck = true
			generation.mu.Unlock()
		}
		return nil
	case *rwv1.WorkerFrame_WorkerError:
		workerErr := frame.GetWorkerError()
		if workerErr == nil {
			return nil
		}
		if generation != nil {
			errorSessionID := strings.TrimSpace(workerErr.GetSessionId())
			if errorSessionID != "" && errorSessionID != strings.TrimSpace(workerSessionID) {
				return fmt.Errorf("worker error session_id does not match authenticated generation")
			}
		}
		a.mu.Lock()
		pending := a.pending[workerSessionID]
		a.mu.Unlock()
		if pending != nil {
			message := strings.TrimSpace(workerErr.GetMessage())
			if message == "" {
				message = "session worker failed before start"
			}
			select {
			case pending.failed <- &RuntimeRequestError{
				Code: "FailedPrecondition", Message: message,
				DependencyError: cloneDependencyError(workerErr.GetDependencyError()),
			}:
			default:
			}
		}
	}
	return nil
}

func (a *Agent) pendingGenerationStart(sessionID string) *pendingSessionStart {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pending[strings.TrimSpace(sessionID)]
}

func (a *Agent) HandleAuthenticatedWorkerFrame(
	ctx context.Context,
	identity WorkerIdentity,
	frame *rwv1.WorkerFrame,
	send func(*rwv1.AgentFrame) error,
) error {
	sessionID := strings.TrimSpace(identity.SessionID)
	if sessionID == "" {
		return fmt.Errorf("authenticated worker session_id is required")
	}
	a.mu.Lock()
	generation := a.generations[sessionID]
	_, oneShot := a.ready[sessionID]
	a.mu.Unlock()
	if generation == nil && !oneShot {
		return fmt.Errorf("stale worker generation: %s", sessionID)
	}
	if generation != nil && !generation.bindAuthenticatedGeneration(identity.Generation) {
		return fmt.Errorf("stale worker generation: %s", sessionID)
	}
	if generation != nil && frame.GetHello() != nil {
		if !generation.markConnected() {
			return fmt.Errorf("worker generation is closing: %s", sessionID)
		}
	}
	return a.HandleWorkerFrame(ctx, sessionID, frame, send)
}

func (a *Agent) invokeWorkerPlatformCallForGeneration(
	ctx context.Context,
	generation *workerGeneration,
	call *rwv1.PlatformCall,
) *rwv1.PlatformCallResult {
	if generation == nil {
		return a.invokeWorkerPlatformCall(ctx, call)
	}
	if !generation.admit(call.GetMethod()) {
		return &rwv1.PlatformCallResult{
			CallId: call.GetCallId(), Ok: false, Error: "worker generation is closing",
		}
	}
	timeout := a.cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	requestedTimeout := time.Duration(call.GetTimeoutMs()) * time.Millisecond
	if requestedTimeout > 0 && requestedTimeout < timeout {
		timeout = requestedTimeout
	}
	lifecycleCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	boundedCall := call
	if call != nil && call.GetTimeoutMs() != timeout.Milliseconds() {
		boundedCall, _ = proto.Clone(call).(*rwv1.PlatformCall)
		boundedCall.TimeoutMs = timeout.Milliseconds()
	}
	result := a.invokeWorkerPlatformCall(lifecycleCtx, boundedCall)
	generation.completePlatformCall()
	return result
}

func (a *Agent) HandleRuntimeData(ctx context.Context, frame *cpv1.RuntimeFrame) error {
	_ = ctx
	agentFrame, sessionID := workerDataFrameFromRuntime(frame)
	if agentFrame == nil || strings.TrimSpace(sessionID) == "" {
		return nil
	}
	a.mu.Lock()
	sender := a.cfg.WorkerSender
	a.mu.Unlock()
	if sender == nil {
		return fmt.Errorf("worker sender is not configured")
	}
	return sender.SendToWorker(sessionID, agentFrame)
}

func (a *Agent) invokeWorkerPlatformCall(ctx context.Context, call *rwv1.PlatformCall) *rwv1.PlatformCallResult {
	if call == nil {
		return &rwv1.PlatformCallResult{Ok: false, Error: "platform call is empty"}
	}
	if a.cfg.PlatformInvoker == nil {
		return &rwv1.PlatformCallResult{
			CallId: call.GetCallId(),
			Ok:     false,
			Error:  "platform invoker is not configured",
		}
	}
	timeout := time.Duration(call.GetTimeoutMs()) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	response, err := a.cfg.PlatformInvoker.InvokePlatformAny(ctx, call.GetMethod(), call.GetRequest(), timeout)
	if err != nil {
		return &rwv1.PlatformCallResult{
			CallId: call.GetCallId(),
			Ok:     false,
			Error:  err.Error(),
		}
	}
	return &rwv1.PlatformCallResult{
		CallId:   call.GetCallId(),
		Ok:       true,
		Response: response,
	}
}

func (a *Agent) handleWorkerFinalStatus(
	ctx context.Context,
	frameID string,
	status *rwv1.FinalStatus,
	send func(*rwv1.AgentFrame) error,
) error {
	if status == nil {
		return fmt.Errorf("final status is empty")
	}
	sessionID := strings.TrimSpace(status.GetSessionId())
	if sessionID == "" {
		return fmt.Errorf("final status session_id is required")
	}
	frameID = strings.TrimSpace(frameID)
	if frameID == "" {
		return fmt.Errorf("final status frame_id is required")
	}
	if send == nil {
		return fmt.Errorf("final status sender is not configured")
	}
	if a.cfg.PlatformInvoker == nil {
		return fmt.Errorf("platform invoker is not configured")
	}
	request, err := terminalRequestFromFinalStatus(status)
	if err != nil {
		return err
	}
	if drainer, ok := a.cfg.WorkerStopper.(WorkerDrainer); ok {
		drainer.MarkSessionWorkerDraining(request.SessionID)
	}
	lifecycle := NewSessionLifecycle(a.indicatorSync, func(publishCtx context.Context, terminal TerminalRequest) error {
		return a.updateSession(
			publishCtx,
			terminal.SessionID,
			terminal.Status,
			terminal.BarsProcessed,
			terminal.Error,
		)
	})
	if err := lifecycle.Complete(ctx, request); err != nil {
		var finalizationErr *IndicatorFinalizationError
		if !errors.As(err, &finalizationErr) {
			return err
		}
		return send(&rwv1.AgentFrame{
			ReplyTo: frameID,
			Payload: &rwv1.AgentFrame_Error{Error: &rwv1.AgentError{
				Code: "INDICATOR_FINALIZATION_FAILED", Message: finalizationErr.Error(),
			}},
		})
	}
	if err := send(&rwv1.AgentFrame{ReplyTo: frameID}); err != nil {
		return err
	}
	return nil
}

func (a *Agent) updateSession(ctx context.Context, sessionID, status string, barsProcessed int64, message string) error {
	if barsProcessed < 0 {
		barsProcessed = 0
	}
	const maxInt32 = int64(1<<31 - 1)
	if barsProcessed > maxInt32 {
		barsProcessed = maxInt32
	}
	req := &portfoliov1.UpdateSessionRequest{
		SessionId: sessionID, Status: status, BarsProcessed: int32(barsProcessed),
		Error: message, RuntimeId: strings.TrimSpace(a.cfg.RuntimeID),
	}
	var response portfoliov1.UpdateSessionResponse
	return a.invokePlatformProto(ctx, "portfolio.UpdateSession", req, &response)
}

func (a *Agent) rememberRunRequest(sessionID string, request *anypb.Any) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || request == nil {
		return
	}
	cloned, ok := proto.Clone(request).(*anypb.Any)
	if !ok || cloned == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runRequests[sessionID] = cloned
}

func (a *Agent) sessionFromCachedRunRequest(sessionID string) *portfoliov1.StrategySessionEntry {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	a.mu.Lock()
	packed := a.runRequests[sessionID]
	a.mu.Unlock()
	if packed == nil {
		return nil
	}
	var runReq strategyv1.RunStrategyRequest
	if err := packed.UnmarshalTo(&runReq); err != nil {
		return nil
	}
	return &portfoliov1.StrategySessionEntry{
		SessionId:     sessionID,
		PortfolioId:   runReq.GetPortfolioId(),
		UserId:        runReq.GetUserId(),
		RuntimeId:     firstNonEmpty(runReq.GetRuntimeId(), a.cfg.RuntimeID),
		RuntimeName:   a.cfg.RuntimeName,
		RuntimeSource: a.cfg.RuntimeSource,
		Status:        "running",
		Interval:      runReq.GetInterval(),
		StartTimeMs:   runReq.GetStartTimeMs(),
		EndTimeMs:     runReq.GetEndTimeMs(),
		Leverage:      runReq.GetLeverage(),
	}
}

func (a *Agent) restartRunRequest(session *portfoliov1.StrategySessionEntry, runtimeID string) *strategyv1.RunStrategyRequest {
	req := &strategyv1.RunStrategyRequest{}
	if session == nil {
		req.RuntimeId = runtimeID
		return req
	}
	if cached := a.cachedRunRequest(session.GetSessionId()); cached != nil {
		req = cached
	}
	if session.GetPortfolioId() > 0 {
		req.PortfolioId = session.GetPortfolioId()
	}
	if strings.TrimSpace(session.GetInterval()) != "" {
		req.Interval = session.GetInterval()
	}
	if session.GetStartTimeMs() != 0 {
		req.StartTimeMs = session.GetStartTimeMs()
	}
	if session.GetEndTimeMs() != 0 {
		req.EndTimeMs = session.GetEndTimeMs()
	}
	if session.GetUserId() > 0 {
		req.UserId = session.GetUserId()
	}
	if session.GetLeverage() > 0 {
		req.Leverage = session.GetLeverage()
	}
	req.RuntimeId = runtimeID
	return req
}

func (a *Agent) cachedRunRequest(sessionID string) *strategyv1.RunStrategyRequest {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	a.mu.Lock()
	packed := a.runRequests[sessionID]
	a.mu.Unlock()
	if packed == nil {
		return nil
	}
	var runReq strategyv1.RunStrategyRequest
	if err := packed.UnmarshalTo(&runReq); err != nil {
		return nil
	}
	return &runReq
}

func responseFrame(correlationID string, message proto.Message) *cpv1.RuntimeFrame {
	packed, err := anypb.New(message)
	if err != nil {
		return runtimeErrorFrame(correlationID, "Internal", fmt.Sprintf("pack response: %v", err))
	}
	return &cpv1.RuntimeFrame{
		CorrelationId: correlationID,
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{Response: &cpv1.StrategyResponse{
			Response: packed,
		}},
	}
}

func responseAnyFrame(correlationID string, packed *anypb.Any) *cpv1.RuntimeFrame {
	if packed == nil {
		return runtimeErrorFrame(correlationID, "Internal", "runtime worker response payload is empty")
	}
	return &cpv1.RuntimeFrame{
		CorrelationId: correlationID,
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{Response: &cpv1.StrategyResponse{
			Response: packed,
		}},
	}
}

func (a *Agent) invokeWorkerUnary(
	ctx context.Context,
	sessionID string,
	method string,
	request *anypb.Any,
	timeout time.Duration,
) (*anypb.Any, error) {
	sessionID = strings.TrimSpace(sessionID)
	method = strings.TrimSpace(method)
	if sessionID == "" {
		return nil, fmt.Errorf("session_id is required")
	}
	if method == "" {
		return nil, fmt.Errorf("runtime method is required")
	}
	if request == nil {
		return nil, fmt.Errorf("runtime request payload is empty")
	}
	if timeout <= 0 {
		timeout = a.cfg.RequestTimeout
	}
	callID := mustRandomToken()
	reply := make(chan *rwv1.PlatformCallResult, 1)
	a.mu.Lock()
	sender := a.cfg.WorkerSender
	a.workerCallReply[callID] = reply
	a.workerCallSession[callID] = sessionID
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.workerCallReply, callID)
		delete(a.workerCallSession, callID)
		a.mu.Unlock()
	}()
	if sender == nil {
		return nil, fmt.Errorf("worker sender is not configured")
	}
	if err := sender.SendToWorker(sessionID, &rwv1.AgentFrame{
		Payload: &rwv1.AgentFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
			CallId:    callID,
			Method:    method,
			Request:   request,
			TimeoutMs: timeout.Milliseconds(),
		}},
	}); err != nil {
		return nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("runtime worker request timed out: %s", method)
	case result := <-reply:
		if result == nil {
			return nil, fmt.Errorf("runtime worker returned empty response")
		}
		if !result.GetOk() {
			message := strings.TrimSpace(result.GetError())
			if message == "" {
				message = "runtime worker request failed"
			}
			return nil, &RuntimeRequestError{
				Code:            "FailedPrecondition",
				Message:         message,
				DependencyError: cloneDependencyError(result.GetDependencyError()),
			}
		}
		if result.GetResponse() == nil {
			return nil, fmt.Errorf("runtime worker response payload is empty")
		}
		return result.GetResponse(), nil
	}
}

func runtimeRequestErrorFrame(correlationID string, err error) *cpv1.RuntimeFrame {
	var requestErr *RuntimeRequestError
	if errors.As(err, &requestErr) {
		code := strings.TrimSpace(requestErr.Code)
		if code == "" {
			code = "FailedPrecondition"
		}
		return runtimeErrorFrameWithDependency(
			correlationID,
			code,
			requestErr.Error(),
			cloneDependencyError(requestErr.DependencyError),
		)
	}
	if err == nil {
		return runtimeErrorFrame(correlationID, "Internal", "runtime worker request failed")
	}
	return runtimeErrorFrame(correlationID, grpcCodeForError(err), err.Error())
}

func cloneDependencyError(detail *strategyv1.RuntimeDependencyError) *strategyv1.RuntimeDependencyError {
	if detail == nil {
		return nil
	}
	cloned, _ := proto.Clone(detail).(*strategyv1.RuntimeDependencyError)
	return cloned
}

func (a *Agent) waitWorkerReady(ctx context.Context, ready <-chan struct{}, worker *ManagedWorker, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = a.cfg.RequestTimeout
	}
	workerExited := worker.processExitedSignal()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("session worker did not connect")
	case <-workerExited:
		select {
		case <-ready:
			return nil
		default:
		}
		return managedWorkerExitError("connecting", worker.processError())
	case <-ready:
		return nil
	}
}

func managedWorkerExitError(stage string, waitErr error) error {
	if waitErr == nil {
		return fmt.Errorf("session worker exited before %s", stage)
	}
	return fmt.Errorf("session worker exited before %s: %w", stage, waitErr)
}

func (a *Agent) timeoutForFrame(frame *cpv1.RuntimeFrame) time.Duration {
	if frame != nil && frame.GetDeadlineUnixMs() > 0 {
		timeout := time.Until(time.UnixMilli(frame.GetDeadlineUnixMs()))
		if timeout > 0 {
			return timeout
		}
		return time.Nanosecond
	}
	return a.cfg.RequestTimeout
}

func (a *Agent) workerEnv() []string {
	return []string{
		"HUSHINE_RUNTIME_ID=" + strings.TrimSpace(a.cfg.RuntimeID),
		"HUSHINE_RUNTIME_SOURCE=" + strings.TrimSpace(a.cfg.RuntimeSource),
		"HUSHINE_RUNTIME_NAME=" + strings.TrimSpace(a.cfg.RuntimeName),
	}
}

func runtimeRequestSessionID(req *cpv1.StrategyRequest) (string, error) {
	if req == nil || req.GetRequest() == nil {
		return "", fmt.Errorf("runtime request payload is empty")
	}
	switch strings.TrimSpace(req.GetMethod()) {
	case "GetStrategyStatus":
		var statusReq strategyv1.GetStrategyStatusRequest
		if err := req.GetRequest().UnmarshalTo(&statusReq); err != nil {
			return "", fmt.Errorf("invalid GetStrategyStatus request payload")
		}
		if strings.TrimSpace(statusReq.GetSessionId()) == "" {
			return "", fmt.Errorf("session_id is required")
		}
		return strings.TrimSpace(statusReq.GetSessionId()), nil
	case "StopStrategy":
		var stopReq strategyv1.StopStrategyRequest
		if err := req.GetRequest().UnmarshalTo(&stopReq); err != nil {
			return "", fmt.Errorf("invalid StopStrategy request payload")
		}
		if strings.TrimSpace(stopReq.GetSessionId()) == "" {
			return "", fmt.Errorf("session_id is required")
		}
		return strings.TrimSpace(stopReq.GetSessionId()), nil
	default:
		return "", fmt.Errorf("unsupported session runtime method: %s", req.GetMethod())
	}
}

func isSessionStartFailureStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "failed", "stop_failed", "recoverable":
		return true
	default:
		return false
	}
}

func grpcCodeForError(err error) string {
	if err == nil {
		return "Internal"
	}
	switch {
	case strings.Contains(err.Error(), "timed out") || strings.Contains(err.Error(), "deadline"):
		return "DeadlineExceeded"
	case strings.Contains(err.Error(), "not configured"):
		return "FailedPrecondition"
	case strings.Contains(err.Error(), "not connected"), strings.Contains(err.Error(), "worker gone"):
		return "Unavailable"
	default:
		return "Internal"
	}
}

func workerDataFrameFromRuntime(frame *cpv1.RuntimeFrame) (*rwv1.AgentFrame, string) {
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH:
		batch := frame.GetLiveKlineBatch()
		if batch == nil {
			return nil, ""
		}
		return &rwv1.AgentFrame{
			Payload: &rwv1.AgentFrame_MarketDataBatch{MarketDataBatch: &rwv1.MarketDataBatch{
				SessionId: batch.GetSessionId(),
				StreamKey: batch.GetStreamKey(),
				Sequence:  batch.GetSequence(),
				Klines:    batch.GetKlines(),
			}},
		}, batch.GetSessionId()
	case cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH:
		batch := frame.GetOrderUpdateBatch()
		if batch == nil {
			return nil, ""
		}
		return &rwv1.AgentFrame{
			Payload: &rwv1.AgentFrame_OrderUpdateBatch{OrderUpdateBatch: &rwv1.OrderUpdateBatch{
				SessionId: batch.GetSessionId(),
				StreamKey: batch.GetStreamKey(),
				Sequence:  batch.GetSequence(),
				Events:    batch.GetEvents(),
			}},
		}, batch.GetSessionId()
	default:
		return nil, ""
	}
}

func mustRandomToken() string {
	token, err := randomToken()
	if err != nil {
		panic(err)
	}
	return token
}
