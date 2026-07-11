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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type WorkerStarter interface {
	StartSessionWorker(ctx context.Context, sessionID string, extraEnv []string) (*ManagedWorker, error)
}

type WorkerStopper interface {
	StopSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error
}

type PlatformInvoker interface {
	InvokePlatformAny(ctx context.Context, method string, request *anypb.Any, timeout time.Duration) (*anypb.Any, error)
}

type WorkerSender interface {
	SendToWorker(sessionID string, frame *rwv1.AgentFrame) error
}

type WorkerSessionBinder interface {
	AliasWorkerSession(existingSessionID string, sessionID string) error
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
	pending           map[string]*pendingSessionStart
	ready             map[string]chan struct{}
	workerCallReply   map[string]chan *rwv1.PlatformCallResult
	workerCallSession map[string]string
	runRequests       map[string]*anypb.Any
	indicatorSync     *IndicatorSyncManager
}

type pendingSessionStart struct {
	started chan string
	failed  chan string
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
	case "PreviewRunStrategy":
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
	pendingID := "pending-" + mustRandomToken()[:16]
	runtimeID := strings.TrimSpace(runReq.GetRuntimeId())
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(a.cfg.RuntimeID)
	}
	start := &rwv1.StartSession{
		SessionId:          pendingID,
		UserId:             runReq.GetUserId(),
		RuntimeId:          runtimeID,
		RunStrategyRequest: req.GetRequest(),
	}
	pending := &pendingSessionStart{
		started: make(chan string, 1),
		failed:  make(chan string, 1),
		start:   start,
	}
	a.mu.Lock()
	a.pending[pendingID] = pending
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.pending, pendingID)
		a.mu.Unlock()
	}()

	worker, err := a.cfg.WorkerStarter.StartSessionWorker(ctx, pendingID, a.workerEnv())
	if err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", err.Error())
	}

	workerExited := waitManagedWorker(worker)
	timer := time.NewTimer(a.cfg.StartTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return runtimeErrorFrame(frame.GetCorrelationId(), "Cancelled", ctx.Err().Error())
	case <-timer.C:
		return runtimeErrorFrame(frame.GetCorrelationId(), "DeadlineExceeded", "session worker did not report started")
	case waitErr := <-workerExited:
		return runtimeErrorFrame(frame.GetCorrelationId(), "Internal", managedWorkerExitError("reporting started", waitErr).Error())
	case message := <-pending.failed:
		if strings.TrimSpace(message) == "" {
			message = "session worker failed before start"
		}
		return runtimeErrorFrame(frame.GetCorrelationId(), "FailedPrecondition", message)
	case sessionID := <-pending.started:
		return responseFrame(frame.GetCorrelationId(), &strategyv1.RunStrategyResponse{SessionId: sessionID})
	}
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
	if err := a.waitWorkerReady(ctx, ready, worker, a.timeoutForFrame(frame)); err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
	}
	resp, err := a.invokeWorkerUnary(ctx, pendingID, req.GetMethod(), req.GetRequest(), a.timeoutForFrame(frame))
	if err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
	}
	return responseAnyFrame(frame.GetCorrelationId(), resp)
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
	resp, err := a.invokeWorkerUnary(ctx, sessionID, req.GetMethod(), req.GetRequest(), a.timeoutForFrame(frame))
	if err != nil {
		return runtimeErrorFrame(frame.GetCorrelationId(), grpcCodeForError(err), err.Error())
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
		statusValue := strings.TrimSpace(strings.ToLower(progress.GetStatus()))
		a.mu.Lock()
		pending := a.pending[workerSessionID]
		a.mu.Unlock()
		if pending != nil && isSessionStartFailureStatus(statusValue) {
			message := strings.TrimSpace(progress.GetError())
			select {
			case pending.failed <- message:
			default:
			}
			return nil
		}
		if statusValue == "running" && realSessionID != "" {
			if realSessionID != workerSessionID {
				a.mu.Lock()
				sender := a.cfg.WorkerSender
				a.mu.Unlock()
				if binder, ok := sender.(WorkerSessionBinder); ok {
					if err := binder.AliasWorkerSession(workerSessionID, realSessionID); err != nil {
						return err
					}
				}
				if binder, ok := a.cfg.WorkerStarter.(WorkerSessionBinder); ok {
					if err := binder.AliasWorkerSession(workerSessionID, realSessionID); err != nil {
						return err
					}
				}
			}
			if pending != nil {
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
		result := a.invokeWorkerPlatformCall(ctx, call)
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
		return a.indicatorSync.ReceiveFrame(frame.GetIndicatorFrame())
	case *rwv1.WorkerFrame_FinalStatus:
		return a.handleWorkerFinalStatus(ctx, frame.GetFrameId(), frame.GetFinalStatus(), send)
	}
	return nil
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

	statusValue := strings.TrimSpace(strings.ToLower(status.GetStatus()))
	if statusValue == "completed" {
		statusValue = "finished"
	}
	switch statusValue {
	case "finished", "failed", "stopped", "stop_failed", "recoverable":
	default:
		return fmt.Errorf("final status must be terminal, got %q", status.GetStatus())
	}
	flushCtx := ctx
	var cancel context.CancelFunc
	if a.cfg.IndicatorFinalizeTimeout > 0 {
		flushCtx, cancel = context.WithTimeout(ctx, a.cfg.IndicatorFinalizeTimeout)
		defer cancel()
	}
	flushErr := a.indicatorSync.FinalizeSession(flushCtx, sessionID)
	if statusValue == "finished" && flushErr != nil {
		message := "indicator finalization failed: " + flushErr.Error()
		if err := a.updateSession(ctx, sessionID, "recoverable", status.GetBarsProcessed(), message); err != nil {
			return err
		}
		return send(&rwv1.AgentFrame{
			ReplyTo: frameID,
			Payload: &rwv1.AgentFrame_Error{Error: &rwv1.AgentError{
				Code: "INDICATOR_FINALIZATION_FAILED", Message: message,
			}},
		})
	}
	if err := a.updateSession(ctx, sessionID, statusValue, status.GetBarsProcessed(), status.GetError()); err != nil {
		return err
	}
	if err := send(&rwv1.AgentFrame{ReplyTo: frameID}); err != nil {
		return err
	}
	a.indicatorSync.ForgetSession(ctx, sessionID)
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
			return nil, errors.New(result.GetError())
		}
		if result.GetResponse() == nil {
			return nil, fmt.Errorf("runtime worker response payload is empty")
		}
		return result.GetResponse(), nil
	}
}

func (a *Agent) waitWorkerReady(ctx context.Context, ready <-chan struct{}, worker *ManagedWorker, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = a.cfg.RequestTimeout
	}
	workerExited := waitManagedWorker(worker)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("session worker did not connect")
	case waitErr := <-workerExited:
		return managedWorkerExitError("connecting", waitErr)
	case <-ready:
		return nil
	}
}

func waitManagedWorker(worker *ManagedWorker) <-chan error {
	if worker == nil || worker.Cmd == nil {
		return nil
	}
	exited := make(chan error, 1)
	go func() {
		exited <- worker.Wait()
	}()
	return exited
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
		return statusReq.GetSessionId(), nil
	case "StopStrategy":
		var stopReq strategyv1.StopStrategyRequest
		if err := req.GetRequest().UnmarshalTo(&stopReq); err != nil {
			return "", fmt.Errorf("invalid StopStrategy request payload")
		}
		if strings.TrimSpace(stopReq.GetSessionId()) == "" {
			return "", fmt.Errorf("session_id is required")
		}
		return stopReq.GetSessionId(), nil
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
