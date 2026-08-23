package runtimeagent

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestAgentRunStrategyPreparesCommitsThenStartsFinalWorker(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{recorder: recorder, commitOK: true}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", RuntimeName: "bare-debug", UserID: 6,
		WorkerStarter: starter, WorkerStopper: starter, WorkerSender: sender,
		PlatformInvoker: platform, StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1", Interval: "1m",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("RunStrategy frame = %+v", frame)
	}
	var response strategyv1.RunStrategyResponse
	if err := frame.GetResponse().GetResponse().UnmarshalTo(&response); err != nil {
		t.Fatal(err)
	}
	if !response.GetOk() || response.GetSessionId() == "" {
		t.Fatalf("RunStrategy response = %+v", &response)
	}
	if got := recorder.snapshot(); !slices.Equal(got, []string{
		"prepare", "commit", "get-session", "final-worker",
	}) {
		t.Fatalf("start events = %v", got)
	}
	if starter.finalStarts != 1 || starter.prepareStarts != 1 {
		t.Fatalf("worker starts prepare=%d final=%d", starter.prepareStarts, starter.finalStarts)
	}
	if platform.commit == nil || platform.commit.GetSession().GetSessionId() != response.GetSessionId() {
		t.Fatalf("commit request = %+v", platform.commit)
	}
	if platform.commit.GetLaunchOperationId() == "" || platform.commit.GetSession().GetLeverage() != 0 {
		t.Fatalf("commit operation/scalar = %+v", platform.commit)
	}
	if starter.finalStart == nil || starter.finalStart.GetSessionBootstrap() == nil {
		t.Fatalf("final StartSession = %+v", starter.finalStart)
	}
	var bootstrap strategyv1.StrategySessionBootstrap
	if err := starter.finalStart.GetSessionBootstrap().UnmarshalTo(&bootstrap); err != nil {
		t.Fatalf("unpack bootstrap: %v", err)
	}
	if bootstrap.GetSessionId() != response.GetSessionId() ||
		bootstrap.GetLaunchOperationId() != platform.commit.GetLaunchOperationId() ||
		bootstrap.GetStrategySourceSha256() != strategyStartDigest ||
		bootstrap.GetEnvironment() != 1 ||
		len(bootstrap.GetConfirmedTargetFacts()) != 2 {
		t.Fatalf("bootstrap = %+v", &bootstrap)
	}
	if starter.finalEnv["HUSHINE_STRATEGY_SESSION_BOOTSTRAP_REQUIRED"] != "1" {
		t.Fatalf("final worker env = %+v", starter.finalEnv)
	}
}

func TestAgentRunStrategyRequiresDurablePendingSessionBeforeFinalWorker(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true, noDurableCommit: true,
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: 50 * time.Millisecond,
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("RunStrategy frame = %+v", frame)
	}
	if starter.finalStarts != 0 {
		t.Fatalf("final worker starts = %d, want 0", starter.finalStarts)
	}
}

func TestAgentRunStrategyReconcilesAmbiguousCommitFromDurableSession(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true, commitErr: errors.New("Unavailable: commit response lost"),
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE || starter.finalStarts != 1 {
		t.Fatalf("RunStrategy frame=%+v final starts=%d", frame, starter.finalStarts)
	}
}

func TestAgentRunStrategyRetainsAmbiguousCommitWhenReconciliationUnavailable(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true,
		commitErr: errors.New("Unavailable: commit response lost"),
		getErr:    errors.New("Unavailable: reconciliation unavailable"),
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: 20 * time.Millisecond,
		StateRoot: t.TempDir(),
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || starter.finalStarts != 0 {
		t.Fatalf("RunStrategy frame=%+v final starts=%d", frame, starter.finalStarts)
	}
	agent.mu.Lock()
	retained := len(agent.generations)
	agent.mu.Unlock()
	if retained != 1 {
		t.Fatalf("ambiguous generations = %d, want 1", retained)
	}
	agent.retryMu.Lock()
	checkpointCount := len(agent.terminalRetries)
	var checkpoint TerminalRetryRecord
	for _, record := range agent.terminalRetries {
		checkpoint = record
	}
	agent.retryMu.Unlock()
	if checkpointCount != 1 || checkpoint.ExpectedStatus != "pending" ||
		checkpoint.DesiredStatus != "failed" {
		t.Fatalf("ambiguous commit checkpoint count=%d record=%+v", checkpointCount, checkpoint)
	}
}

func TestAgentRunStrategyRejectsNonCanonicalDigestBeforeCommit(t *testing.T) {
	for _, digest := range []string{
		strings.Repeat("g", 64),
		strings.ToUpper(strategyStartDigest),
		strings.Repeat("a", 63),
	} {
		t.Run(digest[:8], func(t *testing.T) {
			recorder := &strategyStartRecorder{}
			starter := &strategyStartWorkerStarter{recorder: recorder}
			sender := &strategyStartWorkerSender{
				recorder: recorder, prepareOK: true, sourceDigest: digest,
			}
			platform := &strategyStartPlatform{recorder: recorder, commitOK: true}
			agent := NewAgent(AgentConfig{
				RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
				WorkerSender: sender, PlatformInvoker: platform,
				StartTimeout: time.Second, RequestTimeout: time.Second,
			})
			starter.agent = agent
			sender.agent = agent

			frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
				PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
			})
			if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
				t.Fatalf("RunStrategy frame = %+v", frame)
			}
			if platform.commit != nil || starter.finalStarts != 0 {
				t.Fatalf("commit=%+v final starts=%d", platform.commit, starter.finalStarts)
			}
		})
	}
}

func TestAgentRunStrategyExpectedFailuresNeverStartFinalWorker(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prepareOK    bool
		commitOK     bool
		wantCode     string
		wantRollback bool
	}{
		{name: "preparation", prepareOK: false, commitOK: true, wantCode: "PREPARE_REJECTED"},
		{name: "commit", prepareOK: true, commitOK: false, wantCode: "LEVERAGE_CONFIRM_FAILED", wantRollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &strategyStartRecorder{}
			starter := &strategyStartWorkerStarter{recorder: recorder}
			sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: tc.prepareOK}
			platform := &strategyStartPlatform{recorder: recorder, commitOK: tc.commitOK, rollbackFailed: tc.wantRollback}
			agent := NewAgent(AgentConfig{
				RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
				WorkerSender: sender, PlatformInvoker: platform,
				StartTimeout: time.Second, RequestTimeout: time.Second,
			})
			starter.agent = agent
			sender.agent = agent

			frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
				PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
			})
			if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
				t.Fatalf("RunStrategy frame = %+v", frame)
			}
			var response strategyv1.RunStrategyResponse
			if err := frame.GetResponse().GetResponse().UnmarshalTo(&response); err != nil {
				t.Fatal(err)
			}
			if response.GetOk() || response.GetSessionId() != "" || response.GetCode() != tc.wantCode {
				t.Fatalf("response = %+v", &response)
			}
			if response.GetRollbackFailed() != tc.wantRollback || len(response.GetFailures()) == 0 {
				t.Fatalf("structured failure = %+v", &response)
			}
			if starter.finalStarts != 0 {
				t.Fatalf("final worker starts = %d, want 0", starter.finalStarts)
			}
		})
	}
}

func TestAgentRunStrategyPostCommitLaunchFailureMarksPendingSessionFailed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		finalError error
		timeout    bool
		wantCode   string
	}{
		{name: "launch_failure", finalError: errors.New("final worker launch failed"), wantCode: "Internal"},
		{name: "start_timeout", timeout: true, wantCode: "DeadlineExceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &strategyStartRecorder{}
			starter := &strategyStartWorkerStarter{
				recorder: recorder, finalError: tc.finalError, suppressFinalProgress: tc.timeout,
			}
			sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
			platform := &strategyStartPlatform{recorder: recorder, commitOK: true}
			agent := NewAgent(AgentConfig{
				RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
				WorkerSender: sender, PlatformInvoker: platform,
				StartTimeout: 20 * time.Millisecond, RequestTimeout: time.Second,
			})
			starter.agent = agent
			sender.agent = agent

			frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
				PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
			})
			if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || frame.GetError().GetCode() != tc.wantCode {
				t.Fatalf("RunStrategy frame = %+v", frame)
			}
			updates := platform.snapshotUpdates()
			if len(updates) != 1 || updates[0].GetStatus() != "failed" ||
				updates[0].GetSessionId() != platform.commit.GetSession().GetSessionId() ||
				updates[0].GetExpectedStatus() != "pending" {
				t.Fatalf("cleanup updates = %+v", updates)
			}
			if !strings.Contains(updates[0].GetError(), "worker") {
				t.Fatalf("cleanup error = %q", updates[0].GetError())
			}
		})
	}
}

func TestAgentRunStrategyRejectsIncompleteConfirmedFactsBeforeFinalWorker(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true, omitConfirmedFacts: true,
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})

	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR ||
		frame.GetError().GetCode() != "Internal" {
		t.Fatalf("RunStrategy frame = %+v", frame)
	}
	if starter.finalStarts != 0 {
		t.Fatalf("final worker starts = %d, want 0", starter.finalStarts)
	}
	updates := platform.snapshotUpdates()
	if len(updates) != 1 || updates[0].GetStatus() != "failed" {
		t.Fatalf("cleanup updates = %+v", updates)
	}
}

func TestAgentRestartSessionPreparesFreshSessionOperationSourceAndTargetFacts(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{recorder: recorder}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true,
		restartSession: &portfoliov1.StrategySessionEntry{
			SessionId: "old-session", PortfolioId: 7, StrategyId: 12, Environment: 1,
			UserId: 6, RuntimeId: "rt-1", Status: "running", Interval: "1m",
			Leverage: 99,
			TargetLeverageFacts: []*portfoliov1.SessionTargetLeverageFact{{
				SessionId: "old-session", VenueId: 22, Exchange: 1, Environment: 1,
				Market: 2, Symbol: "BTCUSDT", EffectiveLeverage: 9,
				LeverageSource: "order_target", ConfirmedLeverage: 9,
			}},
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	starter.agent = agent
	sender.agent = agent

	result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "old-session"})
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}
	if result.NewSessionID == "" || result.NewSessionID == "old-session" {
		t.Fatalf("restart result = %+v", result)
	}
	if sender.lastPrepare == nil || sender.lastPrepare.GetSessionId() != result.NewSessionID ||
		sender.lastPrepare.GetLaunchOperationId() == "" {
		t.Fatalf("fresh preparation = %+v", sender.lastPrepare)
	}
	var bootstrap strategyv1.StrategySessionBootstrap
	if starter.finalStart == nil ||
		starter.finalStart.GetSessionBootstrap().UnmarshalTo(&bootstrap) != nil {
		t.Fatalf("final StartSession = %+v", starter.finalStart)
	}
	if bootstrap.GetStrategySourceSha256() != strategyStartDigest ||
		len(bootstrap.GetConfirmedTargetFacts()) != 2 ||
		bootstrap.GetConfirmedTargetFacts()[0].GetConfirmedLeverage() == 9 {
		t.Fatalf("restart reused old bootstrap facts: %+v", &bootstrap)
	}
}

const strategyStartDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func runStrategyFrame(t *testing.T, agent *Agent, request *strategyv1.RunStrategyRequest) *cpv1.RuntimeFrame {
	t.Helper()
	packed, err := anypb.New(request)
	if err != nil {
		t.Fatal(err)
	}
	return agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-strategy-start",
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method: "RunStrategy", Request: packed,
		}},
	})
}

type strategyStartRecorder struct {
	mu     sync.Mutex
	events []string
}

// enableStrategyStartProtocol keeps older worker-lifecycle tests focused on
// their original final-worker boundary while satisfying the new mandatory
// prepare/commit protocol before that boundary.
func enableStrategyStartProtocol(agent *Agent, finalStarter WorkerStarter) {
	harness := &strategyStartProtocolHarness{
		agent:            agent,
		finalStarter:     finalStarter,
		stopperDelegate:  agent.cfg.WorkerStopper,
		platformDelegate: agent.cfg.PlatformInvoker,
		senderDelegate:   agent.cfg.WorkerSender,
	}
	agent.cfg.WorkerStarter = harness
	agent.cfg.WorkerStopper = harness
	agent.cfg.PlatformInvoker = harness
	agent.cfg.WorkerSender = harness
}

type strategyStartProtocolHarness struct {
	mu               sync.Mutex
	agent            *Agent
	finalStarter     WorkerStarter
	stopperDelegate  WorkerStopper
	platformDelegate PlatformInvoker
	senderDelegate   WorkerSender
	committedSession *portfoliov1.StrategySessionEntry
}

func (h *strategyStartProtocolHarness) StopSessionWorker(
	ctx context.Context,
	sessionID string,
	timeout time.Duration,
) error {
	if strings.HasPrefix(sessionID, "control-") {
		return nil
	}
	if h.stopperDelegate != nil {
		return h.stopperDelegate.StopSessionWorker(ctx, sessionID, timeout)
	}
	return nil
}

func (h *strategyStartProtocolHarness) StartSessionWorker(
	ctx context.Context,
	sessionID string,
	extraEnv []string,
) (*ManagedWorker, error) {
	if !strings.HasPrefix(sessionID, "control-") {
		return h.finalStarter.StartSessionWorker(ctx, sessionID, extraEnv)
	}
	go func() {
		_ = h.agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
				SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
			}},
		}, nil)
	}()
	return &ManagedWorker{SessionID: sessionID}, nil
}

func (h *strategyStartProtocolHarness) SendToWorker(
	sessionID string,
	frame *rwv1.AgentFrame,
) error {
	call := frame.GetPlatformCall()
	if call == nil || call.GetMethod() != "PrepareRunStrategyStart" {
		if h.senderDelegate != nil {
			return h.senderDelegate.SendToWorker(sessionID, frame)
		}
		return nil
	}
	var request strategyv1.PrepareRunStrategyStartRequest
	if err := call.GetRequest().UnmarshalTo(&request); err != nil {
		return err
	}
	runRequest := request.GetRunRequest()
	response := &strategyv1.PreparedRunStrategyStart{
		Ok: true, LaunchOperationId: request.GetLaunchOperationId(),
		StrategySourceSha256: strategyStartDigest,
		Session: &strategyv1.StrategySessionMetadata{
			SessionId: request.GetSessionId(), PortfolioId: runRequest.GetPortfolioId(),
			Environment: 0, Interval: runRequest.GetInterval(),
			StartTimeMs: runRequest.GetStartTimeMs(), EndTimeMs: runRequest.GetEndTimeMs(),
			UserId: runRequest.GetUserId(), RuntimeId: runRequest.GetRuntimeId(),
			InitialStatus: "pending",
		},
	}
	packed, err := anypb.New(response)
	if err != nil {
		return err
	}
	go func() {
		_ = h.agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
				CallId: call.GetCallId(), Ok: true, Response: packed,
			}},
		}, nil)
	}()
	return nil
}

func (h *strategyStartProtocolHarness) InvokePlatformAny(
	ctx context.Context,
	method string,
	request *anypb.Any,
	timeout time.Duration,
) (*anypb.Any, error) {
	if method == "portfolio.CommitStrategySessionStart" {
		var commit portfoliov1.CommitStrategySessionStartRequest
		if err := request.UnmarshalTo(&commit); err != nil {
			return nil, err
		}
		facts := []*portfoliov1.SessionTargetLeverageFact{}
		h.mu.Lock()
		h.committedSession = committedStrategySession(&commit, facts)
		h.mu.Unlock()
		return anypb.New(&portfoliov1.CommitStrategySessionStartResponse{Ok: true})
	}
	if method == "portfolio.GetSession" {
		var get portfoliov1.GetSessionRequest
		if err := request.UnmarshalTo(&get); err != nil {
			return nil, err
		}
		h.mu.Lock()
		committed := h.committedSession
		h.mu.Unlock()
		if committed != nil && committed.GetSessionId() == get.GetSessionId() {
			return anypb.New(&portfoliov1.GetSessionResponse{
				Session: proto.Clone(committed).(*portfoliov1.StrategySessionEntry),
			})
		}
	}
	if method == "portfolio.UpdateSession" {
		var update portfoliov1.UpdateSessionRequest
		if err := request.UnmarshalTo(&update); err != nil {
			return nil, err
		}
		var response *anypb.Any
		var err error
		if h.platformDelegate != nil {
			response, err = h.platformDelegate.InvokePlatformAny(ctx, method, request, timeout)
		} else {
			response, err = anypb.New(&portfoliov1.UpdateSessionResponse{})
		}
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		if h.committedSession != nil && h.committedSession.GetSessionId() == update.GetSessionId() {
			h.committedSession.Status = update.GetStatus()
			h.committedSession.Error = update.GetError()
		}
		h.mu.Unlock()
		return response, nil
	}
	if h.platformDelegate != nil {
		return h.platformDelegate.InvokePlatformAny(ctx, method, request, timeout)
	}
	return nil, fmt.Errorf("unexpected platform method: %s", method)
}

func (r *strategyStartRecorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *strategyStartRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type strategyStartWorkerStarter struct {
	agent                 *Agent
	recorder              *strategyStartRecorder
	prepareStarts         int
	finalStarts           int
	finalError            error
	suppressFinalProgress bool
	onFinalStart          func(string)
	finalStart            *rwv1.StartSession
	finalEnv              map[string]string
}

func (s *strategyStartWorkerStarter) StartSessionWorker(
	_ context.Context,
	sessionID string,
	extraEnv []string,
) (*ManagedWorker, error) {
	if strings.HasPrefix(sessionID, "control-") {
		s.prepareStarts++
		go func() {
			_ = s.agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
		return &ManagedWorker{SessionID: sessionID}, nil
	}
	s.finalStarts++
	s.recorder.add("final-worker")
	if s.finalError != nil {
		return nil, s.finalError
	}
	s.finalEnv = map[string]string{}
	for _, item := range extraEnv {
		key, value, found := strings.Cut(item, "=")
		if found {
			s.finalEnv[key] = value
		}
	}
	s.agent.mu.Lock()
	if pending := s.agent.pending[sessionID]; pending != nil {
		s.finalStart = proto.Clone(pending.start).(*rwv1.StartSession)
	}
	s.agent.mu.Unlock()
	if s.onFinalStart != nil {
		s.onFinalStart(sessionID)
	} else if !s.suppressFinalProgress {
		go func() {
			_ = s.agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID, Status: "running",
				}},
			}, nil)
		}()
	}
	return &ManagedWorker{SessionID: sessionID}, nil
}

func (*strategyStartWorkerStarter) StopSessionWorker(context.Context, string, time.Duration) error {
	return nil
}

type strategyStartWorkerSender struct {
	agent        *Agent
	recorder     *strategyStartRecorder
	prepareOK    bool
	sourceDigest string
	lastPrepare  *strategyv1.PrepareRunStrategyStartRequest
}

func (s *strategyStartWorkerSender) SendToWorker(sessionID string, frame *rwv1.AgentFrame) error {
	call := frame.GetPlatformCall()
	if call == nil || call.GetMethod() != "PrepareRunStrategyStart" {
		return fmt.Errorf("unexpected worker frame: %+v", frame)
	}
	var request strategyv1.PrepareRunStrategyStartRequest
	if err := call.GetRequest().UnmarshalTo(&request); err != nil {
		return err
	}
	if request.GetSessionId() == "" || request.GetLaunchOperationId() == "" ||
		request.GetRunRequest().GetRuntimeId() != "rt-1" {
		return fmt.Errorf("invalid preparation request: %+v", &request)
	}
	s.lastPrepare = proto.Clone(&request).(*strategyv1.PrepareRunStrategyStartRequest)
	s.recorder.add("prepare")
	response := preparedStrategyStart(request.GetSessionId(), request.GetLaunchOperationId())
	if s.sourceDigest != "" {
		response.StrategySourceSha256 = s.sourceDigest
	}
	if !s.prepareOK {
		response = &strategyv1.PreparedRunStrategyStart{
			Ok: false,
			Failures: []*strategyv1.PreflightFailureProto{{
				Kind: "declaration", Code: "PREPARE_REJECTED", Reason: "strategy preparation rejected",
			}},
		}
	}
	packed, err := anypb.New(response)
	if err != nil {
		return err
	}
	go func() {
		_ = s.agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
				CallId: call.GetCallId(), Ok: true, Response: packed,
			}},
		}, nil)
	}()
	return nil
}

func preparedStrategyStart(sessionID, operationID string) *strategyv1.PreparedRunStrategyStart {
	return &strategyv1.PreparedRunStrategyStart{
		Ok: true, LaunchOperationId: operationID, StrategySourceSha256: strategyStartDigest,
		Session: &strategyv1.StrategySessionMetadata{
			SessionId: sessionID, PortfolioId: 7, StrategyId: 12, Environment: 1,
			Interval: "1m", UserId: 6, RuntimeId: "rt-1", RuntimeSource: "bare",
			RuntimeName: "bare-debug", SessionType: "demo", RuntimeVersion: "1",
			SessionName: "strategy", InitialStatus: "pending",
		},
		RequiredRoutes: []*strategyv1.StrategyRouteBinding{{Exchange: "binance", Market: "perpetual_futures"}},
		RequiredSymbols: []*strategyv1.StrategyRequiredSymbolBinding{
			{Exchange: "binance", Market: "perpetual_futures", Symbol: "BTCUSDT", OrderTarget: true, RequiredOrderTypes: []string{"MARKET", "LIMIT"}, EffectiveLeverage: 2, LeverageSource: "order_target"},
			{Exchange: "binance", Market: "perpetual_futures", Symbol: "ETHUSDT", OrderTarget: true, RequiredOrderTypes: []string{"MARKET", "LIMIT"}, EffectiveLeverage: 3, LeverageSource: "strategy_default"},
		},
	}
}

type strategyStartPlatform struct {
	mu                 sync.Mutex
	recorder           *strategyStartRecorder
	commitOK           bool
	commitErr          error
	getErr             error
	noDurableCommit    bool
	updateFailures     int
	admissionActive    bool
	beforeUpdate       func(*portfoliov1.UpdateSessionRequest) error
	mutateCommitted    func(*portfoliov1.StrategySessionEntry)
	omitConfirmedFacts bool
	rollbackFailed     bool
	commit             *portfoliov1.CommitStrategySessionStartRequest
	committedSession   *portfoliov1.StrategySessionEntry
	updates            []*portfoliov1.UpdateSessionRequest
	restartSession     *portfoliov1.StrategySessionEntry
}

func (p *strategyStartPlatform) InvokePlatformAny(
	_ context.Context,
	method string,
	request *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	switch method {
	case "portfolio.GetSession":
		var get portfoliov1.GetSessionRequest
		if err := request.UnmarshalTo(&get); err != nil {
			return nil, err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.getErr != nil {
			return nil, p.getErr
		}
		if p.committedSession != nil && p.committedSession.GetSessionId() == get.GetSessionId() {
			p.recorder.add("get-session")
			return anypb.New(&portfoliov1.GetSessionResponse{
				Session: proto.Clone(p.committedSession).(*portfoliov1.StrategySessionEntry),
			})
		}
		if p.restartSession == nil || p.restartSession.GetSessionId() != get.GetSessionId() {
			return nil, errors.New("NotFound: session not found")
		}
		return anypb.New(&portfoliov1.GetSessionResponse{
			Session: proto.Clone(p.restartSession).(*portfoliov1.StrategySessionEntry),
		})
	case "portfolio.CommitStrategySessionStart":
		var commit portfoliov1.CommitStrategySessionStartRequest
		if err := request.UnmarshalTo(&commit); err != nil {
			return nil, err
		}
		p.recorder.add("commit")
		if !p.commitOK {
			p.mu.Lock()
			p.commit = proto.Clone(&commit).(*portfoliov1.CommitStrategySessionStartRequest)
			p.mu.Unlock()
			confirmed := uint32(2)
			return anypb.New(&portfoliov1.CommitStrategySessionStartResponse{
				Ok: false, Code: "LEVERAGE_CONFIRM_FAILED", RollbackFailed: p.rollbackFailed,
				Issues:        []*portfoliov1.PreflightIssue{{Code: "LEVERAGE_CONFIRM_FAILED", Message: "confirmation failed", Exchange: 1, Market: 2, Symbol: "ETHUSDT"}},
				TargetResults: []*portfoliov1.FuturesLeverageTargetResult{{Symbol: "ETHUSDT", EffectiveLeverage: 3, ConfirmedLeverage: &confirmed, Status: "confirm_failed", ErrorCode: "LEVERAGE_CONFIRM_FAILED"}},
			})
		}
		facts := []*portfoliov1.SessionTargetLeverageFact(nil)
		if !p.omitConfirmedFacts {
			facts = []*portfoliov1.SessionTargetLeverageFact{
				{SessionId: commit.GetSession().GetSessionId(), VenueId: 22, Exchange: 1, Environment: 1, Market: 2, Symbol: "BTCUSDT", EffectiveLeverage: 2, LeverageSource: "order_target", ConfirmedLeverage: 2},
				{SessionId: commit.GetSession().GetSessionId(), VenueId: 22, Exchange: 1, Environment: 1, Market: 2, Symbol: "ETHUSDT", EffectiveLeverage: 3, LeverageSource: "strategy_default", ConfirmedLeverage: 3},
			}
		}
		p.mu.Lock()
		p.commit = proto.Clone(&commit).(*portfoliov1.CommitStrategySessionStartRequest)
		if !p.noDurableCommit {
			p.committedSession = committedStrategySession(&commit, facts)
			if p.mutateCommitted != nil {
				p.mutateCommitted(p.committedSession)
			}
			p.admissionActive = true
		}
		commitErr := p.commitErr
		p.mu.Unlock()
		if commitErr != nil {
			return nil, commitErr
		}
		return anypb.New(&portfoliov1.CommitStrategySessionStartResponse{Ok: true, ConfirmedTargetFacts: facts})
	case "portfolio.UpdateSession":
		var update portfoliov1.UpdateSessionRequest
		if err := request.UnmarshalTo(&update); err != nil {
			return nil, err
		}
		if p.beforeUpdate != nil {
			if err := p.beforeUpdate(&update); err != nil {
				return nil, err
			}
		}
		p.mu.Lock()
		p.updates = append(p.updates, proto.Clone(&update).(*portfoliov1.UpdateSessionRequest))
		if p.updateFailures > 0 {
			p.updateFailures--
			p.mu.Unlock()
			return nil, errors.New("Unavailable: UpdateSession acknowledgement lost")
		}
		if p.committedSession != nil && p.committedSession.GetSessionId() == update.GetSessionId() {
			if update.GetExpectedStatus() != "" &&
				!strings.EqualFold(update.GetExpectedStatus(), p.committedSession.GetStatus()) {
				p.mu.Unlock()
				return nil, errors.New("NotFound: Session CAS did not match")
			}
			p.committedSession.Status = update.GetStatus()
			p.committedSession.Error = update.GetError()
			if isTerminalRetryStatus(strings.ToLower(strings.TrimSpace(update.GetStatus()))) {
				p.admissionActive = false
			}
		}
		p.mu.Unlock()
		return anypb.New(&portfoliov1.UpdateSessionResponse{})
	default:
		return nil, fmt.Errorf("unexpected platform method: %s", method)
	}
}

func TestAgentRunStrategyRejectsMismatchedDurableSessionBeforeFinalWorker(t *testing.T) {
	mutations := map[string]func(*portfoliov1.StrategySessionEntry){
		"status":       func(session *portfoliov1.StrategySessionEntry) { session.Status = "running" },
		"user":         func(session *portfoliov1.StrategySessionEntry) { session.UserId++ },
		"portfolio":    func(session *portfoliov1.StrategySessionEntry) { session.PortfolioId++ },
		"strategy":     func(session *portfoliov1.StrategySessionEntry) { session.StrategyId++ },
		"runtime":      func(session *portfoliov1.StrategySessionEntry) { session.RuntimeId = "rt-other" },
		"environment":  func(session *portfoliov1.StrategySessionEntry) { session.Environment = 2 },
		"launch":       func(session *portfoliov1.StrategySessionEntry) { session.LaunchOperationId = "other" },
		"target_facts": func(session *portfoliov1.StrategySessionEntry) { session.TargetLeverageFacts = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			recorder := &strategyStartRecorder{}
			starter := &strategyStartWorkerStarter{recorder: recorder}
			sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
			platform := &strategyStartPlatform{
				recorder: recorder, commitOK: true, mutateCommitted: mutate,
			}
			agent := NewAgent(AgentConfig{
				RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
				WorkerSender: sender, PlatformInvoker: platform,
				StartTimeout: time.Second, RequestTimeout: 20 * time.Millisecond,
			})
			starter.agent = agent
			sender.agent = agent

			frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
				PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
			})
			if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || starter.finalStarts != 0 {
				t.Fatalf("RunStrategy frame=%+v final starts=%d", frame, starter.finalStarts)
			}
		})
	}
}

func TestAgentRunStrategyStartupTimeoutLetsInflightRunningPublicationWin(t *testing.T) {
	recorder := &strategyStartRecorder{}
	runningStarted := make(chan struct{})
	releaseRunning := make(chan struct{})
	var runningOnce sync.Once
	platform := &strategyStartPlatform{
		recorder: recorder,
		commitOK: true,
		beforeUpdate: func(update *portfoliov1.UpdateSessionRequest) error {
			if update.GetStatus() == "running" {
				runningOnce.Do(func() { close(runningStarted) })
				<-releaseRunning
			}
			return nil
		},
	}
	starter := &strategyStartWorkerStarter{recorder: recorder, suppressFinalProgress: true}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: 20 * time.Millisecond, RequestTimeout: time.Second,
	})
	starter.agent = agent
	sender.agent = agent
	starter.onFinalStart = func(sessionID string) {
		go func() {
			request, err := anypb.New(&portfoliov1.UpdateSessionRequest{
				SessionId: sessionID, Status: "running", RuntimeId: "rt-1",
				ExpectedStatus: "pending",
			})
			if err != nil {
				t.Errorf("pack running update: %v", err)
				return
			}
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
					CallId: "running-publication", Method: "portfolio.UpdateSession",
					Request: request, TimeoutMs: 1000,
				}}}, func(frame *rwv1.AgentFrame) error {
				if result := frame.GetPlatformCallResult(); result == nil || !result.GetOk() {
					return fmt.Errorf("running publication failed: %+v", result)
				}
				return agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
					Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
						SessionId: sessionID, Status: "running",
					}}}, nil)
			})
		}()
	}
	go func() {
		<-runningStarted
		time.Sleep(40 * time.Millisecond)
		close(releaseRunning)
	}()

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("RunStrategy frame = %+v", frame)
	}
	platform.mu.Lock()
	statusValue := platform.committedSession.GetStatus()
	updates := append([]*portfoliov1.UpdateSessionRequest(nil), platform.updates...)
	platform.mu.Unlock()
	if statusValue != "running" || len(updates) != 1 ||
		updates[0].GetStatus() != "running" || updates[0].GetExpectedStatus() != "pending" {
		t.Fatalf("running race status=%q updates=%+v", statusValue, updates)
	}
	agent.mu.Lock()
	var generation *workerGeneration
	for _, candidate := range agent.generations {
		generation = candidate
	}
	agent.mu.Unlock()
	if generation == nil || !generation.admit("post-start-probe") {
		t.Fatal("accepted running generation did not reopen admission")
	}
	generation.completePlatformCall()
}

func TestAgentRunStrategyRetriesUnacknowledgedPostCommitCleanup(t *testing.T) {
	recorder := &strategyStartRecorder{}
	starter := &strategyStartWorkerStarter{
		recorder: recorder, finalError: errors.New("final worker launch failed"),
	}
	sender := &strategyStartWorkerSender{recorder: recorder, prepareOK: true}
	platform := &strategyStartPlatform{
		recorder: recorder, commitOK: true, updateFailures: 1,
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter, WorkerStopper: starter,
		WorkerSender: sender, PlatformInvoker: platform,
		StartTimeout: time.Second, RequestTimeout: 100 * time.Millisecond,
		StateRoot: t.TempDir(),
	})
	starter.agent = agent
	sender.agent = agent

	frame := runStrategyFrame(t, agent, &strategyv1.RunStrategyRequest{
		PortfolioId: 7, UserId: 6, RuntimeId: "rt-1",
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("RunStrategy frame = %+v", frame)
	}
	agent.mu.Lock()
	retained := len(agent.generations)
	agent.mu.Unlock()
	if retained != 1 {
		t.Fatalf("generation count after unacknowledged cleanup = %d, want 1", retained)
	}
	agent.retryMu.Lock()
	checkpointCount := len(agent.terminalRetries)
	var checkpoint TerminalRetryRecord
	for _, record := range agent.terminalRetries {
		checkpoint = record
	}
	agent.retryMu.Unlock()
	if checkpointCount != 1 || checkpoint.ExpectedStatus != "pending" ||
		checkpoint.DesiredStatus != "failed" {
		t.Fatalf("startup cleanup checkpoint count=%d record=%+v", checkpointCount, checkpoint)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		agent.mu.Lock()
		retained = len(agent.generations)
		agent.mu.Unlock()
		platform.mu.Lock()
		admissionActive := platform.admissionActive
		statusValue := ""
		if platform.committedSession != nil {
			statusValue = platform.committedSession.GetStatus()
		}
		platform.mu.Unlock()
		if retained == 0 && !admissionActive && statusValue == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	agent.mu.Lock()
	retained = len(agent.generations)
	agent.mu.Unlock()
	platform.mu.Lock()
	admissionActive := platform.admissionActive
	statusValue := platform.committedSession.GetStatus()
	platform.mu.Unlock()
	if retained != 0 || admissionActive || statusValue != "failed" {
		t.Fatalf(
			"cleanup retry retained=%d admission=%v status=%q updates=%+v",
			retained, admissionActive, statusValue, platform.snapshotUpdates(),
		)
	}
	agent.retryMu.Lock()
	retryCount := len(agent.terminalRetries)
	agent.retryMu.Unlock()
	if retryCount != 0 {
		t.Fatalf("terminal retries after acknowledgement = %d", retryCount)
	}
}

func committedStrategySession(
	commit *portfoliov1.CommitStrategySessionStartRequest,
	facts []*portfoliov1.SessionTargetLeverageFact,
) *portfoliov1.StrategySessionEntry {
	session := commit.GetSession()
	entry := &portfoliov1.StrategySessionEntry{
		SessionId: session.GetSessionId(), PortfolioId: session.GetPortfolioId(),
		StrategyId: session.GetStrategyId(), Environment: session.GetEnvironment(),
		Status: "pending", Interval: session.GetInterval(), UserId: session.GetUserId(),
		RuntimeId: session.GetRuntimeId(), RuntimeSource: session.GetRuntimeSource(),
		RuntimeName: session.GetRuntimeName(), SessionType: session.GetSessionType(),
		RuntimeVersion: session.GetRuntimeVersion(), SessionName: session.GetSessionName(),
		LaunchOperationId: commit.GetLaunchOperationId(),
	}
	for _, fact := range facts {
		entry.TargetLeverageFacts = append(
			entry.TargetLeverageFacts,
			proto.Clone(fact).(*portfoliov1.SessionTargetLeverageFact),
		)
	}
	return entry
}

func (p *strategyStartPlatform) snapshotUpdates() []*portfoliov1.UpdateSessionRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*portfoliov1.UpdateSessionRequest(nil), p.updates...)
}

func TestAgentRunStrategyUsesOneCanonicalSessionIDWithoutAlias(t *testing.T) {
	starter := &fakeWorkerStarter{}
	sender := &fakeWorkerSender{}
	var startSessionID string
	agent := NewAgent(AgentConfig{
		RuntimeID:     "rt-1",
		WorkerStarter: starter,
		WorkerSender:  sender,
	})
	enableStrategyStartProtocol(agent, starter)
	starter.onStart = func(sessionID string) {
		agent.mu.Lock()
		startSessionID = agent.pending[sessionID].start.GetSessionId()
		agent.mu.Unlock()
		go func() {
			time.Sleep(10 * time.Millisecond)
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID,
					Status:    "running",
				}},
			}, nil)
		}()
	}

	req, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}
	respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-1",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "RunStrategy",
			Request: req,
		}},
	})

	if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("response frame type = %v error=%v", respFrame.GetFrameType(), respFrame.GetError())
	}
	var resp strategyv1.RunStrategyResponse
	if err := respFrame.GetResponse().GetResponse().UnmarshalTo(&resp); err != nil {
		t.Fatalf("unpack response: %v", err)
	}
	if resp.GetSessionId() != starter.startedSessionID || resp.GetSessionId() != startSessionID {
		t.Fatalf("ids response=%q worker=%q start=%q", resp.GetSessionId(), starter.startedSessionID, startSessionID)
	}
	if len(resp.GetSessionId()) != 32 {
		t.Fatalf("session_id length = %d, want 32", len(resp.GetSessionId()))
	}
	if _, err := hex.DecodeString(resp.GetSessionId()); err != nil {
		t.Fatalf("session_id = %q, want lowercase hex: %v", resp.GetSessionId(), err)
	}
	if resp.GetSessionId() != strings.ToLower(resp.GetSessionId()) {
		t.Fatalf("session_id = %q, want lowercase", resp.GetSessionId())
	}
}

func TestAgentRejectsUnsupportedWorkerProtocolBeforeStartSession(t *testing.T) {
	for _, version := range []uint32{0, 1, 3} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			starter := &fakeWorkerStarter{}
			stopper := &fakeWorkerStopper{}
			sent := make(chan *rwv1.AgentFrame, 2)
			agent := NewAgent(AgentConfig{
				RuntimeID:      "rt-1",
				WorkerStarter:  starter,
				WorkerStopper:  stopper,
				StartTimeout:   20 * time.Millisecond,
				RequestTimeout: 100 * time.Millisecond,
			})
			enableStrategyStartProtocol(agent, starter)
			starter.onStart = func(sessionID string) {
				_ = agent.HandleWorkerFrame(
					context.Background(),
					sessionID,
					&rwv1.WorkerFrame{
						Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
							SessionId:       sessionID,
							ProtocolVersion: version,
						}},
					},
					func(frame *rwv1.AgentFrame) error {
						sent <- frame
						return nil
					},
				)
			}
			packed, err := anypb.New(&strategyv1.RunStrategyRequest{
				PortfolioId: 1,
				UserId:      6,
				RuntimeId:   "rt-1",
			})
			if err != nil {
				t.Fatal(err)
			}

			frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
				CorrelationId: "corr-protocol",
				Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
					Method:  "RunStrategy",
					Request: packed,
				}},
			})

			if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
				t.Fatalf("frame = %+v", frame)
			}
			if got := frame.GetError().GetCode(); got != "RUNTIME_WORKER_PROTOCOL_UNSUPPORTED" {
				t.Fatalf("error code = %q", got)
			}
			wantMessage := fmt.Sprintf(
				"runtime worker protocol unsupported: required=2 received=%d",
				version,
			)
			if got := frame.GetError().GetMessage(); got != wantMessage {
				t.Fatalf("error message = %q, want %q", got, wantMessage)
			}
			close(sent)
			var shutdownSeen bool
			for outbound := range sent {
				if outbound.GetStartSession() != nil {
					t.Fatal("unsupported worker received StartSession")
				}
				if shutdown := outbound.GetShutdownWorker(); shutdown != nil {
					shutdownSeen = true
					if shutdown.GetSessionId() != starter.startedSessionID ||
						shutdown.GetReason() != wantMessage {
						t.Fatalf("shutdown = %+v", shutdown)
					}
				}
			}
			if !shutdownSeen {
				t.Fatal("unsupported worker did not receive ShutdownWorker")
			}
			if stopper.sessionID != starter.startedSessionID {
				t.Fatalf(
					"stopped session = %q, want %q",
					stopper.sessionID,
					starter.startedSessionID,
				)
			}
			agent.mu.Lock()
			defer agent.mu.Unlock()
			if len(agent.pending) != 0 || len(agent.generations) != 0 {
				t.Fatalf(
					"rejected worker state leaked: pending=%d generations=%d",
					len(agent.pending),
					len(agent.generations),
				)
			}
		})
	}
}

func TestAgentRunStrategyRejectsMismatchedCanonicalSessionID(t *testing.T) {
	starter := &fakeWorkerStarter{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", WorkerStarter: starter, StartTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	starter.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: "running",
				}},
			}, nil)
		}()
	}
	packed, err := anypb.New(&strategyv1.RunStrategyRequest{PortfolioId: 1, UserId: 6, RuntimeId: "rt-1"})
	if err != nil {
		t.Fatal(err)
	}
	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-mismatch",
		Payload:       &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{Method: "RunStrategy", Request: packed}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || frame.GetError().GetCode() != "FailedPrecondition" {
		t.Fatalf("frame = %+v", frame)
	}
	if !strings.Contains(frame.GetError().GetMessage(), "mismatched canonical session_id") {
		t.Fatalf("error = %q", frame.GetError().GetMessage())
	}
}

func TestValidateDependencyFailureIsTypedAndOneShotWorkerIsRemoved(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", WorkerStarter: starter, WorkerStopper: stopper,
		WorkerSender: sender, StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	detail := &strategyv1.RuntimeDependencyError{
		Code: "STRATEGY_DEPENDENCY_UNAVAILABLE", Module: "google.cloud",
		RuntimeProfile: "platform-python-3.13", RuntimeProfileVersion: "1.0.0", ImageBuildId: "build-1",
	}
	starter.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
	}
	sender.onSend = func(sessionID string, frame *rwv1.AgentFrame) {
		call := frame.GetPlatformCall()
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
					CallId: call.GetCallId(), Ok: false, Error: "strategy dependency validation failed", DependencyError: detail,
				}},
			}, nil)
		}()
	}
	request, err := anypb.New(&strategyv1.ValidateStrategySourceRequest{Source: "import google.cloud", UserId: 6, RuntimeId: "rt-1"})
	if err != nil {
		t.Fatal(err)
	}
	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-validate",
		Payload:       &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{Method: "ValidateStrategySource", Request: request}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("frame = %+v", frame)
	}
	if got := frame.GetError().GetDependencyError(); got == nil || got.GetModule() != "google.cloud" {
		t.Fatalf("dependency detail = %+v", got)
	}
	if stopper.sessionID != starter.startedSessionID {
		t.Fatalf("stopped session = %q, want %q", stopper.sessionID, starter.startedSessionID)
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if len(agent.ready) != 0 || len(agent.readyFailures) != 0 ||
		len(agent.workerCallReply) != 0 ||
		len(agent.workerCallSession) != 0 {
		t.Fatalf(
			"one-shot state leaked: ready=%d failures=%d replies=%d sessions=%d",
			len(agent.ready),
			len(agent.readyFailures),
			len(agent.workerCallReply),
			len(agent.workerCallSession),
		)
	}
}

func TestValidateDeclarationsRoundTripThroughOneShotWorkerAndCleanup(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", WorkerStarter: starter, WorkerStopper: stopper,
		WorkerSender: sender, StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	starter.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
	}
	sender.onSend = func(sessionID string, frame *rwv1.AgentFrame) {
		call := frame.GetPlatformCall()
		var request strategyv1.ValidateStrategySourceRequest
		if call == nil || call.GetMethod() != "ValidateStrategySource" || call.GetRequest() == nil {
			t.Errorf("worker call = %+v", call)
			return
		}
		if err := call.GetRequest().UnmarshalTo(&request); err != nil {
			t.Errorf("unpack validate request: %v", err)
			return
		}
		if !request.GetIncludeDeclarations() || request.GetRuntimeId() != "rt-1" || request.GetUserId() != 6 {
			t.Errorf("validate request = %+v", &request)
			return
		}
		go func() {
			response, err := anypb.New(&strategyv1.ValidateStrategySourceResponse{
				Ok: true,
				DeclaredInputs: []*strategyv1.StrategyInputDeclaration{{
					StreamId: "spot-btc", Exchange: "binance", Market: "spot",
					Kind: "kline", Symbol: "BTCUSDT", Interval: "1m",
				}},
				DeclaredOrderTargets: []*strategyv1.StrategyOrderTargetBinding{{
					Exchange: "binance", Market: "spot", Symbol: "BTCUSDT",
				}},
			})
			if err != nil {
				t.Errorf("pack validate response: %v", err)
				return
			}
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
					CallId: call.GetCallId(), Ok: true, Response: response,
				}},
			}, nil)
		}()
	}
	request, err := anypb.New(&strategyv1.ValidateStrategySourceRequest{
		Source: "class MyStrategy: pass", UserId: 6, RuntimeId: "rt-1",
		IncludeDeclarations: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-validate-declarations",
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method: "ValidateStrategySource", Request: request,
		}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("frame = %+v", frame)
	}
	var response strategyv1.ValidateStrategySourceResponse
	if err := frame.GetResponse().GetResponse().UnmarshalTo(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.GetDeclaredInputs()) != 1 || response.GetDeclaredInputs()[0].GetStreamId() != "spot-btc" {
		t.Fatalf("declared inputs = %+v", response.GetDeclaredInputs())
	}
	if len(response.GetDeclaredOrderTargets()) != 1 || response.GetDeclaredOrderTargets()[0].GetSymbol() != "BTCUSDT" {
		t.Fatalf("declared targets = %+v", response.GetDeclaredOrderTargets())
	}
	if stopper.sessionID != starter.startedSessionID {
		t.Fatalf("stopped session = %q, want %q", stopper.sessionID, starter.startedSessionID)
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if len(agent.ready) != 0 || len(agent.readyFailures) != 0 ||
		len(agent.workerCallReply) != 0 ||
		len(agent.workerCallSession) != 0 {
		t.Fatalf(
			"one-shot state leaked: ready=%d failures=%d replies=%d sessions=%d",
			len(agent.ready),
			len(agent.readyFailures),
			len(agent.workerCallReply),
			len(agent.workerCallSession),
		)
	}
}

func TestGenerationCleanupDrainsAdmittedSaveBeforeFailedReconciliation(t *testing.T) {
	const sessionID = "11111111111111111111111111111111"
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
		status:      "",
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: platform,
		RequestTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 7)
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	request, err := anypb.New(&portfoliov1.SaveSessionRequest{SessionId: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		callDone <- agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
				CallId: "save-1", Method: "portfolio.SaveSession", Request: request,
			}},
		}, func(*rwv1.AgentFrame) error { return nil })
	}()
	select {
	case <-platform.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("SaveSession was not admitted")
	}
	disconnectDone := make(chan error, 1)
	go func() {
		disconnectDone <- agent.HandleWorkerDisconnect(WorkerIdentity{
			SessionID: sessionID, Generation: 7,
		}, errors.New("worker stream closed"))
	}()
	time.Sleep(20 * time.Millisecond)
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{"SaveSession:start"}) {
		t.Fatalf("events before Save release = %v", got)
	}
	close(platform.releaseSave)
	if err := <-callDone; err != nil {
		t.Fatalf("platform call: %v", err)
	}
	if err := <-disconnectDone; err != nil {
		t.Fatalf("disconnect cleanup: %v", err)
	}
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{
		"SaveSession:start", "SaveSession:end", "GetSession", "UpdateSession:failed",
	}) {
		t.Fatalf("events = %v", got)
	}
	agent.mu.Lock()
	_, retained := agent.generations[sessionID]
	agent.mu.Unlock()
	if retained {
		t.Fatal("generation retained after confirmed failed reconciliation")
	}
}

func TestGenerationCleanupFinalizesIndicatorTailBeforeFailedReconciliation(t *testing.T) {
	const sessionID = "13131313131313131313131313131313"
	var methods []string
	var indicatorReq portfoliov1.SaveStrategyIndicatorsRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			methods = append(methods, method)
			switch method {
			case "portfolio.SaveStrategyIndicators":
				if err := request.UnmarshalTo(&indicatorReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{
					DefinitionsSaved: 1,
					ChunksSaved:      1,
				})
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		PlatformInvoker: invoker,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 11)
	generation.durablePossible = true
	if !generation.bindAuthenticatedGeneration(19) {
		t.Fatal("failed to bind authenticated generation")
	}
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame(sessionID)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}

	if err := agent.HandleWorkerDisconnect(
		WorkerIdentity{SessionID: sessionID, Generation: 19},
		errors.New("worker exited unexpectedly"),
	); err != nil {
		t.Fatalf("HandleWorkerDisconnect: %v", err)
	}

	if !slices.Equal(methods, []string{
		"portfolio.SaveStrategyIndicators",
		"portfolio.GetSession",
		"portfolio.UpdateSession",
	}) {
		t.Fatalf("cleanup methods = %v", methods)
	}
	if len(indicatorReq.GetChunks()) != 1 || !indicatorReq.GetChunks()[0].GetFinalized() {
		t.Fatalf("unexpected disconnect did not finalize indicator tail: %+v", &indicatorReq)
	}
}

func TestGenerationCleanupCheckpointsFailedIndicatorTailAndPublishesPending(t *testing.T) {
	const sessionID = "14141414141414141414141414141414"
	stateRoot := t.TempDir()
	var update portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return nil, errors.New("indicator database unavailable")
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId:     sessionID,
						UserId:        6,
						RuntimeId:     "rt-1",
						Status:        "running",
						BarsProcessed: 1,
					},
				})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&update); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		StateRoot:                stateRoot,
		PlatformInvoker:          invoker,
		RequestTimeout:           60 * time.Millisecond,
		IndicatorFinalizeTimeout: 20 * time.Millisecond,
		IndicatorRetryInitial:    10 * time.Millisecond,
		IndicatorRetryMax:        10 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 12)
	generation.durablePossible = true
	agent.generations[sessionID] = generation
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 19,
			token:      "worker-token",
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}

	err := agent.cleanupWorkerGeneration(
		sessionID,
		generation,
		"worker disconnected",
	)
	agent.mu.Lock()
	delete(agent.generations, sessionID)
	agent.mu.Unlock()
	if err == nil ||
		!strings.Contains(err.Error(), "finalize worker generation indicators") {
		t.Fatalf("cleanup error = %v", err)
	}
	if update.GetStatus() != "recoverable" ||
		update.IndicatorFinalizationPending == nil ||
		!update.GetIndicatorFinalizationPending() {
		t.Fatalf("pending recovery update = %+v", &update)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != sessionID ||
		records[0].Indicators == nil {
		t.Fatalf("cleanup retry records = %+v", records)
	}
}

func TestGenerationCleanupDrainTimeoutPersistsRecoverableRetry(t *testing.T) {
	const sessionID = "18181818181818181818181818181818"
	stateRoot := t.TempDir()
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
		status:      "running",
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       stateRoot,
		PlatformInvoker: platform,
		RequestTimeout:  20 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 18)
	if !generation.admit("portfolio.SaveSession") {
		t.Fatal("admit durable in-flight call")
	}
	if !generation.bindAuthenticatedGeneration(28) {
		t.Fatal("bind worker generation")
	}
	agent.generations[sessionID] = generation

	err := agent.HandleWorkerDisconnect(
		WorkerIdentity{SessionID: sessionID, Generation: 28},
		errors.New("worker disconnected"),
	)
	if err == nil || !strings.Contains(err.Error(), "drain worker generation") {
		t.Fatalf("disconnect drain error = %v", err)
	}
	update := platform.snapshotLastUpdate()
	if update == nil ||
		update.GetStatus() != "recoverable" ||
		update.IndicatorFinalizationPending == nil ||
		!update.GetIndicatorFinalizationPending() {
		t.Fatalf("disconnect drain update = %+v", update)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != sessionID ||
		records[0].Generation != 18 ||
		records[0].EffectiveStatus != "recoverable" {
		t.Fatalf("disconnect drain retry records = %+v", records)
	}
	generation.completePlatformCall()
}

func TestAgentShutdownClosesAdmissionAndFinalizesWorkerGenerations(t *testing.T) {
	const sessionID = "15151515151515151515151515151515"
	workers := &shutdownWorkerStopper{}
	methods := make([]string, 0, 4)
	invoker := &fakePlatformInvoker{
		onInvoke: func(
			method string,
			_ *anypb.Any,
		) (*anypb.Any, error) {
			methods = append(methods, method)
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return anypb.New(
					&portfoliov1.SaveStrategyIndicatorsV2Response{
						DefinitionsSaved: 1,
						ChunksSaved:      1,
					},
				)
			case "portfolio.FinalizeStrategyIndicatorChunksV2":
				return anypb.New(
					&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
						ChunksFinalized: 1,
					},
				)
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				return anypb.New(
					&portfoliov1.UpdateSessionResponse{},
				)
			default:
				return nil, fmt.Errorf(
					"unexpected method: %s",
					method,
				)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       t.TempDir(),
		WorkerStopper:   workers,
		PlatformInvoker: invoker,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 21)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 21,
			token:      "worker-token",
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}

	if err := agent.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if workers.stopAllCalls != 1 {
		t.Fatalf("StopAll calls = %d, want 1", workers.stopAllCalls)
	}
	if !slices.Equal(methods, []string{
		"portfolio.SaveStrategyIndicatorsV2",
		"portfolio.FinalizeStrategyIndicatorChunksV2",
		"portfolio.GetSession",
		"portfolio.UpdateSession",
	}) {
		t.Fatalf("shutdown methods = %v", methods)
	}
	agent.mu.Lock()
	_, retained := agent.generations[sessionID]
	agent.mu.Unlock()
	if retained || agent.indicatorSync.lookupSession(sessionID) != nil {
		t.Fatal("shutdown retained finalized generation or indicator state")
	}
	response := agent.HandleRuntimeRequest(
		context.Background(),
		&cpv1.RuntimeFrame{
			CorrelationId: "after-shutdown",
			Payload: &cpv1.RuntimeFrame_Request{
				Request: &cpv1.StrategyRequest{
					Method: "RunStrategy",
				},
			},
		},
	)
	if response.GetError().GetCode() != "Unavailable" {
		t.Fatalf("post-shutdown response = %+v", response)
	}
}

func TestAgentShutdownClaimsRecoverableBeforeStopAllDisconnect(t *testing.T) {
	const sessionID = "18181818181818181818181818181818"
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
		status:      "running",
	}
	workers := &shutdownWorkerStopper{}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       t.TempDir(),
		WorkerStopper:   workers,
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 24)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	var disconnectErr error
	workers.onStopAll = func() {
		disconnectErr = agent.HandleWorkerDisconnect(
			WorkerIdentity{
				SessionID:  sessionID,
				Generation: 24,
			},
			errors.New("worker exited during runtime shutdown"),
		)
	}

	if err := agent.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if disconnectErr != nil {
		t.Fatalf("shutdown disconnect cleanup: %v", disconnectErr)
	}
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{
		"GetSession",
		"UpdateSession:recoverable",
	}) {
		t.Fatalf(
			"shutdown disconnect events = %v, want recoverable terminal claim",
			got,
		)
	}
}

func TestAgentShutdownWaitsForInFlightFinalStatusLifecycle(t *testing.T) {
	const sessionID = "18191919191919191919191919191919"
	platform := &finalStatusShutdownRacePlatform{
		updateStarted: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
		getStarted:    make(chan struct{}, 1),
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 24)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	finalDone := make(chan error, 1)
	go func() {
		err := agent.handleWorkerFinalStatus(
			context.Background(),
			generation,
			"final-frame",
			&rwv1.FinalStatus{
				SessionId:     sessionID,
				Status:        "finished",
				BarsProcessed: 10,
			},
			func(*rwv1.AgentFrame) error { return nil },
		)
		if err == nil {
			generation.mu.Lock()
			generation.terminalAck = true
			generation.mu.Unlock()
		}
		finalDone <- err
	}()
	select {
	case <-platform.updateStarted:
	case <-time.After(time.Second):
		t.Fatal("FinalStatus did not reach its terminal update")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- agent.Shutdown(context.Background(), time.Second)
	}()
	reconciledEarly := false
	select {
	case <-platform.getStarted:
		reconciledEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(platform.releaseUpdate)
	if err := <-finalDone; err != nil {
		t.Fatalf("FinalStatus: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if reconciledEarly {
		t.Fatal("shutdown reconciliation ran concurrently with FinalStatus")
	}
}

func TestAgentShutdownPersistsStatusRetryWithoutIndicators(t *testing.T) {
	const sessionID = "19191919191919191919191919191919"
	stateRoot := t.TempDir()
	var update portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(
			method string,
			request *anypb.Any,
		) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&update); err != nil {
					return nil, err
				}
				return nil, errors.New("database unavailable")
			default:
				return nil, fmt.Errorf(
					"unexpected method: %s",
					method,
				)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       stateRoot,
		WorkerStopper:   &shutdownWorkerStopper{},
		PlatformInvoker: invoker,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 25)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	if err := agent.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown with durable status retry: %v", err)
	}
	if update.GetStatus() != "recoverable" {
		t.Fatalf("terminal update = %+v, want recoverable", &update)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("terminal retry records = %+v, want one record", records)
	}
	record := records[0]
	if record.SessionID != sessionID ||
		record.Generation != 25 ||
		record.DesiredStatus != "recoverable" ||
		record.EffectiveStatus != "recoverable" ||
		record.Indicators != nil {
		t.Fatalf("terminal retry record = %+v", record)
	}
}

func TestAgentShutdownCanRetryAfterUnsafeTerminalFailure(t *testing.T) {
	const sessionID = "20202020202020202020202020202020"
	updateCalls := 0
	invoker := &fakePlatformInvoker{
		onInvoke: func(
			method string,
			_ *anypb.Any,
		) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				updateCalls++
				if updateCalls == 1 {
					return nil, errors.New("database unavailable")
				}
				return anypb.New(
					&portfoliov1.UpdateSessionResponse{},
				)
			default:
				return nil, fmt.Errorf(
					"unexpected method: %s",
					method,
				)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		WorkerStopper:   &shutdownWorkerStopper{},
		PlatformInvoker: invoker,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 26)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	if err := agent.Shutdown(
		context.Background(),
		time.Second,
	); err == nil {
		t.Fatal("first Shutdown error = nil, want unsafe persistence failure")
	}
	if err := agent.Shutdown(
		context.Background(),
		time.Second,
	); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if updateCalls != 2 {
		t.Fatalf("terminal UpdateSession calls = %d, want 2", updateCalls)
	}
	agent.mu.Lock()
	_, retained := agent.generations[sessionID]
	agent.mu.Unlock()
	if retained {
		t.Fatal("successful shutdown retry retained worker generation")
	}
}

func TestAgentShutdownAcceptsPersistedIndicatorRetryWhenPlatformSaveFails(
	t *testing.T,
) {
	const sessionID = "16161616161616161616161616161616"
	stateRoot := t.TempDir()
	workers := &shutdownWorkerStopper{}
	invoker := &fakePlatformInvoker{
		onInvoke: func(
			method string,
			_ *anypb.Any,
		) (*anypb.Any, error) {
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return nil, errors.New("database unavailable")
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				return anypb.New(
					&portfoliov1.UpdateSessionResponse{},
				)
			default:
				return nil, fmt.Errorf(
					"unexpected method: %s",
					method,
				)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		StateRoot:                stateRoot,
		WorkerStopper:            workers,
		PlatformInvoker:          invoker,
		RequestTimeout:           80 * time.Millisecond,
		IndicatorFinalizeTimeout: 20 * time.Millisecond,
		IndicatorRetryInitial:    10 * time.Millisecond,
		IndicatorRetryMax:        10 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 22)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 22,
			token:      "worker-token",
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}

	if err := agent.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown with durable retry: %v", err)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != sessionID ||
		records[0].Indicators == nil {
		t.Fatalf("shutdown retry records = %+v", records)
	}
}

func TestAgentShutdownUsesSharedDeadlineWhilePersistingIndicatorRetry(
	t *testing.T,
) {
	const sessionID = "17171717171717171717171717171717"
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       t.TempDir(),
		WorkerStopper:   &shutdownWorkerStopper{},
		PlatformInvoker: blockingPlatformInvoker{},
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 23)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 23,
			token:      "worker-token",
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()
	started := time.Now()
	if err := agent.Shutdown(shutdownCtx, time.Second); err != nil {
		t.Fatalf("Shutdown with persisted retry: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf(
			"shutdown elapsed = %v, want shared shutdown deadline",
			elapsed,
		)
	}
}

func TestGenerationAdmissionCapsWorkerRequestedPlatformTimeout(t *testing.T) {
	const sessionID = "12121212121212121212121212121212"
	platform := &contextDeadlinePlatform{done: make(chan struct{})}
	agent := NewAgent(AgentConfig{PlatformInvoker: platform, RequestTimeout: 25 * time.Millisecond})
	generation := newWorkerGeneration(sessionID, 9)
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()
	request, _ := anypb.New(&portfoliov1.GetSessionRequest{SessionId: sessionID})
	callDone := make(chan error, 1)
	go func() {
		callDone <- agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
				CallId: "bounded", Method: "portfolio.GetSession", Request: request,
				TimeoutMs: int64(time.Hour / time.Millisecond),
			}},
		}, func(*rwv1.AgentFrame) error { return nil })
	}()
	select {
	case err := <-callDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("worker-controlled timeout escaped Agent lifecycle bound")
	}
	select {
	case <-platform.done:
	default:
		t.Fatal("platform call context was not cancelled")
	}
}

func TestGenerationCleanupReconcilesRunningOnceAndPreservesTerminalStatuses(t *testing.T) {
	for _, statusValue := range []string{
		"pending",
		"running",
		"stopping",
		"finished",
		"stopped",
		"failed",
		"recoverable",
	} {
		t.Run(statusValue, func(t *testing.T) {
			const sessionID = "22222222222222222222222222222222"
			platform := &admissionCleanupPlatform{
				saveStarted: make(chan struct{}), releaseSave: make(chan struct{}), status: statusValue,
			}
			agent := NewAgent(AgentConfig{
				RuntimeID: "rt-1", UserID: 6, PlatformInvoker: platform, RequestTimeout: time.Second,
			})
			generation := newWorkerGeneration(sessionID, 8)
			generation.durablePossible = true
			generation.runningAccepted = statusValue == "running"
			agent.mu.Lock()
			agent.generations[sessionID] = generation
			agent.mu.Unlock()
			identity := WorkerIdentity{SessionID: sessionID, Generation: 8}
			if err := agent.HandleWorkerDisconnect(identity, errors.New("child exited")); err != nil {
				t.Fatal(err)
			}
			if err := agent.HandleWorkerDisconnect(identity, errors.New("duplicate disconnect")); err != nil {
				t.Fatal(err)
			}
			events := platform.snapshotEvents()
			switch statusValue {
			case "pending":
				if !slices.Equal(events, []string{
					"GetSession",
					"UpdateSession:failed",
				}) {
					t.Fatalf("events = %v", events)
				}
			case "running", "stopping":
				if !slices.Equal(events, []string{
					"GetSession",
					"UpdateSession:recoverable",
				}) {
					t.Fatalf("events = %v", events)
				}
			default:
				if !slices.Equal(events, []string{"GetSession"}) {
					t.Fatalf("terminal events = %v", events)
				}
			}
		})
	}
}

func TestGenerationCleanupClearsPendingAfterRetainedTailFinalizes(t *testing.T) {
	const sessionID = "29292929292929292929292929292929"
	platform := &admissionCleanupPlatform{
		saveStarted:                  make(chan struct{}),
		releaseSave:                  make(chan struct{}),
		status:                       "recoverable",
		indicatorFinalizationPending: true,
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: platform, RequestTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 15)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	if err := agent.HandleWorkerDisconnect(
		WorkerIdentity{SessionID: sessionID, Generation: 15},
		errors.New("worker exited after retained indicator retry"),
	); err != nil {
		t.Fatal(err)
	}
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{
		"GetSession", "UpdateSession:recoverable",
	}) {
		t.Fatalf("events = %v", got)
	}
	update := platform.snapshotLastUpdate()
	if update == nil ||
		update.IndicatorFinalizationPending == nil ||
		update.GetIndicatorFinalizationPending() {
		t.Fatalf("pending-clear update = %+v, want explicit false", update)
	}
}

func TestGenerationCleanupPreservesAcknowledgedExplicitStop(t *testing.T) {
	const sessionID = "23232323232323232323232323232323"
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}), releaseSave: make(chan struct{}), status: "running",
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: platform, RequestTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 10)
	generation.durablePossible = true
	generation.explicitStopAck = true
	generation.explicitStopStatus = "stopped"
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	if err := agent.HandleWorkerDisconnect(
		WorkerIdentity{SessionID: sessionID, Generation: 10},
		errors.New("worker exited after stop acknowledgement"),
	); err != nil {
		t.Fatal(err)
	}
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{
		"GetSession", "UpdateSession:stopped",
	}) {
		t.Fatalf("events = %v", got)
	}
}

func TestStopResponseAlreadyQueuedWinsDisconnectReconciliation(t *testing.T) {
	const sessionID = "26262626262626262626262626262626"
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}), releaseSave: make(chan struct{}), status: "running",
	}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerSender: sender,
		PlatformInvoker: platform, RequestTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 13)
	generation.durablePossible = true
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	disconnectDone := make(chan error, 1)
	sender.onSend = func(sentSessionID string, frame *rwv1.AgentFrame) {
		response, err := anypb.New(&strategyv1.StopStrategyResponse{Stopped: true})
		if err != nil {
			t.Errorf("pack stop response: %v", err)
			return
		}
		_ = agent.HandleWorkerFrame(context.Background(), sentSessionID, &rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
				CallId: frame.GetPlatformCall().GetCallId(), Ok: true, Response: response,
			}},
		}, nil)
		go func() {
			disconnectDone <- agent.HandleWorkerDisconnect(
				WorkerIdentity{SessionID: sessionID, Generation: 13}, nil,
			)
		}()
		select {
		case cleanupErr := <-disconnectDone:
			// The old implementation could finish failed reconciliation here,
			// before the queued StopStrategy result was interpreted.
			disconnectDone <- cleanupErr
		case <-time.After(30 * time.Millisecond):
		}
	}
	request, _ := anypb.New(&strategyv1.StopStrategyRequest{
		SessionId: sessionID, UserId: 6, RuntimeId: "rt-1",
		StopAction: strategyv1.StopAction_STOP_ACTION_STOP_ONLY,
	})
	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-stop-disconnect-race",
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method: "StopStrategy", Request: request,
		}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("stop frame = %+v", frame)
	}
	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatalf("disconnect cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect cleanup did not complete")
	}
	if got := platform.snapshotEvents(); !slices.Equal(got, []string{
		"GetSession", "UpdateSession:stopped",
	}) {
		t.Fatalf("events = %v", got)
	}
}

func TestReconcileWorkerGenerationRejectsEmptySuccessfulGetSession(t *testing.T) {
	packed, err := anypb.New(&portfoliov1.GetSessionResponse{})
	if err != nil {
		t.Fatal(err)
	}
	invoker := &fakePlatformInvoker{response: packed}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: invoker, RequestTimeout: time.Second,
	})
	generation := newWorkerGeneration("24242424242424242424242424242424", 11)
	generation.durablePossible = true

	err = agent.reconcileWorkerGeneration(
		context.Background(),
		"24242424242424242424242424242424",
		generation,
		"worker disconnected",
	)
	if err == nil || !strings.Contains(err.Error(), "missing session") {
		t.Fatalf("reconcile error = %v, want ambiguous empty response error", err)
	}
}

func TestReconcileWorkerGenerationOnlyAcceptsExplicitNotFoundCode(t *testing.T) {
	generation := newWorkerGeneration("25252525252525252525252525252525", 12)
	generation.durablePossible = true

	internal := &fakePlatformInvoker{onInvoke: func(string, *anypb.Any) (*anypb.Any, error) {
		return nil, errors.New("Internal: backing table not found")
	}}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: internal, RequestTimeout: time.Second,
	})
	if err := agent.reconcileWorkerGeneration(
		context.Background(), generation.sessionID, generation, "worker disconnected",
	); err == nil {
		t.Fatal("internal error containing 'not found' was treated as confirmed NotFound")
	}

	notFound := &fakePlatformInvoker{onInvoke: func(string, *anypb.Any) (*anypb.Any, error) {
		return nil, errors.New("NotFound: session not found")
	}}
	agent.cfg.PlatformInvoker = notFound
	if err := agent.reconcileWorkerGeneration(
		context.Background(), generation.sessionID, generation, "worker disconnected",
	); err != nil {
		t.Fatalf("explicit NotFound reconciliation = %v", err)
	}
}

func TestAgentChildExitAfterRunningReconcilesCanonicalRowRecoverable(
	t *testing.T,
) {
	processExited := make(chan struct{})
	platform := &admissionCleanupPlatform{
		saveStarted: make(chan struct{}), releaseSave: make(chan struct{}),
	}
	close(platform.releaseSave)
	starter := &fakeWorkerStarter{worker: &ManagedWorker{processExited: processExited}}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, WorkerStarter: starter,
		PlatformInvoker: platform, StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	starter.onStart = func(sessionID string) {
		go func() {
			save, _ := anypb.New(&portfoliov1.SaveSessionRequest{SessionId: sessionID})
			if err := agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
					CallId: "save", Method: "portfolio.SaveSession", Request: save,
				}},
			}, func(*rwv1.AgentFrame) error { return nil }); err != nil {
				t.Errorf("save call: %v", err)
			}
			update, _ := anypb.New(&portfoliov1.UpdateSessionRequest{SessionId: sessionID, Status: "running"})
			if err := agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
					CallId: "running", Method: "portfolio.UpdateSession", Request: update,
				}},
			}, func(*rwv1.AgentFrame) error { return nil }); err != nil {
				t.Errorf("running call: %v", err)
			}
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID, Status: "running",
				}},
			}, nil)
			close(processExited)
		}()
	}
	request, _ := anypb.New(&strategyv1.RunStrategyRequest{PortfolioId: 1, UserId: 6, RuntimeId: "rt-1"})
	response := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-child-exit",
		Payload:       &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{Method: "RunStrategy", Request: request}},
	})
	if response.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("response = %+v", response)
	}
	deadline := time.Now().Add(time.Second)
	for {
		events := platform.snapshotEvents()
		if len(events) >= 4 &&
			events[len(events)-1] == "UpdateSession:recoverable" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cleanup events = %v", events)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAgentRunStrategyReturnsWorkerStartFailure(t *testing.T) {
	starter := &fakeWorkerStarter{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  starter,
		StartTimeout:   time.Second,
		RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID,
					Status:    "failed",
					Error:     "backtest profile preflight failed",
				}},
			}, nil)
		}()
	}

	req, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}
	respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-1",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "RunStrategy",
			Request: req,
		}},
	})

	if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("response frame type = %v", respFrame.GetFrameType())
	}
	if respFrame.GetError().GetCode() != "FailedPrecondition" || respFrame.GetError().GetMessage() != "backtest profile preflight failed" {
		t.Fatalf("error frame = %+v", respFrame.GetError())
	}
}

func TestAgentRunDependencyFailurePreservesTypedDetail(t *testing.T) {
	starter := &fakeWorkerStarter{}
	agent := NewAgent(AgentConfig{RuntimeID: "rt-1", WorkerStarter: starter, StartTimeout: time.Second})
	enableStrategyStartProtocol(agent, starter)
	detail := &strategyv1.RuntimeDependencyError{
		Code: "STRATEGY_DEPENDENCY_UNAVAILABLE", Module: "pandas_ta",
		RuntimeProfile: "platform-python-3.13", RuntimeProfileVersion: "1.0.0", ImageBuildId: "build-1",
	}
	starter.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID, Status: "failed", Error: "strategy dependency validation failed", DependencyError: detail,
				}},
			}, nil)
		}()
	}
	request, _ := anypb.New(&strategyv1.RunStrategyRequest{PortfolioId: 1, UserId: 6, RuntimeId: "rt-1"})
	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-run-dependency",
		Payload:       &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{Method: "RunStrategy", Request: request}},
	})
	if got := frame.GetError().GetDependencyError(); got == nil || got.GetModule() != "pandas_ta" {
		t.Fatalf("dependency detail = %+v", got)
	}
}

func TestAgentRunPrefersAcceptedRunningOverImmediatelyFollowingFailure(t *testing.T) {
	for iteration := 0; iteration < 64; iteration++ {
		starter := &fakeWorkerStarter{}
		agent := NewAgent(AgentConfig{
			RuntimeID: "rt-1", WorkerStarter: starter, StartTimeout: time.Second,
		})
		enableStrategyStartProtocol(agent, starter)
		starter.onStart = func(sessionID string) {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID, Status: "running",
				}},
			}, nil)
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: sessionID, Status: "failed", Error: "business loop failed immediately",
				}},
			}, nil)
		}
		request, _ := anypb.New(&strategyv1.RunStrategyRequest{
			PortfolioId: 1, UserId: 6, RuntimeId: "rt-1",
		})
		frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
			CorrelationId: fmt.Sprintf("corr-running-race-%d", iteration),
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method: "RunStrategy", Request: request,
			}},
		})
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
			t.Fatalf("iteration %d frame = %+v, want accepted running response", iteration, frame)
		}
	}
}

func TestAgentRunStrategyReturnsWorkerExitBeforeStartTimeout(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_start", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := newLegacyWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_exit_before_start",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  manager,
		StartTimeout:   time.Second,
		RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, manager)
	request, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	startedAt := time.Now()
	respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-run-worker-exit",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "RunStrategy",
			Request: request,
		}},
	})

	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("worker exit surfaced after %v, want before start timeout", elapsed)
	}
	if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("response frame type = %v", respFrame.GetFrameType())
	}
	if got := respFrame.GetError().GetCode(); got != "Internal" {
		t.Fatalf("error code = %q, want Internal", got)
	}
	if got := respFrame.GetError().GetMessage(); !strings.Contains(got, "session worker exited before reporting started") {
		t.Fatalf("error message = %q, want worker exit before reporting started", got)
	}
}

func TestAgentRunStrategyPrefersStartedWhenWorkerAlsoExited(t *testing.T) {
	request, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	for i := 0; i < 100; i++ {
		processExited := make(chan struct{})
		close(processExited)
		starter := &fakeWorkerStarter{worker: &ManagedWorker{
			processExited:  processExited,
			processExitErr: errors.New("worker exited"),
		}}
		agent := NewAgent(AgentConfig{
			RuntimeID:      "rt-1",
			WorkerStarter:  starter,
			StartTimeout:   time.Second,
			RequestTimeout: time.Second,
		})
		enableStrategyStartProtocol(agent, starter)
		starter.onStart = func(pendingSessionID string) {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID,
					Status:    "running",
				}},
			}, nil)
		}

		respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
			CorrelationId: "corr-run-started-and-exited",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "RunStrategy",
				Request: request,
			}},
		})
		if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
			t.Fatalf("iteration %d response frame type = %v error=%v", i, respFrame.GetFrameType(), respFrame.GetError())
		}
	}
}

func TestAgentForwardsWorkerPlatformCall(t *testing.T) {
	packedResponse, err := anypb.New(&strategyv1.GetStrategyStatusResponse{Status: "running"})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	invoker := &fakePlatformInvoker{response: packedResponse}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		PlatformInvoker: invoker,
	})
	packedRequest, err := anypb.New(&strategyv1.GetStrategyStatusRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}
	var sent *rwv1.AgentFrame
	err = agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_PlatformCall{PlatformCall: &rwv1.PlatformCall{
			CallId:    "call-1",
			Method:    "GetStrategyStatus",
			Request:   packedRequest,
			TimeoutMs: 1000,
		}},
	}, func(frame *rwv1.AgentFrame) error {
		sent = frame
		return nil
	})
	if err != nil {
		t.Fatalf("HandleWorkerFrame: %v", err)
	}

	if invoker.method != "GetStrategyStatus" {
		t.Fatalf("invoked method = %q", invoker.method)
	}
	result := sent.GetPlatformCallResult()
	if result == nil || !result.GetOk() || result.GetCallId() != "call-1" {
		t.Fatalf("platform result = %+v", result)
	}
}

func TestAgentPreviewRunStrategyRunsOneShotWorkerUnary(t *testing.T) {
	starter := &fakeWorkerStarter{}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		RuntimeSource:  "bare",
		RuntimeName:    "bare-debug",
		WorkerStarter:  starter,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId:       pendingSessionID,
					Token:           "token",
					Pid:             123,
					ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
	}
	sender.onSend = func(sessionID string, frame *rwv1.AgentFrame) {
		call := frame.GetPlatformCall()
		if call == nil || call.GetMethod() != "PreviewRunStrategy" {
			t.Fatalf("agent sent %+v, want PreviewRunStrategy platform_call", frame)
		}
		go func() {
			response, err := anypb.New(&strategyv1.PreviewRunStrategyResponse{Ok: true, Profile: "backtest"})
			if err != nil {
				t.Errorf("pack response: %v", err)
				return
			}
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
					CallId:   call.GetCallId(),
					Ok:       true,
					Response: response,
				}},
			}, nil)
		}()
	}
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-preview",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "PreviewRunStrategy",
			Request: request,
		}},
	})

	if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("response frame type = %v error=%v", respFrame.GetFrameType(), respFrame.GetError())
	}
	var resp strategyv1.PreviewRunStrategyResponse
	if err := respFrame.GetResponse().GetResponse().UnmarshalTo(&resp); err != nil {
		t.Fatalf("unpack response: %v", err)
	}
	if !resp.GetOk() || resp.GetProfile() != "backtest" {
		t.Fatalf("preview response = %+v", &resp)
	}
	if starter.extraEnv["HUSHINE_RUNTIME_SOURCE"] != "bare" || starter.extraEnv["HUSHINE_RUNTIME_ID"] != "rt-1" {
		t.Fatalf("worker env = %+v", starter.extraEnv)
	}
}

func TestAgentPreviewRejectsUnsupportedOneShotWorkerProtocolBeforeUnary(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	sender := &fakeWorkerSender{}
	var unaryCalls int
	var shutdown *rwv1.ShutdownWorker
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  starter,
		WorkerStopper:  stopper,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	starter.onStart = func(sessionID string) {
		if err := agent.HandleWorkerFrame(
			context.Background(),
			sessionID,
			&rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId:       sessionID,
					ProtocolVersion: 1,
				}},
			},
			func(frame *rwv1.AgentFrame) error {
				shutdown = frame.GetShutdownWorker()
				return nil
			},
		); err != nil {
			t.Errorf("HandleWorkerFrame: %v", err)
		}
	}
	sender.onSend = func(string, *rwv1.AgentFrame) {
		unaryCalls++
	}
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-preview-protocol",
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "PreviewRunStrategy",
			Request: request,
		}},
	})

	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("frame = %+v, want error", frame)
	}
	if got := frame.GetError().GetCode(); got != "RUNTIME_WORKER_PROTOCOL_UNSUPPORTED" {
		t.Fatalf("error code = %q", got)
	}
	if unaryCalls != 0 {
		t.Fatalf("unsupported one-shot worker received %d unary calls", unaryCalls)
	}
	if shutdown == nil || shutdown.GetSessionId() != starter.startedSessionID {
		t.Fatalf("shutdown = %+v, started session = %q", shutdown, starter.startedSessionID)
	}
	if stopper.sessionID != starter.startedSessionID {
		t.Fatalf("stopped session = %q, want %q", stopper.sessionID, starter.startedSessionID)
	}
}

func TestAgentPreviewRunStrategyWaitsForNaturalManagedCleanup(t *testing.T) {
	manager := &blockingWorkerLifecycle{
		waitStarted: make(chan workerExitWait, 1),
		releaseWait: make(chan struct{}),
	}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  manager,
		WorkerStopper:  manager,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	manager.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
	}
	response, err := anypb.New(&strategyv1.PreviewRunStrategyResponse{Ok: true, Profile: "backtest"})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	wireWorkerUnaryResponse(agent, sender, response)
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	responseDone := make(chan *cpv1.RuntimeFrame, 1)
	go func() {
		responseDone <- agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
			CorrelationId: "corr-preview-wait-cleanup",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "PreviewRunStrategy",
				Request: request,
			}},
		})
	}()
	var waitCall workerExitWait
	select {
	case waitCall = <-manager.waitStarted:
	case frame := <-responseDone:
		t.Fatalf("preview response returned before managed cleanup wait: %+v", frame)
	case <-time.After(time.Second):
		t.Fatal("preview did not begin managed cleanup wait")
	}
	if waitCall.sessionID != manager.startedSessionID || waitCall.timeout <= 0 {
		t.Fatalf("worker wait = %+v, want started one-shot session with positive bound", waitCall)
	}
	select {
	case frame := <-responseDone:
		t.Fatalf("preview response returned while managed cleanup was blocked: %+v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	close(manager.releaseWait)
	select {
	case frame := <-responseDone:
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
			t.Fatalf("response frame type = %v error=%v", frame.GetFrameType(), frame.GetError())
		}
	case <-time.After(time.Second):
		t.Fatal("preview response did not return after managed cleanup")
	}
	if got := manager.stopCount(); got != 0 {
		t.Fatalf("preview sent %d stop signals while waiting for natural exit", got)
	}
}

func TestAgentPreviewRunStrategyWaitTimeoutStopsOneShotWorker(t *testing.T) {
	manager := &blockingWorkerLifecycle{waitStarted: make(chan workerExitWait, 1)}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  manager,
		WorkerStopper:  manager,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	manager.onStart = func(sessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
					SessionId: sessionID, ProtocolVersion: RuntimeWorkerProtocolVersion,
				}},
			}, nil)
		}()
	}
	response, err := anypb.New(&strategyv1.PreviewRunStrategyResponse{Ok: true, Profile: "backtest"})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	wireWorkerUnaryResponse(agent, sender, response)
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId:  "corr-preview-wait-timeout",
		DeadlineUnixMs: time.Now().Add(80 * time.Millisecond).UnixMilli(),
		FrameType:      cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "PreviewRunStrategy",
			Request: request,
		}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || frame.GetError().GetCode() != "DeadlineExceeded" {
		t.Fatalf("preview timeout frame = %+v, want DeadlineExceeded", frame)
	}
	select {
	case waitCall := <-manager.waitStarted:
		if waitCall.timeout <= 0 || waitCall.timeout > 100*time.Millisecond {
			t.Fatalf("worker wait timeout = %v, want remaining frame bound", waitCall.timeout)
		}
	default:
		t.Fatal("preview did not wait for managed cleanup")
	}
	if got := manager.stopCount(); got != 1 {
		t.Fatalf("preview timeout sent %d stop signals, want 1", got)
	}
}

func TestAgentPreviewRunStrategyReturnsWorkerExitBeforeReadyTimeout(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_hello", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := newLegacyWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_exit_before_hello",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  manager,
		RequestTimeout: time.Second,
	})
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	startedAt := time.Now()
	respFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-preview-worker-exit",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "PreviewRunStrategy",
			Request: request,
		}},
	})

	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("worker exit surfaced after %v, want before readiness timeout", elapsed)
	}
	if respFrame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
		t.Fatalf("response frame type = %v", respFrame.GetFrameType())
	}
	if got := respFrame.GetError().GetCode(); got != "Internal" {
		t.Fatalf("error code = %q, want Internal", got)
	}
	if got := respFrame.GetError().GetMessage(); !strings.Contains(got, "session worker exited before connecting") {
		t.Fatalf("error message = %q, want worker exit before connecting", got)
	}
}

func TestAgentPreviewRunStrategyWaitsForManagedCleanupAfterProcessExit(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_cleanup", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := newLegacyWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_exit_before_cleanup",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	manager.cleanupSessionRoot = func(string) error {
		close(cleanupStarted)
		<-releaseCleanup
		return nil
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStarter:  manager,
		RequestTimeout: 2 * time.Second,
	})
	request, err := anypb.New(&strategyv1.PreviewRunStrategyRequest{
		PortfolioId: 1,
		UserId:      6,
		RuntimeId:   "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	response := make(chan *cpv1.RuntimeFrame, 1)
	go func() {
		response <- agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
			CorrelationId: "corr-preview-cleanup-blocked",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "PreviewRunStrategy",
				Request: request,
			}},
		})
	}()

	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("worker cleanup did not start")
	}
	select {
	case respFrame := <-response:
		t.Fatalf("response returned before managed cleanup: %+v", respFrame)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCleanup)
	select {
	case respFrame := <-response:
		if respFrame.GetError().GetCode() != "Internal" {
			t.Fatalf("error code = %q, want Internal", respFrame.GetError().GetCode())
		}
		if got := respFrame.GetError().GetMessage(); !strings.Contains(got, "session worker exited before connecting") {
			t.Fatalf("error message = %q, want worker exit before connecting", got)
		}
	case <-time.After(time.Second):
		t.Fatal("response did not return after managed cleanup")
	}
}

func TestWaitWorkerReadyPrefersReadyWhenProcessAlsoExited(t *testing.T) {
	agent := NewAgent(AgentConfig{RequestTimeout: time.Second})
	for i := 0; i < 100; i++ {
		ready := make(chan struct{}, 1)
		ready <- struct{}{}
		processExited := make(chan struct{})
		close(processExited)
		worker := &ManagedWorker{
			processExited:  processExited,
			processExitErr: errors.New("worker exited"),
		}
		if err := agent.waitWorkerReady(context.Background(), ready, nil, worker, time.Second); err != nil {
			t.Fatalf("iteration %d waitWorkerReady: %v", i, err)
		}
	}
}

func TestAgentRoutesStatusAndStopToRunningWorker(t *testing.T) {
	sender := &fakeWorkerSender{}
	stopper := &fakeWorkerStopper{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerSender:   sender,
		WorkerStopper:  stopper,
		RequestTimeout: time.Second,
	})
	sender.onSend = func(sessionID string, frame *rwv1.AgentFrame) {
		call := frame.GetPlatformCall()
		if call == nil {
			t.Fatalf("agent sent %+v, want platform_call", frame)
		}
		var response *anypb.Any
		var err error
		switch call.GetMethod() {
		case "GetStrategyStatus":
			response, err = anypb.New(&strategyv1.GetStrategyStatusResponse{Status: "running", BarsProcessed: 7})
		case "StopStrategy":
			response, err = anypb.New(&strategyv1.StopStrategyResponse{Stopped: true})
		default:
			t.Fatalf("unexpected method %q", call.GetMethod())
		}
		if err != nil {
			t.Errorf("pack response: %v", err)
			return
		}
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
					CallId:   call.GetCallId(),
					Ok:       true,
					Response: response,
				}},
			}, nil)
		}()
	}

	statusReq, _ := anypb.New(&strategyv1.GetStrategyStatusRequest{SessionId: "sess-1", UserId: 6, RuntimeId: "rt-1"})
	statusFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-status",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "GetStrategyStatus",
			Request: statusReq,
		}},
	})
	var statusResp strategyv1.GetStrategyStatusResponse
	if err := statusFrame.GetResponse().GetResponse().UnmarshalTo(&statusResp); err != nil {
		t.Fatalf("unpack status: %v", err)
	}
	if statusResp.GetStatus() != "running" || statusResp.GetBarsProcessed() != 7 {
		t.Fatalf("status response = %+v", &statusResp)
	}

	stopReq, _ := anypb.New(&strategyv1.StopStrategyRequest{SessionId: "sess-1", UserId: 6, RuntimeId: "rt-1"})
	stopFrame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId: "corr-stop",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "StopStrategy",
			Request: stopReq,
		}},
	})
	var stopResp strategyv1.StopStrategyResponse
	if err := stopFrame.GetResponse().GetResponse().UnmarshalTo(&stopResp); err != nil {
		t.Fatalf("unpack stop: %v", err)
	}
	if !stopResp.GetStopped() {
		t.Fatalf("stop response = %+v", &stopResp)
	}
	if stopper.waitSessionID != "sess-1" || stopper.waitTimeout <= 0 {
		t.Fatalf("worker exit wait = %q/%v, want sess-1 with positive bound", stopper.waitSessionID, stopper.waitTimeout)
	}
}

func TestAgentStopStrategyWaitsForManagedCleanupBeforeResponse(t *testing.T) {
	manager := &blockingWorkerLifecycle{
		waitStarted: make(chan workerExitWait, 1),
		releaseWait: make(chan struct{}),
	}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStopper:  manager,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	response, err := anypb.New(&strategyv1.StopStrategyResponse{Stopped: true})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	wireWorkerUnaryResponse(agent, sender, response)
	request, err := anypb.New(&strategyv1.StopStrategyRequest{
		SessionId: "sess-stop-wait",
		UserId:    6,
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	responseDone := make(chan *cpv1.RuntimeFrame, 1)
	go func() {
		responseDone <- agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
			CorrelationId: "corr-stop-wait-cleanup",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "StopStrategy",
				Request: request,
			}},
		})
	}()
	select {
	case waitCall := <-manager.waitStarted:
		if waitCall.sessionID != "sess-stop-wait" || waitCall.timeout <= 0 {
			t.Fatalf("worker wait = %+v", waitCall)
		}
	case frame := <-responseDone:
		t.Fatalf("stop response returned before managed cleanup wait: %+v", frame)
	case <-time.After(time.Second):
		t.Fatal("StopStrategy did not begin managed cleanup wait")
	}
	select {
	case frame := <-responseDone:
		t.Fatalf("stop response returned while cleanup was blocked: %+v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	close(manager.releaseWait)
	select {
	case frame := <-responseDone:
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
			t.Fatalf("response frame type = %v error=%v", frame.GetFrameType(), frame.GetError())
		}
	case <-time.After(time.Second):
		t.Fatal("stop response did not return after managed cleanup")
	}
	if got := manager.stopCount(); got != 0 {
		t.Fatalf("StopStrategy wait sent %d agent-side stop signals", got)
	}
}

func TestAgentStopStrategyWaitCancellationDoesNotSignalWorker(t *testing.T) {
	manager := &blockingWorkerLifecycle{waitStarted: make(chan workerExitWait, 1)}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStopper:  manager,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	response, err := anypb.New(&strategyv1.StopStrategyResponse{Stopped: true})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	wireWorkerUnaryResponse(agent, sender, response)
	request, err := anypb.New(&strategyv1.StopStrategyRequest{
		SessionId: "sess-stop-cancel",
		UserId:    6,
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	responseDone := make(chan *cpv1.RuntimeFrame, 1)
	go func() {
		responseDone <- agent.HandleRuntimeRequest(ctx, &cpv1.RuntimeFrame{
			CorrelationId: "corr-stop-wait-cancel",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "StopStrategy",
				Request: request,
			}},
		})
	}()
	select {
	case <-manager.waitStarted:
	case frame := <-responseDone:
		t.Fatalf("stop response returned before managed cleanup wait: %+v", frame)
	case <-time.After(time.Second):
		t.Fatal("StopStrategy did not begin managed cleanup wait")
	}
	cancel()
	select {
	case frame := <-responseDone:
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR {
			t.Fatalf("canceled stop frame = %+v, want error", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled StopStrategy wait did not return")
	}
	if got := manager.stopCount(); got != 0 {
		t.Fatalf("canceled StopStrategy wait sent %d agent-side stop signals", got)
	}
}

func TestAgentStopStrategyWaitTimeoutDoesNotSignalWorker(t *testing.T) {
	manager := &blockingWorkerLifecycle{waitStarted: make(chan workerExitWait, 1)}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerStopper:  manager,
		WorkerSender:   sender,
		RequestTimeout: time.Second,
	})
	response, err := anypb.New(&strategyv1.StopStrategyResponse{Stopped: true})
	if err != nil {
		t.Fatalf("pack response: %v", err)
	}
	wireWorkerUnaryResponse(agent, sender, response)
	request, err := anypb.New(&strategyv1.StopStrategyRequest{
		SessionId: "sess-stop-timeout",
		UserId:    6,
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}

	frame := agent.HandleRuntimeRequest(context.Background(), &cpv1.RuntimeFrame{
		CorrelationId:  "corr-stop-wait-timeout",
		DeadlineUnixMs: time.Now().Add(80 * time.Millisecond).UnixMilli(),
		FrameType:      cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "StopStrategy",
			Request: request,
		}},
	})
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ERROR || frame.GetError().GetCode() != "DeadlineExceeded" {
		t.Fatalf("StopStrategy timeout frame = %+v, want DeadlineExceeded", frame)
	}
	select {
	case waitCall := <-manager.waitStarted:
		if waitCall.sessionID != "sess-stop-timeout" || waitCall.timeout <= 0 || waitCall.timeout > 100*time.Millisecond {
			t.Fatalf("worker wait = %+v, want remaining frame bound", waitCall)
		}
	default:
		t.Fatal("StopStrategy did not wait for managed cleanup")
	}
	if got := manager.stopCount(); got != 0 {
		t.Fatalf("timed-out StopStrategy wait sent %d agent-side stop signals", got)
	}
}

func TestAgentRuntimeDataReturnsDeliveryError(t *testing.T) {
	sender := &fakeWorkerSender{sendErr: errors.New("worker gone")}
	agent := NewAgent(AgentConfig{WorkerSender: sender})

	err := agent.HandleRuntimeData(context.Background(), &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		Payload: &cpv1.RuntimeFrame_LiveKlineBatch{LiveKlineBatch: &cpv1.RuntimeLiveKlineBatch{
			SessionId: "sess-1",
			StreamKey: "futures:ZECUSDT:1m",
			Sequence:  42,
		}},
	})

	if err == nil || err.Error() != "worker gone" {
		t.Fatalf("HandleRuntimeData error = %v", err)
	}
}

func TestAgentIndicatorFrameBuffersWithoutImmediatePlatformWrite(t *testing.T) {
	invoker := &fakePlatformInvoker{}
	agent := NewAgent(AgentConfig{
		PlatformInvoker: invoker,
	})

	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_IndicatorFrame{IndicatorFrame: &rwv1.IndicatorFrame{
			SessionId:    "sess-1",
			UserId:       6,
			StrategyId:   12,
			StreamKey:    "futures:ZECUSDT:1m",
			MarketTimeMs: 123000,
			IntervalMs:   60000,
			Definitions: []*rwv1.IndicatorDefinition{{
				IndicatorKey: "bb_mid",
				Name:         "BB Mid",
				Type:         "line",
				Pane:         "price",
				Color:        "#22c55e",
			}},
			Values: []*rwv1.IndicatorValue{{
				IndicatorKey: "bb_mid",
				Value:        100.5,
				HasValue:     true,
			}},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("HandleWorkerFrame indicator: %v", err)
	}
	if invoker.method != "" {
		t.Fatalf("platform method = %q before flush, want empty", invoker.method)
	}
	if err := agent.indicatorSync.FlushSession(context.Background(), "sess-1", false); err != nil {
		t.Fatalf("FlushSession: %v", err)
	}

	if invoker.method != "portfolio.SaveStrategyIndicators" {
		t.Fatalf("platform method = %q", invoker.method)
	}
	var req portfoliov1.SaveStrategyIndicatorsRequest
	if err := invoker.request.UnmarshalTo(&req); err != nil {
		t.Fatalf("unpack save indicators request: %v", err)
	}
	if req.GetSessionId() != "sess-1" || req.GetUserId() != 6 {
		t.Fatalf("save request session/user = %q/%d", req.GetSessionId(), req.GetUserId())
	}
	if len(req.GetDefinitions()) != 1 || req.GetDefinitions()[0].GetIndicatorKey() != "bb_mid" {
		t.Fatalf("definitions = %+v", req.GetDefinitions())
	}
	if len(req.GetChunks()) != 1 {
		t.Fatalf("chunks = %+v", req.GetChunks())
	}
	chunk := req.GetChunks()[0]
	if chunk.GetIndicatorKey() != "bb_mid" || chunk.GetCount() != 1 || chunk.GetFinalized() {
		t.Fatalf("chunk = %+v", chunk)
	}
	if chunk.GetValuesJson() != `{"values":[100.5],"times":null}` {
		t.Fatalf("values_json = %q", chunk.GetValuesJson())
	}
}

func TestAgentFinalStatusFlushesThenPersistsFinishedThenAcknowledges(t *testing.T) {
	invoker := &agentFinalPlatform{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", PlatformInvoker: invoker,
		IndicatorFinalizeTimeout: time.Second, IndicatorRetryInitial: time.Millisecond,
	})
	bufferAgentIndicator(t, agent, "sess-1")
	var ack *rwv1.AgentFrame
	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		FrameId: "final-1",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-1", Status: "finished", BarsProcessed: 1440,
		}},
	}, func(frame *rwv1.AgentFrame) error { ack = frame; return nil })
	if err != nil {
		t.Fatalf("HandleWorkerFrame final: %v", err)
	}
	methods, updates := invoker.snapshot()
	if len(methods) != 2 || methods[0] != "portfolio.SaveStrategyIndicators" || methods[1] != "portfolio.UpdateSession" {
		t.Fatalf("platform methods = %v", methods)
	}
	if len(updates) != 1 || updates[0].GetStatus() != "finished" || updates[0].GetBarsProcessed() != 1440 {
		t.Fatalf("updates = %+v", updates)
	}
	if ack == nil || ack.GetReplyTo() != "final-1" || ack.GetPayload() != nil {
		t.Fatalf("ack = %+v, want payloadless reply_to final-1", ack)
	}
}

func TestAgentFinalStatusWaitsForGenerationAdmissionToDrain(t *testing.T) {
	const sessionID = "61616161616161616161616161616161"
	platformCall := make(chan string, 8)
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, _ *anypb.Any) (*anypb.Any, error) {
			platformCall <- method
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return anypb.New(
					&portfoliov1.SaveStrategyIndicatorsV2Response{
						DefinitionsSaved: 1,
						ChunksSaved:      1,
					},
				)
			case "portfolio.FinalizeStrategyIndicatorChunksV2":
				return anypb.New(
					&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
						ChunksFinalized: 1,
					},
				)
			case "portfolio.UpdateSession":
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		PlatformInvoker:          invoker,
		RequestTimeout:           time.Second,
		IndicatorFinalizeTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 1)
	generation.durablePossible = true
	agent.generations[sessionID] = generation
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 7,
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}
	if !generation.admit("test-in-flight-frame") {
		t.Fatal("admit in-flight frame")
	}

	finalDone := make(chan error, 1)
	go func() {
		finalDone <- agent.HandleWorkerFrame(
			context.Background(),
			sessionID,
			&rwv1.WorkerFrame{
				FrameId: "final-drain",
				Payload: &rwv1.WorkerFrame_FinalStatus{
					FinalStatus: &rwv1.FinalStatus{
						SessionId: sessionID,
						Status:    "finished",
					},
				},
			},
			func(*rwv1.AgentFrame) error { return nil },
		)
	}()

	select {
	case method := <-platformCall:
		t.Fatalf(
			"terminal platform method %q ran before admitted frame drained",
			method,
		)
	case err := <-finalDone:
		t.Fatalf("final status returned before admitted frame drained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	generation.completePlatformCall()
	select {
	case err := <-finalDone:
		if err != nil {
			t.Fatalf("HandleWorkerFrame final: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("final status did not continue after admission drained")
	}
}

func TestAgentFinalStatusDrainTimeoutPersistsTerminalRetry(t *testing.T) {
	const sessionID = "71717171717171717171717171717171"
	stateRoot := t.TempDir()
	var update portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			if method != "portfolio.UpdateSession" {
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
			if err := request.UnmarshalTo(&update); err != nil {
				return nil, err
			}
			return anypb.New(&portfoliov1.UpdateSessionResponse{})
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		StateRoot:       stateRoot,
		PlatformInvoker: invoker,
		RequestTimeout:  20 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 17)
	if !generation.admit("portfolio.SaveSession") {
		t.Fatal("admit durable in-flight call")
	}
	agent.generations[sessionID] = generation

	err := agent.HandleWorkerFrame(
		context.Background(),
		sessionID,
		&rwv1.WorkerFrame{
			FrameId: "final-drain-timeout",
			Payload: &rwv1.WorkerFrame_FinalStatus{
				FinalStatus: &rwv1.FinalStatus{
					SessionId:     sessionID,
					Status:        "finished",
					BarsProcessed: 9,
				},
			},
		},
		func(*rwv1.AgentFrame) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "worker frame drain failed") {
		t.Fatalf("final drain error = %v", err)
	}
	if update.GetStatus() != "recoverable" ||
		update.IndicatorFinalizationPending == nil ||
		!update.GetIndicatorFinalizationPending() {
		t.Fatalf("recoverable drain update = %+v", &update)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != sessionID ||
		records[0].Generation != 17 ||
		records[0].DesiredStatus != "finished" ||
		records[0].EffectiveStatus != "recoverable" {
		t.Fatalf("terminal drain retry records = %+v", records)
	}
	generation.completePlatformCall()
}

func TestAgentTerminalStatusPersistFailureRetriesDesiredStatusBeforeCleanup(
	t *testing.T,
) {
	const sessionID = "62626262626262626262626262626262"
	stateRoot := t.TempDir()
	var (
		mu             sync.Mutex
		updateStatuses []string
		updateErrors   []string
		updateCalls    int
	)
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return anypb.New(
					&portfoliov1.SaveStrategyIndicatorsV2Response{
						DefinitionsSaved: 1,
						ChunksSaved:      1,
					},
				)
			case "portfolio.FinalizeStrategyIndicatorChunksV2":
				return anypb.New(
					&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
						ChunksFinalized: 1,
					},
				)
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId: sessionID,
						UserId:    6,
						RuntimeId: "rt-1",
						Status:    "running",
					},
				})
			case "portfolio.UpdateSession":
				var update portfoliov1.UpdateSessionRequest
				if err := request.UnmarshalTo(&update); err != nil {
					return nil, err
				}
				mu.Lock()
				updateCalls++
				call := updateCalls
				updateStatuses = append(updateStatuses, update.GetStatus())
				updateErrors = append(updateErrors, update.GetError())
				mu.Unlock()
				if call == 1 {
					return nil, errors.New("temporary status persistence failure")
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		StateRoot:                stateRoot,
		PlatformInvoker:          invoker,
		RequestTimeout:           time.Second,
		IndicatorFinalizeTimeout: time.Second,
	})
	generation := newWorkerGeneration(sessionID, 3)
	generation.durablePossible = true
	agent.generations[sessionID] = generation
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 7,
		},
		indicatorSyncFrameV2(
			sessionID,
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}

	err := agent.HandleWorkerFrame(
		context.Background(),
		sessionID,
		&rwv1.WorkerFrame{
			FrameId: "final-persist-retry",
			Payload: &rwv1.WorkerFrame_FinalStatus{
				FinalStatus: &rwv1.FinalStatus{
					SessionId:     sessionID,
					Status:        "finished",
					BarsProcessed: 1,
				},
			},
		},
		func(*rwv1.AgentFrame) error { return nil },
	)
	if err == nil ||
		!strings.Contains(err.Error(), "temporary status persistence failure") {
		t.Fatalf("final status error = %v, want persistence failure", err)
	}
	if err := agent.HandleWorkerDisconnect(
		WorkerIdentity{SessionID: sessionID, Generation: 3},
		errors.New("worker exited after failed terminal acknowledgement"),
	); err != nil {
		t.Fatalf("HandleWorkerDisconnect: %v", err)
	}

	mu.Lock()
	gotStatuses := append([]string(nil), updateStatuses...)
	gotErrors := append([]string(nil), updateErrors...)
	mu.Unlock()
	if !slices.Equal(gotStatuses, []string{"finished", "finished"}) {
		t.Fatalf(
			"terminal update statuses = %v, want desired status retried without failed downgrade",
			gotStatuses,
		)
	}
	if !slices.Equal(gotErrors, []string{"", ""}) {
		t.Fatalf(
			"terminal update errors = %v, persistence diagnostics must not become session errors",
			gotErrors,
		)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("completed terminal retry records = %+v", records)
	}
}

func TestAgentFinalStatusMarksWorkerDrainingBeforePlatformUpdate(t *testing.T) {
	probe := &finalStatusDrainProbe{}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		WorkerStopper:   probe,
		PlatformInvoker: probe,
	})
	err := agent.HandleWorkerFrame(context.Background(), "sess-draining", &rwv1.WorkerFrame{
		FrameId: "final-draining",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-draining",
			Status:    "finished",
		}},
	}, func(*rwv1.AgentFrame) error { return nil })
	if err != nil {
		t.Fatalf("HandleWorkerFrame final: %v", err)
	}
	markedSession, updateBeforeDrain := probe.snapshot()
	if markedSession != "sess-draining" {
		t.Fatalf("marked draining session = %q, want sess-draining", markedSession)
	}
	if updateBeforeDrain {
		t.Fatal("terminal platform update occurred before worker was marked draining")
	}
}

func TestAgentFinalStatusDrainingLetsConcurrentStopAllWaitForAckExit(t *testing.T) {
	requirePOSIXSignals(t)
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	ack := filepath.Join(dir, "ack")
	signals := filepath.Join(dir, "signals")
	writePythonWorkerModule(t, dir, "worker_final_status_ack", fmt.Sprintf(`
import signal
import time
from pathlib import Path

ready = Path(%q)
ack = Path(%q)
signals = Path(%q)

def stop(_signum, _frame):
    with signals.open("a", encoding="utf-8") as output:
        output.write("SIGTERM\n")

signal.signal(signal.SIGTERM, stop)
ready.write_text("ready", encoding="utf-8")
while not ack.exists():
    time.sleep(0.01)
`, ready, ack, signals))
	manager, worker := startWorkerModule(t, dir, "worker_final_status_ack", "sess-final-ack")
	waitForWorkerFile(t, ready)
	platform := &blockingFinalUpdatePlatform{
		updateStarted: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseUpdate := func() {
		releaseOnce.Do(func() { close(platform.releaseUpdate) })
	}
	defer releaseUpdate()
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		WorkerStopper:   manager,
		PlatformInvoker: platform,
	})
	finalDone := make(chan error, 1)
	go func() {
		finalDone <- agent.HandleWorkerFrame(context.Background(), worker.SessionID, &rwv1.WorkerFrame{
			FrameId: "final-ack",
			Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
				SessionId: worker.SessionID,
				Status:    "finished",
			}},
		}, func(*rwv1.AgentFrame) error {
			return os.WriteFile(ack, []byte("ack"), 0o600)
		})
	}()
	select {
	case <-platform.updateStarted:
	case <-time.After(time.Second):
		t.Fatal("terminal platform update did not start")
	}
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- manager.StopAll(context.Background(), 400*time.Millisecond)
	}()
	observeUntil := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(observeUntil) {
		if _, err := os.Stat(signals); err == nil {
			t.Fatal("StopAll signaled draining worker before FinalStatus acknowledgement")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat worker signals: %v", err)
		}
		select {
		case err := <-stopDone:
			t.Fatalf("StopAll returned before FinalStatus acknowledgement: %v", err)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	releaseUpdate()
	select {
	case err := <-finalDone:
		if err != nil {
			t.Fatalf("HandleWorkerFrame final: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FinalStatus acknowledgement did not complete")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopAll: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopAll did not observe ACK-driven natural worker exit")
	}
	if _, err := os.Stat(signals); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("draining worker received a stop signal, marker error = %v", err)
	}
	if got := manager.ShutdownSummary().ForcedStops; got != 0 {
		t.Fatalf("forced stops = %d, want ACK-driven natural exit", got)
	}
}

func TestAgentFinalStatusFlushFailurePersistsRecoverableAndReturnsErrorAck(t *testing.T) {
	invoker := &agentFinalPlatform{saveErr: errors.New("database unavailable")}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", PlatformInvoker: invoker,
		IndicatorFinalizeTimeout: 10 * time.Millisecond,
		IndicatorRetryInitial:    time.Millisecond, IndicatorRetryMax: 2 * time.Millisecond,
	})
	bufferAgentIndicator(t, agent, "sess-1")
	var ack *rwv1.AgentFrame
	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		FrameId: "final-1",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-1", Status: "finished", BarsProcessed: 1440,
		}},
	}, func(frame *rwv1.AgentFrame) error { ack = frame; return nil })
	var finalizationErr *IndicatorFinalizationError
	if !errors.As(err, &finalizationErr) {
		t.Fatalf("HandleWorkerFrame final error = %v, want IndicatorFinalizationError", err)
	}
	_, updates := invoker.snapshot()
	if len(updates) != 1 || updates[0].GetStatus() != "recoverable" ||
		!strings.HasPrefix(updates[0].GetError(), "indicator finalization failed:") {
		t.Fatalf("updates = %+v", updates)
	}
	if ack == nil || ack.GetReplyTo() != "final-1" || ack.GetError().GetCode() != "INDICATOR_FINALIZATION_FAILED" {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestAgentSpotStoppedFlushFailurePersistsRecoverableAndReturnsErrorAck(t *testing.T) {
	invoker := &agentFinalPlatform{saveErr: errors.New("database unavailable")}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", PlatformInvoker: invoker,
		IndicatorFinalizeTimeout: 10 * time.Millisecond,
		IndicatorRetryInitial:    time.Millisecond, IndicatorRetryMax: 2 * time.Millisecond,
	})
	bufferAgentIndicator(t, agent, "sess-spot-stop")
	var ack *rwv1.AgentFrame
	err := agent.HandleWorkerFrame(context.Background(), "sess-spot-stop", &rwv1.WorkerFrame{
		FrameId: "final-spot-stop",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-spot-stop", Status: "stopped", BarsProcessed: 17,
			ReconciliationRunId: "recon-123",
		}},
	}, func(frame *rwv1.AgentFrame) error { ack = frame; return nil })
	var finalizationErr *IndicatorFinalizationError
	if !errors.As(err, &finalizationErr) {
		t.Fatalf("HandleWorkerFrame final error = %v, want IndicatorFinalizationError", err)
	}
	_, updates := invoker.snapshot()
	if len(updates) != 1 || updates[0].GetStatus() != "recoverable" {
		t.Fatalf("updates = %+v", updates)
	}
	if ack == nil || ack.GetReplyTo() != "final-spot-stop" || ack.GetError().GetCode() != "INDICATOR_FINALIZATION_FAILED" {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestAgentReloadsDurableIndicatorTailAndClearsPendingAfterRetry(t *testing.T) {
	const sessionID = "51515151515151515151515151515151"
	stateRoot := t.TempDir()
	var firstUpdate portfoliov1.UpdateSessionRequest
	failingPlatform := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return nil, errors.New("database unavailable")
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&firstUpdate); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf(
					"unexpected first platform method: %s",
					method,
				)
			}
		},
	}
	first := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		StateRoot:                stateRoot,
		PlatformInvoker:          failingPlatform,
		RequestTimeout:           50 * time.Millisecond,
		IndicatorFinalizeTimeout: 15 * time.Millisecond,
		IndicatorRetryInitial:    time.Millisecond,
		IndicatorRetryMax:        2 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 1)
	generation.durablePossible = true
	first.generations[sessionID] = generation
	frame := indicatorSyncFrameV2(
		sessionID,
		"binance:spot:BTCUSDT:1m",
		0,
		60_000,
	)
	if err := first.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 7,
			token:      "worker-token",
		},
		frame,
	); err != nil {
		t.Fatalf("ReceiveFrameV2: %v", err)
	}
	var ack *rwv1.AgentFrame
	err := first.HandleWorkerFrame(
		context.Background(),
		sessionID,
		&rwv1.WorkerFrame{
			FrameId: "final-durable-retry",
			Payload: &rwv1.WorkerFrame_FinalStatus{
				FinalStatus: &rwv1.FinalStatus{
					SessionId:     sessionID,
					Status:        "finished",
					BarsProcessed: 1,
				},
			},
		},
		func(frame *rwv1.AgentFrame) error {
			ack = frame
			return nil
		},
	)
	var finalizationErr *IndicatorFinalizationError
	if !errors.As(err, &finalizationErr) {
		t.Fatalf(
			"final status error = %v, want IndicatorFinalizationError",
			err,
		)
	}
	if firstUpdate.GetStatus() != "recoverable" ||
		firstUpdate.IndicatorFinalizationPending == nil ||
		!firstUpdate.GetIndicatorFinalizationPending() {
		t.Fatalf("first recoverable update = %+v", &firstUpdate)
	}
	if ack == nil ||
		ack.GetError().GetCode() != "INDICATOR_FINALIZATION_FAILED" {
		t.Fatalf("failure acknowledgement = %+v", ack)
	}

	var retryMethods []string
	var retryUpdate portfoliov1.UpdateSessionRequest
	retryPlatform := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			retryMethods = append(retryMethods, method)
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				return anypb.New(
					&portfoliov1.SaveStrategyIndicatorsV2Response{
						DefinitionsSaved: 1,
						ChunksSaved:      1,
					},
				)
			case "portfolio.FinalizeStrategyIndicatorChunksV2":
				return anypb.New(
					&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
						ChunksFinalized: 1,
					},
				)
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId:                    sessionID,
						UserId:                       6,
						RuntimeId:                    "rt-1",
						Status:                       "recoverable",
						BarsProcessed:                1,
						IndicatorFinalizationPending: true,
					},
				})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&retryUpdate); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf(
					"unexpected retry platform method: %s",
					method,
				)
			}
		},
	}
	restarted := NewAgent(AgentConfig{
		RuntimeID:                "rt-1",
		UserID:                   6,
		StateRoot:                stateRoot,
		PlatformInvoker:          retryPlatform,
		RequestTimeout:           time.Second,
		IndicatorFinalizeTimeout: time.Second,
	})
	if err := restarted.RetryInitializationError(); err != nil {
		t.Fatalf("retry initialization: %v", err)
	}
	if err := restarted.RetryTerminalSessions(
		context.Background(),
	); err != nil {
		t.Fatalf("RetryTerminalSessions: %v", err)
	}
	wantMethods := []string{
		"portfolio.SaveStrategyIndicatorsV2",
		"portfolio.FinalizeStrategyIndicatorChunksV2",
		"portfolio.GetSession",
		"portfolio.UpdateSession",
	}
	if !slices.Equal(retryMethods, wantMethods) {
		t.Fatalf("retry methods = %v, want %v", retryMethods, wantMethods)
	}
	if retryUpdate.GetStatus() != "recoverable" ||
		retryUpdate.IndicatorFinalizationPending == nil ||
		retryUpdate.GetIndicatorFinalizationPending() {
		t.Fatalf("retry pending-clear update = %+v", &retryUpdate)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("completed retry records = %+v", records)
	}
	if restarted.indicatorSync.lookupSession(sessionID) != nil {
		t.Fatal("completed retry retained indicator state")
	}
}

func TestRetryTerminalSessionsClaimsRecordBeforePlatformReplay(t *testing.T) {
	const sessionID = "52525252525252525252525252525252"
	platform := &concurrentTerminalRetryPlatform{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		UserID:          6,
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
	})
	record := TerminalRetryRecord{
		SchemaVersion:   indicatorTerminalRetrySchemaVersion,
		SessionID:       sessionID,
		Generation:      1,
		DesiredStatus:   "finished",
		EffectiveStatus: "recoverable",
		BarsProcessed:   10,
		Reason:          "retry",
	}
	agent.terminalRetries[terminalRetryKey(sessionID, 1)] = record

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- agent.RetryTerminalSessions(context.Background())
	}()
	select {
	case <-platform.started:
	case <-time.After(time.Second):
		t.Fatal("first terminal retry did not reach the platform")
	}
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- agent.RetryTerminalSessions(context.Background())
	}()
	replayedConcurrently := false
	select {
	case <-platform.started:
		replayedConcurrently = true
	case <-time.After(100 * time.Millisecond):
	}
	close(platform.release)
	_ = <-firstDone
	_ = <-secondDone
	if replayedConcurrently || platform.maximumConcurrent() != 1 {
		t.Fatalf(
			"terminal retry max concurrency = %d, want one claimed replay",
			platform.maximumConcurrent(),
		)
	}
}

func TestStartupCleanupRetryDoesNotFinalizeAcceptedRunningSession(t *testing.T) {
	const sessionID = "53535353535353535353535353535353"
	var methods []string
	platform := &fakePlatformInvoker{
		onInvoke: func(method string, _ *anypb.Any) (*anypb.Any, error) {
			methods = append(methods, method)
			if method != "portfolio.GetSession" {
				return nil, fmt.Errorf("unexpected startup cleanup method: %s", method)
			}
			return anypb.New(&portfoliov1.GetSessionResponse{
				Session: &portfoliov1.StrategySessionEntry{
					SessionId: sessionID,
					UserId:    6,
					RuntimeId: "rt-1",
					Status:    "running",
				},
			})
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: platform,
		IndicatorRetryInitial: time.Millisecond,
		IndicatorRetryMax:     2 * time.Millisecond,
	})
	bufferAgentIndicator(t, agent, sessionID)
	record := TerminalRetryRecord{
		SchemaVersion: indicatorTerminalRetrySchemaVersion,
		SessionID:     sessionID, Generation: 1, DesiredStatus: "failed",
		EffectiveStatus: "failed", Reason: "startup timed out",
		ExpectedStatus: "pending",
	}
	agent.terminalRetries[terminalRetryKey(sessionID, 1)] = record

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := agent.RetryTerminalSessions(ctx); err != nil {
		t.Fatalf("RetryTerminalSessions: %v", err)
	}
	if !slices.Equal(methods, []string{"portfolio.GetSession"}) {
		t.Fatalf("startup cleanup methods = %v, want readback only", methods)
	}
	if agent.indicatorSync.lookupSession(sessionID) == nil {
		t.Fatal("accepted running Session indicator state was finalized")
	}
}

func TestTerminalRequestFromFinalStatusPreservesReconciliationRunID(t *testing.T) {
	request, err := terminalRequestFromFinalStatus(&rwv1.FinalStatus{
		SessionId: "sess-1", Status: "stop_failed", BarsProcessed: 17,
		Error: "Spot close requires reconciliation", ReconciliationRunId: "recon-123",
	})
	if err != nil {
		t.Fatalf("terminalRequestFromFinalStatus: %v", err)
	}
	if request.ReconciliationRunID != "recon-123" || request.Status != "stop_failed" {
		t.Fatalf("request = %+v", request)
	}
}

func TestAgentFinalStatusPreservesFailedStatus(t *testing.T) {
	invoker := &agentFinalPlatform{}
	agent := NewAgent(AgentConfig{RuntimeID: "rt-1", PlatformInvoker: invoker})
	bufferAgentIndicator(t, agent, "sess-1")
	var ack *rwv1.AgentFrame
	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		FrameId: "final-1",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-1", Status: "failed", BarsProcessed: 17, Error: "strategy error",
		}},
	}, func(frame *rwv1.AgentFrame) error { ack = frame; return nil })
	if err != nil {
		t.Fatalf("HandleWorkerFrame final: %v", err)
	}
	_, updates := invoker.snapshot()
	if len(updates) != 1 || updates[0].GetStatus() != "failed" || updates[0].GetError() != "strategy error" {
		t.Fatalf("updates = %+v", updates)
	}
	if ack == nil || ack.GetReplyTo() != "final-1" || ack.GetPayload() != nil {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestAgentFinalStatusRejectsNonTerminalStatus(t *testing.T) {
	invoker := &agentFinalPlatform{}
	agent := NewAgent(AgentConfig{RuntimeID: "rt-1", PlatformInvoker: invoker})
	var ack *rwv1.AgentFrame
	err := agent.HandleWorkerFrame(context.Background(), "sess-1", &rwv1.WorkerFrame{
		FrameId: "final-1",
		Payload: &rwv1.WorkerFrame_FinalStatus{FinalStatus: &rwv1.FinalStatus{
			SessionId: "sess-1", Status: "running", BarsProcessed: 17,
		}},
	}, func(frame *rwv1.AgentFrame) error { ack = frame; return nil })
	if err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("HandleWorkerFrame error = %v, want terminal-status rejection", err)
	}
	methods, _ := invoker.snapshot()
	if len(methods) != 0 || ack != nil {
		t.Fatalf("methods/ack = %v/%+v, want no side effects", methods, ack)
	}
}

func TestAgentCleanupForgetsOnlyRequestedIndicatorSession(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-old")); err != nil {
		t.Fatalf("ReceiveFrame old: %v", err)
	}
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-new")); err != nil {
		t.Fatalf("ReceiveFrame new: %v", err)
	}
	agent.cleanupSessionState("sess-old", "test cleanup")
	if agent.indicatorSync.lookupSession("sess-old") != nil {
		t.Fatal("old session indicator state was not cleared")
	}
	if agent.indicatorSync.lookupSession("sess-new") == nil {
		t.Fatal("new session indicator state was cleared")
	}
}

func TestAgentRestartWaitsForFlushThenForgetsOldSession(t *testing.T) {
	invoker := &blockingRestartPlatform{
		saveStarted: make(chan struct{}, 1),
		releaseSave: make(chan struct{}),
	}
	starter := &fakeWorkerStarter{}
	stopper := &signalingWorkerStopper{stopped: make(chan struct{}, 1)}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", UserID: 6, PlatformInvoker: invoker,
		WorkerStarter: starter, WorkerStopper: stopper, StartTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	for _, sessionID := range []string{"sess-old", "sess-new"} {
		if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame(sessionID)); err != nil {
			t.Fatalf("ReceiveFrame %s: %v", sessionID, err)
		}
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- agent.indicatorSync.FlushSession(context.Background(), "sess-old", false)
	}()
	select {
	case <-invoker.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for old-session flush")
	}
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID, Status: "running",
				}},
			}, nil)
		}()
	}
	restartDone := make(chan error, 1)
	go func() {
		_, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
		restartDone <- err
	}()
	select {
	case <-stopper.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker stop")
	}
	select {
	case err := <-restartDone:
		t.Fatalf("restart finished before flush release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if agent.indicatorSync.lookupSession("sess-old") == nil {
		t.Fatal("old session was forgotten while its flush was active")
	}
	close(invoker.releaseSave)
	if err := <-flushDone; err != nil {
		t.Fatalf("FlushSession: %v", err)
	}
	if err := <-restartDone; err != nil {
		t.Fatalf("RestartSession: %v", err)
	}
	if agent.indicatorSync.lookupSession("sess-old") != nil {
		t.Fatal("old session indicator state was not forgotten")
	}
	if agent.indicatorSync.lookupSession("sess-new") == nil {
		t.Fatal("unrelated new session indicator state was forgotten")
	}
}

func TestAgentRestartSessionStopsOldWorkerMarksRecoverableClearsBuffersAndStartsNewWorker(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	var updateReq portfoliov1.UpdateSessionRequest
	var indicatorReq portfoliov1.SaveStrategyIndicatorsRequest
	var platformMethods []string
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			platformMethods = append(platformMethods, method)
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId:     "sess-old",
					PortfolioId:   7,
					StrategyId:    12,
					UserId:        6,
					RuntimeId:     "rt-1",
					RuntimeSource: "bare",
					RuntimeName:   "bare-debug",
					Status:        "running",
					Interval:      "1m",
					StartTimeMs:   1000,
					EndTimeMs:     2000,
					BarsProcessed: 19,
					Leverage:      3,
				}})
			case "portfolio.SaveStrategyIndicators":
				if err := request.UnmarshalTo(&indicatorReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&updateReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		RuntimeSource:   "bare",
		RuntimeName:     "bare-debug",
		UserID:          6,
		WorkerStarter:   starter,
		WorkerStopper:   stopper,
		PlatformInvoker: invoker,
		StartTimeout:    time.Second,
		RequestTimeout:  time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-old")); err != nil {
		t.Fatalf("ReceiveFrame old indicators: %v", err)
	}
	agent.ready["sess-old"] = make(chan struct{}, 1)
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID,
					Status:    "running",
				}},
			}, nil)
		}()
	}

	result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}

	if result.OldSessionID != "sess-old" || result.NewSessionID != starter.startedSessionID || result.RuntimeID != "rt-1" {
		t.Fatalf("restart result = %+v", result)
	}
	if stopper.sessionID != "sess-old" {
		t.Fatalf("stopped session = %q", stopper.sessionID)
	}
	if updateReq.GetSessionId() != "sess-old" || updateReq.GetStatus() != "recoverable" || updateReq.GetBarsProcessed() != 19 || updateReq.GetRuntimeId() != "rt-1" {
		t.Fatalf("update session request = %+v", &updateReq)
	}
	if updateReq.GetError() == "" {
		t.Fatalf("update session error should explain local bare restart")
	}
	if len(indicatorReq.GetChunks()) != 1 || indicatorReq.GetChunks()[0].GetCount() != 1 || !indicatorReq.GetChunks()[0].GetFinalized() {
		t.Fatalf("restart indicator finalization = %+v, want one finalized tail", &indicatorReq)
	}
	saveIndex, updateIndex := -1, -1
	for index, method := range platformMethods {
		switch method {
		case "portfolio.SaveStrategyIndicators":
			saveIndex = index
		case "portfolio.UpdateSession":
			updateIndex = index
		}
	}
	if saveIndex < 0 || updateIndex < 0 || saveIndex > updateIndex {
		t.Fatalf("restart platform method order = %v, want indicator save before recoverable update", platformMethods)
	}
	if agent.indicatorSync.lookupSession("sess-old") != nil {
		t.Fatalf("old session indicator buffer was not cleared")
	}
	if _, ok := agent.ready["sess-old"]; ok {
		t.Fatalf("old session ready state was not cleared")
	}
	if starter.startedSessionID == "" {
		t.Fatalf("new worker was not started")
	}
	if starter.extraEnv["HUSHINE_RUNTIME_ID"] != "rt-1" || starter.extraEnv["HUSHINE_RUNTIME_SOURCE"] != "bare" {
		t.Fatalf("worker env = %+v", starter.extraEnv)
	}
}

func TestAgentConcurrentRestartSessionReusesOneReplacementWorker(t *testing.T) {
	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	var updateOnce sync.Once
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, _ *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-old", PortfolioId: 7, StrategyId: 12, UserId: 6,
					RuntimeId: "rt-1", RuntimeSource: "bare", Status: "running",
					Interval: "1m", StartTimeMs: 1000, EndTimeMs: 2000,
				}})
			case "portfolio.UpdateSession":
				updateOnce.Do(func() { close(updateStarted) })
				<-releaseUpdate
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	starter := &concurrentWorkerStarter{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", UserID: 6,
		WorkerStarter: starter, WorkerStopper: &fakeWorkerStopper{}, PlatformInvoker: invoker,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID, Status: "running",
				}},
			}, nil)
		}()
	}

	results := make(chan RestartSessionResult, 2)
	errs := make(chan error, 2)
	restart := func() {
		result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
		results <- result
		errs <- err
	}
	go restart()
	select {
	case <-updateStarted:
	case <-time.After(time.Second):
		t.Fatal("first restart did not reach recoverable update")
	}
	go restart()
	time.Sleep(20 * time.Millisecond)
	close(releaseUpdate)

	firstResult, secondResult := <-results, <-results
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("RestartSession call %d: %v", i+1, err)
		}
	}
	if firstResult.NewSessionID == "" || secondResult.NewSessionID != firstResult.NewSessionID {
		t.Fatalf("concurrent restart results = %+v and %+v", firstResult, secondResult)
	}
	if starts := starter.snapshotStarts(); len(starts) != 1 {
		t.Fatalf("replacement workers started = %v, want exactly one", starts)
	}
}

func TestAgentRestartSessionDoesNotMutateOldSessionAfterShutdownBegins(t *testing.T) {
	getSessionStarted := make(chan struct{})
	releaseGetSession := make(chan struct{})
	var updateCalls int
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, _ *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				close(getSessionStarted)
				<-releaseGetSession
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-old", PortfolioId: 7, StrategyId: 12, UserId: 6,
					RuntimeId: "rt-1", RuntimeSource: "bare", Status: "running",
					Interval: "1m", StartTimeMs: 1000, EndTimeMs: 2000,
				}})
			case "portfolio.UpdateSession":
				updateCalls++
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", UserID: 6,
		WorkerStarter: starter, WorkerStopper: stopper, PlatformInvoker: invoker,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})

	restartDone := make(chan error, 1)
	go func() {
		_, err := agent.RestartSession(
			context.Background(),
			RestartSessionOptions{SessionID: "sess-old"},
		)
		restartDone <- err
	}()
	select {
	case <-getSessionStarted:
	case <-time.After(time.Second):
		t.Fatal("restart did not begin resolving the old session")
	}
	if err := agent.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	close(releaseGetSession)

	err := <-restartDone
	if err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("RestartSession error = %v, want Agent shutdown rejection", err)
	}
	if stopper.sessionID != "" {
		t.Fatalf("restart stopped old worker %q after shutdown began", stopper.sessionID)
	}
	if updateCalls != 0 {
		t.Fatalf("restart updated old session %d times after shutdown began", updateCalls)
	}
	if starter.startedSessionID != "" {
		t.Fatalf("restart started replacement worker %q after shutdown began", starter.startedSessionID)
	}
}

func TestAgentRestartSessionRejectsIndicatorAfterFinalizationAdmissionCloses(t *testing.T) {
	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	var indicatorReq portfoliov1.SaveStrategyIndicatorsRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-old", PortfolioId: 7, StrategyId: 12, UserId: 6,
					RuntimeId: "rt-1", RuntimeSource: "bare", Status: "running",
					Interval: "1m", StartTimeMs: 1000, EndTimeMs: 2000,
				}})
			case "portfolio.SaveStrategyIndicators":
				if err := request.UnmarshalTo(&indicatorReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{})
			case "portfolio.UpdateSession":
				close(updateStarted)
				<-releaseUpdate
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	starter := &fakeWorkerStarter{}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", UserID: 6,
		WorkerStarter: starter, WorkerStopper: &fakeWorkerStopper{}, PlatformInvoker: invoker,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	generation := newWorkerGeneration("sess-old", 1)
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("failed to bind authenticated generation")
	}
	generation.connected = true
	agent.generations["sess-old"] = generation
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-old")); err != nil {
		t.Fatalf("ReceiveFrame initial indicator: %v", err)
	}
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID, Status: "running",
				}},
			}, nil)
		}()
	}

	restartDone := make(chan error, 1)
	go func() {
		_, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
		restartDone <- err
	}()
	select {
	case <-updateStarted:
	case <-time.After(time.Second):
		t.Fatal("restart did not reach recoverable update after indicator finalization")
	}
	late := agentIndicatorFrame("sess-old")
	late.MarketTimeMs += late.IntervalMs
	if err := agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		WorkerIdentity{SessionID: "sess-old", Generation: 7},
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrame{IndicatorFrame: late},
		},
		nil,
	); err == nil {
		t.Fatal("closing worker generation accepted an indicator after finalization")
	}
	close(releaseUpdate)
	if err := <-restartDone; err != nil {
		t.Fatalf("RestartSession: %v", err)
	}
	if len(indicatorReq.GetChunks()) != 1 || indicatorReq.GetChunks()[0].GetCount() != 1 {
		t.Fatalf("finalized request absorbed a late indicator: %+v", &indicatorReq)
	}
}

func TestAgentRestartSessionDrainTimeoutPersistsRecoverableRetry(t *testing.T) {
	const sessionID = "sess-old"
	stateRoot := t.TempDir()
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	var update portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId:     sessionID,
						PortfolioId:   7,
						StrategyId:    12,
						UserId:        6,
						RuntimeId:     "rt-1",
						RuntimeSource: "bare",
						Status:        "running",
						Interval:      "1m",
						StartTimeMs:   1000,
						EndTimeMs:     2000,
						BarsProcessed: 19,
					},
				})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&update); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		RuntimeSource:   "bare",
		UserID:          6,
		StateRoot:       stateRoot,
		WorkerStarter:   starter,
		WorkerStopper:   stopper,
		PlatformInvoker: invoker,
		StartTimeout:    time.Second,
		RequestTimeout:  20 * time.Millisecond,
	})
	generation := newWorkerGeneration(sessionID, 27)
	if !generation.admit("portfolio.SaveSession") {
		t.Fatal("admit durable in-flight call")
	}
	agent.generations[sessionID] = generation

	_, err := agent.RestartSession(
		context.Background(),
		RestartSessionOptions{SessionID: sessionID},
	)
	if err == nil || !strings.Contains(err.Error(), "worker restart drain failed") {
		t.Fatalf("restart drain error = %v", err)
	}
	if update.GetStatus() != "recoverable" ||
		update.IndicatorFinalizationPending == nil ||
		!update.GetIndicatorFinalizationPending() {
		t.Fatalf("restart drain update = %+v", &update)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != sessionID ||
		records[0].Generation != 27 ||
		records[0].DesiredStatus != "recoverable" {
		t.Fatalf("restart drain retry records = %+v", records)
	}
	if starter.startedSessionID != "" {
		t.Fatalf("replacement worker started after drain timeout: %s", starter.startedSessionID)
	}
	generation.completePlatformCall()
}

func TestAgentRestartSessionFinalizationFailureKeepsTailAndDoesNotStartNewWorker(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	stateRoot := t.TempDir()
	var updateReq portfoliov1.UpdateSessionRequest
	saveCalls := 0
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-old", PortfolioId: 7, StrategyId: 12, UserId: 6,
					RuntimeId: "rt-1", RuntimeSource: "bare", Status: "running",
					Interval: "1m", StartTimeMs: 1000, EndTimeMs: 2000, BarsProcessed: 19,
				}})
			case "portfolio.SaveStrategyIndicatorsV2":
				saveCalls++
				return nil, errors.New("indicator store unavailable")
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&updateReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", UserID: 6,
		StateRoot:     stateRoot,
		WorkerStarter: starter, WorkerStopper: stopper, PlatformInvoker: invoker,
		StartTimeout: time.Second, RequestTimeout: time.Second,
		IndicatorFinalizeTimeout: 20 * time.Millisecond,
		IndicatorRetryInitial:    time.Millisecond,
		IndicatorRetryMax:        2 * time.Millisecond,
	})
	generation := newWorkerGeneration("sess-old", 7)
	generation.durablePossible = true
	agent.generations["sess-old"] = generation
	if err := agent.indicatorSync.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  "sess-old",
			PID:        123,
			Generation: 11,
			token:      "worker-token",
		},
		indicatorSyncFrameV2(
			"sess-old",
			"binance:spot:BTCUSDT:1m",
			0,
			60_000,
		),
	); err != nil {
		t.Fatalf("ReceiveFrame old indicators: %v", err)
	}

	_, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
	if err == nil || !strings.Contains(err.Error(), "indicator finalization failed") {
		t.Fatalf("RestartSession error = %v, want indicator finalization failure", err)
	}
	if saveCalls < 1 {
		t.Fatal("restart did not attempt indicator finalization")
	}
	if updateReq.GetStatus() != "recoverable" || !strings.Contains(updateReq.GetError(), "indicator finalization failed") {
		t.Fatalf("recoverable update = %+v", &updateReq)
	}
	if updateReq.IndicatorFinalizationPending == nil ||
		!updateReq.GetIndicatorFinalizationPending() {
		t.Fatalf("recoverable update must retain indicator finalization: %+v", &updateReq)
	}
	if agent.indicatorSync.lookupSession("sess-old") == nil {
		t.Fatal("failed finalization discarded the retryable indicator tail")
	}
	if starter.startedSessionID != "" {
		t.Fatalf("new worker started despite finalization failure: %s", starter.startedSessionID)
	}
	store, err := NewTerminalRetryStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		records[0].SessionID != "sess-old" ||
		records[0].Generation != 7 ||
		records[0].Indicators == nil {
		t.Fatalf("restart retry records = %+v", records)
	}
}

func TestAgentRestartSessionOwnsDisconnectCleanupUntilIndicatorTailIsFinalized(t *testing.T) {
	starter := &fakeWorkerStarter{}
	var indicatorReq portfoliov1.SaveStrategyIndicatorsRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-old", PortfolioId: 7, StrategyId: 12, UserId: 6,
					RuntimeId: "rt-1", RuntimeSource: "bare", Status: "running",
					Interval: "1m", StartTimeMs: 1000, EndTimeMs: 2000,
				}})
			case "portfolio.SaveStrategyIndicators":
				if err := request.UnmarshalTo(&indicatorReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{})
			case "portfolio.UpdateSession":
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	var agent *Agent
	stopper := &callbackWorkerStopper{}
	agent = NewAgent(AgentConfig{
		RuntimeID: "rt-1", RuntimeSource: "bare", UserID: 6,
		WorkerStarter: starter, WorkerStopper: stopper, PlatformInvoker: invoker,
		StartTimeout: time.Second, RequestTimeout: time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	generation := newWorkerGeneration("sess-old", 1)
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("failed to bind worker generation")
	}
	generation.connected = true
	agent.generations["sess-old"] = generation
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-old")); err != nil {
		t.Fatalf("ReceiveFrame old indicators: %v", err)
	}
	stopper.onFirstStop = func() {
		started := make(chan struct{})
		go func() {
			close(started)
			_ = agent.HandleWorkerDisconnect(WorkerIdentity{SessionID: "sess-old", Generation: 7}, errors.New("worker stopped"))
		}()
		<-started
		time.Sleep(20 * time.Millisecond)
	}
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID, Status: "running",
				}},
			}, nil)
		}()
	}

	if _, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"}); err != nil {
		t.Fatalf("RestartSession: %v", err)
	}
	if len(indicatorReq.GetChunks()) != 1 || !indicatorReq.GetChunks()[0].GetFinalized() {
		t.Fatalf("disconnect raced away the restart indicator tail: %+v", &indicatorReq)
	}
}

func TestAgentRestartSessionUsesCachedRunRequestWhenGetSessionIsUnsupported(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	var updateReq portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return nil, errors.New("Unimplemented: unsupported runtime platform method: portfolio.GetSession")
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&updateReq); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		RuntimeSource:   "bare",
		RuntimeName:     "bare-debug",
		UserID:          6,
		WorkerStarter:   starter,
		WorkerStopper:   stopper,
		PlatformInvoker: invoker,
		StartTimeout:    time.Second,
		RequestTimeout:  time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	cachedRun, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 7,
		Interval:    "1m",
		StartTimeMs: 1000,
		EndTimeMs:   2000,
		UserId:      6,
		RuntimeId:   "rt-1",
		Leverage:    2,
	})
	if err != nil {
		t.Fatalf("pack cached run: %v", err)
	}
	agent.runRequests["sess-old"] = cachedRun
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID,
					Status:    "running",
				}},
			}, nil)
		}()
	}

	result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}

	if result.NewSessionID != starter.startedSessionID {
		t.Fatalf("restart result = %+v", result)
	}
	if updateReq.GetSessionId() != "sess-old" || updateReq.GetStatus() != "recoverable" || updateReq.GetRuntimeId() != "rt-1" {
		t.Fatalf("update request = %+v", &updateReq)
	}
}

func TestAgentRestartSessionPreservesCachedRunRequestOptions(t *testing.T) {
	starter := &fakeWorkerStarter{}
	stopper := &fakeWorkerStopper{}
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			switch method {
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
					SessionId:   "sess-old",
					PortfolioId: 7,
					UserId:      6,
					RuntimeId:   "rt-1",
					Status:      "running",
					Interval:    "1m",
					StartTimeMs: 1000,
					EndTimeMs:   2000,
					Leverage:    3,
				}})
			case "portfolio.UpdateSession":
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, errors.New("unexpected method: " + method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "rt-1",
		RuntimeSource:   "bare",
		RuntimeName:     "bare-debug",
		UserID:          6,
		WorkerStarter:   starter,
		WorkerStopper:   stopper,
		PlatformInvoker: invoker,
		StartTimeout:    time.Second,
		RequestTimeout:  time.Second,
	})
	enableStrategyStartProtocol(agent, starter)
	cachedRun, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId:     99,
		StrategyPath:    "custom.strategy",
		Interval:        "5m",
		StartTimeMs:     1111,
		EndTimeMs:       2222,
		UserId:          6,
		RuntimeId:       "rt-1",
		MaxLossClosePct: 0.17,
		Leverage:        2,
	})
	if err != nil {
		t.Fatalf("pack cached run: %v", err)
	}
	agent.runRequests["sess-old"] = cachedRun
	var restartedReq strategyv1.RunStrategyRequest
	starter.onStart = func(pendingSessionID string) {
		agent.mu.Lock()
		pending := agent.pending[pendingSessionID]
		agent.mu.Unlock()
		if pending == nil || pending.start == nil || pending.start.GetRunStrategyRequest() == nil {
			t.Errorf("missing pending restart request")
			return
		}
		if err := pending.start.GetRunStrategyRequest().UnmarshalTo(&restartedReq); err != nil {
			t.Errorf("unpack restart request: %v", err)
			return
		}
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: pendingSessionID,
					Status:    "running",
				}},
			}, nil)
		}()
	}

	if _, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"}); err != nil {
		t.Fatalf("RestartSession: %v", err)
	}

	if restartedReq.GetMaxLossClosePct() != 0.17 {
		t.Fatalf("max_loss_close_pct = %v", restartedReq.GetMaxLossClosePct())
	}
	if restartedReq.GetStrategyPath() != "custom.strategy" {
		t.Fatalf("strategy_path = %q", restartedReq.GetStrategyPath())
	}
	if restartedReq.GetPortfolioId() != 7 || restartedReq.GetInterval() != "1m" || restartedReq.GetLeverage() != 3 {
		t.Fatalf("restart request did not apply session fields: %+v", &restartedReq)
	}
}

type concurrentWorkerStarter struct {
	mu      sync.Mutex
	starts  []string
	onStart func(string)
}

func (s *concurrentWorkerStarter) StartSessionWorker(
	_ context.Context,
	sessionID string,
	_ []string,
) (*ManagedWorker, error) {
	s.mu.Lock()
	s.starts = append(s.starts, sessionID)
	callback := s.onStart
	s.mu.Unlock()
	if callback != nil {
		callback(sessionID)
	}
	return &ManagedWorker{SessionID: sessionID}, nil
}

func (s *concurrentWorkerStarter) snapshotStarts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.starts...)
}

type fakeWorkerStarter struct {
	startedSessionID string
	onStart          func(string)
	extraEnv         map[string]string
	worker           *ManagedWorker
}

func (s *fakeWorkerStarter) StartSessionWorker(ctx context.Context, sessionID string, extraEnv []string) (*ManagedWorker, error) {
	s.startedSessionID = sessionID
	s.extraEnv = map[string]string{}
	for _, item := range extraEnv {
		for i := 0; i < len(item); i++ {
			if item[i] == '=' {
				s.extraEnv[item[:i]] = item[i+1:]
				break
			}
		}
	}
	if s.onStart != nil {
		s.onStart(sessionID)
	}
	if s.worker != nil {
		s.worker.SessionID = sessionID
		return s.worker, nil
	}
	return &ManagedWorker{SessionID: sessionID}, nil
}

type workerExitWait struct {
	sessionID string
	timeout   time.Duration
}

type blockingWorkerLifecycle struct {
	fakeWorkerStarter
	waitStarted chan workerExitWait
	releaseWait chan struct{}
	mu          sync.Mutex
	stopCalls   int
}

func (m *blockingWorkerLifecycle) StopSessionWorker(context.Context, string, time.Duration) error {
	m.mu.Lock()
	m.stopCalls++
	m.mu.Unlock()
	return nil
}

func (m *blockingWorkerLifecycle) WaitSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error {
	if m.waitStarted != nil {
		select {
		case m.waitStarted <- workerExitWait{sessionID: sessionID, timeout: timeout}:
		default:
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	case <-m.releaseWait:
		return nil
	}
}

func (m *blockingWorkerLifecycle) stopCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalls
}

func wireWorkerUnaryResponse(agent *Agent, sender *fakeWorkerSender, response *anypb.Any) {
	sender.onSend = func(sessionID string, frame *rwv1.AgentFrame) {
		call := frame.GetPlatformCall()
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_PlatformCallResult{PlatformCallResult: &rwv1.PlatformCallResult{
					CallId:   call.GetCallId(),
					Ok:       true,
					Response: response,
				}},
			}, nil)
		}()
	}
}

type agentFinalPlatform struct {
	mu      sync.Mutex
	methods []string
	updates []*portfoliov1.UpdateSessionRequest
	saveErr error
}

type finalStatusDrainProbe struct {
	mu                sync.Mutex
	markedSession     string
	updateBeforeDrain bool
}

func (p *finalStatusDrainProbe) StopSessionWorker(context.Context, string, time.Duration) error {
	return nil
}

func (p *finalStatusDrainProbe) MarkSessionWorkerDraining(sessionID string) {
	p.mu.Lock()
	p.markedSession = sessionID
	p.mu.Unlock()
}

func (p *finalStatusDrainProbe) InvokePlatformAny(_ context.Context, method string, _ *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	if method != "portfolio.UpdateSession" {
		return nil, errors.New("unexpected method: " + method)
	}
	p.mu.Lock()
	p.updateBeforeDrain = p.markedSession == ""
	p.mu.Unlock()
	return anypb.New(&portfoliov1.UpdateSessionResponse{})
}

func (p *finalStatusDrainProbe) snapshot() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.markedSession, p.updateBeforeDrain
}

type blockingFinalUpdatePlatform struct {
	updateOnce    sync.Once
	updateStarted chan struct{}
	releaseUpdate chan struct{}
}

func (p *blockingFinalUpdatePlatform) InvokePlatformAny(_ context.Context, method string, _ *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	if method != "portfolio.UpdateSession" {
		return nil, errors.New("unexpected method: " + method)
	}
	p.updateOnce.Do(func() { close(p.updateStarted) })
	<-p.releaseUpdate
	return anypb.New(&portfoliov1.UpdateSessionResponse{})
}

type blockingRestartPlatform struct {
	saveStarted chan struct{}
	releaseSave chan struct{}
}

func (p *blockingRestartPlatform) InvokePlatformAny(_ context.Context, method string, _ *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	switch method {
	case "portfolio.SaveStrategyIndicators":
		select {
		case p.saveStarted <- struct{}{}:
		default:
		}
		<-p.releaseSave
		return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{DefinitionsSaved: 1, ChunksSaved: 1})
	case "portfolio.GetSession":
		return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-old", UserId: 6, RuntimeId: "rt-1", Status: "running",
		}})
	case "portfolio.UpdateSession":
		return anypb.New(&portfoliov1.UpdateSessionResponse{})
	default:
		return nil, errors.New("unexpected method: " + method)
	}
}

type signalingWorkerStopper struct {
	stopped chan struct{}
}

func (s *signalingWorkerStopper) StopSessionWorker(_ context.Context, _ string, _ time.Duration) error {
	select {
	case s.stopped <- struct{}{}:
	default:
	}
	return nil
}

type callbackWorkerStopper struct {
	mu          sync.Mutex
	called      bool
	onFirstStop func()
}

func (s *callbackWorkerStopper) StopSessionWorker(context.Context, string, time.Duration) error {
	s.mu.Lock()
	if s.called {
		s.mu.Unlock()
		return nil
	}
	s.called = true
	callback := s.onFirstStop
	s.mu.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}

func (p *agentFinalPlatform) InvokePlatformAny(_ context.Context, method string, request *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	p.mu.Lock()
	p.methods = append(p.methods, method)
	p.mu.Unlock()
	switch method {
	case "portfolio.SaveStrategyIndicators":
		if p.saveErr != nil {
			return nil, p.saveErr
		}
		return anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{DefinitionsSaved: 1, ChunksSaved: 1})
	case "portfolio.UpdateSession":
		update := &portfoliov1.UpdateSessionRequest{}
		if err := request.UnmarshalTo(update); err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.updates = append(p.updates, update)
		p.mu.Unlock()
		return anypb.New(&portfoliov1.UpdateSessionResponse{})
	default:
		return nil, errors.New("unexpected method: " + method)
	}
}

func (p *agentFinalPlatform) snapshot() ([]string, []*portfoliov1.UpdateSessionRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	methods := append([]string(nil), p.methods...)
	updates := append([]*portfoliov1.UpdateSessionRequest(nil), p.updates...)
	return methods, updates
}

func bufferAgentIndicator(t *testing.T, agent *Agent, sessionID string) {
	t.Helper()
	err := agent.HandleWorkerFrame(context.Background(), sessionID, &rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_IndicatorFrame{IndicatorFrame: agentIndicatorFrame(sessionID)},
	}, nil)
	if err != nil {
		t.Fatalf("buffer indicator: %v", err)
	}
}

func TestAuthenticatedIndicatorRejectsDuringRegistryRemovalWithoutRecreatingState(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	const sessionID = "31313131313131313131313131313131"
	agent := NewAgent(AgentConfig{})
	generation := newWorkerGeneration(sessionID, 17)
	agent.mu.Lock()
	agent.generations[sessionID] = generation
	agent.mu.Unlock()

	generation.mu.Lock()
	agent.mu.Lock()
	result := make(chan error, 1)
	go func() {
		result <- agent.HandleAuthenticatedWorkerFrame(
			context.Background(),
			WorkerIdentity{SessionID: sessionID, Generation: 23},
			&rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_IndicatorFrame{
					IndicatorFrame: agentIndicatorFrame(sessionID),
				},
			},
			nil,
		)
	}()
	runtime.Gosched()
	agent.mu.Unlock()
	runtime.Gosched()

	agent.mu.Lock()
	delete(agent.generations, sessionID)
	agent.mu.Unlock()
	generation.closing = true
	generation.mu.Unlock()

	select {
	case err := <-result:
		// Registry removal races with the initial generation lookup. A request
		// that captured the generation is rejected by its closing admission;
		// one that observes removal first is rejected as stale. Neither result
		// may recreate indicator state.
		if err == nil ||
			(!strings.Contains(err.Error(), "worker generation is closing") &&
				!strings.Contains(err.Error(), "stale worker generation")) {
			t.Fatalf(
				"late authenticated indicator error = %v, want closing or stale rejection",
				err,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("authenticated indicator did not complete")
	}
	if agent.indicatorSync.lookupSession(sessionID) != nil {
		t.Fatal("late authenticated indicator recreated state after generation removal")
	}
}

func agentIndicatorFrame(sessionID string) *rwv1.IndicatorFrame {
	return &rwv1.IndicatorFrame{
		SessionId: sessionID, UserId: 6, StrategyId: 12,
		StreamKey: "futures:ZECUSDT:1m", MarketTimeMs: 123000, IntervalMs: 60000,
		Definitions: []*rwv1.IndicatorDefinition{{
			IndicatorKey: "bb_mid", Name: "BB Mid", Type: "line", Pane: "price",
		}},
		Values: []*rwv1.IndicatorValue{{IndicatorKey: "bb_mid", Value: 100.5, HasValue: true}},
	}
}

func TestAuthenticatedIndicatorV2RejectsPayloadSessionBeforeBuffering(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	const sessionID = "41414141414141414141414141414141"
	generation := newWorkerGeneration(sessionID, 1)
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("bind authenticated generation")
	}
	agent.generations[sessionID] = generation
	frame := indicatorSyncFrameV2(
		"other-session",
		"binance:spot:BTCUSDT:1m",
		0,
		60_000,
	)
	var shutdown *rwv1.ShutdownWorker

	err := agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		WorkerIdentity{
			SessionID:  sessionID,
			PID:        123,
			Generation: 7,
			token:      "worker-token",
		},
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: frame,
			},
		},
		func(frame *rwv1.AgentFrame) error {
			shutdown = frame.GetShutdownWorker()
			return nil
		},
	)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) ||
		!strings.Contains(protocolErr.Error(), "payload session_id") {
		t.Fatalf("error = %v, want payload-session protocol error", err)
	}
	if agent.indicatorSync.lookupSession(sessionID) != nil ||
		agent.indicatorSync.lookupSession("other-session") != nil {
		t.Fatal("cross-session frame created indicator state")
	}
	generation.mu.Lock()
	closing := generation.closing
	generation.mu.Unlock()
	if !closing {
		t.Fatal("cross-session protocol failure left worker generation open")
	}
	if shutdown == nil || shutdown.GetSessionId() != sessionID {
		t.Fatalf("shutdown = %+v", shutdown)
	}
}

func TestAuthenticatedIndicatorV2ProtocolFailureClosesGenerationImmediately(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	const sessionID = "42424242424242424242424242424242"
	generation := newWorkerGeneration(sessionID, 1)
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("bind authenticated generation")
	}
	agent.generations[sessionID] = generation
	runRequest, err := anypb.New(&strategyv1.RunStrategyRequest{
		UserId:    6,
		RuntimeId: "runtime-v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.cfg.RuntimeID = "runtime-v2"
	agent.rememberRunRequest(sessionID, runRequest)
	identity := WorkerIdentity{
		SessionID:  sessionID,
		PID:        123,
		Generation: 7,
		token:      "worker-token",
	}
	first := indicatorSyncFrameV2(
		sessionID,
		"binance:spot:BTCUSDT:1m",
		0,
		60_000,
	)
	if err := agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		identity,
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: first,
			},
		},
		func(*rwv1.AgentFrame) error { return nil },
	); err != nil {
		t.Fatalf("first frame: %v", err)
	}

	conflict := proto.Clone(first).(*rwv1.IndicatorFrameV2)
	conflict.Samples[0].ScalarValue = proto.Float64(2)
	var sent []*rwv1.AgentFrame
	err = agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		identity,
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: conflict,
			},
		},
		func(frame *rwv1.AgentFrame) error {
			sent = append(sent, frame)
			return nil
		},
	)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error = %v, want IndicatorProtocolError", err)
	}
	generation.mu.Lock()
	closing := generation.closing
	inFlight := generation.inFlight
	generation.mu.Unlock()
	if !closing || inFlight != 0 {
		t.Fatalf("generation closing=%v inFlight=%d", closing, inFlight)
	}
	if len(sent) != 1 ||
		sent[0].GetShutdownWorker() == nil ||
		sent[0].GetShutdownWorker().GetSessionId() != sessionID {
		t.Fatalf("sent = %+v", sent)
	}
}

func TestAuthenticatedIndicatorV2ProtocolFailureDisconnectFinalizesAndRecovers(t *testing.T) {
	const sessionID = "43434343434343434343434343434343"
	var methods []string
	var update portfoliov1.UpdateSessionRequest
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
			methods = append(methods, method)
			switch method {
			case "portfolio.SaveStrategyIndicatorsV2":
				var save portfoliov1.SaveStrategyIndicatorsV2Request
				if err := request.UnmarshalTo(&save); err != nil {
					return nil, err
				}
				if len(save.GetChunks()) != 1 || save.GetChunks()[0].GetCount() != 1 {
					return nil, fmt.Errorf("unexpected V2 save: %+v", &save)
				}
				return anypb.New(&portfoliov1.SaveStrategyIndicatorsV2Response{
					DefinitionsSaved: 1,
					ChunksSaved:      1,
				})
			case "portfolio.FinalizeStrategyIndicatorChunksV2":
				var finalize portfoliov1.FinalizeStrategyIndicatorChunksV2Request
				if err := request.UnmarshalTo(&finalize); err != nil {
					return nil, err
				}
				if len(finalize.GetChunks()) != 1 ||
					finalize.GetChunks()[0].GetExpectedRevision() != 1 {
					return nil, fmt.Errorf("unexpected V2 finalization: %+v", &finalize)
				}
				return anypb.New(
					&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
						ChunksFinalized: 1,
					},
				)
			case "portfolio.GetSession":
				return anypb.New(&portfoliov1.GetSessionResponse{
					Session: &portfoliov1.StrategySessionEntry{
						SessionId:     sessionID,
						UserId:        6,
						RuntimeId:     "runtime-v2",
						Status:        "running",
						BarsProcessed: 1,
					},
				})
			case "portfolio.UpdateSession":
				if err := request.UnmarshalTo(&update); err != nil {
					return nil, err
				}
				return anypb.New(&portfoliov1.UpdateSessionResponse{})
			default:
				return nil, fmt.Errorf("unexpected method: %s", method)
			}
		},
	}
	agent := NewAgent(AgentConfig{
		RuntimeID:       "runtime-v2",
		UserID:          6,
		PlatformInvoker: invoker,
		RequestTimeout:  time.Second,
	})
	generation := newWorkerGeneration(sessionID, 1)
	generation.durablePossible = true
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("bind authenticated generation")
	}
	agent.generations[sessionID] = generation
	runRequest, err := anypb.New(&strategyv1.RunStrategyRequest{
		UserId:    6,
		RuntimeId: "runtime-v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.rememberRunRequest(sessionID, runRequest)
	identity := WorkerIdentity{
		SessionID:  sessionID,
		PID:        123,
		Generation: 7,
		token:      "worker-token",
	}
	first := indicatorSyncFrameV2(
		sessionID,
		"binance:spot:BTCUSDT:1m",
		0,
		60_000,
	)
	if err := agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		identity,
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: first,
			},
		},
		func(*rwv1.AgentFrame) error { return nil },
	); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	conflict := proto.Clone(first).(*rwv1.IndicatorFrameV2)
	conflict.Samples[0].ScalarValue = proto.Float64(2)
	protocolErr := agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		identity,
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: conflict,
			},
		},
		func(*rwv1.AgentFrame) error { return nil },
	)
	if protocolErr == nil {
		t.Fatal("conflicting duplicate was accepted")
	}

	if err := agent.HandleWorkerDisconnect(identity, protocolErr); err != nil {
		t.Fatalf("HandleWorkerDisconnect: %v", err)
	}
	wantMethods := []string{
		"portfolio.SaveStrategyIndicatorsV2",
		"portfolio.FinalizeStrategyIndicatorChunksV2",
		"portfolio.GetSession",
		"portfolio.UpdateSession",
	}
	if !slices.Equal(methods, wantMethods) {
		t.Fatalf("cleanup methods = %v, want %v", methods, wantMethods)
	}
	if update.GetStatus() != "recoverable" ||
		!strings.Contains(update.GetError(), "conflicting duplicate payload") {
		t.Fatalf("protocol recovery update = %+v", &update)
	}
	if update.IndicatorFinalizationPending == nil ||
		update.GetIndicatorFinalizationPending() {
		t.Fatalf("successful protocol cleanup must clear finalization pending: %+v", &update)
	}
	agent.mu.Lock()
	_, retained := agent.generations[sessionID]
	agent.mu.Unlock()
	if retained || agent.indicatorSync.lookupSession(sessionID) != nil {
		t.Fatal("successful protocol cleanup retained generation or indicator state")
	}
}

type fakePlatformInvoker struct {
	method   string
	request  *anypb.Any
	response *anypb.Any
	onInvoke func(method string, request *anypb.Any) (*anypb.Any, error)
}

type admissionCleanupPlatform struct {
	mu                           sync.Mutex
	events                       []string
	saveStarted                  chan struct{}
	releaseSave                  chan struct{}
	status                       string
	indicatorFinalizationPending bool
	lastUpdate                   *portfoliov1.UpdateSessionRequest
}

type finalStatusShutdownRacePlatform struct {
	updateOnce    sync.Once
	updateStarted chan struct{}
	releaseUpdate chan struct{}
	getStarted    chan struct{}
}

func (p *finalStatusShutdownRacePlatform) InvokePlatformAny(
	ctx context.Context,
	method string,
	_ *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	switch method {
	case "portfolio.UpdateSession":
		p.updateOnce.Do(func() { close(p.updateStarted) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.releaseUpdate:
			return anypb.New(&portfoliov1.UpdateSessionResponse{})
		}
	case "portfolio.GetSession":
		select {
		case p.getStarted <- struct{}{}:
		default:
		}
		return anypb.New(&portfoliov1.GetSessionResponse{
			Session: &portfoliov1.StrategySessionEntry{
				SessionId: "18191919191919191919191919191919",
				UserId:    6,
				RuntimeId: "rt-1",
				Status:    "running",
			},
		})
	default:
		return nil, fmt.Errorf("unexpected method: %s", method)
	}
}

type concurrentTerminalRetryPlatform struct {
	mu        sync.Mutex
	active    int
	maxActive int
	started   chan struct{}
	release   chan struct{}
}

func (p *concurrentTerminalRetryPlatform) InvokePlatformAny(
	ctx context.Context,
	method string,
	_ *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	switch method {
	case "portfolio.GetSession":
		p.mu.Lock()
		p.active++
		if p.active > p.maxActive {
			p.maxActive = p.active
		}
		p.mu.Unlock()
		p.started <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.release:
		}
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return anypb.New(&portfoliov1.GetSessionResponse{
			Session: &portfoliov1.StrategySessionEntry{
				SessionId:                    "52525252525252525252525252525252",
				UserId:                       6,
				RuntimeId:                    "rt-1",
				Status:                       "recoverable",
				BarsProcessed:                10,
				IndicatorFinalizationPending: true,
			},
		})
	case "portfolio.UpdateSession":
		return anypb.New(&portfoliov1.UpdateSessionResponse{})
	default:
		return nil, fmt.Errorf("unexpected method: %s", method)
	}
}

func (p *concurrentTerminalRetryPlatform) maximumConcurrent() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

type shutdownWorkerStopper struct {
	stopAllCalls int
	onStopAll    func()
}

func (s *shutdownWorkerStopper) StopAll(
	context.Context,
	time.Duration,
) error {
	s.stopAllCalls++
	if s.onStopAll != nil {
		s.onStopAll()
	}
	return nil
}

func (*shutdownWorkerStopper) StopSessionWorker(
	context.Context,
	string,
	time.Duration,
) error {
	return nil
}

type contextDeadlinePlatform struct {
	done chan struct{}
}

type blockingPlatformInvoker struct{}

func (blockingPlatformInvoker) InvokePlatformAny(
	ctx context.Context,
	_ string,
	_ *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (p *contextDeadlinePlatform) InvokePlatformAny(
	ctx context.Context,
	_ string,
	_ *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	<-ctx.Done()
	close(p.done)
	return nil, ctx.Err()
}

func (p *admissionCleanupPlatform) InvokePlatformAny(
	_ context.Context,
	method string,
	request *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	switch method {
	case "portfolio.SaveSession":
		p.recordEvent("SaveSession:start")
		close(p.saveStarted)
		<-p.releaseSave
		p.mu.Lock()
		p.status = "pending"
		p.events = append(p.events, "SaveSession:end")
		p.mu.Unlock()
		return anypb.New(&portfoliov1.SaveSessionResponse{})
	case "portfolio.GetSession":
		p.recordEvent("GetSession")
		var get portfoliov1.GetSessionRequest
		if err := request.UnmarshalTo(&get); err != nil {
			return nil, err
		}
		p.mu.Lock()
		status := p.status
		indicatorFinalizationPending := p.indicatorFinalizationPending
		p.mu.Unlock()
		return anypb.New(&portfoliov1.GetSessionResponse{Session: &portfoliov1.StrategySessionEntry{
			SessionId: get.GetSessionId(), UserId: 6, RuntimeId: "rt-1", Status: status,
			IndicatorFinalizationPending: indicatorFinalizationPending,
		}})
	case "portfolio.UpdateSession":
		var update portfoliov1.UpdateSessionRequest
		if err := request.UnmarshalTo(&update); err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.status = update.GetStatus()
		if update.IndicatorFinalizationPending != nil {
			p.indicatorFinalizationPending = update.GetIndicatorFinalizationPending()
		}
		p.lastUpdate = proto.Clone(&update).(*portfoliov1.UpdateSessionRequest)
		p.events = append(p.events, "UpdateSession:"+update.GetStatus())
		p.mu.Unlock()
		return anypb.New(&portfoliov1.UpdateSessionResponse{})
	default:
		return nil, fmt.Errorf("unexpected method: %s", method)
	}
}

func (p *admissionCleanupPlatform) recordEvent(event string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *admissionCleanupPlatform) snapshotEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func (p *admissionCleanupPlatform) snapshotLastUpdate() *portfoliov1.UpdateSessionRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastUpdate == nil {
		return nil
	}
	return proto.Clone(p.lastUpdate).(*portfoliov1.UpdateSessionRequest)
}

func (i *fakePlatformInvoker) InvokePlatformAny(ctx context.Context, method string, request *anypb.Any, timeout time.Duration) (*anypb.Any, error) {
	i.method = method
	i.request = request
	if i.onInvoke != nil {
		return i.onInvoke(method, request)
	}
	if i.response == nil {
		resp, _ := anypb.New(&portfoliov1.SaveStrategyIndicatorsResponse{DefinitionsSaved: 1, ChunksSaved: 1})
		i.response = resp
	}
	return i.response, nil
}

type fakeWorkerStopper struct {
	sessionID     string
	timeout       time.Duration
	waitSessionID string
	waitTimeout   time.Duration
	waitErr       error
}

func (s *fakeWorkerStopper) StopSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error {
	s.sessionID = sessionID
	s.timeout = timeout
	return nil
}

func (s *fakeWorkerStopper) WaitSessionWorker(_ context.Context, sessionID string, timeout time.Duration) error {
	s.waitSessionID = sessionID
	s.waitTimeout = timeout
	return s.waitErr
}

type fakeWorkerSender struct {
	sendErr error
	onSend  func(string, *rwv1.AgentFrame)
}

func (s *fakeWorkerSender) SendToWorker(sessionID string, frame *rwv1.AgentFrame) error {
	if s.onSend != nil {
		s.onSend(sessionID, frame)
	}
	return s.sendErr
}
