package runtimeagent

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestAgentRunStrategyStartsWorkerAndReturnsWorkerSessionID(t *testing.T) {
	starter := &fakeWorkerStarter{}
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:     "rt-1",
		WorkerStarter: starter,
		WorkerSender:  sender,
	})
	starter.onStart = func(pendingSessionID string) {
		go func() {
			time.Sleep(10 * time.Millisecond)
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: "sess-real",
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
	if resp.GetSessionId() != "sess-real" {
		t.Fatalf("session_id = %q", resp.GetSessionId())
	}
	if starter.startedSessionID == "" {
		t.Fatalf("worker was not started")
	}
	if sender.aliasFrom != starter.startedSessionID || sender.aliasTo != "sess-real" {
		t.Fatalf("alias = %q -> %q, want %q -> sess-real", sender.aliasFrom, sender.aliasTo, starter.startedSessionID)
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

func TestAgentRunStrategyReturnsWorkerExitBeforeStartTimeout(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_start", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := NewWorkerManager(WorkerManagerConfig{
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
		starter.onStart = func(pendingSessionID string) {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: "sess-real",
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
					SessionId: pendingSessionID,
					Token:     "token",
					Pid:       123,
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

func TestAgentPreviewRunStrategyReturnsWorkerExitBeforeReadyTimeout(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_hello", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := NewWorkerManager(WorkerManagerConfig{
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

func TestAgentPreviewRunStrategyReportsProcessExitBeforeCleanupCompletes(t *testing.T) {
	dir := t.TempDir()
	writePythonWorkerModule(t, dir, "worker_exit_before_cleanup", `
raise RuntimeError("worker bootstrap failed")
`)
	manager := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "worker_exit_before_cleanup",
		AgentAddr:        "127.0.0.1:59000",
		WorkDir:          dir,
		StateRoot:        filepath.Join(dir, "state"),
		PythonPath:       []string{dir},
	})
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	defer close(releaseCleanup)
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
		if respFrame.GetError().GetCode() != "Internal" {
			t.Fatalf("error code = %q, want Internal", respFrame.GetError().GetCode())
		}
		if got := respFrame.GetError().GetMessage(); !strings.Contains(got, "session worker exited before connecting") {
			t.Fatalf("error message = %q, want worker exit before connecting", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker exit was hidden behind blocked cleanup")
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
		if err := agent.waitWorkerReady(context.Background(), ready, worker, time.Second); err != nil {
			t.Fatalf("iteration %d waitWorkerReady: %v", i, err)
		}
	}
}

func TestAgentRoutesStatusAndStopToRunningWorker(t *testing.T) {
	sender := &fakeWorkerSender{}
	agent := NewAgent(AgentConfig{
		RuntimeID:      "rt-1",
		WorkerSender:   sender,
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
	if err != nil {
		t.Fatalf("HandleWorkerFrame final: %v", err)
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
					SessionId: "sess-restarted", Status: "running",
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
	invoker := &fakePlatformInvoker{
		onInvoke: func(method string, request *anypb.Any) (*anypb.Any, error) {
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
	if err := agent.indicatorSync.ReceiveFrame(agentIndicatorFrame("sess-old")); err != nil {
		t.Fatalf("ReceiveFrame old indicators: %v", err)
	}
	agent.ready["sess-old"] = make(chan struct{}, 1)
	starter.onStart = func(pendingSessionID string) {
		go func() {
			_ = agent.HandleWorkerFrame(context.Background(), pendingSessionID, &rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_Progress{Progress: &rwv1.SessionProgress{
					SessionId: "sess-new",
					Status:    "running",
				}},
			}, nil)
		}()
	}

	result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}

	if result.OldSessionID != "sess-old" || result.NewSessionID != "sess-new" || result.RuntimeID != "rt-1" {
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
					SessionId: "sess-new",
					Status:    "running",
				}},
			}, nil)
		}()
	}

	result, err := agent.RestartSession(context.Background(), RestartSessionOptions{SessionID: "sess-old"})
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}

	if result.NewSessionID != "sess-new" {
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
					SessionId: "sess-new",
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

type agentFinalPlatform struct {
	mu      sync.Mutex
	methods []string
	updates []*portfoliov1.UpdateSessionRequest
	saveErr error
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

type fakePlatformInvoker struct {
	method   string
	request  *anypb.Any
	response *anypb.Any
	onInvoke func(method string, request *anypb.Any) (*anypb.Any, error)
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
	sessionID string
	timeout   time.Duration
}

func (s *fakeWorkerStopper) StopSessionWorker(ctx context.Context, sessionID string, timeout time.Duration) error {
	s.sessionID = sessionID
	s.timeout = timeout
	return nil
}

type fakeWorkerSender struct {
	aliasFrom string
	aliasTo   string
	sendErr   error
	onSend    func(string, *rwv1.AgentFrame)
}

func (s *fakeWorkerSender) SendToWorker(sessionID string, frame *rwv1.AgentFrame) error {
	if s.onSend != nil {
		s.onSend(sessionID, frame)
	}
	return s.sendErr
}

func (s *fakeWorkerSender) AliasWorkerSession(existingSessionID string, sessionID string) error {
	s.aliasFrom = existingSessionID
	s.aliasTo = sessionID
	return nil
}
