//go:build integration

package runtimeagent

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const blockedWorkerRuntimeID = "bare-6-blocked-worker"

func TestBlockedWorkerKeepsRuntimeHeartbeatAndCanBeReplaced(t *testing.T) {
	blockSeconds := blockedWorkerDuration(t, "HUSHINE_BLOCKED_WORKER_SECONDS", 8)
	observeSeconds := blockedWorkerDuration(t, "HUSHINE_BLOCKED_WORKER_OBSERVE_SECONDS", 3)
	if blockSeconds <= observeSeconds {
		t.Fatalf(
			"HUSHINE_BLOCKED_WORKER_SECONDS=%v must exceed observation=%v",
			blockSeconds,
			observeSeconds,
		)
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	strategyCode, err := os.ReadFile(filepath.Join(
		repoRoot,
		"tests",
		"strategies",
		"block_after_first_indicator.py",
	))
	if err != nil {
		t.Fatalf("read blocked worker strategy: %v", err)
	}
	barrierRoot := t.TempDir()
	markerPath := filepath.Join(barrierRoot, "blocked-worker.marker")
	replayStartPath := filepath.Join(barrierRoot, "replacement-start.pb")
	replayBatchPath := filepath.Join(barrierRoot, "replacement-income.pb")
	replayEventsPath := filepath.Join(barrierRoot, "replacement-events.json")
	replayACKReleasePath := filepath.Join(barrierRoot, "release-income-ack")
	replayACKEnqueuedPath := filepath.Join(barrierRoot, "income-ack-enqueued")
	stateRoot := filepath.Join(t.TempDir(), "worker-state")

	workerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen worker IPC: %v", err)
	}
	defer workerListener.Close()
	venvDirectory := "bin"
	venvPython := "python"
	if runtime.GOOS == "windows" {
		venvDirectory = "Scripts"
		venvPython = "python.exe"
	}
	launchSpec, err := ResolveWorkerLaunchSpec(WorkerManagerConfig{
		PythonExecutable: filepath.Join(
			repoRoot,
			".venv",
			venvDirectory,
			venvPython,
		),
		WorkerModule: "strategy_service.session_worker_entry",
		AgentAddr:    workerListener.Addr().String(),
		WorkDir:      repoRoot,
		StateRoot:    stateRoot,
	}, "bare", os.Environ())
	if err != nil {
		t.Fatalf("resolve worker launch: %v", err)
	}
	workerManager, err := NewWorkerManager(launchSpec)
	if err != nil {
		t.Fatalf("new worker manager: %v", err)
	}
	originalEnvironmentBuilder := workerManager.buildEnvironment
	workerManager.buildEnvironment = func(
		cfg WorkerManagerConfig,
		spec WorkerStartSpec,
		extraEnv []string,
		cleanup workerSessionCleanup,
	) ([]string, string, string, error) {
		env, sessionRoot, executable, buildErr := originalEnvironmentBuilder(
			cfg,
			spec,
			extraEnv,
			cleanup,
		)
		if buildErr != nil {
			return nil, sessionRoot, executable, buildErr
		}
		env = append(
			env,
			"HUSHINE_BLOCKED_WORKER_MARKER="+markerPath,
			"HUSHINE_BLOCKED_WORKER_SECONDS="+strconv.FormatFloat(
				blockSeconds.Seconds(),
				'f',
				3,
				64,
			),
			"HUSHINE_INCOME_REPLAY_START="+replayStartPath,
			"HUSHINE_INCOME_REPLAY_BATCH="+replayBatchPath,
			"HUSHINE_INCOME_REPLAY_EVENTS="+replayEventsPath,
			"HUSHINE_INCOME_REPLAY_ACK_RELEASE="+replayACKReleasePath,
			"HUSHINE_INCOME_REPLAY_ACK_ENQUEUED="+replayACKEnqueuedPath,
		)
		return env, sessionRoot, executable, nil
	}

	control := newBlockedWorkerControl(string(strategyCode))
	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake RuntimeChannel: %v", err)
	}
	defer controlListener.Close()
	controlServer := grpc.NewServer()
	cpv1.RegisterControlPanelServiceServer(controlServer, control)
	go func() { _ = controlServer.Serve(controlListener) }()
	defer controlServer.Stop()

	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	var agent *Agent
	runtimeClient := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: controlListener.Addr().String(),
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         blockedWorkerRuntimeID,
			Name:              "blocked-worker-test",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		HeartbeatSeconds: 1,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		RequestHandler: func(
			ctx context.Context,
			frame *cpv1.RuntimeFrame,
		) *cpv1.RuntimeFrame {
			return agent.HandleRuntimeRequest(ctx, frame)
		},
		DataHandler: func(
			ctx context.Context,
			frame *cpv1.RuntimeFrame,
		) error {
			return agent.HandleRuntimeData(ctx, frame)
		},
	})
	agent = NewAgent(AgentConfig{
		RuntimeID:              blockedWorkerRuntimeID,
		RuntimeSource:          "bare",
		RuntimeName:            "blocked-worker-test",
		UserID:                 6,
		StateRoot:              stateRoot,
		WorkerStarter:          workerManager,
		WorkerStopper:          workerManager,
		PlatformInvoker:        runtimeClient,
		StartTimeout:           30 * time.Second,
		RequestTimeout:         10 * time.Second,
		IndicatorFlushInterval: time.Hour,
	})
	workerServer := NewAuthenticatedWorkerIPCServer(
		workerManager.Registry(),
		agent.HandleAuthenticatedWorkerFrame,
		func(identity WorkerIdentity, cause error) {
			_ = agent.HandleWorkerDisconnect(identity, cause)
		},
	)
	agent.SetWorkerSender(workerServer)
	workerGRPC := grpc.NewServer()
	rwv1.RegisterRuntimeWorkerAgentServer(workerGRPC, workerServer)
	go func() { _ = workerGRPC.Serve(workerListener) }()
	defer workerGRPC.Stop()
	go agent.RunSyncLoop(serviceCtx)

	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- runtimeClient.Run(serviceCtx) }()
	select {
	case <-control.connected:
	case err := <-runtimeDone:
		t.Fatalf("RuntimeChannel stopped before hello: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RuntimeChannel did not connect")
	}

	runRequest, err := anypb.New(&strategyv1.RunStrategyRequest{
		PortfolioId: 7,
		Interval:    "1m",
		StartTimeMs: 1_780_000_000_000,
		EndTimeMs:   1_780_000_120_000,
		UserId:      6,
		RuntimeId:   blockedWorkerRuntimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := control.send(&cpv1.RuntimeFrame{
		CorrelationId: "run-blocked-worker",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		DeadlineUnixMs: time.Now().
			Add(30 * time.Second).
			UnixMilli(),
		Payload: &cpv1.RuntimeFrame_Request{
			Request: &cpv1.StrategyRequest{
				Method:  "RunStrategy",
				Request: runRequest,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	runFrame := control.waitForResult(t, "run-blocked-worker", 35*time.Second)
	if runFrame.GetError() != nil {
		t.Fatalf("RunStrategy failed: %+v", runFrame.GetError())
	}
	var runResponse strategyv1.RunStrategyResponse
	if runFrame.GetResponse() == nil ||
		runFrame.GetResponse().GetResponse() == nil ||
		runFrame.GetResponse().GetResponse().UnmarshalTo(&runResponse) != nil {
		t.Fatalf("invalid RunStrategy response: %+v", runFrame)
	}
	oldSessionID := runResponse.GetSessionId()
	if oldSessionID == "" {
		t.Fatal("RunStrategy returned an empty session_id")
	}
	if !runResponse.GetOk() {
		t.Fatalf("RunStrategy returned structured failure: %+v", &runResponse)
	}
	oldIdentity, ok := workerManager.Registry().ActiveWorker(oldSessionID)
	if !ok {
		t.Fatalf("old worker identity is unavailable: %s", oldSessionID)
	}
	waitForBlockedWorkerMarker(t, markerPath, runtimeDone, 20*time.Second)
	incomeFrame := blockedWorkerIncomeRuntimeFrame(
		oldSessionID,
		10,
		"0.010000000000000001",
	)
	expectedWorkerFrame, mappedSessionID, err := workerDataFrameFromRuntime(incomeFrame)
	if err != nil {
		t.Fatalf("map expected Income frame: %v", err)
	}
	if mappedSessionID != oldSessionID || expectedWorkerFrame.GetIncomeBatch() == nil {
		t.Fatalf("mapped expected Income = %+v session=%q", expectedWorkerFrame, mappedSessionID)
	}
	if err := control.send(incomeFrame); err != nil {
		t.Fatal(err)
	}
	if err := control.send(blockedWorkerIncomeRuntimeFrame(
		oldSessionID,
		11,
		"0.020000000000000001",
	)); err != nil {
		t.Fatal(err)
	}
	backpressure := control.waitForDataBackpressure(t, 5*time.Second)
	if backpressure.GetSessionId() != oldSessionID ||
		backpressure.GetStreamKey() != "income/"+oldSessionID ||
		!strings.Contains(backpressure.GetReason(), "already pending") {
		t.Fatalf("blocked Income backpressure = %+v", backpressure)
	}
	control.assertNoRuntimeDataACK(t)

	heartbeatTimes := observeBlockedWorkerHeartbeats(
		t,
		control,
		runtimeDone,
		observeSeconds,
	)
	if observeSeconds >= 10*time.Minute && len(heartbeatTimes) < 500 {
		t.Fatalf(
			"observed %d heartbeats during %v, want at least 500",
			len(heartbeatTimes),
			observeSeconds,
		)
	}
	for index := 1; index < len(heartbeatTimes); index++ {
		if gap := heartbeatTimes[index].Sub(heartbeatTimes[index-1]); gap > 5*time.Second {
			t.Fatalf("heartbeat gap = %v, want <= 5s", gap)
		}
	}
	if control.heartbeatACKCount() < len(heartbeatTimes) {
		t.Fatalf(
			"Runtime heartbeat ACKs = %d, want at least %d",
			control.heartbeatACKCount(),
			len(heartbeatTimes),
		)
	}
	control.assertNoRuntimeDataACK(t)
	assertExactPendingIncome(t, agent, oldSessionID, expectedWorkerFrame)
	assertManagedWorkerAlive(t, workerManager, oldSessionID)

	oldWorker := workerManager.findWorker(oldSessionID)
	if oldWorker == nil || oldWorker.Cmd == nil || oldWorker.Cmd.Process == nil {
		t.Fatalf("old managed worker process is unavailable: %s", oldSessionID)
	}
	agent.mu.Lock()
	wantStart := proto.Clone(agent.sessionStarts[oldSessionID])
	agent.mu.Unlock()
	if wantStart == nil {
		t.Fatal("replacement replay StartSession is unavailable")
	}
	workerManager.mu.Lock()
	workerManager.cfg.WorkerModule = "strategy_service.runtime_income_replay_testworker"
	workerManager.mu.Unlock()
	if err := oldWorker.Cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill blocked worker before Income ACK: %v", err)
	}
	select {
	case <-oldWorker.processExitedSignal():
	case <-time.After(10 * time.Second):
		t.Fatalf("blocked worker process did not exit: %s", oldSessionID)
	}
	assertExactPendingIncome(t, agent, oldSessionID, expectedWorkerFrame)
	waitForWorkerFile(t, replayStartPath)
	waitForWorkerFile(t, replayBatchPath)
	waitForWorkerFile(t, replayEventsPath)
	assertReplacementIncomeReplay(
		t,
		replayStartPath,
		replayBatchPath,
		replayEventsPath,
		wantStart.(*rwv1.StartSession),
		expectedWorkerFrame.GetIncomeBatch(),
	)
	control.assertNoRuntimeDataACK(t)
	if err := os.WriteFile(replayACKReleasePath, []byte("durable\n"), 0o600); err != nil {
		t.Fatalf("release durable Income ACK: %v", err)
	}
	waitForWorkerFile(t, replayACKEnqueuedPath)
	dataACK := control.waitForDataACK(t, 5*time.Second)
	if dataACK.GetSessionId() != oldSessionID ||
		dataACK.GetStreamKey() != "income/"+oldSessionID ||
		dataACK.GetSequence() != 10 {
		t.Fatalf("Runtime Income DATA_ACK = %+v", dataACK)
	}
	control.assertExactlyOneRuntimeDataACK(t)
	agent.mu.Lock()
	pendingAfterACK := agent.pendingIncome[oldSessionID]
	agent.mu.Unlock()
	if pendingAfterACK != nil {
		t.Fatal("durable WorkerDataAck did not clear retained Income")
	}
	workerManager.mu.Lock()
	workerManager.cfg.WorkerModule = "strategy_service.session_worker_entry"
	workerManager.mu.Unlock()

	restartCtx, cancelRestart := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	restartResult, err := agent.RestartSession(
		restartCtx,
		RestartSessionOptions{SessionID: oldSessionID},
	)
	cancelRestart()
	if err != nil {
		t.Fatalf("RestartSession: %v", err)
	}
	if restartResult.OldSessionID != oldSessionID ||
		restartResult.NewSessionID == "" ||
		restartResult.NewSessionID == oldSessionID {
		t.Fatalf("restart result = %+v", restartResult)
	}
	control.assertFinalizedBeforeRecoverable(t, oldSessionID)
	control.assertOneFinalizedIndicatorPoint(t, oldSessionID)

	if err := agent.HandleWorkerDisconnect(
		oldIdentity,
		errors.New("late generation-1 close"),
	); err != nil {
		t.Fatalf("late old-generation disconnect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	assertManagedWorkerAlive(t, workerManager, restartResult.NewSessionID)
	if err := agent.HandleRuntimeData(
		context.Background(),
		blockedWorkerIncomeRuntimeFrame(oldSessionID, 12, "0.030000000000000001"),
	); err == nil {
		t.Fatal("old-Session Income attached after user-command restart")
	}
	agent.mu.Lock()
	oldPending := agent.pendingIncome[oldSessionID]
	newPending := agent.pendingIncome[restartResult.NewSessionID]
	agent.mu.Unlock()
	if oldPending != nil || newPending != nil {
		t.Fatalf(
			"post-restart old Income retained old=%+v new=%+v",
			oldPending,
			newPending,
		)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	if err := agent.Shutdown(shutdownCtx, time.Second); err != nil {
		cancelShutdown()
		t.Fatalf("Agent.Shutdown: %v", err)
	}
	cancelShutdown()
	cancelService()
	select {
	case err := <-runtimeDone:
		if err != nil {
			t.Fatalf("RuntimeChannel shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RuntimeChannel did not stop")
	}
}

func blockedWorkerDuration(
	t *testing.T,
	name string,
	defaultSeconds float64,
) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return time.Duration(defaultSeconds * float64(time.Second))
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		t.Fatalf("%s=%q is not a positive duration in seconds", name, raw)
	}
	return time.Duration(seconds * float64(time.Second))
}

func waitForBlockedWorkerMarker(
	t *testing.T,
	path string,
	runtimeDone <-chan error,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if body, err := os.ReadFile(path); err == nil &&
				string(body) == "blocked\n" {
				return
			}
		case err := <-runtimeDone:
			t.Fatalf("RuntimeChannel stopped while waiting for blocked worker: %v", err)
		case <-deadline.C:
			t.Fatalf("blocked worker marker was not written: %s", path)
		}
	}
}

func observeBlockedWorkerHeartbeats(
	t *testing.T,
	control *blockedWorkerControl,
	runtimeDone <-chan error,
	duration time.Duration,
) []time.Time {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	noHeartbeat := time.NewTimer(5 * time.Second)
	defer noHeartbeat.Stop()
	timestamps := make([]time.Time, 0, int(duration/time.Second)+1)
	for {
		select {
		case timestamp := <-control.heartbeats:
			timestamps = append(timestamps, timestamp)
			if !noHeartbeat.Stop() {
				select {
				case <-noHeartbeat.C:
				default:
				}
			}
			noHeartbeat.Reset(5 * time.Second)
		case err := <-runtimeDone:
			t.Fatalf("RuntimeChannel stopped while worker was blocked: %v", err)
		case <-noHeartbeat.C:
			t.Fatalf(
				"heartbeat stopped for 5 seconds while worker was blocked; observed=%d",
				len(timestamps),
			)
		case <-deadline.C:
			if len(timestamps) < 3 {
				t.Fatalf(
					"observed %d heartbeats while worker was blocked, want at least 3",
					len(timestamps),
				)
			}
			return timestamps
		}
	}
}

func assertManagedWorkerAlive(
	t *testing.T,
	manager *WorkerManager,
	sessionID string,
) {
	t.Helper()
	worker := manager.findWorker(sessionID)
	if worker == nil || worker.Cmd == nil || worker.Cmd.Process == nil {
		t.Fatalf("managed worker is unavailable: %s", sessionID)
	}
	select {
	case <-worker.processExitedSignal():
		t.Fatalf("managed worker exited unexpectedly: %s", sessionID)
	default:
	}
}

func assertExactPendingIncome(
	t *testing.T,
	agent *Agent,
	sessionID string,
	want *rwv1.AgentFrame,
) {
	t.Helper()
	agent.mu.Lock()
	pending := agent.pendingIncome[sessionID]
	var got *rwv1.AgentFrame
	if pending != nil {
		got = proto.Clone(pending.frame).(*rwv1.AgentFrame)
	}
	agent.mu.Unlock()
	if pending == nil || !proto.Equal(got, want) {
		t.Fatalf("retained Income = %+v, want exact %+v", got, want)
	}
}

type capturedIncomeEvent struct {
	SessionID string `json:"session_id"`
	StreamKey string `json:"stream_key"`
	Sequence  int64  `json:"sequence"`
	BatchEnd  bool   `json:"batch_end"`
	EntryHex  string `json:"entry_hex"`
}

func assertReplacementIncomeReplay(
	t *testing.T,
	startPath string,
	batchPath string,
	eventsPath string,
	wantStart *rwv1.StartSession,
	wantBatch *rwv1.IncomeBatch,
) {
	t.Helper()
	startBytes, err := os.ReadFile(startPath)
	if err != nil {
		t.Fatal(err)
	}
	var gotStart rwv1.StartSession
	if err := proto.Unmarshal(startBytes, &gotStart); err != nil {
		t.Fatalf("decode replacement StartSession: %v", err)
	}
	if !proto.Equal(&gotStart, wantStart) {
		t.Fatalf("replacement StartSession = %+v, want exact %+v", &gotStart, wantStart)
	}
	batchBytes, err := os.ReadFile(batchPath)
	if err != nil {
		t.Fatal(err)
	}
	wantBatchBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(wantBatch)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(batchBytes, wantBatchBytes) {
		t.Fatalf("replacement Income bytes differ: got=%x want=%x", batchBytes, wantBatchBytes)
	}
	eventBytes, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	var events []capturedIncomeEvent
	if err := json.Unmarshal(eventBytes, &events); err != nil {
		t.Fatalf("decode replacement Income events: %v", err)
	}
	if len(events) != len(wantBatch.GetEntries()) {
		t.Fatalf("decoded replacement Income events = %d, want %d", len(events), len(wantBatch.GetEntries()))
	}
	for index, event := range events {
		if event.SessionID != wantBatch.GetSessionId() ||
			event.StreamKey != wantBatch.GetStreamKey() ||
			event.Sequence != wantBatch.GetSequence() ||
			event.BatchEnd != (index == len(events)-1) {
			t.Fatalf("decoded Income event[%d] metadata = %+v", index, event)
		}
		entryBytes, err := hex.DecodeString(event.EntryHex)
		if err != nil {
			t.Fatalf("decode Income event[%d] bytes: %v", index, err)
		}
		var gotEntry portfoliov1.VenueIncomeEntry
		if err := proto.Unmarshal(entryBytes, &gotEntry); err != nil {
			t.Fatalf("unmarshal Income event[%d]: %v", index, err)
		}
		var wantEntry portfoliov1.VenueIncomeEntry
		if err := wantBatch.GetEntries()[index].UnmarshalTo(&wantEntry); err != nil {
			t.Fatalf("unmarshal expected Income event[%d]: %v", index, err)
		}
		if !proto.Equal(&gotEntry, &wantEntry) {
			t.Fatalf("decoded Income event[%d] = %+v, want exact %+v", index, &gotEntry, &wantEntry)
		}
	}
}

func blockedWorkerIncomeRuntimeFrame(
	sessionID string,
	sequence int64,
	amount string,
) *cpv1.RuntimeFrame {
	entries := []*portfoliov1.VenueIncomeEntry{
		{
			IncomeEntryId: sequence - 1, SessionId: sessionID, VenueId: 71,
			IncomeType: "FUNDING_FEE", Source: "exchange", ExternalTransactionId: "income-external-1",
			SettlementKey: "funding-v1-1", Symbol: "BTCUSDT", Asset: "USDT",
			CalculatedAmountDecimal: "0.010000000000000002", ExchangeAmountDecimal: amount,
			AppliedAmountDecimal: amount, ReconciliationDeltaDecimal: "-0.000000000000000001",
			CalculationDetailsJson: `[{"quantity":"0.100000000000000001"}]`, Status: "confirmed",
			OccurredAt: timestamppb.New(time.UnixMilli(1_780_000_000_000)),
		},
		{
			IncomeEntryId: sequence, SessionId: sessionID, VenueId: 71,
			IncomeType: "FUNDING_FEE", Source: "backtest", SettlementKey: "funding-v1-2",
			Symbol: "ETHUSDT", Asset: "USDT", CalculatedAmountDecimal: amount,
			AppliedAmountDecimal: amount, ReconciliationDeltaDecimal: "0",
			CalculationDetailsJson: `[{"quantity":"-0.200000000000000001"}]`, Status: "calculated",
			OccurredAt: timestamppb.New(time.UnixMilli(1_780_000_060_000)),
		},
	}
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_INCOME_BATCH,
		Payload: &cpv1.RuntimeFrame_IncomeBatch{IncomeBatch: &cpv1.RuntimeIncomeBatch{
			SessionId: sessionID,
			StreamKey: "income/" + sessionID,
			Sequence:  sequence,
			Entries:   entries,
		}},
	}
}

type blockedWorkerControl struct {
	cpv1.UnimplementedControlPanelServiceServer

	strategyCode string
	connected    chan struct{}
	connectOnce  sync.Once
	outbound     chan *cpv1.RuntimeFrame
	results      chan *cpv1.RuntimeFrame
	heartbeats   chan time.Time
	dataACKs     chan *cpv1.RuntimeDataAck
	backpressure chan *cpv1.RuntimeDataBackpressure

	mu             sync.Mutex
	events         []string
	sessions       map[string]*portfoliov1.StrategySessionEntry
	indicatorSaves map[string][]*portfoliov1.SaveStrategyIndicatorsV2Request
	heartbeatACKs  int
	dataACKCount   int
}

func newBlockedWorkerControl(strategyCode string) *blockedWorkerControl {
	return &blockedWorkerControl{
		strategyCode:   strategyCode,
		connected:      make(chan struct{}),
		outbound:       make(chan *cpv1.RuntimeFrame, 16),
		results:        make(chan *cpv1.RuntimeFrame, 16),
		heartbeats:     make(chan time.Time, 1024),
		dataACKs:       make(chan *cpv1.RuntimeDataAck, 16),
		backpressure:   make(chan *cpv1.RuntimeDataBackpressure, 16),
		sessions:       map[string]*portfoliov1.StrategySessionEntry{},
		indicatorSaves: map[string][]*portfoliov1.SaveStrategyIndicatorsV2Request{},
	}
}

func (s *blockedWorkerControl) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HELLO {
		return fmt.Errorf("first runtime frame is not HELLO")
	}
	if err := stream.Send(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{
			HelloAck: &cpv1.RuntimeHelloAck{
				RuntimeId: blockedWorkerRuntimeID,
			},
		},
	}); err != nil {
		return err
	}
	s.connectOnce.Do(func() { close(s.connected) })

	inbound := make(chan *cpv1.RuntimeFrame)
	recvErr := make(chan error, 1)
	go func() {
		defer close(inbound)
		for {
			frame, receiveErr := stream.Recv()
			if receiveErr != nil {
				recvErr <- receiveErr
				return
			}
			inbound <- frame
		}
	}()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-recvErr:
			if errors.Is(err, io.EOF) || stream.Context().Err() != nil {
				return nil
			}
			return err
		case frame := <-s.outbound:
			if err := stream.Send(frame); err != nil {
				return err
			}
		case frame, ok := <-inbound:
			if !ok {
				return nil
			}
			switch frame.GetFrameType() {
			case cpv1.FrameType_FRAME_TYPE_HEARTBEAT:
				s.heartbeats <- time.Now()
				if err := stream.Send(&cpv1.RuntimeFrame{
					FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK,
					Payload: &cpv1.RuntimeFrame_HeartbeatAck{
						HeartbeatAck: &cpv1.RuntimeHeartbeatAck{
							RuntimeId: blockedWorkerRuntimeID,
						},
					},
				}); err != nil {
					return err
				}
				s.mu.Lock()
				s.heartbeatACKs++
				s.mu.Unlock()
			case cpv1.FrameType_FRAME_TYPE_REQUEST:
				if err := stream.Send(s.platformResponse(frame)); err != nil {
					return err
				}
			case cpv1.FrameType_FRAME_TYPE_RESPONSE,
				cpv1.FrameType_FRAME_TYPE_ERROR:
				s.results <- frame
			case cpv1.FrameType_FRAME_TYPE_DATA_ACK:
				ack := proto.Clone(frame.GetDataAck()).(*cpv1.RuntimeDataAck)
				s.mu.Lock()
				s.dataACKCount++
				s.mu.Unlock()
				s.dataACKs <- ack
			case cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE:
				s.backpressure <- proto.Clone(
					frame.GetDataBackpressure(),
				).(*cpv1.RuntimeDataBackpressure)
			}
		}
	}
}

func (s *blockedWorkerControl) send(frame *cpv1.RuntimeFrame) error {
	select {
	case s.outbound <- frame:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("fake RuntimeChannel outbound queue is blocked")
	}
}

func (s *blockedWorkerControl) waitForResult(
	t *testing.T,
	correlationID string,
	timeout time.Duration,
) *cpv1.RuntimeFrame {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case frame := <-s.results:
			if frame.GetCorrelationId() == correlationID {
				return frame
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for RuntimeChannel result %s", correlationID)
		}
	}
}

func (s *blockedWorkerControl) waitForDataBackpressure(
	t *testing.T,
	timeout time.Duration,
) *cpv1.RuntimeDataBackpressure {
	t.Helper()
	select {
	case backpressure := <-s.backpressure:
		return backpressure
	case <-time.After(timeout):
		t.Fatal("timed out waiting for Income DATA_BACKPRESSURE")
		return nil
	}
}

func (s *blockedWorkerControl) waitForDataACK(
	t *testing.T,
	timeout time.Duration,
) *cpv1.RuntimeDataAck {
	t.Helper()
	select {
	case ack := <-s.dataACKs:
		return ack
	case <-time.After(timeout):
		t.Fatal("timed out waiting for Income DATA_ACK")
		return nil
	}
}

func (s *blockedWorkerControl) assertNoRuntimeDataACK(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dataACKCount != 0 {
		t.Fatalf("Runtime DATA_ACK count = %d before explicit WorkerDataAck", s.dataACKCount)
	}
}

func (s *blockedWorkerControl) assertExactlyOneRuntimeDataACK(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dataACKCount != 1 {
		t.Fatalf("Runtime DATA_ACK count = %d, want exactly 1", s.dataACKCount)
	}
}

func (s *blockedWorkerControl) heartbeatACKCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatACKs
}

func (s *blockedWorkerControl) platformResponse(
	frame *cpv1.RuntimeFrame,
) *cpv1.RuntimeFrame {
	method := strings.TrimSpace(frame.GetRequest().GetMethod())
	switch method {
	case "portfolio.GetPortfolioSnapshot":
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.GetPortfolioSnapshotResponse{
				Snapshot: blockedWorkerPortfolioSnapshot(),
			},
		)
	case "portfolio.GetActiveStrategy":
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.GetActiveStrategyResponse{
				StrategyId: 17,
				Code:       s.strategyCode,
				Name:       "blocked-worker-acceptance",
				Version:    "1",
			},
		)
	case "portfolio.PreflightStrategySession":
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.PreflightStrategySessionResponse{Ok: true},
		)
	case "portfolio.CommitStrategySessionStart":
		var request portfoliov1.CommitStrategySessionStartRequest
		if err := frame.GetRequest().GetRequest().UnmarshalTo(&request); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
		}
		sessionRequest := request.GetSession()
		if sessionRequest == nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", "missing session")
		}
		s.mu.Lock()
		s.sessions[sessionRequest.GetSessionId()] = &portfoliov1.StrategySessionEntry{
			SessionId:         sessionRequest.GetSessionId(),
			PortfolioId:       sessionRequest.GetPortfolioId(),
			StrategyId:        sessionRequest.GetStrategyId(),
			UserId:            6,
			RuntimeId:         sessionRequest.GetRuntimeId(),
			RuntimeSource:     sessionRequest.GetRuntimeSource(),
			RuntimeName:       sessionRequest.GetRuntimeName(),
			Environment:       sessionRequest.GetEnvironment(),
			Status:            "pending",
			Interval:          sessionRequest.GetInterval(),
			StartTimeMs:       sessionRequest.GetStartTimeMs(),
			EndTimeMs:         sessionRequest.GetEndTimeMs(),
			LaunchOperationId: request.GetLaunchOperationId(),
		}
		s.events = append(s.events, "CommitStrategySessionStart:"+sessionRequest.GetSessionId())
		s.mu.Unlock()
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.CommitStrategySessionStartResponse{Ok: true},
		)
	case "portfolio.GetSession":
		var request portfoliov1.GetSessionRequest
		if err := frame.GetRequest().GetRequest().UnmarshalTo(&request); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
		}
		s.mu.Lock()
		session := proto.Clone(s.sessions[request.GetSessionId()])
		s.mu.Unlock()
		if session == nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "NotFound", "session not found")
		}
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.GetSessionResponse{
				Session: session.(*portfoliov1.StrategySessionEntry),
			},
		)
	case "portfolio.UpdateSession":
		var request portfoliov1.UpdateSessionRequest
		if err := frame.GetRequest().GetRequest().UnmarshalTo(&request); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
		}
		s.mu.Lock()
		session := s.sessions[request.GetSessionId()]
		if session != nil && request.GetExpectedStatus() != "" &&
			session.GetStatus() != request.GetExpectedStatus() {
			s.mu.Unlock()
			return runtimeErrorFrame(
				frame.GetCorrelationId(),
				"NotFound",
				"session status changed",
			)
		}
		if session != nil {
			session.Status = request.GetStatus()
			session.BarsProcessed = request.GetBarsProcessed()
			session.Error = request.GetError()
			if request.IndicatorFinalizationPending != nil {
				session.IndicatorFinalizationPending =
					request.GetIndicatorFinalizationPending()
			}
		}
		s.events = append(
			s.events,
			"UpdateSession:"+request.GetSessionId()+":"+request.GetStatus(),
		)
		s.mu.Unlock()
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.UpdateSessionResponse{},
		)
	case "portfolio.UpdatePortfolioWalletState":
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.UpdatePortfolioWalletStateResponse{
				Wallet: blockedWorkerPortfolioSnapshot().GetWallet(),
			},
		)
	case "portfolio.SaveStrategyIndicatorsV2":
		var request portfoliov1.SaveStrategyIndicatorsV2Request
		if err := frame.GetRequest().GetRequest().UnmarshalTo(&request); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
		}
		s.mu.Lock()
		s.indicatorSaves[request.GetSessionId()] = append(
			s.indicatorSaves[request.GetSessionId()],
			proto.Clone(&request).(*portfoliov1.SaveStrategyIndicatorsV2Request),
		)
		s.events = append(s.events, "SaveIndicatorsV2:"+request.GetSessionId())
		s.mu.Unlock()
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.SaveStrategyIndicatorsV2Response{
				DefinitionsSaved: int32(len(request.GetDefinitions())),
				ChunksSaved:      int32(len(request.GetChunks())),
			},
		)
	case "portfolio.FinalizeStrategyIndicatorChunksV2":
		var request portfoliov1.FinalizeStrategyIndicatorChunksV2Request
		if err := frame.GetRequest().GetRequest().UnmarshalTo(&request); err != nil {
			return runtimeErrorFrame(frame.GetCorrelationId(), "InvalidArgument", err.Error())
		}
		s.mu.Lock()
		s.events = append(s.events, "FinalizeIndicatorsV2:"+request.GetSessionId())
		s.mu.Unlock()
		return responseFrame(
			frame.GetCorrelationId(),
			&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
				ChunksFinalized: int32(len(request.GetChunks())),
			},
		)
	case "marketdata.FetchKlines":
		return responseFrame(frame.GetCorrelationId(), blockedWorkerKlines(1))
	case "marketdata.FetchBacktestPage":
		return responseFrame(frame.GetCorrelationId(), blockedWorkerKlines(2))
	case "order.ListOrderLifecycleEvents":
		return responseAnyFrame(frame.GetCorrelationId(), &anypb.Any{
			TypeUrl: "type.googleapis.com/order.v1.ListOrderLifecycleEventsResponse",
			Value:   nil,
		})
	default:
		return runtimeErrorFrame(
			frame.GetCorrelationId(),
			"Unimplemented",
			"unexpected blocked-worker platform method: "+method,
		)
	}
}

func blockedWorkerPortfolioSnapshot() *portfoliov1.PortfolioSnapshot {
	wallet := &portfoliov1.PortfolioWalletState{
		Environment: 0,
		TotalValue:  1000,
		Futures: &portfoliov1.FuturesWallet{
			MarginMode:              "cross",
			PositionMode:            "one_way",
			InitialBalance:          1000,
			WalletBalance:           1000,
			AvailableBalance:        1000,
			TotalMarginBalance:      1000,
			TotalCrossWalletBalance: 1000,
			MarginBalance:           1000,
		},
	}
	return &portfoliov1.PortfolioSnapshot{
		PortfolioId:      7,
		UserId:           6,
		TotalValue:       1000,
		WalletBalance:    1000,
		AvailableBalance: 1000,
		Wallet:           wallet,
		Venues: []*portfoliov1.VenueSnapshot{{
			VenueId:          71,
			Exchange:         1,
			Environment:      0,
			Market:           2,
			TotalValue:       1000,
			WalletBalance:    1000,
			AvailableBalance: 1000,
			Wallet:           wallet,
		}},
	}
}

func blockedWorkerKlines(count int) *structpb.Struct {
	klines := make([]any, 0, count)
	for index := 0; index < count; index++ {
		openTime := int64(1_780_000_000_000 + index*60_000)
		klines = append(klines, map[string]any{
			"symbol":     "BTCUSDT",
			"interval":   "1m",
			"market":     "futures",
			"open_time":  float64(openTime),
			"close_time": float64(openTime + 59_999),
			"timestamp":  float64(openTime + 59_999),
			"open":       100.0 + float64(index),
			"high":       101.0 + float64(index),
			"low":        99.0 + float64(index),
			"close":      100.5 + float64(index),
			"volume":     10.0,
		})
	}
	value, err := structpb.NewStruct(map[string]any{
		"stream_key":          "binance/futures/kline/BTCUSDT/1m",
		"klines":              klines,
		"next_cursor_time_ms": float64(1_780_000_060_000),
		"has_more":            false,
	})
	if err != nil {
		panic(err)
	}
	return value
}

func (s *blockedWorkerControl) assertFinalizedBeforeRecoverable(
	t *testing.T,
	sessionID string,
) {
	t.Helper()
	s.mu.Lock()
	events := append([]string(nil), s.events...)
	s.mu.Unlock()
	saveIndex := -1
	finalizeIndex := -1
	recoverableIndex := -1
	for index, event := range events {
		switch event {
		case "SaveIndicatorsV2:" + sessionID:
			saveIndex = index
		case "FinalizeIndicatorsV2:" + sessionID:
			finalizeIndex = index
		case "UpdateSession:" + sessionID + ":recoverable":
			recoverableIndex = index
		}
	}
	if saveIndex < 0 || finalizeIndex < 0 || recoverableIndex < 0 ||
		!(saveIndex < finalizeIndex && finalizeIndex < recoverableIndex) {
		t.Fatalf(
			"old session terminal order = %v; want SaveV2 < FinalizeV2 < recoverable",
			events,
		)
	}
}

func (s *blockedWorkerControl) assertOneFinalizedIndicatorPoint(
	t *testing.T,
	sessionID string,
) {
	t.Helper()
	s.mu.Lock()
	saves := append(
		[]*portfoliov1.SaveStrategyIndicatorsV2Request(nil),
		s.indicatorSaves[sessionID]...,
	)
	s.mu.Unlock()
	if len(saves) != 1 || len(saves[0].GetChunks()) != 1 {
		t.Fatalf("indicator saves for %s = %+v", sessionID, saves)
	}
	chunk := saves[0].GetChunks()[0]
	if chunk.GetCount() != 1 ||
		len(chunk.GetTimesMs()) != 1 ||
		len(chunk.GetScalarValues()) != 1 {
		t.Fatalf("finalized indicator tail = %+v, want one completed point", chunk)
	}
}
