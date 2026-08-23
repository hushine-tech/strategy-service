package runtimeagent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func newIndicatorSyncManager(platform PlatformInvoker, limit int) *IndicatorSyncManager {
	return NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: platform,
		IndicatorLimit:  limit,
		FlushInterval:   time.Hour,
		RequestTimeout:  time.Second,
		FinalizeTimeout: time.Second,
		RetryInitial:    time.Millisecond,
		RetryMax:        2 * time.Millisecond,
	})
}

func indicatorSyncFrameV2(
	sessionID string,
	streamKey string,
	sequence uint64,
	timeMS int64,
) *rwv1.IndicatorFrameV2 {
	frame := &rwv1.IndicatorFrameV2{
		SessionId:      sessionID,
		UserId:         6,
		StrategyId:     12,
		StreamKey:      streamKey,
		StreamSequence: sequence,
		MarketTimeMs:   timeMS,
		IntervalMs:     60_000,
		Samples: []*rwv1.IndicatorSampleV2{{
			IndicatorKey: "alpha",
			ScalarValue:  proto.Float64(1),
		}},
	}
	if sequence == 0 {
		frame.Definitions = []*rwv1.IndicatorDefinition{{
			IndicatorKey: "alpha",
			Name:         "Alpha",
			Type:         "line",
			Pane:         "strategy",
		}}
	}
	return frame
}

func receiveIndicatorV2(
	manager *IndicatorSyncManager,
	frame *rwv1.IndicatorFrameV2,
) error {
	return manager.ReceiveFrameV2(
		WorkerIdentity{
			SessionID:  frame.GetSessionId(),
			PID:        123,
			Generation: 7,
			token:      "worker-token",
		},
		frame,
	)
}

func TestIndicatorSyncV2AcceptsExactDuplicateOnlyAndPreservesClock(t *testing.T) {
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
	first := indicatorSyncFrameV2(
		"sess-v2",
		"binance:spot:BTCUSDT:1m",
		0,
		1_000,
	)
	if err := receiveIndicatorV2(manager, first); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	if err := receiveIndicatorV2(manager, proto.Clone(first).(*rwv1.IndicatorFrameV2)); err != nil {
		t.Fatalf("exact duplicate: %v", err)
	}

	state := manager.lookupSession("sess-v2")
	state.mu.Lock()
	stream := state.streamsV2[first.GetStreamKey()]
	gotNext := stream.clock.NextSequence
	gotCount := stream.series["alpha"].buffer.SnapshotDirtyForFlush().Chunks[0].Count
	state.mu.Unlock()
	if gotNext != 1 || gotCount != 1 {
		t.Fatalf("next sequence=%d count=%d, want 1/1", gotNext, gotCount)
	}

	conflict := proto.Clone(first).(*rwv1.IndicatorFrameV2)
	conflict.Samples[0].ScalarValue = proto.Float64(2)
	err := receiveIndicatorV2(manager, conflict)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) ||
		protocolErr.Code() != "RUNTIME_INDICATOR_PROTOCOL_ERROR" ||
		protocolErr.Sequence != 0 {
		t.Fatalf("conflicting duplicate error = %v", err)
	}

	state.mu.Lock()
	gotNext = stream.clock.NextSequence
	gotCount = stream.series["alpha"].buffer.SnapshotDirtyForFlush().Chunks[0].Count
	state.mu.Unlock()
	if gotNext != 1 || gotCount != 1 {
		t.Fatalf("rejected duplicate mutated state: next=%d count=%d", gotNext, gotCount)
	}
}

func TestIndicatorSyncV2RejectsFrameWithoutPartiallyAdvancingSeries(t *testing.T) {
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
	const (
		sessionID = "sess-v2-atomic"
		streamKey = "binance:spot:BTCUSDT:1m"
	)
	first := &rwv1.IndicatorFrameV2{
		SessionId:      sessionID,
		UserId:         6,
		StrategyId:     12,
		StreamKey:      streamKey,
		StreamSequence: 0,
		MarketTimeMs:   1_000,
		IntervalMs:     60_000,
		Definitions: []*rwv1.IndicatorDefinition{
			{IndicatorKey: "alpha", Type: "line", Pane: "strategy"},
			{IndicatorKey: "beta", Type: "line", Pane: "strategy"},
		},
		Samples: []*rwv1.IndicatorSampleV2{
			{IndicatorKey: "alpha", ScalarValue: proto.Float64(1)},
			{IndicatorKey: "beta", ScalarValue: proto.Float64(10)},
		},
	}
	if err := receiveIndicatorV2(manager, first); err != nil {
		t.Fatalf("first frame: %v", err)
	}

	state := manager.lookupSession(sessionID)
	state.mu.Lock()
	stream := state.streamsV2[streamKey]
	if err := stream.series["beta"].buffer.Append(
		1,
		61_000,
		60_000,
		proto.Float64(99),
		nil,
	); err != nil {
		state.mu.Unlock()
		t.Fatalf("desynchronize beta buffer: %v", err)
	}
	state.mu.Unlock()

	next := &rwv1.IndicatorFrameV2{
		SessionId:      sessionID,
		UserId:         6,
		StrategyId:     12,
		StreamKey:      streamKey,
		StreamSequence: 1,
		MarketTimeMs:   61_000,
		IntervalMs:     60_000,
		Samples: []*rwv1.IndicatorSampleV2{
			{IndicatorKey: "alpha", ScalarValue: proto.Float64(2)},
			{IndicatorKey: "beta", ScalarValue: proto.Float64(20)},
		},
	}
	err := receiveIndicatorV2(manager, next)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("ReceiveFrameV2 error = %v, want IndicatorProtocolError", err)
	}

	state.mu.Lock()
	alphaSnapshot := stream.series["alpha"].buffer.SnapshotDirtyForFlush()
	gotNextSequence := stream.clock.NextSequence
	state.mu.Unlock()
	if len(alphaSnapshot.Chunks) != 1 || alphaSnapshot.Chunks[0].Count != 1 {
		t.Fatalf(
			"rejected frame partially advanced alpha: %+v",
			alphaSnapshot.Chunks,
		)
	}
	if gotNextSequence != 1 {
		t.Fatalf("rejected frame advanced stream clock to %d, want 1", gotNextSequence)
	}
}

func TestIndicatorSyncV2RejectsClockAndDefinitionViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rwv1.IndicatorFrameV2)
		reason string
	}{
		{
			name: "duplicate time mismatch",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.StreamSequence = 0
				frame.MarketTimeMs = 1_001
				frame.Definitions = indicatorSyncFrameV2("", "", 0, 1).Definitions
			},
			reason: "lower sequence",
		},
		{
			name: "sequence gap",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.StreamSequence = 2
				frame.MarketTimeMs = 3_000
			},
			reason: "sequence gap",
		},
		{
			name: "equal time",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.MarketTimeMs = 1_000
			},
			reason: "strictly increase",
		},
		{
			name: "time rollback",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.MarketTimeMs = 999
			},
			reason: "strictly increase",
		},
		{
			name: "interval changes",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.IntervalMs = 300_000
			},
			reason: "interval",
		},
		{
			name: "definition changes",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Definitions = []*rwv1.IndicatorDefinition{{
					IndicatorKey: "alpha",
					Name:         "Alpha",
					Type:         "line",
					Pane:         "price",
				}}
			},
			reason: "definitions changed",
		},
		{
			name: "unknown sample",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Samples = []*rwv1.IndicatorSampleV2{{
					IndicatorKey: "unknown",
					ScalarValue:  proto.Float64(1),
				}}
			},
			reason: "unknown indicator",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
			first := indicatorSyncFrameV2(
				"sess-v2",
				"binance:perpetual_futures:BTCUSDT:1m",
				0,
				1_000,
			)
			if err := receiveIndicatorV2(manager, first); err != nil {
				t.Fatal(err)
			}
			next := indicatorSyncFrameV2(
				"sess-v2",
				first.GetStreamKey(),
				1,
				9_000,
			)
			test.mutate(next)
			err := receiveIndicatorV2(manager, next)
			var protocolErr *IndicatorProtocolError
			if !errors.As(err, &protocolErr) ||
				!strings.Contains(protocolErr.Reason, test.reason) {
				t.Fatalf("error = %v, want protocol reason containing %q", err, test.reason)
			}
		})
	}
}

func TestIndicatorSyncV2RejectedFirstFrameDoesNotCreateSessionState(t *testing.T) {
	manager := newIndicatorSyncManager(&indicatorSyncPlatformV2{}, 1024)
	frame := indicatorSyncFrameV2(
		"sess-invalid-first",
		"binance:spot:BTCUSDT:1m",
		0,
		1_000,
	)
	frame.Definitions[0].Type = "unsupported"

	err := receiveIndicatorV2(manager, frame)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("ReceiveFrameV2 error = %v, want IndicatorProtocolError", err)
	}
	if state := manager.lookupSession("sess-invalid-first"); state != nil {
		t.Fatalf("rejected first frame created session state: %#v", state)
	}
}

func TestIndicatorSyncV2CheckpointRestoresExactTailWithoutWorkerToken(t *testing.T) {
	const (
		sessionID = "sess-v2-checkpoint"
		streamKey = "binance:spot:BTCUSDT:1m"
	)
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
	frame := indicatorSyncFrameV2(sessionID, streamKey, 0, 1_000)
	if err := receiveIndicatorV2(manager, frame); err != nil {
		t.Fatalf("receive frame: %v", err)
	}

	checkpoint, err := manager.CheckpointSessionV2(sessionID)
	if err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	if strings.Contains(string(raw), "worker-token") {
		t.Fatalf("checkpoint persisted opaque worker token: %s", raw)
	}

	platform := &indicatorSyncPlatformV2{}
	restored := NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
	})
	if err := restored.RestoreSessionV2(checkpoint); err != nil {
		t.Fatalf("restore session: %v", err)
	}
	if err := restored.FinalizeSession(context.Background(), sessionID); err != nil {
		t.Fatalf("finalize restored session: %v", err)
	}
	platform.mu.Lock()
	defer platform.mu.Unlock()
	if len(platform.saves) != 1 ||
		len(platform.saves[0].GetChunks()) != 1 ||
		platform.saves[0].GetChunks()[0].GetCount() != 1 {
		t.Fatalf("restored saves = %+v", platform.saves)
	}
	if len(platform.finalizations) != 1 ||
		platform.finalizations[0].GetChunks()[0].GetExpectedRevision() != 1 {
		t.Fatalf("restored finalizations = %+v", platform.finalizations)
	}
}

func TestIndicatorSyncV2RejectsDefinitionsCoreCannotPersist(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rwv1.IndicatorDefinition)
		reason string
	}{
		{
			name: "empty pane",
			mutate: func(definition *rwv1.IndicatorDefinition) {
				definition.Pane = ""
			},
			reason: "pane",
		},
		{
			name: "invalid config JSON",
			mutate: func(definition *rwv1.IndicatorDefinition) {
				definition.ConfigJson = `{"threshold":NaN}`
			},
			reason: "config_json",
		},
		{
			name: "non-object config JSON",
			mutate: func(definition *rwv1.IndicatorDefinition) {
				definition.ConfigJson = `[]`
			},
			reason: "config_json",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
			frame := indicatorSyncFrameV2(
				"sess-invalid-definition",
				"binance:spot:BTCUSDT:1m",
				0,
				1_000,
			)
			test.mutate(frame.Definitions[0])

			err := receiveIndicatorV2(manager, frame)

			var protocolErr *IndicatorProtocolError
			if !errors.As(err, &protocolErr) ||
				!strings.Contains(protocolErr.Reason, test.reason) {
				t.Fatalf(
					"error = %v, want protocol reason containing %q",
					err,
					test.reason,
				)
			}
			if state := manager.lookupSession(frame.GetSessionId()); state != nil {
				t.Fatalf("invalid definition created state: %#v", state)
			}
		})
	}
}

func TestIndicatorSyncV2RejectsNonFiniteSamplesBeforeCreatingState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rwv1.IndicatorFrameV2)
		reason string
	}{
		{
			name: "scalar NaN",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Samples[0].ScalarValue = proto.Float64(math.NaN())
			},
			reason: "finite",
		},
		{
			name: "marker price infinity",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Definitions[0].Type = "marker"
				frame.Samples[0].ScalarValue = nil
				frame.Samples[0].Markers = []*rwv1.IndicatorMarkerV2{{
					Text:  "BUY",
					Price: proto.Float64(math.Inf(1)),
				}}
			},
			reason: "finite",
		},
		{
			name: "nil marker",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Definitions[0].Type = "marker"
				frame.Samples[0].ScalarValue = nil
				frame.Samples[0].Markers = []*rwv1.IndicatorMarkerV2{nil}
			},
			reason: "nil",
		},
		{
			name: "invalid marker position",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Definitions[0].Type = "marker"
				frame.Samples[0].ScalarValue = nil
				frame.Samples[0].Markers = []*rwv1.IndicatorMarkerV2{{
					Position: "middle",
				}}
			},
			reason: "position",
		},
		{
			name: "invalid marker shape",
			mutate: func(frame *rwv1.IndicatorFrameV2) {
				frame.Definitions[0].Type = "marker"
				frame.Samples[0].ScalarValue = nil
				frame.Samples[0].Markers = []*rwv1.IndicatorMarkerV2{{
					Shape: "triangle",
				}}
			},
			reason: "shape",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
			frame := indicatorSyncFrameV2(
				"sess-non-finite-first",
				"binance:spot:BTCUSDT:1m",
				0,
				1_000,
			)
			test.mutate(frame)

			err := receiveIndicatorV2(manager, frame)

			var protocolErr *IndicatorProtocolError
			if !errors.As(err, &protocolErr) ||
				!strings.Contains(protocolErr.Reason, test.reason) {
				t.Fatalf(
					"error = %v, want protocol reason containing %q",
					err,
					test.reason,
				)
			}
			if state := manager.lookupSession(frame.GetSessionId()); state != nil {
				t.Fatalf("non-finite first frame created state: %#v", state)
			}
		})
	}
}

func TestIndicatorSyncV2KeepsStreamClocksIndependentAndAllowsTimeGaps(t *testing.T) {
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{})
	streams := []string{
		"binance:spot:BTCUSDT:1m",
		"binance:perpetual_futures:BTCUSDT:1m",
		"binance:spot:ETHUSDT:5m",
	}
	for _, streamKey := range streams {
		first := indicatorSyncFrameV2("sess-v2", streamKey, 0, 1_000)
		if strings.HasSuffix(streamKey, ":5m") {
			first.IntervalMs = 300_000
		}
		if err := receiveIndicatorV2(
			manager,
			first,
		); err != nil {
			t.Fatalf("first %s: %v", streamKey, err)
		}
	}
	for _, streamKey := range streams {
		next := indicatorSyncFrameV2("sess-v2", streamKey, 1, 900_000)
		if strings.HasSuffix(streamKey, ":5m") {
			next.IntervalMs = 300_000
		}
		if err := receiveIndicatorV2(manager, next); err != nil {
			t.Fatalf("second %s: %v", streamKey, err)
		}
	}

	state := manager.lookupSession("sess-v2")
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, streamKey := range streams {
		if got := state.streamsV2[streamKey].clock.NextSequence; got != 2 {
			t.Fatalf("stream %s next sequence = %d, want 2", streamKey, got)
		}
	}
}

type indicatorSyncPlatformV2 struct {
	mu            sync.Mutex
	methods       []string
	saves         []*portfoliov1.SaveStrategyIndicatorsV2Request
	finalizations []*portfoliov1.FinalizeStrategyIndicatorChunksV2Request
}

func (p *indicatorSyncPlatformV2) InvokePlatformAny(
	_ context.Context,
	method string,
	packed *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.methods = append(p.methods, method)
	switch method {
	case "portfolio.SaveStrategyIndicatorsV2":
		var request portfoliov1.SaveStrategyIndicatorsV2Request
		if err := packed.UnmarshalTo(&request); err != nil {
			return nil, err
		}
		p.saves = append(
			p.saves,
			proto.Clone(&request).(*portfoliov1.SaveStrategyIndicatorsV2Request),
		)
		return anypb.New(&portfoliov1.SaveStrategyIndicatorsV2Response{
			DefinitionsSaved: int32(len(request.GetDefinitions())),
			ChunksSaved:      int32(len(request.GetChunks())),
		})
	case "portfolio.FinalizeStrategyIndicatorChunksV2":
		var request portfoliov1.FinalizeStrategyIndicatorChunksV2Request
		if err := packed.UnmarshalTo(&request); err != nil {
			return nil, err
		}
		p.finalizations = append(
			p.finalizations,
			proto.Clone(&request).(*portfoliov1.FinalizeStrategyIndicatorChunksV2Request),
		)
		return anypb.New(
			&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
				ChunksFinalized: int32(len(request.GetChunks())),
			},
		)
	default:
		return nil, errors.New("unexpected method: " + method)
	}
}

func TestIndicatorSyncV2Persists1023ThenTwoBarsAsFinalAndNewOpen(t *testing.T) {
	platform := &indicatorSyncPlatformV2{}
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
		FinalizeTimeout: time.Second,
	})
	const (
		sessionID = "sess-v2-persist"
		streamKey = "binance:spot:BTCUSDT:1m"
	)
	for sequence := uint64(0); sequence < 1023; sequence++ {
		frame := indicatorSyncFrameV2(
			sessionID,
			streamKey,
			sequence,
			int64(sequence+1)*60_000,
		)
		if err := receiveIndicatorV2(manager, frame); err != nil {
			t.Fatalf("receive %d: %v", sequence, err)
		}
	}
	if err := manager.FlushSession(context.Background(), sessionID, false); err != nil {
		t.Fatalf("flush 1023: %v", err)
	}
	if len(platform.saves) != 1 ||
		len(platform.saves[0].GetChunks()) != 1 ||
		platform.saves[0].GetChunks()[0].GetCount() != 1023 ||
		platform.saves[0].GetChunks()[0].GetFinalized() {
		t.Fatalf("first saves = %+v", platform.saves)
	}
	if len(platform.finalizations) != 0 {
		t.Fatalf("premature finalization = %+v", platform.finalizations)
	}

	for sequence := uint64(1023); sequence < 1025; sequence++ {
		frame := indicatorSyncFrameV2(
			sessionID,
			streamKey,
			sequence,
			int64(sequence+1)*60_000,
		)
		if err := receiveIndicatorV2(manager, frame); err != nil {
			t.Fatalf("receive %d: %v", sequence, err)
		}
	}
	if err := manager.FlushSession(context.Background(), sessionID, false); err != nil {
		t.Fatalf("flush 1025: %v", err)
	}
	if len(platform.saves) != 2 {
		t.Fatalf("save calls = %d, want 2", len(platform.saves))
	}
	chunks := platform.saves[1].GetChunks()
	if len(chunks) != 2 ||
		chunks[0].GetChunkIndex() != 0 ||
		chunks[0].GetCount() != 1024 ||
		chunks[0].GetFinalized() ||
		chunks[1].GetChunkIndex() != 1 ||
		chunks[1].GetCount() != 1 ||
		chunks[1].GetFinalized() {
		t.Fatalf("boundary chunks = %+v", chunks)
	}
	if len(platform.finalizations) != 1 ||
		len(platform.finalizations[0].GetChunks()) != 1 ||
		platform.finalizations[0].GetChunks()[0].GetChunkIndex() != 0 ||
		platform.finalizations[0].GetChunks()[0].GetExpectedRevision() != 1024 {
		t.Fatalf("boundary finalizations = %+v", platform.finalizations)
	}

	if err := manager.FlushSession(context.Background(), sessionID, false); err != nil {
		t.Fatal(err)
	}
	if len(platform.saves) != 2 || len(platform.finalizations) != 1 {
		t.Fatalf(
			"clean flush made calls: saves=%d finalizations=%d",
			len(platform.saves),
			len(platform.finalizations),
		)
	}
}

type blockingIndicatorSyncPlatformV2 struct {
	indicatorSyncPlatformV2
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingIndicatorSyncPlatformV2) InvokePlatformAny(
	ctx context.Context,
	method string,
	packed *anypb.Any,
	timeout time.Duration,
) (*anypb.Any, error) {
	if method == "portfolio.SaveStrategyIndicatorsV2" {
		block := false
		p.once.Do(func() {
			block = true
			close(p.started)
		})
		if block {
			select {
			case <-p.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return p.indicatorSyncPlatformV2.InvokePlatformAny(
		ctx,
		method,
		packed,
		timeout,
	)
}

func TestIndicatorSyncV2Old1023AckCannotClearAdvancedBoundary(t *testing.T) {
	platform := &blockingIndicatorSyncPlatformV2{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: platform,
		RequestTimeout:  time.Second,
	})
	const (
		sessionID = "sess-v2-race"
		streamKey = "binance:perpetual_futures:BTCUSDT:1m"
	)
	for sequence := uint64(0); sequence < 1023; sequence++ {
		if err := receiveIndicatorV2(
			manager,
			indicatorSyncFrameV2(
				sessionID,
				streamKey,
				sequence,
				int64(sequence+1)*60_000,
			),
		); err != nil {
			t.Fatal(err)
		}
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- manager.FlushSession(
			context.Background(),
			sessionID,
			false,
		)
	}()
	select {
	case <-platform.started:
	case <-time.After(time.Second):
		t.Fatal("1023 save did not block")
	}
	for sequence := uint64(1023); sequence < 1025; sequence++ {
		if err := receiveIndicatorV2(
			manager,
			indicatorSyncFrameV2(
				sessionID,
				streamKey,
				sequence,
				int64(sequence+1)*60_000,
			),
		); err != nil {
			t.Fatal(err)
		}
	}
	close(platform.release)
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if err := manager.FlushSession(
		context.Background(),
		sessionID,
		false,
	); err != nil {
		t.Fatal(err)
	}

	platform.mu.Lock()
	defer platform.mu.Unlock()
	if len(platform.saves) != 2 {
		t.Fatalf("save calls = %d, want 2", len(platform.saves))
	}
	chunks := platform.saves[1].GetChunks()
	if len(chunks) != 2 ||
		chunks[0].GetCount() != 1024 ||
		chunks[1].GetCount() != 1 {
		t.Fatalf("post-race chunks = %+v", chunks)
	}
	if len(platform.finalizations) != 1 ||
		platform.finalizations[0].GetChunks()[0].GetExpectedRevision() != 1024 {
		t.Fatalf("post-race finalizations = %+v", platform.finalizations)
	}
}
