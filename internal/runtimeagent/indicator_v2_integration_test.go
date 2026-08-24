package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type indicatorV2DurablePlatform struct {
	mu sync.Mutex

	definitions map[string]*portfoliov1.StrategyIndicatorDefinitionV2
	chunks      map[string]*portfoliov1.StrategyIndicatorChunkV2
	saveCalls   []*portfoliov1.SaveStrategyIndicatorsV2Request
	finalCalls  []*portfoliov1.FinalizeStrategyIndicatorChunksV2Request

	blockFirstSave bool
	firstSaveOnce  sync.Once
	firstSaveStart chan struct{}
	firstSaveAllow chan struct{}
}

func newIndicatorV2DurablePlatform(blockFirstSave bool) *indicatorV2DurablePlatform {
	return &indicatorV2DurablePlatform{
		definitions:    map[string]*portfoliov1.StrategyIndicatorDefinitionV2{},
		chunks:         map[string]*portfoliov1.StrategyIndicatorChunkV2{},
		blockFirstSave: blockFirstSave,
		firstSaveStart: make(chan struct{}),
		firstSaveAllow: make(chan struct{}),
	}
}

func (p *indicatorV2DurablePlatform) InvokePlatformAny(
	ctx context.Context,
	method string,
	payload *anypb.Any,
	_ time.Duration,
) (*anypb.Any, error) {
	switch method {
	case "portfolio.SaveStrategyIndicatorsV2":
		var request portfoliov1.SaveStrategyIndicatorsV2Request
		if err := payload.UnmarshalTo(&request); err != nil {
			return nil, err
		}
		block := false
		if p.blockFirstSave {
			p.firstSaveOnce.Do(func() {
				block = true
				close(p.firstSaveStart)
			})
		}
		if block {
			select {
			case <-p.firstSaveAllow:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if err := p.save(&request); err != nil {
			return nil, err
		}
		return anypb.New(&portfoliov1.SaveStrategyIndicatorsV2Response{
			DefinitionsSaved: int32(len(request.GetDefinitions())),
			ChunksSaved:      int32(len(request.GetChunks())),
		})
	case "portfolio.FinalizeStrategyIndicatorChunksV2":
		var request portfoliov1.FinalizeStrategyIndicatorChunksV2Request
		if err := payload.UnmarshalTo(&request); err != nil {
			return nil, err
		}
		if err := p.finalize(&request); err != nil {
			return nil, err
		}
		return anypb.New(
			&portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
				ChunksFinalized: int32(len(request.GetChunks())),
			},
		)
	default:
		return nil, fmt.Errorf("unexpected platform method %q", method)
	}
}

func (p *indicatorV2DurablePlatform) save(
	request *portfoliov1.SaveStrategyIndicatorsV2Request,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saveCalls = append(
		p.saveCalls,
		proto.Clone(request).(*portfoliov1.SaveStrategyIndicatorsV2Request),
	)
	for _, definition := range request.GetDefinitions() {
		key := indicatorV2DefinitionTestKey(
			definition.GetSessionId(),
			definition.GetStreamKey(),
			definition.GetIndicatorKey(),
		)
		existing := p.definitions[key]
		if existing != nil && !proto.Equal(existing, definition) {
			return fmt.Errorf("definition changed for %s", key)
		}
		p.definitions[key] = proto.Clone(definition).(*portfoliov1.StrategyIndicatorDefinitionV2)
	}
	for _, chunk := range request.GetChunks() {
		key := indicatorV2ChunkTestKey(chunk)
		existing := p.chunks[key]
		if existing != nil {
			if existing.GetFinalized() {
				return fmt.Errorf("finalized chunk changed for %s", key)
			}
			if chunk.GetRevision() < existing.GetRevision() {
				return fmt.Errorf("stale revision for %s", key)
			}
			if chunk.GetRevision() == existing.GetRevision() {
				if !proto.Equal(existing, chunk) {
					return fmt.Errorf("conflicting revision for %s", key)
				}
				continue
			}
		}
		p.chunks[key] = proto.Clone(chunk).(*portfoliov1.StrategyIndicatorChunkV2)
	}
	return nil
}

func (p *indicatorV2DurablePlatform) finalize(
	request *portfoliov1.FinalizeStrategyIndicatorChunksV2Request,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.finalCalls = append(
		p.finalCalls,
		proto.Clone(request).(*portfoliov1.FinalizeStrategyIndicatorChunksV2Request),
	)
	for _, finalization := range request.GetChunks() {
		key := indicatorV2DefinitionTestKey(
			request.GetSessionId(),
			finalization.GetStreamKey(),
			finalization.GetIndicatorKey(),
		) + fmt.Sprintf("\x00%d", finalization.GetChunkIndex())
		chunk := p.chunks[key]
		if chunk == nil {
			return fmt.Errorf("missing chunk for finalization %s", key)
		}
		if chunk.GetRevision() != finalization.GetExpectedRevision() {
			return fmt.Errorf("revision mismatch for finalization %s", key)
		}
		chunk.Finalized = true
	}
	return nil
}

func (p *indicatorV2DurablePlatform) chunk(
	sessionID string,
	streamKey string,
	indicatorKey string,
	chunkIndex uint32,
) *portfoliov1.StrategyIndicatorChunkV2 {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := indicatorV2DefinitionTestKey(
		sessionID,
		streamKey,
		indicatorKey,
	) + fmt.Sprintf("\x00%d", chunkIndex)
	if p.chunks[key] == nil {
		return nil
	}
	return proto.Clone(p.chunks[key]).(*portfoliov1.StrategyIndicatorChunkV2)
}

func (p *indicatorV2DurablePlatform) callCounts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.saveCalls), len(p.finalCalls)
}

func indicatorV2DefinitionTestKey(
	sessionID string,
	streamKey string,
	indicatorKey string,
) string {
	return sessionID + "\x00" + streamKey + "\x00" + indicatorKey
}

func indicatorV2ChunkTestKey(
	chunk *portfoliov1.StrategyIndicatorChunkV2,
) string {
	return indicatorV2DefinitionTestKey(
		chunk.GetSessionId(),
		chunk.GetStreamKey(),
		chunk.GetIndicatorKey(),
	) + fmt.Sprintf("\x00%d", chunk.GetChunkIndex())
}

func indicatorV2IntegrationFrame(
	sessionID string,
	streamKey string,
	sequence uint64,
) *rwv1.IndicatorFrameV2 {
	frame := &rwv1.IndicatorFrameV2{
		SessionId:      sessionID,
		UserId:         6,
		StrategyId:     12,
		StreamKey:      streamKey,
		StreamSequence: sequence,
		MarketTimeMs:   int64(sequence+1) * 60_000,
		IntervalMs:     60_000,
	}
	if sequence == 0 {
		frame.Definitions = []*rwv1.IndicatorDefinition{
			{
				IndicatorKey: "alpha",
				Name:         "Alpha",
				Type:         "line",
				Pane:         "strategy",
				ConfigJson:   "{}",
			},
			{
				IndicatorKey: "trades",
				Name:         "Trades",
				Type:         "marker",
				Pane:         "price",
				ConfigJson:   "{}",
			},
		}
	}
	if sequence%5 != 0 {
		frame.Samples = append(frame.Samples, &rwv1.IndicatorSampleV2{
			IndicatorKey: "alpha",
			ScalarValue:  proto.Float64(float64(sequence)),
		})
	}
	if sequence == 4 || sequence == 9 || sequence == 1438 {
		sample := &rwv1.IndicatorSampleV2{IndicatorKey: "trades"}
		sample.Markers = append(sample.Markers, &rwv1.IndicatorMarkerV2{
			Text:     "BUY",
			Price:    proto.Float64(float64(sequence) + 0.5),
			Color:    "#16a34a",
			Position: "belowBar",
			Shape:    "arrowUp",
		})
		if sequence == 9 {
			sample.Markers = append(sample.Markers, &rwv1.IndicatorMarkerV2{
				Text:     "SCALE",
				Color:    "#0284c7",
				Position: "inBar",
				Shape:    "circle",
			})
		}
		frame.Samples = append(frame.Samples, sample)
	}
	return frame
}

func TestIndicatorV2Integration1023ThenTwoFrames(t *testing.T) {
	const (
		sessionID = "45454545454545454545454545454545"
		streamA   = "binance:spot:BTCUSDT:1m"
		streamB   = "binance:spot:ETHUSDT:1m"
		streamC   = "binance:perpetual_futures:BTCUSDT:5m"
	)
	platform := newIndicatorV2DurablePlatform(true)
	agent := NewAgent(AgentConfig{
		RuntimeID:       "runtime-indicator-v2-integration",
		UserID:          6,
		PlatformInvoker: platform,
		RequestTimeout:  2 * time.Second,
	})
	generation := newWorkerGeneration(sessionID, 1)
	if !generation.bindAuthenticatedGeneration(7) {
		t.Fatal("bind authenticated worker generation")
	}
	agent.generations[sessionID] = generation
	runRequest, err := anypb.New(&strategyv1.RunStrategyRequest{
		UserId:    6,
		RuntimeId: "runtime-indicator-v2-integration",
	})
	if err != nil {
		t.Fatalf("marshal run facts: %v", err)
	}
	agent.rememberRunRequest(sessionID, runRequest)
	identity := WorkerIdentity{
		SessionID:  sessionID,
		PID:        1234,
		Generation: 7,
		token:      "indicator-v2-integration-token",
	}
	send := func(frame *rwv1.IndicatorFrameV2) error {
		return agent.HandleAuthenticatedWorkerFrame(
			context.Background(),
			identity,
			&rwv1.WorkerFrame{
				Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
					IndicatorFrameV2: frame,
				},
			},
			func(*rwv1.AgentFrame) error { return nil },
		)
	}

	for sequence := uint64(0); sequence < 1023; sequence++ {
		if err := send(indicatorV2IntegrationFrame(
			sessionID,
			streamA,
			sequence,
		)); err != nil {
			t.Fatalf("stream A sequence %d: %v", sequence, err)
		}
	}
	for _, streamKey := range []string{streamB, streamC} {
		if err := send(indicatorV2IntegrationFrame(
			sessionID,
			streamKey,
			0,
		)); err != nil {
			t.Fatalf("independent stream %s: %v", streamKey, err)
		}
	}

	firstFlush := make(chan error, 1)
	go func() {
		firstFlush <- agent.indicatorSync.FlushSession(
			context.Background(),
			sessionID,
			false,
		)
	}()
	select {
	case <-platform.firstSaveStart:
	case <-time.After(time.Second):
		t.Fatal("1023 save did not become in flight")
	}
	for sequence := uint64(1023); sequence < 1025; sequence++ {
		if err := send(indicatorV2IntegrationFrame(
			sessionID,
			streamA,
			sequence,
		)); err != nil {
			t.Fatalf("stream A boundary sequence %d: %v", sequence, err)
		}
	}
	if err := send(indicatorV2IntegrationFrame(
		sessionID,
		streamA,
		1024,
	)); err != nil {
		t.Fatalf("exact immediate duplicate: %v", err)
	}
	close(platform.firstSaveAllow)
	if err := <-firstFlush; err != nil {
		t.Fatalf("first flush: %v", err)
	}

	if err := agent.indicatorSync.FlushSession(
		context.Background(),
		sessionID,
		false,
	); err != nil {
		t.Fatalf("flush 1025: %v", err)
	}
	assertIndicatorV2DurableChunk(
		t,
		platform.chunk(sessionID, streamA, "alpha", 0),
		1024,
		true,
	)
	assertIndicatorV2DurableChunk(
		t,
		platform.chunk(sessionID, streamA, "alpha", 1),
		1,
		false,
	)
	for _, streamKey := range []string{streamB, streamC} {
		assertIndicatorV2DurableChunk(
			t,
			platform.chunk(sessionID, streamKey, "alpha", 0),
			1,
			false,
		)
	}

	savesBefore, finalsBefore := platform.callCounts()
	if err := agent.indicatorSync.FlushSession(
		context.Background(),
		sessionID,
		false,
	); err != nil {
		t.Fatalf("clean duplicate flush: %v", err)
	}
	savesAfter, finalsAfter := platform.callCounts()
	if savesAfter != savesBefore || finalsAfter != finalsBefore {
		platform.mu.Lock()
		for index, request := range platform.saveCalls {
			chunks := make([]string, 0, len(request.GetChunks()))
			for _, chunk := range request.GetChunks() {
				chunks = append(
					chunks,
					fmt.Sprintf(
						"%s/%s/%d=count:%d,revision:%d",
						chunk.GetStreamKey(),
						chunk.GetIndicatorKey(),
						chunk.GetChunkIndex(),
						chunk.GetCount(),
						chunk.GetRevision(),
					),
				)
			}
			t.Logf(
				"save[%d] definitions=%d chunks=%v",
				index,
				len(request.GetDefinitions()),
				chunks,
			)
		}
		platform.mu.Unlock()
		t.Fatalf(
			"clean flush changed call counts: save %d->%d finalize %d->%d",
			savesBefore,
			savesAfter,
			finalsBefore,
			finalsAfter,
		)
	}

	for sequence := uint64(1025); sequence < 2049; sequence++ {
		if err := send(indicatorV2IntegrationFrame(
			sessionID,
			streamA,
			sequence,
		)); err != nil {
			t.Fatalf("stream A sequence %d: %v", sequence, err)
		}
	}
	if err := agent.indicatorSync.FlushSession(
		context.Background(),
		sessionID,
		false,
	); err != nil {
		t.Fatalf("flush 2049: %v", err)
	}
	for chunkIndex, wantCount := range []uint32{1024, 1024, 1} {
		assertIndicatorV2DurableChunk(
			t,
			platform.chunk(
				sessionID,
				streamA,
				"alpha",
				uint32(chunkIndex),
			),
			wantCount,
			chunkIndex < 2,
		)
	}
	markerChunk := platform.chunk(sessionID, streamA, "trades", 1)
	if markerChunk == nil || len(markerChunk.GetMarkers()) != 1 {
		t.Fatalf("marker chunk 1 = %+v", markerChunk)
	}
	marker := markerChunk.GetMarkers()[0]
	if marker.GetSequence() != 1438 ||
		marker.GetOffset() != 414 ||
		marker.GetTimeMs() != int64(1439)*60_000 ||
		marker.GetTimeMs() != markerChunk.GetTimesMs()[marker.GetOffset()] {
		t.Fatalf("marker 1438 identity/time = %+v", marker)
	}
	firstMarkerChunk := platform.chunk(sessionID, streamA, "trades", 0)
	if firstMarkerChunk == nil ||
		len(firstMarkerChunk.GetMarkers()) != 3 ||
		firstMarkerChunk.GetMarkers()[1].GetSequence() != 9 ||
		firstMarkerChunk.GetMarkers()[2].GetSequence() != 9 {
		t.Fatalf("sparse and same-bar markers = %+v", firstMarkerChunk)
	}
	alphaChunk := platform.chunk(sessionID, streamA, "alpha", 0)
	if alphaChunk.GetScalarValues()[5].Value != nil ||
		alphaChunk.GetScalarValues()[6].GetValue() != 6 {
		t.Fatalf(
			"sparse scalar alignment around sequence 5 = %+v",
			alphaChunk.GetScalarValues()[4:7],
		)
	}

	conflict := indicatorV2IntegrationFrame(sessionID, streamA, 2048)
	conflict.Samples = []*rwv1.IndicatorSampleV2{{
		IndicatorKey: "alpha",
		ScalarValue:  proto.Float64(999),
	}}
	var shutdown *rwv1.ShutdownWorker
	err = agent.HandleAuthenticatedWorkerFrame(
		context.Background(),
		identity,
		&rwv1.WorkerFrame{
			Payload: &rwv1.WorkerFrame_IndicatorFrameV2{
				IndicatorFrameV2: conflict,
			},
		},
		func(frame *rwv1.AgentFrame) error {
			shutdown = frame.GetShutdownWorker()
			return nil
		},
	)
	var protocolErr *IndicatorProtocolError
	if !errors.As(err, &protocolErr) ||
		shutdown == nil ||
		shutdown.GetSessionId() != sessionID {
		t.Fatalf(
			"conflicting duplicate error/shutdown = %v / %+v",
			err,
			shutdown,
		)
	}
	generation.mu.Lock()
	closing := generation.closing
	generation.mu.Unlock()
	if !closing {
		t.Fatal("conflicting duplicate did not close generation admission")
	}
}

func assertIndicatorV2DurableChunk(
	t *testing.T,
	chunk *portfoliov1.StrategyIndicatorChunkV2,
	wantCount uint32,
	wantFinalized bool,
) {
	t.Helper()
	if chunk == nil ||
		chunk.GetCount() != wantCount ||
		chunk.GetRevision() != uint64(wantCount) ||
		chunk.GetFinalized() != wantFinalized ||
		chunk.GetProtocolVersion() != 2 {
		t.Fatalf(
			"durable chunk = %+v, want count=%d finalized=%t protocol=2",
			chunk,
			wantCount,
			wantFinalized,
		)
	}
}
