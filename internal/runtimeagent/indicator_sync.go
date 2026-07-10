package runtimeagent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type IndicatorSyncConfig struct {
	PlatformInvoker PlatformInvoker
	IndicatorLimit  int
	FlushInterval   time.Duration
	RequestTimeout  time.Duration
	FinalizeTimeout time.Duration
	RetryInitial    time.Duration
	RetryMax        time.Duration
}

type IndicatorSyncManager struct {
	cfg IndicatorSyncConfig

	mu        sync.Mutex
	sessions  map[string]*indicatorSessionState
	immediate chan string
}

type indicatorSeriesState struct {
	userID               int64
	strategyID           int64
	streamKey            string
	indicatorKey         string
	definition           *rwv1.IndicatorDefinition
	definitionDirty      bool
	definitionGeneration uint64
	buffer               *IndicatorBuffer
}

type indicatorSessionState struct {
	mu         sync.Mutex
	flushMu    sync.Mutex
	series     map[string]*indicatorSeriesState
	outOfOrder uint64
}

type indicatorSeriesFlush struct {
	series               *indicatorSeriesState
	definitionGeneration uint64
	definition           *portfoliov1.StrategyIndicatorDefinition
	snapshot             IndicatorFlushSnapshot
}

func NewIndicatorSyncManager(cfg IndicatorSyncConfig) *IndicatorSyncManager {
	if cfg.IndicatorLimit <= 0 {
		cfg.IndicatorLimit = 1024
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.FinalizeTimeout <= 0 {
		cfg.FinalizeTimeout = 30 * time.Second
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = 100 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 2 * time.Second
	}
	if cfg.RetryMax < cfg.RetryInitial {
		cfg.RetryMax = cfg.RetryInitial
	}
	return &IndicatorSyncManager{
		cfg:       cfg,
		sessions:  map[string]*indicatorSessionState{},
		immediate: make(chan string, 256),
	}
}

func (m *IndicatorSyncManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case sessionID := <-m.immediate:
			_ = m.FlushSession(ctx, sessionID, false)
		case <-ticker.C:
			for _, sessionID := range m.sessionIDs() {
				if ctx.Err() != nil {
					return
				}
				_ = m.FlushSession(ctx, sessionID, false)
			}
		}
	}
}

func (m *IndicatorSyncManager) ReceiveFrame(frame *rwv1.IndicatorFrame) error {
	if frame == nil {
		return fmt.Errorf("indicator frame is nil")
	}
	sessionID := strings.TrimSpace(frame.GetSessionId())
	if sessionID == "" {
		return fmt.Errorf("indicator frame session_id is required")
	}
	streamKey := strings.TrimSpace(frame.GetStreamKey())
	if streamKey == "" {
		return fmt.Errorf("indicator frame stream_key is required")
	}

	state := m.session(sessionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	types := make(map[string]string, len(frame.GetDefinitions()))
	for _, definition := range frame.GetDefinitions() {
		indicatorKey := strings.TrimSpace(definition.GetIndicatorKey())
		if indicatorKey == "" {
			continue
		}
		types[indicatorKey] = definition.GetType()
		series := m.series(state, frame, streamKey, indicatorKey, definition.GetType())
		if series.definition == nil || !proto.Equal(series.definition, definition) {
			series.definition = proto.Clone(definition).(*rwv1.IndicatorDefinition)
			series.definitionDirty = true
			series.definitionGeneration++
		}
	}

	sealed := false
	for _, value := range frame.GetValues() {
		indicatorKey := strings.TrimSpace(value.GetIndicatorKey())
		if indicatorKey == "" {
			continue
		}
		series := m.series(state, frame, streamKey, indicatorKey, types[indicatorKey])
		pointValue := "null"
		if marker := strings.TrimSpace(value.GetMarkerJson()); marker != "" {
			pointValue = marker
		} else if value.GetHasValue() {
			pointValue = strconv.FormatFloat(value.GetValue(), 'f', -1, 64)
		}
		result := series.buffer.AddPoint(IndicatorPoint{
			MarketTimeMS: frame.GetMarketTimeMs(),
			IntervalMS:   frame.GetIntervalMs(),
			ValueJSON:    pointValue,
		})
		if result.Disposition == IndicatorPointOutOfOrder {
			state.outOfOrder++
		}
		sealed = sealed || result.Sealed
	}
	if sealed {
		select {
		case m.immediate <- sessionID:
		default:
		}
	}
	return nil
}

func (m *IndicatorSyncManager) FlushSession(ctx context.Context, sessionID string, sealOpen bool) error {
	state := m.lookupSession(sessionID)
	if state == nil {
		return nil
	}
	state.flushMu.Lock()
	defer state.flushMu.Unlock()

	state.mu.Lock()
	if sealOpen {
		for _, series := range state.series {
			series.buffer.SealOpen()
		}
	}
	flushes, request := snapshotIndicatorFlush(sessionID, state)
	state.mu.Unlock()
	if len(request.GetDefinitions()) == 0 && len(request.GetChunks()) == 0 {
		return nil
	}
	if m.cfg.PlatformInvoker == nil {
		return fmt.Errorf("platform invoker is not configured")
	}
	packed, err := anypb.New(request)
	if err != nil {
		return err
	}
	if _, err := m.cfg.PlatformInvoker.InvokePlatformAny(ctx, "portfolio.SaveStrategyIndicators", packed, m.cfg.RequestTimeout); err != nil {
		return err
	}

	state.mu.Lock()
	for _, flush := range flushes {
		if flush.definition != nil && flush.series.definitionGeneration == flush.definitionGeneration {
			flush.series.definitionDirty = false
		}
		flush.series.buffer.MarkFlushAcked(flush.snapshot)
	}
	state.mu.Unlock()
	return nil
}

func (m *IndicatorSyncManager) FinalizeSession(ctx context.Context, sessionID string) error {
	if _, ok := ctx.Deadline(); !ok && m.cfg.FinalizeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.cfg.FinalizeTimeout)
		defer cancel()
	}
	delay := m.cfg.RetryInitial
	var lastErr error
	for {
		err := m.FlushSession(ctx, sessionID, true)
		if err == nil {
			if !m.sessionDirty(sessionID) {
				return nil
			}
			continue
		}
		lastErr = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("final indicator flush: %w (last error: %v)", ctx.Err(), lastErr)
		case <-timer.C:
		}
		if delay < m.cfg.RetryMax {
			delay *= 2
			if delay > m.cfg.RetryMax {
				delay = m.cfg.RetryMax
			}
		}
	}
}

func (m *IndicatorSyncManager) ForgetSession(_ context.Context, sessionID string) {
	state := m.lookupSession(sessionID)
	if state == nil {
		return
	}
	state.flushMu.Lock()
	defer state.flushMu.Unlock()
	m.mu.Lock()
	if m.sessions[strings.TrimSpace(sessionID)] == state {
		delete(m.sessions, strings.TrimSpace(sessionID))
	}
	m.mu.Unlock()
}

func (m *IndicatorSyncManager) session(sessionID string) *indicatorSessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.sessions[sessionID]; state != nil {
		return state
	}
	state := &indicatorSessionState{series: map[string]*indicatorSeriesState{}}
	m.sessions[sessionID] = state
	return state
}

func (m *IndicatorSyncManager) lookupSession(sessionID string) *indicatorSessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[strings.TrimSpace(sessionID)]
}

func (m *IndicatorSyncManager) sessionIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.sessions))
	for sessionID := range m.sessions {
		ids = append(ids, sessionID)
	}
	return ids
}

func (m *IndicatorSyncManager) series(state *indicatorSessionState, frame *rwv1.IndicatorFrame, streamKey, indicatorKey, kind string) *indicatorSeriesState {
	key := indicatorSeriesKey(streamKey, indicatorKey)
	if series := state.series[key]; series != nil {
		series.userID = frame.GetUserId()
		series.strategyID = frame.GetStrategyId()
		return series
	}
	series := &indicatorSeriesState{
		userID:       frame.GetUserId(),
		strategyID:   frame.GetStrategyId(),
		streamKey:    streamKey,
		indicatorKey: indicatorKey,
		buffer:       NewIndicatorBufferForType(m.cfg.IndicatorLimit, kind),
	}
	state.series[key] = series
	return series
}

func (m *IndicatorSyncManager) sessionDirty(sessionID string) bool {
	state := m.lookupSession(sessionID)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, series := range state.series {
		snapshot := series.buffer.SnapshotDirtyForFlush()
		if series.definitionDirty || len(snapshot.Finals) > 0 || snapshot.Open.Count > 0 {
			return true
		}
	}
	return false
}

func snapshotIndicatorFlush(sessionID string, state *indicatorSessionState) ([]indicatorSeriesFlush, *portfoliov1.SaveStrategyIndicatorsRequest) {
	request := &portfoliov1.SaveStrategyIndicatorsRequest{SessionId: sessionID}
	flushes := make([]indicatorSeriesFlush, 0, len(state.series))
	for _, series := range state.series {
		if request.UserId == 0 {
			request.UserId = series.userID
		}
		flush := indicatorSeriesFlush{
			series:               series,
			definitionGeneration: series.definitionGeneration,
			snapshot:             series.buffer.SnapshotDirtyForFlush(),
		}
		if series.definitionDirty && series.definition != nil {
			definition := series.definition
			flush.definition = &portfoliov1.StrategyIndicatorDefinition{
				SessionId: sessionID, StrategyId: series.strategyID, StreamKey: series.streamKey,
				IndicatorKey: series.indicatorKey, Name: definition.GetName(), Type: definition.GetType(),
				Pane: definition.GetPane(), Color: definition.GetColor(), Unit: definition.GetUnit(),
				Description: definition.GetDescription(), ConfigJson: definition.GetConfigJson(),
			}
			request.Definitions = append(request.Definitions, flush.definition)
		}
		for _, chunk := range flush.snapshot.Finals {
			request.Chunks = append(request.Chunks, syncPortfolioIndicatorChunk(sessionID, series, chunk))
		}
		if flush.snapshot.Open.Count > 0 {
			request.Chunks = append(request.Chunks, syncPortfolioIndicatorChunk(sessionID, series, flush.snapshot.Open))
		}
		if flush.definition != nil || len(flush.snapshot.Finals) > 0 || flush.snapshot.Open.Count > 0 {
			flushes = append(flushes, flush)
		}
	}
	return flushes, request
}

func syncPortfolioIndicatorChunk(sessionID string, series *indicatorSeriesState, chunk IndicatorChunk) *portfoliov1.StrategyIndicatorChunk {
	return &portfoliov1.StrategyIndicatorChunk{
		SessionId: sessionID, StreamKey: series.streamKey, IndicatorKey: series.indicatorKey,
		ChunkIndex: int32(chunk.ChunkIndex), StartTimeMs: chunk.StartTimeMS, EndTimeMs: chunk.EndTimeMS,
		IntervalMs: chunk.IntervalMS, Count: int32(chunk.Count), ValuesJson: chunk.ValuesJSON,
		Finalized: chunk.Finalized,
	}
}

func indicatorSeriesKey(streamKey, indicatorKey string) string {
	return streamKey + "\x00" + indicatorKey
}
