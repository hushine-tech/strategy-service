package runtimeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
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

	mu               sync.Mutex
	sessions         map[string]*indicatorSessionState
	immediate        chan struct{}
	immediatePending map[string]struct{}
}

type indicatorSessionState struct {
	mu            sync.Mutex
	flushMu       sync.Mutex
	streamsV2     map[string]*indicatorStreamStateV2
	identityV2    WorkerIdentity
	hasIdentityV2 bool
	userIDV2      int64
	strategyIDV2  int64
}

type indicatorSeriesStateV2 struct {
	definition      *rwv1.IndicatorDefinition
	definitionDirty bool
	buffer          *IndicatorBufferV2
}

type indicatorStreamStateV2 struct {
	clock       indicatorStreamClock
	definitions []*rwv1.IndicatorDefinition
	series      map[string]*indicatorSeriesStateV2
}

type IndicatorFrameDispositionV2 int

const (
	IndicatorFrameExpected IndicatorFrameDispositionV2 = iota
	IndicatorFrameDuplicate
	IndicatorFrameRejected
)

type IndicatorProtocolError struct {
	SessionID string
	StreamKey string
	Sequence  uint64
	Reason    string
}

func (e *IndicatorProtocolError) Error() string {
	if e == nil {
		return "runtime indicator protocol error"
	}
	return fmt.Sprintf(
		"runtime indicator protocol error: session=%s stream=%s sequence=%d reason=%s",
		e.SessionID,
		e.StreamKey,
		e.Sequence,
		e.Reason,
	)
}

func (*IndicatorProtocolError) Code() string {
	return "RUNTIME_INDICATOR_PROTOCOL_ERROR"
}

type indicatorStreamClock struct {
	NextSequence    uint64
	LastTimeMS      int64
	IntervalMS      int64
	HasLast         bool
	LastPayloadHash [32]byte
}

func (c *indicatorStreamClock) Classify(
	sessionID string,
	streamKey string,
	sequence uint64,
	timeMS int64,
	intervalMS int64,
	payloadHash [32]byte,
) (IndicatorFrameDispositionV2, error) {
	reject := func(reason string) (IndicatorFrameDispositionV2, error) {
		return IndicatorFrameRejected, &IndicatorProtocolError{
			SessionID: sessionID,
			StreamKey: streamKey,
			Sequence:  sequence,
			Reason:    reason,
		}
	}
	if timeMS <= 0 {
		return reject("market_time_ms must be positive")
	}
	if intervalMS <= 0 {
		return reject("interval_ms must be positive")
	}
	if c.HasLast && intervalMS != c.IntervalMS {
		return reject(fmt.Sprintf(
			"interval changed: expected=%d received=%d",
			c.IntervalMS,
			intervalMS,
		))
	}
	if c.HasLast && c.NextSequence > 0 &&
		sequence == c.NextSequence-1 &&
		timeMS == c.LastTimeMS {
		if payloadHash == c.LastPayloadHash {
			return IndicatorFrameDuplicate, nil
		}
		return reject("conflicting duplicate payload")
	}
	if sequence < c.NextSequence {
		return reject("duplicate time mismatch or lower sequence")
	}
	if sequence > c.NextSequence {
		return reject(fmt.Sprintf(
			"sequence gap: expected=%d received=%d",
			c.NextSequence,
			sequence,
		))
	}
	if c.HasLast && timeMS <= c.LastTimeMS {
		return reject(fmt.Sprintf(
			"market time must strictly increase: previous=%d received=%d",
			c.LastTimeMS,
			timeMS,
		))
	}
	return IndicatorFrameExpected, nil
}

func (c *indicatorStreamClock) Commit(
	sequence uint64,
	timeMS int64,
	intervalMS int64,
	payloadHash [32]byte,
) error {
	if sequence != c.NextSequence {
		return fmt.Errorf(
			"indicator stream commit sequence = %d, want %d",
			sequence,
			c.NextSequence,
		)
	}
	c.NextSequence++
	c.LastTimeMS = timeMS
	if !c.HasLast {
		c.IntervalMS = intervalMS
	}
	c.HasLast = true
	c.LastPayloadHash = payloadHash
	return nil
}

type indicatorSeriesFlushV2 struct {
	series         *indicatorSeriesStateV2
	definitionSent bool
	snapshot       IndicatorFlushSnapshotV2
}

type indicatorSeriesFinalizationV2 struct {
	series *indicatorSeriesStateV2
	token  IndicatorFinalizeTokenV2
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
		cfg:              cfg,
		sessions:         map[string]*indicatorSessionState{},
		immediate:        make(chan struct{}, 1),
		immediatePending: map[string]struct{}{},
	}
}

func (m *IndicatorSyncManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.immediate:
			m.flushImmediateSessions(ctx)
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

func (m *IndicatorSyncManager) ReceiveFrameV2(
	identity WorkerIdentity,
	frame *rwv1.IndicatorFrameV2,
) error {
	if frame == nil {
		return fmt.Errorf("indicator V2 frame is nil")
	}
	sessionID := strings.TrimSpace(identity.SessionID)
	streamKey := strings.TrimSpace(frame.GetStreamKey())
	sequence := frame.GetStreamSequence()
	protocolError := func(reason string) error {
		return &IndicatorProtocolError{
			SessionID: sessionID,
			StreamKey: streamKey,
			Sequence:  sequence,
			Reason:    reason,
		}
	}
	if sessionID == "" {
		return protocolError("authenticated session_id is required")
	}
	if identity.Generation == 0 {
		return protocolError("authenticated worker generation is required")
	}
	if strings.TrimSpace(frame.GetSessionId()) != sessionID {
		return protocolError("payload session_id does not match authenticated worker")
	}
	if streamKey == "" {
		return protocolError("stream_key is required")
	}
	if frame.GetUserId() <= 0 || frame.GetStrategyId() <= 0 {
		return protocolError("user_id and strategy_id must be positive")
	}
	payloadHash, err := canonicalIndicatorFramePayloadV2(frame)
	if err != nil {
		return protocolError("canonical payload encoding failed")
	}
	if m.lookupSession(sessionID) == nil {
		if err := validateNewIndicatorStreamFrameV2(
			sessionID,
			streamKey,
			frame,
			payloadHash,
		); err != nil {
			if _, ok := err.(*IndicatorProtocolError); ok {
				return err
			}
			return protocolError(err.Error())
		}
	}

	state := m.session(sessionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.streamsV2 == nil {
		state.streamsV2 = map[string]*indicatorStreamStateV2{}
	}
	if state.hasIdentityV2 && !sameWorkerIdentity(state.identityV2, identity) {
		return protocolError("authenticated worker generation changed")
	}
	if state.userIDV2 != 0 && state.userIDV2 != frame.GetUserId() {
		return protocolError("user_id changed within session")
	}
	if state.strategyIDV2 != 0 && state.strategyIDV2 != frame.GetStrategyId() {
		return protocolError("strategy_id changed within session")
	}

	stream := state.streamsV2[streamKey]
	clock := indicatorStreamClock{}
	if stream != nil {
		clock = stream.clock
	}
	disposition, err := clock.Classify(
		sessionID,
		streamKey,
		sequence,
		frame.GetMarketTimeMs(),
		frame.GetIntervalMs(),
		payloadHash,
	)
	if err != nil {
		return err
	}
	if disposition == IndicatorFrameDuplicate {
		return nil
	}

	definitions, err := indicatorDefinitionsForFrameV2(stream, frame)
	if err != nil {
		return protocolError(err.Error())
	}
	samples, err := indicatorSamplesForFrameV2(definitions, frame.GetSamples())
	if err != nil {
		return protocolError(err.Error())
	}
	if err := clock.Commit(
		sequence,
		frame.GetMarketTimeMs(),
		frame.GetIntervalMs(),
		payloadHash,
	); err != nil {
		return protocolError(err.Error())
	}

	newStream := stream == nil
	if newStream {
		stream = &indicatorStreamStateV2{
			definitions: cloneIndicatorDefinitionsV2(definitions),
			series:      map[string]*indicatorSeriesStateV2{},
		}
		for _, definition := range definitions {
			key := strings.TrimSpace(definition.GetIndicatorKey())
			stream.series[key] = &indicatorSeriesStateV2{
				definition:      proto.Clone(definition).(*rwv1.IndicatorDefinition),
				definitionDirty: true,
				buffer:          NewIndicatorBufferV2(definition.GetType()),
			}
		}
	}
	operations := make([]indicatorBufferAppendV2, 0, len(stream.definitions))
	for _, definition := range stream.definitions {
		key := strings.TrimSpace(definition.GetIndicatorKey())
		sample := samples[key]
		var scalar *float64
		var markers []IndicatorMarkerValueV2
		if sample != nil {
			scalar = cloneFloat64(sample.ScalarValue)
			for _, marker := range sample.GetMarkers() {
				markers = append(markers, IndicatorMarkerValueV2{
					Text:     marker.GetText(),
					Price:    cloneFloat64(marker.Price),
					Color:    marker.GetColor(),
					Position: marker.GetPosition(),
					Shape:    marker.GetShape(),
				})
			}
		}
		operations = append(operations, indicatorBufferAppendV2{
			buffer:     stream.series[key].buffer,
			sequence:   sequence,
			timeMS:     frame.GetMarketTimeMs(),
			intervalMS: frame.GetIntervalMs(),
			scalar:     scalar,
			markers:    markers,
		})
	}
	if err := appendIndicatorBuffersV2(operations); err != nil {
		return protocolError(err.Error())
	}
	if newStream {
		state.streamsV2[streamKey] = stream
	}
	stream.clock = clock
	if !state.hasIdentityV2 {
		state.identityV2 = identity
		state.hasIdentityV2 = true
	}
	if state.userIDV2 == 0 {
		state.userIDV2 = frame.GetUserId()
	}
	if state.strategyIDV2 == 0 {
		state.strategyIDV2 = frame.GetStrategyId()
	}

	if sequence%indicatorChunkSize == indicatorChunkSize-1 {
		m.requestImmediateFlush(sessionID)
	}
	return nil
}

func validateNewIndicatorStreamFrameV2(
	sessionID string,
	streamKey string,
	frame *rwv1.IndicatorFrameV2,
	payloadHash [32]byte,
) error {
	clock := indicatorStreamClock{}
	disposition, err := clock.Classify(
		sessionID,
		streamKey,
		frame.GetStreamSequence(),
		frame.GetMarketTimeMs(),
		frame.GetIntervalMs(),
		payloadHash,
	)
	if err != nil {
		return err
	}
	if disposition != IndicatorFrameExpected {
		return fmt.Errorf("first indicator frame must be expected")
	}
	definitions, err := indicatorDefinitionsForFrameV2(nil, frame)
	if err != nil {
		return err
	}
	samples, err := indicatorSamplesForFrameV2(
		definitions,
		frame.GetSamples(),
	)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		key := strings.TrimSpace(definition.GetIndicatorKey())
		sample := samples[key]
		var scalar *float64
		var markers []IndicatorMarkerValueV2
		if sample != nil {
			scalar = cloneFloat64(sample.ScalarValue)
			for _, marker := range sample.GetMarkers() {
				markers = append(markers, IndicatorMarkerValueV2{
					Text:     marker.GetText(),
					Price:    cloneFloat64(marker.Price),
					Color:    marker.GetColor(),
					Position: marker.GetPosition(),
					Shape:    marker.GetShape(),
				})
			}
		}
		if err := NewIndicatorBufferV2(definition.GetType()).Append(
			frame.GetStreamSequence(),
			frame.GetMarketTimeMs(),
			frame.GetIntervalMs(),
			scalar,
			markers,
		); err != nil {
			return err
		}
	}
	return nil
}

func canonicalIndicatorFramePayloadV2(
	frame *rwv1.IndicatorFrameV2,
) ([32]byte, error) {
	cloned, ok := proto.Clone(frame).(*rwv1.IndicatorFrameV2)
	if !ok || cloned == nil {
		return [32]byte{}, fmt.Errorf("clone indicator V2 frame")
	}
	cloned.SessionId = ""
	cloned.UserId = 0
	cloned.StrategyId = 0
	cloned.StreamKey = ""
	cloned.StreamSequence = 0
	cloned.MarketTimeMs = 0
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

func sameWorkerIdentity(left WorkerIdentity, right WorkerIdentity) bool {
	return left.SessionID == right.SessionID &&
		left.PID == right.PID &&
		left.Generation == right.Generation &&
		left.token == right.token
}

func indicatorDefinitionsForFrameV2(
	stream *indicatorStreamStateV2,
	frame *rwv1.IndicatorFrameV2,
) ([]*rwv1.IndicatorDefinition, error) {
	incoming := frame.GetDefinitions()
	if stream == nil {
		if frame.GetStreamSequence() != 0 {
			return nil, fmt.Errorf("first stream sequence must be zero")
		}
		if len(incoming) == 0 {
			return nil, fmt.Errorf("sequence zero requires indicator definitions")
		}
		if err := validateIndicatorDefinitionsV2(incoming); err != nil {
			return nil, err
		}
		return incoming, nil
	}
	if len(incoming) == 0 {
		return stream.definitions, nil
	}
	if len(incoming) != len(stream.definitions) {
		return nil, fmt.Errorf("indicator definitions changed within stream")
	}
	for index := range incoming {
		if !proto.Equal(incoming[index], stream.definitions[index]) {
			return nil, fmt.Errorf("indicator definitions changed within stream")
		}
	}
	return stream.definitions, nil
}

func validateIndicatorDefinitionsV2(
	definitions []*rwv1.IndicatorDefinition,
) error {
	seen := make(map[string]struct{}, len(definitions))
	for index, definition := range definitions {
		if definition == nil {
			return fmt.Errorf("indicator definition[%d] is nil", index)
		}
		key := strings.TrimSpace(definition.GetIndicatorKey())
		if key == "" {
			return fmt.Errorf("indicator definition[%d] key is required", index)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate indicator definition key %q", key)
		}
		seen[key] = struct{}{}
		if strings.TrimSpace(definition.GetPane()) == "" {
			return fmt.Errorf("indicator %q pane is required", key)
		}
		configJSON := strings.TrimSpace(definition.GetConfigJson())
		if configJSON != "" {
			var config map[string]json.RawMessage
			if json.Unmarshal([]byte(configJSON), &config) != nil ||
				config == nil {
				return fmt.Errorf(
					"indicator %q config_json must be a JSON object",
					key,
				)
			}
		}
		switch strings.TrimSpace(strings.ToLower(definition.GetType())) {
		case "line", "histogram", "marker":
		default:
			return fmt.Errorf(
				"indicator %q has unsupported type %q",
				key,
				definition.GetType(),
			)
		}
	}
	return nil
}

func indicatorSamplesForFrameV2(
	definitions []*rwv1.IndicatorDefinition,
	incoming []*rwv1.IndicatorSampleV2,
) (map[string]*rwv1.IndicatorSampleV2, error) {
	types := make(map[string]string, len(definitions))
	for _, definition := range definitions {
		types[strings.TrimSpace(definition.GetIndicatorKey())] =
			strings.TrimSpace(strings.ToLower(definition.GetType()))
	}
	samples := make(map[string]*rwv1.IndicatorSampleV2, len(incoming))
	for index, sample := range incoming {
		if sample == nil {
			return nil, fmt.Errorf("indicator sample[%d] is nil", index)
		}
		key := strings.TrimSpace(sample.GetIndicatorKey())
		kind, exists := types[key]
		if !exists {
			return nil, fmt.Errorf("unknown indicator sample key %q", key)
		}
		if _, duplicate := samples[key]; duplicate {
			return nil, fmt.Errorf("duplicate indicator sample key %q", key)
		}
		switch kind {
		case "line", "histogram":
			if len(sample.GetMarkers()) != 0 {
				return nil, fmt.Errorf(
					"%s indicator %q cannot contain markers",
					kind,
					key,
				)
			}
			if sample.ScalarValue != nil &&
				(math.IsNaN(sample.GetScalarValue()) ||
					math.IsInf(sample.GetScalarValue(), 0)) {
				return nil, fmt.Errorf(
					"%s indicator %q scalar value must be finite",
					kind,
					key,
				)
			}
		case "marker":
			if sample.ScalarValue != nil {
				return nil, fmt.Errorf(
					"marker indicator %q cannot contain a scalar",
					key,
				)
			}
			for markerIndex, marker := range sample.GetMarkers() {
				if marker == nil {
					return nil, fmt.Errorf(
						"marker indicator %q marker[%d] is nil",
						key,
						markerIndex,
					)
				}
				if marker.Price != nil &&
					(math.IsNaN(marker.GetPrice()) ||
						math.IsInf(marker.GetPrice(), 0)) {
					return nil, fmt.Errorf(
						"marker indicator %q price[%d] must be finite",
						key,
						markerIndex,
					)
				}
				switch strings.TrimSpace(marker.GetPosition()) {
				case "", "aboveBar", "belowBar", "inBar":
				default:
					return nil, fmt.Errorf(
						"marker indicator %q position[%d] is invalid",
						key,
						markerIndex,
					)
				}
				switch strings.TrimSpace(marker.GetShape()) {
				case "", "circle", "square", "arrowUp", "arrowDown":
				default:
					return nil, fmt.Errorf(
						"marker indicator %q shape[%d] is invalid",
						key,
						markerIndex,
					)
				}
			}
		}
		samples[key] = sample
	}
	return samples, nil
}

func cloneIndicatorDefinitionsV2(
	definitions []*rwv1.IndicatorDefinition,
) []*rwv1.IndicatorDefinition {
	cloned := make([]*rwv1.IndicatorDefinition, 0, len(definitions))
	for _, definition := range definitions {
		cloned = append(
			cloned,
			proto.Clone(definition).(*rwv1.IndicatorDefinition),
		)
	}
	return cloned
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
		for _, stream := range state.streamsV2 {
			for _, series := range stream.series {
				series.buffer.SealOpen()
			}
		}
	}
	flushesV2, requestV2 := snapshotIndicatorFlushV2(sessionID, state)
	state.mu.Unlock()

	if len(requestV2.GetDefinitions()) > 0 || len(requestV2.GetChunks()) > 0 {
		if err := m.invokeIndicatorPlatform(
			ctx,
			"portfolio.SaveStrategyIndicatorsV2",
			requestV2,
		); err != nil {
			return err
		}
		state.mu.Lock()
		for _, flush := range flushesV2 {
			if flush.definitionSent {
				flush.series.definitionDirty = false
			}
			for _, token := range flush.snapshot.Tokens {
				flush.series.buffer.MarkSaveAcked(token)
			}
		}
		state.mu.Unlock()
	}

	state.mu.Lock()
	finalizations, finalizeRequest :=
		snapshotIndicatorFinalizationsV2(sessionID, state)
	state.mu.Unlock()
	if len(finalizeRequest.GetChunks()) > 0 {
		if err := m.invokeIndicatorPlatform(
			ctx,
			"portfolio.FinalizeStrategyIndicatorChunksV2",
			finalizeRequest,
		); err != nil {
			return err
		}
		state.mu.Lock()
		for _, finalization := range finalizations {
			finalization.series.buffer.MarkFinalizeAcked(
				finalization.token,
			)
		}
		state.mu.Unlock()
	}
	return nil
}

func (m *IndicatorSyncManager) invokeIndicatorPlatform(
	ctx context.Context,
	method string,
	request proto.Message,
) error {
	if m.cfg.PlatformInvoker == nil {
		return fmt.Errorf("platform invoker is not configured")
	}
	packed, err := anypb.New(request)
	if err != nil {
		return err
	}
	_, err = m.cfg.PlatformInvoker.InvokePlatformAny(
		ctx,
		method,
		packed,
		m.cfg.RequestTimeout,
	)
	return err
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
		delete(m.immediatePending, strings.TrimSpace(sessionID))
	}
	m.mu.Unlock()
}

func (m *IndicatorSyncManager) requestImmediateFlush(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	m.mu.Lock()
	m.immediatePending[sessionID] = struct{}{}
	m.mu.Unlock()
	select {
	case m.immediate <- struct{}{}:
	default:
	}
}

func (m *IndicatorSyncManager) flushImmediateSessions(ctx context.Context) {
	for {
		m.mu.Lock()
		sessionIDs := make([]string, 0, len(m.immediatePending))
		for sessionID := range m.immediatePending {
			sessionIDs = append(sessionIDs, sessionID)
			delete(m.immediatePending, sessionID)
		}
		m.mu.Unlock()
		if len(sessionIDs) == 0 {
			return
		}
		sort.Strings(sessionIDs)
		for _, sessionID := range sessionIDs {
			if ctx.Err() != nil {
				return
			}
			_ = m.FlushSession(ctx, sessionID, false)
		}
	}
}

func (m *IndicatorSyncManager) session(sessionID string) *indicatorSessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.sessions[sessionID]; state != nil {
		return state
	}
	state := &indicatorSessionState{
		streamsV2: map[string]*indicatorStreamStateV2{},
	}
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

func (m *IndicatorSyncManager) sessionDirty(sessionID string) bool {
	state := m.lookupSession(sessionID)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, stream := range state.streamsV2 {
		for _, series := range stream.series {
			if series.definitionDirty ||
				series.buffer.HasPendingPersistence() {
				return true
			}
		}
	}
	return false
}

func snapshotIndicatorFlushV2(
	sessionID string,
	state *indicatorSessionState,
) (
	[]indicatorSeriesFlushV2,
	*portfoliov1.SaveStrategyIndicatorsV2Request,
) {
	request := &portfoliov1.SaveStrategyIndicatorsV2Request{
		SessionId: sessionID,
		UserId:    state.userIDV2,
	}
	var flushes []indicatorSeriesFlushV2
	streamKeys := make([]string, 0, len(state.streamsV2))
	for streamKey := range state.streamsV2 {
		streamKeys = append(streamKeys, streamKey)
	}
	sort.Strings(streamKeys)
	for _, streamKey := range streamKeys {
		stream := state.streamsV2[streamKey]
		indicatorKeys := make([]string, 0, len(stream.series))
		for indicatorKey := range stream.series {
			indicatorKeys = append(indicatorKeys, indicatorKey)
		}
		sort.Strings(indicatorKeys)
		for _, indicatorKey := range indicatorKeys {
			series := stream.series[indicatorKey]
			flush := indicatorSeriesFlushV2{
				series:   series,
				snapshot: series.buffer.SnapshotDirtyForFlush(),
			}
			if series.definitionDirty && series.definition != nil {
				definition := series.definition
				request.Definitions = append(
					request.Definitions,
					&portfoliov1.StrategyIndicatorDefinitionV2{
						SessionId:       sessionID,
						StrategyId:      state.strategyIDV2,
						StreamKey:       streamKey,
						IndicatorKey:    indicatorKey,
						Name:            definition.GetName(),
						Type:            definition.GetType(),
						Pane:            definition.GetPane(),
						Color:           definition.GetColor(),
						Unit:            definition.GetUnit(),
						Description:     definition.GetDescription(),
						ConfigJson:      definition.GetConfigJson(),
						ProtocolVersion: IndicatorV2ProtocolVersion,
					},
				)
				flush.definitionSent = true
			}
			for _, chunk := range flush.snapshot.Chunks {
				request.Chunks = append(
					request.Chunks,
					syncPortfolioIndicatorChunkV2(
						sessionID,
						streamKey,
						indicatorKey,
						chunk,
					),
				)
			}
			if flush.definitionSent || len(flush.snapshot.Chunks) > 0 {
				flushes = append(flushes, flush)
			}
		}
	}
	return flushes, request
}

func snapshotIndicatorFinalizationsV2(
	sessionID string,
	state *indicatorSessionState,
) (
	[]indicatorSeriesFinalizationV2,
	*portfoliov1.FinalizeStrategyIndicatorChunksV2Request,
) {
	request := &portfoliov1.FinalizeStrategyIndicatorChunksV2Request{
		SessionId: sessionID,
		UserId:    state.userIDV2,
	}
	var finalizations []indicatorSeriesFinalizationV2
	streamKeys := make([]string, 0, len(state.streamsV2))
	for streamKey := range state.streamsV2 {
		streamKeys = append(streamKeys, streamKey)
	}
	sort.Strings(streamKeys)
	for _, streamKey := range streamKeys {
		stream := state.streamsV2[streamKey]
		indicatorKeys := make([]string, 0, len(stream.series))
		for indicatorKey := range stream.series {
			indicatorKeys = append(indicatorKeys, indicatorKey)
		}
		sort.Strings(indicatorKeys)
		for _, indicatorKey := range indicatorKeys {
			series := stream.series[indicatorKey]
			for _, token := range series.buffer.SnapshotFinalizations() {
				request.Chunks = append(
					request.Chunks,
					&portfoliov1.StrategyIndicatorChunkFinalizationV2{
						StreamKey:        streamKey,
						IndicatorKey:     indicatorKey,
						ChunkIndex:       token.ChunkIndex,
						ExpectedRevision: token.ExpectedRevision,
					},
				)
				finalizations = append(
					finalizations,
					indicatorSeriesFinalizationV2{
						series: series,
						token:  token,
					},
				)
			}
		}
	}
	return finalizations, request
}

func syncPortfolioIndicatorChunkV2(
	sessionID string,
	streamKey string,
	indicatorKey string,
	chunk IndicatorChunkV2,
) *portfoliov1.StrategyIndicatorChunkV2 {
	scalars := make(
		[]*portfoliov1.NullableDoubleV2,
		len(chunk.ScalarValues),
	)
	for index, value := range chunk.ScalarValues {
		scalars[index] = &portfoliov1.NullableDoubleV2{}
		if value != nil {
			scalars[index].Value = cloneFloat64(value)
		}
	}
	markers := make(
		[]*portfoliov1.StrategyIndicatorMarkerV2,
		0,
		len(chunk.Markers),
	)
	for _, marker := range chunk.Markers {
		out := &portfoliov1.StrategyIndicatorMarkerV2{
			Sequence: marker.Sequence,
			Offset:   marker.Offset,
			TimeMs:   marker.TimeMS,
			Text:     marker.Text,
			Color:    marker.Color,
			Position: marker.Position,
			Shape:    marker.Shape,
		}
		if marker.Price != nil {
			out.Price = cloneFloat64(marker.Price)
		}
		markers = append(markers, out)
	}
	return &portfoliov1.StrategyIndicatorChunkV2{
		SessionId:       sessionID,
		StreamKey:       streamKey,
		IndicatorKey:    indicatorKey,
		ChunkIndex:      chunk.ChunkIndex,
		StartSequence:   chunk.StartSequence,
		EndSequence:     chunk.EndSequence,
		StartTimeMs:     chunk.StartTimeMS,
		EndTimeMs:       chunk.EndTimeMS,
		IntervalMs:      chunk.IntervalMS,
		Count:           chunk.Count,
		TimesMs:         append([]int64(nil), chunk.TimesMS...),
		ScalarValues:    scalars,
		Markers:         markers,
		Revision:        chunk.Revision,
		Finalized:       false,
		ProtocolVersion: IndicatorV2ProtocolVersion,
	}
}
