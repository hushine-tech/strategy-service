package runtimeagent

import (
	"fmt"
	"sort"
	"strings"

	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
)

const indicatorSessionCheckpointSchemaV2 = 1

type IndicatorSessionCheckpointV2 struct {
	SchemaVersion      int                           `json:"schema_version"`
	SessionID          string                        `json:"session_id"`
	UserID             int64                         `json:"user_id"`
	StrategyID         int64                         `json:"strategy_id"`
	IdentityPID        int64                         `json:"identity_pid"`
	IdentityGeneration uint64                        `json:"identity_generation"`
	Streams            []indicatorStreamCheckpointV2 `json:"streams"`
}

type indicatorStreamCheckpointV2 struct {
	StreamKey   string                            `json:"stream_key"`
	Clock       indicatorStreamClockCheckpointV2  `json:"clock"`
	Definitions []indicatorDefinitionCheckpointV2 `json:"definitions"`
	Series      []indicatorSeriesCheckpointV2     `json:"series"`
}

type indicatorStreamClockCheckpointV2 struct {
	NextSequence    uint64   `json:"next_sequence"`
	LastTimeMS      int64    `json:"last_time_ms"`
	IntervalMS      int64    `json:"interval_ms"`
	HasLast         bool     `json:"has_last"`
	LastPayloadHash [32]byte `json:"last_payload_hash"`
}

type indicatorDefinitionCheckpointV2 struct {
	IndicatorKey string `json:"indicator_key"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Pane         string `json:"pane"`
	Color        string `json:"color"`
	Unit         string `json:"unit"`
	Description  string `json:"description"`
	ConfigJSON   string `json:"config_json"`
}

type indicatorSeriesCheckpointV2 struct {
	IndicatorKey    string                      `json:"indicator_key"`
	DefinitionDirty bool                        `json:"definition_dirty"`
	Buffer          indicatorBufferCheckpointV2 `json:"buffer"`
}

func (m *IndicatorSyncManager) CheckpointSessionV2(
	sessionID string,
) (*IndicatorSessionCheckpointV2, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("indicator checkpoint session_id is required")
	}
	state := m.lookupSession(sessionID)
	if state == nil {
		return nil, nil
	}
	state.flushMu.Lock()
	defer state.flushMu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	checkpoint := &IndicatorSessionCheckpointV2{
		SchemaVersion: indicatorSessionCheckpointSchemaV2,
		SessionID:     sessionID,
		UserID:        state.userIDV2,
		StrategyID:    state.strategyIDV2,
	}
	if state.hasIdentityV2 {
		checkpoint.IdentityPID = state.identityV2.PID
		checkpoint.IdentityGeneration = state.identityV2.Generation
	}
	streamKeys := make([]string, 0, len(state.streamsV2))
	for streamKey := range state.streamsV2 {
		streamKeys = append(streamKeys, streamKey)
	}
	sort.Strings(streamKeys)
	for _, streamKey := range streamKeys {
		stream := state.streamsV2[streamKey]
		if stream == nil {
			return nil, fmt.Errorf(
				"indicator checkpoint stream %q is nil",
				streamKey,
			)
		}
		saved := indicatorStreamCheckpointV2{
			StreamKey: streamKey,
			Clock: indicatorStreamClockCheckpointV2{
				NextSequence:    stream.clock.NextSequence,
				LastTimeMS:      stream.clock.LastTimeMS,
				IntervalMS:      stream.clock.IntervalMS,
				HasLast:         stream.clock.HasLast,
				LastPayloadHash: stream.clock.LastPayloadHash,
			},
		}
		for _, definition := range stream.definitions {
			if definition == nil {
				return nil, fmt.Errorf(
					"indicator checkpoint stream %q has nil definition",
					streamKey,
				)
			}
			saved.Definitions = append(
				saved.Definitions,
				checkpointIndicatorDefinitionV2(definition),
			)
			key := strings.TrimSpace(definition.GetIndicatorKey())
			series := stream.series[key]
			if series == nil || series.buffer == nil {
				return nil, fmt.Errorf(
					"indicator checkpoint stream %q series %q is missing",
					streamKey,
					key,
				)
			}
			saved.Series = append(
				saved.Series,
				indicatorSeriesCheckpointV2{
					IndicatorKey:    key,
					DefinitionDirty: series.definitionDirty,
					Buffer:          series.buffer.checkpoint(),
				},
			)
		}
		if len(stream.series) != len(saved.Series) {
			return nil, fmt.Errorf(
				"indicator checkpoint stream %q has undeclared series",
				streamKey,
			)
		}
		checkpoint.Streams = append(checkpoint.Streams, saved)
	}
	return checkpoint, nil
}

func (m *IndicatorSyncManager) RestoreSessionV2(
	checkpoint *IndicatorSessionCheckpointV2,
) error {
	if checkpoint == nil {
		return nil
	}
	if checkpoint.SchemaVersion != indicatorSessionCheckpointSchemaV2 {
		return fmt.Errorf(
			"indicator checkpoint schema = %d, want %d",
			checkpoint.SchemaVersion,
			indicatorSessionCheckpointSchemaV2,
		)
	}
	sessionID := strings.TrimSpace(checkpoint.SessionID)
	if sessionID == "" || sessionID != checkpoint.SessionID {
		return fmt.Errorf("indicator checkpoint session_id is invalid")
	}
	if checkpoint.UserID <= 0 || checkpoint.StrategyID <= 0 {
		return fmt.Errorf(
			"indicator checkpoint user_id and strategy_id must be positive",
		)
	}
	if checkpoint.IdentityGeneration == 0 {
		return fmt.Errorf(
			"indicator checkpoint worker generation is required",
		)
	}
	state := &indicatorSessionState{
		streamsV2: map[string]*indicatorStreamStateV2{},
		identityV2: WorkerIdentity{
			SessionID:  sessionID,
			PID:        checkpoint.IdentityPID,
			Generation: checkpoint.IdentityGeneration,
		},
		hasIdentityV2: true,
		userIDV2:      checkpoint.UserID,
		strategyIDV2:  checkpoint.StrategyID,
	}
	for _, savedStream := range checkpoint.Streams {
		streamKey := strings.TrimSpace(savedStream.StreamKey)
		if streamKey == "" || streamKey != savedStream.StreamKey {
			return fmt.Errorf("indicator checkpoint stream_key is invalid")
		}
		if _, exists := state.streamsV2[streamKey]; exists {
			return fmt.Errorf(
				"indicator checkpoint stream %q is duplicated",
				streamKey,
			)
		}
		clock, err := restoreIndicatorStreamClockV2(savedStream.Clock)
		if err != nil {
			return fmt.Errorf(
				"restore indicator checkpoint stream %q clock: %w",
				streamKey,
				err,
			)
		}
		definitions := make(
			[]*rwv1.IndicatorDefinition,
			0,
			len(savedStream.Definitions),
		)
		definitionByKey := make(map[string]*rwv1.IndicatorDefinition)
		for _, savedDefinition := range savedStream.Definitions {
			definition := restoreIndicatorDefinitionV2(savedDefinition)
			key := strings.TrimSpace(definition.GetIndicatorKey())
			if key == "" {
				return fmt.Errorf(
					"indicator checkpoint stream %q has empty definition key",
					streamKey,
				)
			}
			if _, exists := definitionByKey[key]; exists {
				return fmt.Errorf(
					"indicator checkpoint stream %q definition %q is duplicated",
					streamKey,
					key,
				)
			}
			definitionByKey[key] = definition
			definitions = append(definitions, definition)
		}
		if err := validateIndicatorDefinitionsV2(definitions); err != nil {
			return fmt.Errorf(
				"restore indicator checkpoint stream %q definitions: %w",
				streamKey,
				err,
			)
		}
		if len(savedStream.Series) != len(definitions) {
			return fmt.Errorf(
				"indicator checkpoint stream %q series count is invalid",
				streamKey,
			)
		}
		stream := &indicatorStreamStateV2{
			clock:       clock,
			definitions: cloneIndicatorDefinitionsV2(definitions),
			series:      map[string]*indicatorSeriesStateV2{},
		}
		for _, savedSeries := range savedStream.Series {
			key := strings.TrimSpace(savedSeries.IndicatorKey)
			definition := definitionByKey[key]
			if definition == nil {
				return fmt.Errorf(
					"indicator checkpoint stream %q series %q is undeclared",
					streamKey,
					key,
				)
			}
			if _, exists := stream.series[key]; exists {
				return fmt.Errorf(
					"indicator checkpoint stream %q series %q is duplicated",
					streamKey,
					key,
				)
			}
			buffer, err := restoreIndicatorBufferV2(savedSeries.Buffer)
			if err != nil {
				return fmt.Errorf(
					"restore indicator checkpoint stream %q series %q: %w",
					streamKey,
					key,
					err,
				)
			}
			if savedSeries.Buffer.Kind != definition.GetType() ||
				savedSeries.Buffer.NextSequence != clock.NextSequence ||
				savedSeries.Buffer.LastTimeMS != clock.LastTimeMS ||
				savedSeries.Buffer.IntervalMS != clock.IntervalMS ||
				savedSeries.Buffer.HasLast != clock.HasLast {
				return fmt.Errorf(
					"indicator checkpoint stream %q series %q clock diverged",
					streamKey,
					key,
				)
			}
			stream.series[key] = &indicatorSeriesStateV2{
				definition: restoreIndicatorDefinitionV2(
					checkpointIndicatorDefinitionV2(definition),
				),
				definitionDirty: savedSeries.DefinitionDirty,
				buffer:          buffer,
			}
		}
		state.streamsV2[streamKey] = stream
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.sessions[sessionID]; exists {
		return fmt.Errorf(
			"indicator checkpoint session %q is already active",
			sessionID,
		)
	}
	m.sessions[sessionID] = state
	return nil
}

func checkpointIndicatorDefinitionV2(
	definition *rwv1.IndicatorDefinition,
) indicatorDefinitionCheckpointV2 {
	return indicatorDefinitionCheckpointV2{
		IndicatorKey: definition.GetIndicatorKey(),
		Name:         definition.GetName(),
		Type:         definition.GetType(),
		Pane:         definition.GetPane(),
		Color:        definition.GetColor(),
		Unit:         definition.GetUnit(),
		Description:  definition.GetDescription(),
		ConfigJSON:   definition.GetConfigJson(),
	}
}

func restoreIndicatorDefinitionV2(
	saved indicatorDefinitionCheckpointV2,
) *rwv1.IndicatorDefinition {
	return &rwv1.IndicatorDefinition{
		IndicatorKey: saved.IndicatorKey,
		Name:         saved.Name,
		Type:         saved.Type,
		Pane:         saved.Pane,
		Color:        saved.Color,
		Unit:         saved.Unit,
		Description:  saved.Description,
		ConfigJson:   saved.ConfigJSON,
	}
}

func restoreIndicatorStreamClockV2(
	saved indicatorStreamClockCheckpointV2,
) (indicatorStreamClock, error) {
	if saved.HasLast != (saved.NextSequence > 0) {
		return indicatorStreamClock{}, fmt.Errorf(
			"last-state does not match next sequence",
		)
	}
	if saved.HasLast &&
		(saved.LastTimeMS <= 0 || saved.IntervalMS <= 0 ||
			saved.LastPayloadHash == [32]byte{}) {
		return indicatorStreamClock{}, fmt.Errorf(
			"active stream clock is incomplete",
		)
	}
	if !saved.HasLast &&
		(saved.LastTimeMS != 0 || saved.IntervalMS != 0 ||
			saved.LastPayloadHash != [32]byte{}) {
		return indicatorStreamClock{}, fmt.Errorf(
			"empty stream clock carries active state",
		)
	}
	return indicatorStreamClock{
		NextSequence:    saved.NextSequence,
		LastTimeMS:      saved.LastTimeMS,
		IntervalMS:      saved.IntervalMS,
		HasLast:         saved.HasLast,
		LastPayloadHash: saved.LastPayloadHash,
	}, nil
}
