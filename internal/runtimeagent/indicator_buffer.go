package runtimeagent

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

const indicatorChunkSize uint64 = 1024

type IndicatorMarkerValueV2 struct {
	Text     string
	Price    *float64
	Color    string
	Position string
	Shape    string
}

type IndicatorMarkerV2 struct {
	Sequence uint64
	Offset   uint32
	TimeMS   int64
	Text     string
	Price    *float64
	Color    string
	Position string
	Shape    string
}

type IndicatorChunkV2 struct {
	ChunkIndex    uint32
	StartSequence uint64
	EndSequence   uint64
	StartTimeMS   int64
	EndTimeMS     int64
	IntervalMS    int64
	Count         uint32
	TimesMS       []int64
	ScalarValues  []*float64
	Markers       []IndicatorMarkerV2
	Revision      uint64
	Finalized     bool
}

type IndicatorSaveTokenV2 struct {
	ChunkIndex  uint32
	Revision    uint64
	Count       uint32
	PayloadHash [32]byte
}

type IndicatorFinalizeTokenV2 struct {
	ChunkIndex       uint32
	ExpectedRevision uint64
}

type IndicatorFlushSnapshotV2 struct {
	Chunks []IndicatorChunkV2
	Tokens []IndicatorSaveTokenV2
}

type indicatorChunkStateV2 struct {
	chunk         IndicatorChunkV2
	ackedRevision uint64
}

type indicatorChunkCheckpointV2 struct {
	Chunk         IndicatorChunkV2 `json:"chunk"`
	AckedRevision uint64           `json:"acked_revision"`
}

type indicatorBufferCheckpointV2 struct {
	Kind         string                       `json:"kind"`
	NextSequence uint64                       `json:"next_sequence"`
	LastTimeMS   int64                        `json:"last_time_ms"`
	IntervalMS   int64                        `json:"interval_ms"`
	HasLast      bool                         `json:"has_last"`
	Chunks       []indicatorChunkCheckpointV2 `json:"chunks"`
}

type IndicatorBufferV2 struct {
	mu           sync.Mutex
	kind         string
	nextSequence uint64
	lastTimeMS   int64
	intervalMS   int64
	hasLast      bool
	chunks       map[uint32]*indicatorChunkStateV2
}

type indicatorBufferAppendV2 struct {
	buffer     *IndicatorBufferV2
	sequence   uint64
	timeMS     int64
	intervalMS int64
	scalar     *float64
	markers    []IndicatorMarkerValueV2
}

func NewIndicatorBufferV2(kind string) *IndicatorBufferV2 {
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind == "" {
		kind = "line"
	}
	return &IndicatorBufferV2{
		kind:   kind,
		chunks: map[uint32]*indicatorChunkStateV2{},
	}
}

func (b *IndicatorBufferV2) Append(
	sequence uint64,
	timeMS int64,
	intervalMS int64,
	scalar *float64,
	markers []IndicatorMarkerValueV2,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.validateAppendLocked(
		sequence,
		timeMS,
		intervalMS,
		scalar,
		markers,
	); err != nil {
		return err
	}
	b.appendLocked(sequence, timeMS, intervalMS, scalar, markers)
	return nil
}

func appendIndicatorBuffersV2(operations []indicatorBufferAppendV2) error {
	seen := make(map[*IndicatorBufferV2]struct{}, len(operations))
	for _, operation := range operations {
		if operation.buffer == nil {
			return fmt.Errorf("indicator buffer is nil")
		}
		if _, exists := seen[operation.buffer]; exists {
			return fmt.Errorf("indicator buffer appears more than once in frame")
		}
		seen[operation.buffer] = struct{}{}
		operation.buffer.mu.Lock()
	}
	defer func() {
		for index := len(operations) - 1; index >= 0; index-- {
			operations[index].buffer.mu.Unlock()
		}
	}()

	for _, operation := range operations {
		if err := operation.buffer.validateAppendLocked(
			operation.sequence,
			operation.timeMS,
			operation.intervalMS,
			operation.scalar,
			operation.markers,
		); err != nil {
			return err
		}
	}
	for _, operation := range operations {
		operation.buffer.appendLocked(
			operation.sequence,
			operation.timeMS,
			operation.intervalMS,
			operation.scalar,
			operation.markers,
		)
	}
	return nil
}

func (b *IndicatorBufferV2) validateAppendLocked(
	sequence uint64,
	timeMS int64,
	intervalMS int64,
	scalar *float64,
	markers []IndicatorMarkerValueV2,
) error {
	if sequence != b.nextSequence {
		return fmt.Errorf(
			"indicator buffer sequence = %d, want %d",
			sequence,
			b.nextSequence,
		)
	}
	if timeMS <= 0 || intervalMS <= 0 {
		return fmt.Errorf("indicator buffer time and interval must be positive")
	}
	if b.hasLast {
		if intervalMS != b.intervalMS {
			return fmt.Errorf(
				"indicator buffer interval = %d, want %d",
				intervalMS,
				b.intervalMS,
			)
		}
		if timeMS <= b.lastTimeMS {
			return fmt.Errorf(
				"indicator buffer time = %d, want greater than %d",
				timeMS,
				b.lastTimeMS,
			)
		}
	}
	switch b.kind {
	case "line", "histogram":
		if len(markers) != 0 {
			return fmt.Errorf("%s indicator cannot contain markers", b.kind)
		}
	case "marker":
		if scalar != nil {
			return fmt.Errorf("marker indicator cannot contain a scalar")
		}
	default:
		return fmt.Errorf("unsupported indicator type %q", b.kind)
	}
	return nil
}

func (b *IndicatorBufferV2) appendLocked(
	sequence uint64,
	timeMS int64,
	intervalMS int64,
	scalar *float64,
	markers []IndicatorMarkerValueV2,
) {
	chunkIndex := uint32(sequence / indicatorChunkSize)
	state := b.chunks[chunkIndex]
	if state == nil {
		state = &indicatorChunkStateV2{
			chunk: IndicatorChunkV2{
				ChunkIndex:    chunkIndex,
				StartSequence: uint64(chunkIndex) * indicatorChunkSize,
				IntervalMS:    intervalMS,
			},
		}
		b.chunks[chunkIndex] = state
	}
	chunk := &state.chunk
	if chunk.Count == 0 {
		chunk.StartTimeMS = timeMS
	}
	chunk.EndSequence = sequence
	chunk.EndTimeMS = timeMS
	chunk.Count++
	chunk.TimesMS = append(chunk.TimesMS, timeMS)
	if b.kind == "marker" {
		offset := uint32(sequence - chunk.StartSequence)
		for _, marker := range markers {
			chunk.Markers = append(chunk.Markers, IndicatorMarkerV2{
				Sequence: sequence,
				Offset:   offset,
				TimeMS:   timeMS,
				Text:     marker.Text,
				Price:    cloneFloat64(marker.Price),
				Color:    marker.Color,
				Position: marker.Position,
				Shape:    marker.Shape,
			})
		}
	} else {
		chunk.ScalarValues = append(chunk.ScalarValues, cloneFloat64(scalar))
	}
	chunk.Revision = uint64(len(chunk.TimesMS))
	if chunk.Count == uint32(indicatorChunkSize) {
		chunk.Finalized = true
	}

	b.nextSequence++
	b.lastTimeMS = timeMS
	b.intervalMS = intervalMS
	b.hasLast = true
}

func (b *IndicatorBufferV2) SealOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.chunks) == 0 {
		return false
	}
	if b.nextSequence > 0 {
		index := uint32((b.nextSequence - 1) / indicatorChunkSize)
		if state := b.chunks[index]; state != nil {
			state.chunk.Finalized = true
		}
	}
	return true
}

func (b *IndicatorBufferV2) SnapshotDirtyForFlush() IndicatorFlushSnapshotV2 {
	b.mu.Lock()
	defer b.mu.Unlock()

	indexes := make([]int, 0, len(b.chunks))
	for index := range b.chunks {
		indexes = append(indexes, int(index))
	}
	sort.Ints(indexes)
	snapshot := IndicatorFlushSnapshotV2{}
	for _, rawIndex := range indexes {
		state := b.chunks[uint32(rawIndex)]
		if state == nil || state.chunk.Revision <= state.ackedRevision {
			continue
		}
		chunk := cloneIndicatorChunkV2(state.chunk)
		hash := hashIndicatorChunkV2(chunk)
		snapshot.Chunks = append(snapshot.Chunks, chunk)
		snapshot.Tokens = append(snapshot.Tokens, IndicatorSaveTokenV2{
			ChunkIndex:  chunk.ChunkIndex,
			Revision:    chunk.Revision,
			Count:       chunk.Count,
			PayloadHash: hash,
		})
	}
	return snapshot
}

func (b *IndicatorBufferV2) MarkSaveAcked(token IndicatorSaveTokenV2) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.chunks[token.ChunkIndex]
	if state == nil || token.Revision == 0 || token.Revision > state.chunk.Revision {
		return
	}
	if token.Revision == state.chunk.Revision {
		if token.Count != state.chunk.Count ||
			token.PayloadHash != hashIndicatorChunkV2(state.chunk) {
			return
		}
	}
	if token.Revision > state.ackedRevision {
		state.ackedRevision = token.Revision
	}
}

func (b *IndicatorBufferV2) SnapshotFinalizations() []IndicatorFinalizeTokenV2 {
	b.mu.Lock()
	defer b.mu.Unlock()
	indexes := make([]int, 0, len(b.chunks))
	for index := range b.chunks {
		indexes = append(indexes, int(index))
	}
	sort.Ints(indexes)
	var out []IndicatorFinalizeTokenV2
	for _, rawIndex := range indexes {
		state := b.chunks[uint32(rawIndex)]
		if state == nil || !state.chunk.Finalized ||
			state.ackedRevision != state.chunk.Revision {
			continue
		}
		out = append(out, IndicatorFinalizeTokenV2{
			ChunkIndex:       state.chunk.ChunkIndex,
			ExpectedRevision: state.chunk.Revision,
		})
	}
	return out
}

func (b *IndicatorBufferV2) MarkFinalizeAcked(
	token IndicatorFinalizeTokenV2,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.chunks[token.ChunkIndex]
	if state == nil || !state.chunk.Finalized ||
		state.chunk.Revision != token.ExpectedRevision ||
		state.ackedRevision != token.ExpectedRevision {
		return
	}
	delete(b.chunks, token.ChunkIndex)
}

func (b *IndicatorBufferV2) HasPendingPersistence() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, state := range b.chunks {
		if state != nil &&
			(state.chunk.Revision > state.ackedRevision ||
				state.chunk.Finalized) {
			return true
		}
	}
	return false
}

func (b *IndicatorBufferV2) checkpoint() indicatorBufferCheckpointV2 {
	b.mu.Lock()
	defer b.mu.Unlock()
	checkpoint := indicatorBufferCheckpointV2{
		Kind:         b.kind,
		NextSequence: b.nextSequence,
		LastTimeMS:   b.lastTimeMS,
		IntervalMS:   b.intervalMS,
		HasLast:      b.hasLast,
	}
	indexes := make([]int, 0, len(b.chunks))
	for index := range b.chunks {
		indexes = append(indexes, int(index))
	}
	sort.Ints(indexes)
	for _, rawIndex := range indexes {
		state := b.chunks[uint32(rawIndex)]
		if state == nil {
			continue
		}
		checkpoint.Chunks = append(
			checkpoint.Chunks,
			indicatorChunkCheckpointV2{
				Chunk:         cloneIndicatorChunkV2(state.chunk),
				AckedRevision: state.ackedRevision,
			},
		)
	}
	return checkpoint
}

func restoreIndicatorBufferV2(
	checkpoint indicatorBufferCheckpointV2,
) (*IndicatorBufferV2, error) {
	kind := strings.TrimSpace(strings.ToLower(checkpoint.Kind))
	switch kind {
	case "line", "histogram", "marker":
	default:
		return nil, fmt.Errorf(
			"indicator checkpoint type %q is unsupported",
			checkpoint.Kind,
		)
	}
	if checkpoint.HasLast != (checkpoint.NextSequence > 0) {
		return nil, fmt.Errorf(
			"indicator checkpoint last-state does not match next sequence",
		)
	}
	if checkpoint.HasLast &&
		(checkpoint.LastTimeMS <= 0 || checkpoint.IntervalMS <= 0) {
		return nil, fmt.Errorf(
			"indicator checkpoint time and interval must be positive",
		)
	}
	if !checkpoint.HasLast &&
		(checkpoint.LastTimeMS != 0 || checkpoint.IntervalMS != 0) {
		return nil, fmt.Errorf(
			"empty indicator checkpoint carries stream time state",
		)
	}

	buffer := NewIndicatorBufferV2(kind)
	buffer.nextSequence = checkpoint.NextSequence
	buffer.lastTimeMS = checkpoint.LastTimeMS
	buffer.intervalMS = checkpoint.IntervalMS
	buffer.hasLast = checkpoint.HasLast
	var (
		previousIndex uint32
		haveIndex     bool
	)
	for _, saved := range checkpoint.Chunks {
		chunk := saved.Chunk
		if haveIndex && chunk.ChunkIndex <= previousIndex {
			return nil, fmt.Errorf(
				"indicator checkpoint chunk indexes are not strictly increasing",
			)
		}
		haveIndex = true
		previousIndex = chunk.ChunkIndex
		if err := validateIndicatorChunkCheckpointV2(
			kind,
			checkpoint,
			saved,
		); err != nil {
			return nil, err
		}
		buffer.chunks[chunk.ChunkIndex] = &indicatorChunkStateV2{
			chunk:         cloneIndicatorChunkV2(chunk),
			ackedRevision: saved.AckedRevision,
		}
	}
	if len(checkpoint.Chunks) > 0 {
		last := checkpoint.Chunks[len(checkpoint.Chunks)-1].Chunk
		if last.EndSequence+1 == checkpoint.NextSequence &&
			last.EndTimeMS != checkpoint.LastTimeMS {
			return nil, fmt.Errorf(
				"indicator checkpoint last time does not match open tail",
			)
		}
		if last.EndSequence >= checkpoint.NextSequence {
			return nil, fmt.Errorf(
				"indicator checkpoint chunk extends beyond stream sequence",
			)
		}
	}
	return buffer, nil
}

func validateIndicatorChunkCheckpointV2(
	kind string,
	buffer indicatorBufferCheckpointV2,
	saved indicatorChunkCheckpointV2,
) error {
	chunk := saved.Chunk
	if chunk.Count == 0 || chunk.Count > uint32(indicatorChunkSize) {
		return fmt.Errorf(
			"indicator checkpoint chunk %d count is invalid",
			chunk.ChunkIndex,
		)
	}
	wantStart := uint64(chunk.ChunkIndex) * indicatorChunkSize
	if chunk.StartSequence != wantStart ||
		chunk.EndSequence != wantStart+uint64(chunk.Count)-1 {
		return fmt.Errorf(
			"indicator checkpoint chunk %d sequence range is invalid",
			chunk.ChunkIndex,
		)
	}
	if chunk.Revision != uint64(chunk.Count) ||
		saved.AckedRevision > chunk.Revision {
		return fmt.Errorf(
			"indicator checkpoint chunk %d revision is invalid",
			chunk.ChunkIndex,
		)
	}
	if chunk.IntervalMS != buffer.IntervalMS ||
		len(chunk.TimesMS) != int(chunk.Count) ||
		chunk.StartTimeMS != chunk.TimesMS[0] ||
		chunk.EndTimeMS != chunk.TimesMS[len(chunk.TimesMS)-1] {
		return fmt.Errorf(
			"indicator checkpoint chunk %d time shape is invalid",
			chunk.ChunkIndex,
		)
	}
	for index, timeMS := range chunk.TimesMS {
		if timeMS <= 0 ||
			(index > 0 && timeMS <= chunk.TimesMS[index-1]) {
			return fmt.Errorf(
				"indicator checkpoint chunk %d times are invalid",
				chunk.ChunkIndex,
			)
		}
	}
	switch kind {
	case "line", "histogram":
		if len(chunk.ScalarValues) != int(chunk.Count) ||
			len(chunk.Markers) != 0 {
			return fmt.Errorf(
				"indicator checkpoint chunk %d scalar shape is invalid",
				chunk.ChunkIndex,
			)
		}
		for _, value := range chunk.ScalarValues {
			if value != nil && !math.IsNaN(*value) &&
				!math.IsInf(*value, 0) {
				continue
			}
			if value != nil {
				return fmt.Errorf(
					"indicator checkpoint chunk %d scalar is not finite",
					chunk.ChunkIndex,
				)
			}
		}
	case "marker":
		if len(chunk.ScalarValues) != 0 {
			return fmt.Errorf(
				"indicator checkpoint chunk %d marker scalar shape is invalid",
				chunk.ChunkIndex,
			)
		}
		for _, marker := range chunk.Markers {
			if marker.Sequence < chunk.StartSequence ||
				marker.Sequence > chunk.EndSequence ||
				marker.Offset !=
					uint32(marker.Sequence-chunk.StartSequence) ||
				marker.TimeMS != chunk.TimesMS[marker.Offset] ||
				(marker.Price != nil &&
					(math.IsNaN(*marker.Price) ||
						math.IsInf(*marker.Price, 0))) {
				return fmt.Errorf(
					"indicator checkpoint chunk %d marker is invalid",
					chunk.ChunkIndex,
				)
			}
		}
	}
	return nil
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneIndicatorChunkV2(chunk IndicatorChunkV2) IndicatorChunkV2 {
	cloned := chunk
	cloned.TimesMS = append([]int64(nil), chunk.TimesMS...)
	if chunk.ScalarValues != nil {
		cloned.ScalarValues = make([]*float64, len(chunk.ScalarValues))
		for index, value := range chunk.ScalarValues {
			cloned.ScalarValues[index] = cloneFloat64(value)
		}
	}
	cloned.Markers = append([]IndicatorMarkerV2(nil), chunk.Markers...)
	for index := range cloned.Markers {
		cloned.Markers[index].Price = cloneFloat64(cloned.Markers[index].Price)
	}
	return cloned
}

func hashIndicatorChunkV2(chunk IndicatorChunkV2) [32]byte {
	raw, err := json.Marshal(chunk)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}
