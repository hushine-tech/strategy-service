package runtimeagent

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type IndicatorPoint struct {
	MarketTimeMS int64
	IntervalMS   int64
	ValueJSON    string
}

type IndicatorChunk struct {
	ChunkIndex  int
	StartTimeMS int64
	EndTimeMS   int64
	IntervalMS  int64
	Count       int
	ValuesJSON  string
	Finalized   bool
}

type IndicatorPointDisposition int

const (
	IndicatorPointAccepted IndicatorPointDisposition = iota
	IndicatorPointDuplicate
	IndicatorPointOutOfOrder
)

type IndicatorAddResult struct {
	Disposition IndicatorPointDisposition
	Sealed      bool
}

type IndicatorFlushSnapshot struct {
	Finals         []IndicatorChunk
	Open           IndicatorChunk
	OpenGeneration uint64
}

type IndicatorBuffer struct {
	mu               sync.Mutex
	limit            int
	kind             string
	active           IndicatorChunk
	activeRaw        []string
	pending          map[int]IndicatorChunk
	dirty            bool
	generation       uint64
	lastMarketTimeMS int64
	hasMarketTime    bool
}

func NewIndicatorBuffer(limit int) *IndicatorBuffer {
	return NewIndicatorBufferForType(limit, "line")
}

func NewIndicatorBufferForType(limit int, kind string) *IndicatorBuffer {
	if limit <= 0 {
		limit = 1024
	}
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind == "" {
		kind = "line"
	}
	return &IndicatorBuffer{
		limit:   limit,
		kind:    kind,
		pending: map[int]IndicatorChunk{},
	}
}

func (b *IndicatorBuffer) AddPoint(point IndicatorPoint) IndicatorAddResult {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.hasMarketTime {
		if point.MarketTimeMS == b.lastMarketTimeMS {
			return IndicatorAddResult{Disposition: IndicatorPointDuplicate}
		}
		if point.MarketTimeMS < b.lastMarketTimeMS {
			return IndicatorAddResult{Disposition: IndicatorPointOutOfOrder}
		}
	}
	b.lastMarketTimeMS = point.MarketTimeMS
	b.hasMarketTime = true
	b.generation++
	b.dirty = true

	if b.active.Count == 0 {
		b.active.StartTimeMS = point.MarketTimeMS
		b.active.IntervalMS = point.IntervalMS
	}
	b.active.EndTimeMS = point.MarketTimeMS
	b.active.Count++
	if b.kind == "marker" {
		b.activeRaw = append(b.activeRaw, markersWithOffset(point.ValueJSON, b.active.Count-1)...)
		b.active.ValuesJSON = `{"markers":[` + strings.Join(b.activeRaw, ",") + `]}`
	} else {
		b.activeRaw = append(b.activeRaw, normalizeJSONValue(point.ValueJSON))
		b.active.ValuesJSON = `{"values":[` + strings.Join(b.activeRaw, ",") + `],"times":null}`
	}

	if b.active.Count < b.limit {
		return IndicatorAddResult{Disposition: IndicatorPointAccepted}
	}

	b.sealOpenLocked()
	return IndicatorAddResult{Disposition: IndicatorPointAccepted, Sealed: true}
}

func (b *IndicatorBuffer) SnapshotDirtyForFlush() IndicatorFlushSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshot := IndicatorFlushSnapshot{
		Finals:         b.pendingChunksLocked(),
		OpenGeneration: b.generation,
	}
	if b.dirty && b.active.Count > 0 {
		snapshot.Open = b.active
		snapshot.Open.Finalized = false
	}
	return snapshot
}

func (b *IndicatorBuffer) SealOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.active.Count == 0 {
		return false
	}
	b.generation++
	b.sealOpenLocked()
	return true
}

func (b *IndicatorBuffer) MarkFlushAcked(snapshot IndicatorFlushSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, chunk := range snapshot.Finals {
		delete(b.pending, chunk.ChunkIndex)
	}
	if snapshot.Open.Count > 0 &&
		snapshot.OpenGeneration == b.generation &&
		snapshot.Open.ChunkIndex == b.active.ChunkIndex {
		b.dirty = false
	}
}

// SnapshotForFlush is kept until all callers move to dirty snapshots.
func (b *IndicatorBuffer) SnapshotForFlush() ([]IndicatorChunk, IndicatorChunk) {
	b.mu.Lock()
	defer b.mu.Unlock()

	open := b.active
	open.Finalized = false
	return b.pendingChunksLocked(), open
}

// MarkFinalizedAcked is kept until all callers move to exact snapshot ACKs.
func (b *IndicatorBuffer) MarkFinalizedAcked(indexes []int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, index := range indexes {
		delete(b.pending, index)
	}
}

func (b *IndicatorBuffer) pendingChunksLocked() []IndicatorChunk {
	indexes := make([]int, 0, len(b.pending))
	for index := range b.pending {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	finals := make([]IndicatorChunk, 0, len(indexes))
	for _, index := range indexes {
		finals = append(finals, b.pending[index])
	}
	return finals
}

func (b *IndicatorBuffer) sealOpenLocked() {
	sealed := b.active
	sealed.Finalized = true
	b.pending[sealed.ChunkIndex] = sealed
	b.active = IndicatorChunk{ChunkIndex: sealed.ChunkIndex + 1}
	b.activeRaw = nil
	b.dirty = false
}

func normalizeJSONValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "null"
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	if value == "null" {
		return value
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(raw)
}

func markersWithOffset(value string, offset int) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var markers []map[string]any
	if err := json.Unmarshal([]byte(value), &markers); err != nil {
		var marker map[string]any
		if err := json.Unmarshal([]byte(value), &marker); err != nil {
			return nil
		}
		markers = []map[string]any{marker}
	}
	out := make([]string, 0, len(markers))
	for _, marker := range markers {
		if marker == nil {
			continue
		}
		marker["offset"] = offset
		raw, err := json.Marshal(marker)
		if err != nil {
			continue
		}
		out = append(out, string(raw))
	}
	return out
}
