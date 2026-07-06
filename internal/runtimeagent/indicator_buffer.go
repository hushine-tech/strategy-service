package runtimeagent

import (
	"sort"
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

type IndicatorBuffer struct {
	mu        sync.Mutex
	limit     int
	active    IndicatorChunk
	activeRaw []string
	pending   map[int]IndicatorChunk
}

func NewIndicatorBuffer(limit int) *IndicatorBuffer {
	if limit <= 0 {
		limit = 1024
	}
	return &IndicatorBuffer{
		limit:   limit,
		pending: map[int]IndicatorChunk{},
	}
}

func (b *IndicatorBuffer) AddPoint(point IndicatorPoint) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.active.Count == 0 {
		b.active.StartTimeMS = point.MarketTimeMS
		b.active.IntervalMS = point.IntervalMS
	}
	b.active.EndTimeMS = point.MarketTimeMS
	b.active.Count++
	b.activeRaw = append(b.activeRaw, normalizeJSONValue(point.ValueJSON))
	b.active.ValuesJSON = "[" + strings.Join(b.activeRaw, ",") + "]"

	if b.active.Count < b.limit {
		return false
	}

	sealed := b.active
	sealed.Finalized = true
	b.pending[sealed.ChunkIndex] = sealed
	b.active = IndicatorChunk{ChunkIndex: sealed.ChunkIndex + 1}
	b.activeRaw = nil
	return true
}

func (b *IndicatorBuffer) SnapshotForFlush() ([]IndicatorChunk, IndicatorChunk) {
	b.mu.Lock()
	defer b.mu.Unlock()

	indexes := make([]int, 0, len(b.pending))
	for index := range b.pending {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	finals := make([]IndicatorChunk, 0, len(indexes))
	for _, index := range indexes {
		finals = append(finals, b.pending[index])
	}
	open := b.active
	open.Finalized = false
	return finals, open
}

func (b *IndicatorBuffer) MarkFinalizedAcked(indexes []int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, index := range indexes {
		delete(b.pending, index)
	}
}

func normalizeJSONValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "null"
	}
	return value
}
