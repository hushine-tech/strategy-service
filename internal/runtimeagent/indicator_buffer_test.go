package runtimeagent

import (
	"fmt"
	"testing"
)

func float64Ptr(value float64) *float64 {
	return &value
}

func TestIndicatorBufferV2DeterministicChunkCounts(t *testing.T) {
	tests := []struct {
		bars   int
		counts []uint32
	}{
		{bars: 1, counts: []uint32{1}},
		{bars: 1023, counts: []uint32{1023}},
		{bars: 1024, counts: []uint32{1024}},
		{bars: 1025, counts: []uint32{1024, 1}},
		{bars: 2049, counts: []uint32{1024, 1024, 1}},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("bars_%d", test.bars), func(t *testing.T) {
			buffer := NewIndicatorBufferV2("line")
			for sequence := 0; sequence < test.bars; sequence++ {
				value := float64(sequence)
				if err := buffer.Append(
					uint64(sequence),
					int64(1_000+sequence*60_000),
					60_000,
					&value,
					nil,
				); err != nil {
					t.Fatalf("append sequence %d: %v", sequence, err)
				}
			}
			if !buffer.SealOpen() {
				t.Fatal("SealOpen returned false")
			}
			snapshot := buffer.SnapshotDirtyForFlush()
			if len(snapshot.Chunks) != len(test.counts) {
				t.Fatalf(
					"chunk count = %d, want %d",
					len(snapshot.Chunks),
					len(test.counts),
				)
			}
			for index, wantCount := range test.counts {
				chunk := snapshot.Chunks[index]
				if chunk.ChunkIndex != uint32(index) ||
					uint32(len(chunk.TimesMS)) != wantCount ||
					chunk.Revision != uint64(wantCount) ||
					!chunk.Finalized {
					t.Fatalf("chunk[%d] = %+v, want count=%d finalized", index, chunk, wantCount)
				}
			}
		})
	}
}

func TestIndicatorBufferV2AdvancesSparseScalarAndMarkerSeriesPerBar(t *testing.T) {
	scalars := NewIndicatorBufferV2("line")
	markers := NewIndicatorBufferV2("marker")
	var expectedTimes []int64
	for sequence := uint64(0); sequence < 10; sequence++ {
		timeMS := int64(1_000 + sequence*60_000)
		if sequence >= 5 {
			timeMS += 120_000
		}
		expectedTimes = append(expectedTimes, timeMS)
		var scalar *float64
		if sequence%2 == 0 {
			scalar = float64Ptr(float64(sequence))
		}
		if err := scalars.Append(sequence, timeMS, 60_000, scalar, nil); err != nil {
			t.Fatalf("append scalar sequence %d: %v", sequence, err)
		}
		var markerValues []IndicatorMarkerValueV2
		switch sequence {
		case 4:
			markerValues = []IndicatorMarkerValueV2{{Text: "BUY"}}
		case 9:
			markerValues = []IndicatorMarkerValueV2{
				{Text: "SELL"},
				{Text: "EXIT", Price: float64Ptr(123.5)},
			}
		}
		if err := markers.Append(sequence, timeMS, 60_000, nil, markerValues); err != nil {
			t.Fatalf("append marker sequence %d: %v", sequence, err)
		}
	}

	scalarChunk := scalars.SnapshotDirtyForFlush().Chunks[0]
	markerChunk := markers.SnapshotDirtyForFlush().Chunks[0]
	if scalarChunk.Count != 10 || markerChunk.Count != 10 {
		t.Fatalf("counts scalar=%d marker=%d, want 10", scalarChunk.Count, markerChunk.Count)
	}
	for index, wantTime := range expectedTimes {
		if scalarChunk.TimesMS[index] != wantTime || markerChunk.TimesMS[index] != wantTime {
			t.Fatalf("time[%d] scalar=%d marker=%d want=%d", index, scalarChunk.TimesMS[index], markerChunk.TimesMS[index], wantTime)
		}
		if index%2 == 0 {
			if scalarChunk.ScalarValues[index] == nil ||
				*scalarChunk.ScalarValues[index] != float64(index) {
				t.Fatalf("scalar[%d] = %v", index, scalarChunk.ScalarValues[index])
			}
		} else if scalarChunk.ScalarValues[index] != nil {
			t.Fatalf("scalar[%d] = %v, want nil", index, scalarChunk.ScalarValues[index])
		}
	}
	if len(markerChunk.ScalarValues) != 0 {
		t.Fatalf("marker scalar slots = %d, want 0", len(markerChunk.ScalarValues))
	}
	if len(markerChunk.Markers) != 3 {
		t.Fatalf("markers = %+v", markerChunk.Markers)
	}
	for index, want := range []struct {
		sequence uint64
		offset   uint32
		timeMS   int64
	}{
		{sequence: 4, offset: 4, timeMS: expectedTimes[4]},
		{sequence: 9, offset: 9, timeMS: expectedTimes[9]},
		{sequence: 9, offset: 9, timeMS: expectedTimes[9]},
	} {
		got := markerChunk.Markers[index]
		if got.Sequence != want.sequence || got.Offset != want.offset || got.TimeMS != want.timeMS {
			t.Fatalf("marker[%d] = %+v, want %+v", index, got, want)
		}
	}
}

func TestIndicatorBufferV2FlushAt1023ThenTwoBarsKeepsBoundaryAndNewOpen(t *testing.T) {
	buffer := NewIndicatorBufferV2("line")
	for sequence := uint64(0); sequence < 1023; sequence++ {
		if err := buffer.Append(sequence, int64(sequence+1)*60_000, 60_000, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	first := buffer.SnapshotDirtyForFlush()
	if len(first.Chunks) != 1 || first.Chunks[0].Count != 1023 ||
		first.Chunks[0].Finalized {
		t.Fatalf("first snapshot = %+v", first.Chunks)
	}
	buffer.MarkSaveAcked(first.Tokens[0])

	for sequence := uint64(1023); sequence < 1025; sequence++ {
		if err := buffer.Append(sequence, int64(sequence+1)*60_000, 60_000, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	next := buffer.SnapshotDirtyForFlush()
	if len(next.Chunks) != 2 {
		t.Fatalf("next chunks = %+v", next.Chunks)
	}
	if next.Chunks[0].ChunkIndex != 0 || next.Chunks[0].Count != 1024 ||
		!next.Chunks[0].Finalized {
		t.Fatalf("sealed chunk = %+v", next.Chunks[0])
	}
	if next.Chunks[1].ChunkIndex != 1 || next.Chunks[1].Count != 1 ||
		next.Chunks[1].Finalized {
		t.Fatalf("open chunk = %+v", next.Chunks[1])
	}
}

func TestIndicatorBufferV2FinalizesOnlyTheSavedRevision(t *testing.T) {
	buffer := NewIndicatorBufferV2("line")
	for sequence := uint64(0); sequence < 1024; sequence++ {
		if err := buffer.Append(
			sequence,
			int64(sequence+1)*60_000,
			60_000,
			nil,
			nil,
		); err != nil {
			t.Fatal(err)
		}
	}
	save := buffer.SnapshotDirtyForFlush()
	if got := buffer.SnapshotFinalizations(); len(got) != 0 {
		t.Fatalf("finalization preceded save ACK: %+v", got)
	}
	buffer.MarkSaveAcked(save.Tokens[0])
	finalizations := buffer.SnapshotFinalizations()
	if len(finalizations) != 1 ||
		finalizations[0].ChunkIndex != 0 ||
		finalizations[0].ExpectedRevision != 1024 {
		t.Fatalf("finalizations = %+v", finalizations)
	}
	buffer.MarkFinalizeAcked(finalizations[0])
	if got := buffer.SnapshotDirtyForFlush(); len(got.Chunks) != 0 {
		t.Fatalf("finalized chunk remained dirty: %+v", got.Chunks)
	}
	if got := buffer.SnapshotFinalizations(); len(got) != 0 {
		t.Fatalf("finalized chunk remained pending: %+v", got)
	}
}

func TestIndicatorBufferV2MarkerSaveAckDoesNotRemainDirty(t *testing.T) {
	buffer := NewIndicatorBufferV2("marker")
	if err := buffer.Append(0, 60_000, 60_000, nil, nil); err != nil {
		t.Fatal(err)
	}
	save := buffer.SnapshotDirtyForFlush()
	if len(save.Tokens) != 1 {
		t.Fatalf("save tokens = %+v, want one", save.Tokens)
	}
	buffer.MarkSaveAcked(save.Tokens[0])
	if dirty := buffer.SnapshotDirtyForFlush(); len(dirty.Chunks) != 0 {
		t.Fatalf("acknowledged marker chunk remained dirty: %+v", dirty.Chunks)
	}
}

func TestIndicatorBufferV2CheckpointRoundTripPreservesRetryState(t *testing.T) {
	buffer := NewIndicatorBufferV2("line")
	for sequence := uint64(0); sequence < 1025; sequence++ {
		if err := buffer.Append(
			sequence,
			int64(sequence+1)*60_000,
			60_000,
			float64Ptr(float64(sequence)),
			nil,
		); err != nil {
			t.Fatalf("append sequence %d: %v", sequence, err)
		}
	}
	save := buffer.SnapshotDirtyForFlush()
	if len(save.Tokens) != 2 {
		t.Fatalf("save tokens = %+v, want two chunks", save.Tokens)
	}
	buffer.MarkSaveAcked(save.Tokens[0])

	checkpoint := buffer.checkpoint()
	restored, err := restoreIndicatorBufferV2(checkpoint)
	if err != nil {
		t.Fatalf("restore checkpoint: %v", err)
	}
	finalizations := restored.SnapshotFinalizations()
	if len(finalizations) != 1 ||
		finalizations[0].ChunkIndex != 0 ||
		finalizations[0].ExpectedRevision != 1024 {
		t.Fatalf("restored finalizations = %+v", finalizations)
	}
	dirty := restored.SnapshotDirtyForFlush()
	if len(dirty.Chunks) != 1 ||
		dirty.Chunks[0].ChunkIndex != 1 ||
		dirty.Chunks[0].Count != 1 {
		t.Fatalf("restored dirty chunks = %+v", dirty.Chunks)
	}
	if err := restored.Append(
		1025,
		1026*60_000,
		60_000,
		float64Ptr(1025),
		nil,
	); err != nil {
		t.Fatalf("append after restore: %v", err)
	}
}
