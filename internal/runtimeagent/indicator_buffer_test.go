package runtimeagent

import "testing"

func TestIndicatorBufferSealsChunkAtLimitAndKeepsNewOpenChunk(t *testing.T) {
	buf := NewIndicatorBuffer(3)
	for i := 0; i < 3; i++ {
		got := buf.AddPoint(IndicatorPoint{MarketTimeMS: int64(i), IntervalMS: 60000, ValueJSON: "1"})
		if got.Sealed != (i == 2) {
			t.Fatalf("point %d sealed = %v, want %v", i, got.Sealed, i == 2)
		}
	}

	snapshot := buf.SnapshotDirtyForFlush()
	if len(snapshot.Finals) != 1 {
		t.Fatalf("final chunks len = %d, want 1", len(snapshot.Finals))
	}
	if !snapshot.Finals[0].Finalized {
		t.Fatalf("sealed chunk Finalized = false, want true")
	}
	if snapshot.Open.Count != 0 {
		t.Fatalf("dirty open count = %d, want 0", snapshot.Open.Count)
	}

	got := buf.AddPoint(IndicatorPoint{MarketTimeMS: 3, IntervalMS: 60000, ValueJSON: "2"})
	if got.Sealed {
		t.Fatal("first point in next chunk unexpectedly sealed")
	}
	snapshot = buf.SnapshotDirtyForFlush()
	if snapshot.Open.ChunkIndex != 1 || snapshot.Open.Count != 1 {
		t.Fatalf("open chunk = index %d count %d, want index 1 count 1", snapshot.Open.ChunkIndex, snapshot.Open.Count)
	}
}

func TestIndicatorBufferKeepsSealedChunkUntilAck(t *testing.T) {
	buf := NewIndicatorBuffer(2)
	buf.AddPoint(IndicatorPoint{MarketTimeMS: 1, IntervalMS: 60000, ValueJSON: "1"})
	buf.AddPoint(IndicatorPoint{MarketTimeMS: 2, IntervalMS: 60000, ValueJSON: "2"})

	snapshot := buf.SnapshotDirtyForFlush()
	if len(snapshot.Finals) != 1 {
		t.Fatalf("final chunks len = %d, want 1", len(snapshot.Finals))
	}

	snapshot = buf.SnapshotDirtyForFlush()
	if len(snapshot.Finals) != 1 {
		t.Fatalf("final chunks after no ack = %d, want 1", len(snapshot.Finals))
	}

	buf.MarkFlushAcked(snapshot)
	snapshot = buf.SnapshotDirtyForFlush()
	if len(snapshot.Finals) != 0 {
		t.Fatalf("final chunks after ack = %d, want 0", len(snapshot.Finals))
	}
}

func TestIndicatorBufferRejectsDuplicateAndOutOfOrderMarketTimes(t *testing.T) {
	b := NewIndicatorBuffer(1024)
	if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "2"}); got.Disposition != IndicatorPointAccepted {
		t.Fatalf("first = %+v", got)
	}
	if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "9"}); got.Disposition != IndicatorPointDuplicate {
		t.Fatalf("duplicate = %+v", got)
	}
	if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"}); got.Disposition != IndicatorPointOutOfOrder {
		t.Fatalf("out of order = %+v", got)
	}
	if got := b.SnapshotDirtyForFlush().Open; got.Count != 1 || got.ValuesJSON != `{"values":[2],"times":null}` {
		t.Fatalf("open = %+v", got)
	}
}

func TestIndicatorBufferAckDoesNotClearNewerGeneration(t *testing.T) {
	b := NewIndicatorBuffer(1024)
	b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"})
	old := b.SnapshotDirtyForFlush()
	b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "2"})
	b.MarkFlushAcked(old)
	if got := b.SnapshotDirtyForFlush().Open.Count; got != 2 {
		t.Fatalf("open count = %d, want 2", got)
	}
}

func TestIndicatorBufferSealsPartialOpenOnTerminal(t *testing.T) {
	b := NewIndicatorBuffer(1024)
	b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"})
	if !b.SealOpen() {
		t.Fatal("SealOpen returned false")
	}
	got := b.SnapshotDirtyForFlush()
	if len(got.Finals) != 1 || got.Finals[0].Count != 1 || !got.Finals[0].Finalized || got.Open.Count != 0 {
		t.Fatalf("snapshot = %+v", got)
	}
}
