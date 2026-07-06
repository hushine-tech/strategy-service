package runtimeagent

import "testing"

func TestIndicatorBufferSealsChunkAtLimitAndKeepsNewOpenChunk(t *testing.T) {
	buf := NewIndicatorBuffer(3)
	for i := 0; i < 3; i++ {
		buf.AddPoint(IndicatorPoint{MarketTimeMS: int64(i), IntervalMS: 60000, ValueJSON: "1"})
	}

	finals, open := buf.SnapshotForFlush()
	if len(finals) != 1 {
		t.Fatalf("final chunks len = %d, want 1", len(finals))
	}
	if !finals[0].Finalized {
		t.Fatalf("sealed chunk Finalized = false, want true")
	}
	if open.ChunkIndex != 1 || open.Count != 0 {
		t.Fatalf("open chunk = index %d count %d, want index 1 count 0", open.ChunkIndex, open.Count)
	}
}

func TestIndicatorBufferKeepsSealedChunkUntilAck(t *testing.T) {
	buf := NewIndicatorBuffer(2)
	buf.AddPoint(IndicatorPoint{MarketTimeMS: 1, IntervalMS: 60000, ValueJSON: "1"})
	buf.AddPoint(IndicatorPoint{MarketTimeMS: 2, IntervalMS: 60000, ValueJSON: "2"})

	finals, _ := buf.SnapshotForFlush()
	if len(finals) != 1 {
		t.Fatalf("final chunks len = %d, want 1", len(finals))
	}

	finals, _ = buf.SnapshotForFlush()
	if len(finals) != 1 {
		t.Fatalf("final chunks after no ack = %d, want 1", len(finals))
	}

	buf.MarkFinalizedAcked([]int{0})
	finals, _ = buf.SnapshotForFlush()
	if len(finals) != 0 {
		t.Fatalf("final chunks after ack = %d, want 0", len(finals))
	}
}
