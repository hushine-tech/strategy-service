package runtimeagent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type indicatorSyncPlatform struct {
	mu            sync.Mutex
	requests      []*portfoliov1.SaveStrategyIndicatorsRequest
	failures      int
	active        int
	maxConcurrent int
	started       chan struct{}
	release       chan struct{}
}

func (p *indicatorSyncPlatform) InvokePlatformAny(_ context.Context, method string, packed *anypb.Any, _ time.Duration) (*anypb.Any, error) {
	if method != "portfolio.SaveStrategyIndicators" {
		return nil, errors.New("unexpected method: " + method)
	}
	request := &portfoliov1.SaveStrategyIndicatorsRequest{}
	if err := packed.UnmarshalTo(request); err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.requests = append(p.requests, proto.Clone(request).(*portfoliov1.SaveStrategyIndicatorsRequest))
	p.active++
	if p.active > p.maxConcurrent {
		p.maxConcurrent = p.active
	}
	fail := p.failures > 0
	if fail {
		p.failures--
	}
	started := p.started
	release := p.release
	p.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}

	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	if fail {
		return nil, errors.New("database unavailable")
	}
	return &anypb.Any{}, nil
}

func (p *indicatorSyncPlatform) snapshot() ([]*portfoliov1.SaveStrategyIndicatorsRequest, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	requests := make([]*portfoliov1.SaveStrategyIndicatorsRequest, len(p.requests))
	copy(requests, p.requests)
	return requests, p.maxConcurrent
}

func newIndicatorSyncManager(platform PlatformInvoker, limit int) *IndicatorSyncManager {
	return NewIndicatorSyncManager(IndicatorSyncConfig{
		PlatformInvoker: platform,
		IndicatorLimit:  limit,
		FlushInterval:   time.Hour,
		RequestTimeout:  time.Second,
		FinalizeTimeout: time.Second,
		RetryInitial:    time.Millisecond,
		RetryMax:        2 * time.Millisecond,
	})
}

func indicatorSyncFrame(sessionID string, marketTimeMS int64) *rwv1.IndicatorFrame {
	return &rwv1.IndicatorFrame{
		SessionId: sessionID, StreamKey: "binance:perpetual_futures:TESTUSDT:1m",
		MarketTimeMs: marketTimeMS, IntervalMs: 60_000, UserId: 7, StrategyId: 11,
		Definitions: []*rwv1.IndicatorDefinition{{
			IndicatorKey: "alpha_score", Name: "Alpha Score", Type: "line", Pane: "strategy", ConfigJson: "{}",
		}},
		Values: []*rwv1.IndicatorValue{{IndicatorKey: "alpha_score", Value: float64(marketTimeMS), HasValue: true}},
	}
}

func TestIndicatorSyncManagerReceiveDoesNotCallPlatform(t *testing.T) {
	platform := &indicatorSyncPlatform{}
	manager := newIndicatorSyncManager(platform, 1024)
	if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", 1000)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}
	if requests, _ := platform.snapshot(); len(requests) != 0 {
		t.Fatalf("platform calls = %d, want 0", len(requests))
	}
}

func TestIndicatorSyncManagerFlushesOnlyDirtyOpenChunk(t *testing.T) {
	platform := &indicatorSyncPlatform{}
	manager := newIndicatorSyncManager(platform, 1024)
	if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", 1000)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}
	if err := manager.FlushSession(context.Background(), "sess-1", false); err != nil {
		t.Fatalf("first FlushSession: %v", err)
	}
	if err := manager.FlushSession(context.Background(), "sess-1", false); err != nil {
		t.Fatalf("second FlushSession: %v", err)
	}
	requests, _ := platform.snapshot()
	if len(requests) != 1 || len(requests[0].GetChunks()) != 1 || requests[0].GetChunks()[0].GetCount() != 1 {
		t.Fatalf("requests = %+v, want one open chunk call", requests)
	}
}

func TestIndicatorSyncManagerRetainsFinalizedChunkUntilAck(t *testing.T) {
	platform := &indicatorSyncPlatform{failures: 1}
	manager := newIndicatorSyncManager(platform, 2)
	for _, marketTime := range []int64{1000, 2000} {
		if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", marketTime)); err != nil {
			t.Fatalf("ReceiveFrame: %v", err)
		}
	}
	if err := manager.FlushSession(context.Background(), "sess-1", false); err == nil {
		t.Fatal("first FlushSession succeeded, want failure")
	}
	if err := manager.FlushSession(context.Background(), "sess-1", false); err != nil {
		t.Fatalf("second FlushSession: %v", err)
	}
	requests, _ := platform.snapshot()
	if len(requests) != 2 {
		t.Fatalf("platform calls = %d, want 2", len(requests))
	}
	for index, request := range requests {
		if len(request.GetChunks()) != 1 || !request.GetChunks()[0].GetFinalized() || request.GetChunks()[0].GetCount() != 2 {
			t.Fatalf("request %d chunks = %+v", index, request.GetChunks())
		}
	}
}

func TestIndicatorSyncManagerImmediateFlushesFullChunk(t *testing.T) {
	platform := &indicatorSyncPlatform{started: make(chan struct{}, 2)}
	manager := newIndicatorSyncManager(platform, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)
	for _, marketTime := range []int64{1000, 2000} {
		if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", marketTime)); err != nil {
			t.Fatalf("ReceiveFrame: %v", err)
		}
	}
	select {
	case <-platform.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for immediate full-chunk flush")
	}
	requests, _ := platform.snapshot()
	if len(requests) != 1 || len(requests[0].GetChunks()) != 1 || !requests[0].GetChunks()[0].GetFinalized() {
		t.Fatalf("requests = %+v, want one finalized chunk", requests)
	}
}

func TestIndicatorSyncManagerFinalizesPartialOpenChunk(t *testing.T) {
	platform := &indicatorSyncPlatform{}
	manager := newIndicatorSyncManager(platform, 1024)
	if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", 1000)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}
	if err := manager.FinalizeSession(context.Background(), "sess-1"); err != nil {
		t.Fatalf("FinalizeSession: %v", err)
	}
	requests, _ := platform.snapshot()
	if len(requests) != 1 || len(requests[0].GetChunks()) != 1 || !requests[0].GetChunks()[0].GetFinalized() || requests[0].GetChunks()[0].GetCount() != 1 {
		t.Fatalf("requests = %+v, want one finalized partial chunk", requests)
	}
}

func TestIndicatorSyncManagerSerializesPeriodicAndFinalFlush(t *testing.T) {
	release := make(chan struct{})
	platform := &indicatorSyncPlatform{started: make(chan struct{}, 2), release: release}
	manager := newIndicatorSyncManager(platform, 1024)
	if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", 1000)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}
	periodicDone := make(chan error, 1)
	go func() { periodicDone <- manager.FlushSession(context.Background(), "sess-1", false) }()
	select {
	case <-platform.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first flush")
	}
	finalDone := make(chan error, 1)
	go func() { finalDone <- manager.FinalizeSession(context.Background(), "sess-1") }()
	time.Sleep(20 * time.Millisecond)
	if _, maxConcurrent := platform.snapshot(); maxConcurrent != 1 {
		t.Fatalf("max concurrent platform calls = %d, want 1", maxConcurrent)
	}
	close(release)
	if err := <-periodicDone; err != nil {
		t.Fatalf("periodic flush: %v", err)
	}
	if err := <-finalDone; err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if _, maxConcurrent := platform.snapshot(); maxConcurrent != 1 {
		t.Fatalf("final max concurrent platform calls = %d, want 1", maxConcurrent)
	}
}

func TestIndicatorSyncManagerRetriesFinalFlushWithinDeadline(t *testing.T) {
	platform := &indicatorSyncPlatform{failures: 2}
	manager := newIndicatorSyncManager(platform, 1024)
	if err := manager.ReceiveFrame(indicatorSyncFrame("sess-1", 1000)); err != nil {
		t.Fatalf("ReceiveFrame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.FinalizeSession(ctx, "sess-1"); err != nil {
		t.Fatalf("FinalizeSession: %v", err)
	}
	requests, _ := platform.snapshot()
	if len(requests) != 3 {
		t.Fatalf("platform calls = %d, want 3", len(requests))
	}
}
