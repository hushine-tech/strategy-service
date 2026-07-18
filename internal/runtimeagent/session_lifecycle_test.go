package runtimeagent

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type recordingTerminalIndicators struct {
	events      *[]string
	finalizeErr error
}

func (r *recordingTerminalIndicators) FinalizeSession(context.Context, string) error {
	*r.events = append(*r.events, "finalize-indicator-tail")
	return r.finalizeErr
}

func (r *recordingTerminalIndicators) ForgetSession(context.Context, string) {
	*r.events = append(*r.events, "forget-indicator-tail")
}

func TestSessionLifecyclePublishesSpotStopOnlyAfterIndicatorTail(t *testing.T) {
	events := []string{"close-admission"}
	indicators := &recordingTerminalIndicators{events: &events}
	lifecycle := NewSessionLifecycle(indicators, func(_ context.Context, request TerminalRequest) error {
		events = append(events, "update-session:"+request.Status+":"+request.ReconciliationRunID)
		return nil
	})

	err := lifecycle.Complete(context.Background(), TerminalRequest{
		SessionID: "sess-1", Status: "stopped", BarsProcessed: 17,
		ReconciliationRunID: "recon-123",
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := []string{
		"close-admission",
		"finalize-indicator-tail",
		"update-session:stopped:recon-123",
		"forget-indicator-tail",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestSessionLifecycleFinalizationFailurePublishesRecoverableAndRetainsTail(t *testing.T) {
	events := []string{"close-admission"}
	indicators := &recordingTerminalIndicators{
		events: &events, finalizeErr: errors.New("database unavailable"),
	}
	var published TerminalRequest
	lifecycle := NewSessionLifecycle(indicators, func(_ context.Context, request TerminalRequest) error {
		published = request
		events = append(events, "update-session:"+request.Status+":"+request.ReconciliationRunID)
		return nil
	})

	err := lifecycle.Complete(context.Background(), TerminalRequest{
		SessionID: "sess-1", Status: "stopped", BarsProcessed: 17,
		ReconciliationRunID: "recon-123",
	})
	var finalizationErr *IndicatorFinalizationError
	if !errors.As(err, &finalizationErr) {
		t.Fatalf("Complete error = %v, want IndicatorFinalizationError", err)
	}
	if published.Status != "recoverable" || published.ReconciliationRunID != "recon-123" {
		t.Fatalf("published = %+v", published)
	}
	want := []string{
		"close-admission",
		"finalize-indicator-tail",
		"update-session:recoverable:recon-123",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}
