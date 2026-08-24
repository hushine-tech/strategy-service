package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
)

// TerminalRequest is the worker's desired terminal outcome. The reconciliation
// identity is carried through the lifecycle coordinator; UpdateSession stores
// only the Session status and indicator-finalization state.
type TerminalRequest struct {
	SessionID                    string
	Status                       string
	BarsProcessed                int64
	Error                        string
	ReconciliationRunID          string
	IndicatorFinalizationPending *bool
	ExpectedStatus               string
	committedStartBinding        *committedStartBinding
}

type terminalIndicatorLifecycle interface {
	FinalizeSession(context.Context, string) error
	ForgetSession(context.Context, string)
}

type terminalPublisher func(context.Context, TerminalRequest) error

type SessionLifecycle struct {
	indicators terminalIndicatorLifecycle
	publish    terminalPublisher
}

func NewSessionLifecycle(indicators terminalIndicatorLifecycle, publish terminalPublisher) *SessionLifecycle {
	return &SessionLifecycle{indicators: indicators, publish: publish}
}

type IndicatorFinalizationError struct {
	Message string
	Cause   error
}

func (e *IndicatorFinalizationError) Error() string { return e.Message }
func (e *IndicatorFinalizationError) Unwrap() error { return e.Cause }

func (l *SessionLifecycle) Complete(ctx context.Context, request TerminalRequest) error {
	if l == nil || l.indicators == nil {
		return fmt.Errorf("terminal indicator lifecycle is not configured")
	}
	if l.publish == nil {
		return fmt.Errorf("terminal publisher is not configured")
	}
	if err := l.indicators.FinalizeSession(ctx, request.SessionID); err != nil {
		message := "indicator finalization failed: " + err.Error()
		recoverable := request
		recoverable.Status = "recoverable"
		recoverable.Error = message
		pending := true
		recoverable.IndicatorFinalizationPending = &pending
		if publishErr := l.publish(ctx, recoverable); publishErr != nil {
			return errors.Join(
				&IndicatorFinalizationError{
					Message: message,
					Cause:   err,
				},
				fmt.Errorf(
					"publish recoverable indicator finalization state: %w",
					publishErr,
				),
			)
		}
		return &IndicatorFinalizationError{Message: message, Cause: err}
	}
	if request.IndicatorFinalizationPending == nil {
		pending := false
		request.IndicatorFinalizationPending = &pending
	}
	if err := l.publish(ctx, request); err != nil {
		return err
	}
	l.indicators.ForgetSession(ctx, request.SessionID)
	return nil
}

func terminalRequestFromFinalStatus(status *rwv1.FinalStatus) (TerminalRequest, error) {
	if status == nil {
		return TerminalRequest{}, fmt.Errorf("final status is empty")
	}
	sessionID := strings.TrimSpace(status.GetSessionId())
	if sessionID == "" {
		return TerminalRequest{}, fmt.Errorf("final status session_id is required")
	}
	statusValue := strings.TrimSpace(strings.ToLower(status.GetStatus()))
	switch statusValue {
	case "finished", "failed", "stopped", "stop_failed", "recoverable":
	default:
		return TerminalRequest{}, fmt.Errorf("final status must be terminal, got %q", status.GetStatus())
	}
	return TerminalRequest{
		SessionID:           sessionID,
		Status:              statusValue,
		BarsProcessed:       status.GetBarsProcessed(),
		Error:               status.GetError(),
		ReconciliationRunID: strings.TrimSpace(status.GetReconciliationRunId()),
	}, nil
}
