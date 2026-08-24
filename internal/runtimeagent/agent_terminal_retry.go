package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	portfoliov1 "github.com/hushine-tech/strategy-service/gen/portfoliov1"
)

var errTerminalRetryInProgress = errors.New(
	"terminal retry is already in progress",
)

func (a *Agent) initializeTerminalRetries() {
	root := strings.TrimSpace(a.cfg.StateRoot)
	if root == "" {
		return
	}
	store, err := NewTerminalRetryStore(root)
	if err != nil {
		a.retryInitErr = err
		return
	}
	records, err := store.LoadAll()
	if err != nil {
		a.retryInitErr = err
		return
	}
	for _, record := range records {
		if err := a.indicatorSync.RestoreSessionV2(
			record.Indicators,
		); err != nil {
			a.retryInitErr = fmt.Errorf(
				"restore terminal retry session %s: %w",
				record.SessionID,
				err,
			)
			return
		}
		a.terminalRetries[terminalRetryKey(
			record.SessionID,
			record.Generation,
		)] = record
	}
	a.retryStore = store
}

func (a *Agent) RetryInitializationError() error {
	if a == nil {
		return fmt.Errorf("runtime Agent is nil")
	}
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	return a.retryInitErr
}

func (a *Agent) checkpointTerminalRetry(
	request TerminalRequest,
	generation uint64,
	effectiveStatus string,
	reason string,
) error {
	if a == nil {
		return fmt.Errorf("runtime Agent is nil")
	}
	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID == "" {
		return fmt.Errorf("terminal retry session_id is required")
	}
	if generation == 0 {
		a.mu.Lock()
		current := a.generations[sessionID]
		a.mu.Unlock()
		if current != nil {
			generation = current.generation
		}
	}
	if generation == 0 {
		return fmt.Errorf("terminal retry generation is required")
	}
	desiredStatus := strings.TrimSpace(strings.ToLower(request.Status))
	effectiveStatus = strings.TrimSpace(strings.ToLower(effectiveStatus))
	if effectiveStatus == "" {
		effectiveStatus = "recoverable"
	}
	record := TerminalRetryRecord{
		SchemaVersion:   indicatorTerminalRetrySchemaVersion,
		SessionID:       sessionID,
		Generation:      generation,
		DesiredStatus:   desiredStatus,
		EffectiveStatus: effectiveStatus,
		BarsProcessed:   request.BarsProcessed,
		Reason:          strings.TrimSpace(reason),
		ExpectedStatus:  strings.TrimSpace(strings.ToLower(request.ExpectedStatus)),
	}
	if request.committedStartBinding != nil {
		binding := *request.committedStartBinding
		record.CommittedStartBinding = &binding
	}
	if record.BarsProcessed < 0 {
		record.BarsProcessed = 0
	}

	a.retryMu.Lock()
	store := a.retryStore
	initErr := a.retryInitErr
	a.retryMu.Unlock()
	if initErr != nil {
		return initErr
	}
	if store != nil {
		checkpoint, err := a.indicatorSync.CheckpointSessionV2(sessionID)
		if err != nil {
			return fmt.Errorf(
				"checkpoint terminal indicator retry: %w",
				err,
			)
		}
		record.Indicators = checkpoint
		if err := store.Save(record); err != nil {
			return err
		}
	}
	a.retryMu.Lock()
	a.terminalRetries[terminalRetryKey(sessionID, generation)] = record
	a.retryMu.Unlock()
	return nil
}

func (a *Agent) retainWorkerGenerationFinalizationFailure(
	parent context.Context,
	sessionID string,
	generation *workerGeneration,
	reason string,
) error {
	if parent == nil {
		parent = context.Background()
	}
	if generation == nil {
		return fmt.Errorf("worker generation is required")
	}
	generation.mu.Lock()
	durablePossible := generation.durablePossible
	generationNumber := generation.generation
	generation.mu.Unlock()
	if !durablePossible {
		return nil
	}

	request := TerminalRequest{
		SessionID: sessionID,
		Status:    "recoverable",
		Error:     strings.TrimSpace(reason),
	}
	effectiveStatus := "recoverable"
	var publishErr error
	if a.cfg.PlatformInvoker == nil {
		publishErr = fmt.Errorf("platform invoker is not configured")
	} else {
		timeout := a.cfg.RequestTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		publishCtx, cancel := context.WithTimeout(
			parent,
			timeout,
		)
		var response portfoliov1.GetSessionResponse
		publishErr = a.invokePlatformProto(
			publishCtx,
			"portfolio.GetSession",
			&portfoliov1.GetSessionRequest{
				SessionId: sessionID,
				UserId:    a.cfg.UserID,
			},
			&response,
		)
		if publishErr == nil {
			session := response.GetSession()
			switch {
			case session == nil:
				publishErr = fmt.Errorf(
					"indicator retry reconciliation response is missing session",
				)
			case strings.TrimSpace(session.GetSessionId()) != sessionID:
				publishErr = fmt.Errorf(
					"indicator retry reconciliation returned mismatched session_id",
				)
			case strings.TrimSpace(session.GetRuntimeId()) != "" &&
				strings.TrimSpace(session.GetRuntimeId()) !=
					strings.TrimSpace(a.cfg.RuntimeID):
				publishErr = fmt.Errorf(
					"indicator retry reconciliation returned mismatched runtime_id",
				)
			case a.cfg.UserID > 0 &&
				session.GetUserId() > 0 &&
				session.GetUserId() != a.cfg.UserID:
				publishErr = fmt.Errorf(
					"indicator retry reconciliation returned mismatched user_id",
				)
			default:
				request.BarsProcessed = int64(session.GetBarsProcessed())
				statusValue := strings.TrimSpace(
					strings.ToLower(session.GetStatus()),
				)
				if isTerminalRetryStatus(statusValue) {
					effectiveStatus = statusValue
				}
				pending := true
				publishErr = a.updateSessionWithIndicatorFinalization(
					publishCtx,
					sessionID,
					effectiveStatus,
					request.BarsProcessed,
					request.Error,
					&pending,
				)
			}
		}
		cancel()
	}

	checkpointErr := a.checkpointTerminalRetry(
		request,
		generationNumber,
		effectiveStatus,
		reason,
	)
	return errors.Join(publishErr, checkpointErr)
}

func (a *Agent) RetryTerminalSessions(ctx context.Context) error {
	if err := a.RetryInitializationError(); err != nil {
		return err
	}
	a.retryMu.Lock()
	records := make([]TerminalRetryRecord, 0, len(a.terminalRetries))
	for _, record := range a.terminalRetries {
		records = append(records, record)
	}
	a.retryMu.Unlock()
	sort.Slice(records, func(left, right int) bool {
		if records[left].SessionID != records[right].SessionID {
			return records[left].SessionID < records[right].SessionID
		}
		return records[left].Generation < records[right].Generation
	})
	var retryErrors []error
	for _, record := range records {
		if ctx.Err() != nil {
			retryErrors = append(retryErrors, ctx.Err())
			break
		}
		if err := a.retryTerminalSession(ctx, record); err != nil {
			retryErrors = append(
				retryErrors,
				fmt.Errorf(
					"retry terminal session %s: %w",
					record.SessionID,
					err,
				),
			)
		}
	}
	return errors.Join(retryErrors...)
}

func (a *Agent) RunTerminalRetryLoop(
	ctx context.Context,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	_ = a.RetryTerminalSessions(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = a.RetryTerminalSessions(ctx)
		}
	}
}

func (a *Agent) retryTerminalSession(
	ctx context.Context,
	record TerminalRetryRecord,
) error {
	return a.retryTerminalSessionWithClaim(ctx, record, true)
}

func (a *Agent) retryTerminalSessionWithinLifecycle(
	ctx context.Context,
	record TerminalRetryRecord,
) error {
	return a.retryTerminalSessionWithClaim(ctx, record, false)
}

func (a *Agent) retryTerminalSessionWithClaim(
	ctx context.Context,
	record TerminalRetryRecord,
	lockGenerationLifecycle bool,
) error {
	key := terminalRetryKey(record.SessionID, record.Generation)
	a.retryMu.Lock()
	if _, retained := a.terminalRetries[key]; !retained {
		a.retryMu.Unlock()
		return nil
	}
	if _, claimed := a.retryClaims[key]; claimed {
		a.retryMu.Unlock()
		return errTerminalRetryInProgress
	}
	a.retryClaims[key] = struct{}{}
	a.retryMu.Unlock()
	defer func() {
		a.retryMu.Lock()
		delete(a.retryClaims, key)
		a.retryMu.Unlock()
	}()

	if lockGenerationLifecycle {
		a.mu.Lock()
		generation := a.generations[strings.TrimSpace(record.SessionID)]
		a.mu.Unlock()
		if generation != nil &&
			generation.generation == record.Generation {
			generation.lifecycleMu.Lock()
			defer generation.lifecycleMu.Unlock()
		}
	}
	return a.replayTerminalSession(ctx, record)
}

func (a *Agent) replayTerminalSession(
	ctx context.Context,
	record TerminalRetryRecord,
) error {
	if a.cfg.PlatformInvoker == nil {
		return fmt.Errorf("platform invoker is not configured")
	}
	var response portfoliov1.GetSessionResponse
	readSession := func() error {
		return a.invokePlatformProto(
			ctx,
			"portfolio.GetSession",
			&portfoliov1.GetSessionRequest{
				SessionId: record.SessionID,
				UserId:    a.cfg.UserID,
			},
			&response,
		)
	}
	validatedSession := func() (*portfoliov1.StrategySessionEntry, error) {
		session := response.GetSession()
		if session == nil ||
			strings.TrimSpace(session.GetSessionId()) != record.SessionID {
			return nil, fmt.Errorf("terminal retry returned mismatched session")
		}
		if runtimeID := strings.TrimSpace(session.GetRuntimeId()); runtimeID != "" && runtimeID != strings.TrimSpace(a.cfg.RuntimeID) {
			return nil, fmt.Errorf("terminal retry returned mismatched runtime_id")
		}
		if a.cfg.UserID > 0 &&
			session.GetUserId() > 0 &&
			session.GetUserId() != a.cfg.UserID {
			return nil, fmt.Errorf("terminal retry returned mismatched user_id")
		}
		return session, nil
	}
	validateCommittedStartup := func(session *portfoliov1.StrategySessionEntry) error {
		if record.ExpectedStatus == "" {
			return nil
		}
		if record.CommittedStartBinding == nil {
			return fmt.Errorf("terminal retry committed startup binding is unavailable")
		}
		return validateCommittedStartBinding(session, *record.CommittedStartBinding)
	}
	if record.ExpectedStatus != "" {
		if record.CommittedStartBinding == nil {
			// Checkpoints written before the committed binding was persisted
			// cannot prove ownership after restart. They remain fail-closed and
			// require operator reconciliation rather than a read or mutation.
			return fmt.Errorf("terminal retry committed startup binding is unavailable")
		}
		if err := readSession(); err != nil {
			if isExplicitPlatformNotFound(err) {
				// Startup cleanup is recorded only after a durable committed row
				// was read back. Core filters this request by user_id, so NotFound
				// is ownership/absence ambiguity and cannot authorize deleting the
				// checkpoint or its restored indicator state.
				return indeterminateCommittedStartNotFound(err)
			}
			return err
		}
		session, err := validatedSession()
		if err != nil {
			return err
		}
		if err := validateCommittedStartup(session); err != nil {
			return err
		}
		observedStatus := strings.TrimSpace(strings.ToLower(session.GetStatus()))
		if observedStatus == "running" {
			return a.deleteTerminalRetry(record.SessionID, record.Generation)
		}
		if observedStatus != record.ExpectedStatus && isTerminalRetryStatus(observedStatus) {
			return a.completeTerminalRetry(record)
		}
	}
	if err := a.indicatorSync.FinalizeSession(
		ctx,
		record.SessionID,
	); err != nil {
		return err
	}
	if record.ExpectedStatus == "" {
		if err := readSession(); err != nil {
			if isExplicitPlatformNotFound(err) {
				// Preserve the historical absence-is-terminal behavior for legacy
				// terminal retries that have no committed-start pending binding.
				return a.completeTerminalRetry(record)
			}
			return err
		}
	}
	session, err := validatedSession()
	if err != nil {
		return err
	}
	if err := validateCommittedStartup(session); err != nil {
		return err
	}
	observedStatus := strings.TrimSpace(strings.ToLower(session.GetStatus()))
	if record.ExpectedStatus != "" && observedStatus != record.ExpectedStatus {
		if observedStatus == "running" {
			return a.deleteTerminalRetry(record.SessionID, record.Generation)
		}
		if isTerminalRetryStatus(observedStatus) {
			return a.completeTerminalRetry(record)
		}
		return fmt.Errorf(
			"terminal retry expected session status %q, got %q",
			record.ExpectedStatus,
			observedStatus,
		)
	}
	statusValue := observedStatus
	if !isTerminalRetryStatus(statusValue) {
		statusValue = record.EffectiveStatus
	}
	if !isTerminalRetryStatus(statusValue) {
		statusValue = "recoverable"
	}
	barsProcessed := int64(session.GetBarsProcessed())
	if barsProcessed < record.BarsProcessed {
		barsProcessed = record.BarsProcessed
	}
	message := session.GetError()
	if strings.TrimSpace(message) == "" ||
		!isTerminalRetryStatus(
			strings.TrimSpace(strings.ToLower(session.GetStatus())),
		) {
		message = record.Reason
	}
	pending := false
	if err := a.updateSessionWithIndicatorFinalization(
		ctx,
		record.SessionID,
		statusValue,
		barsProcessed,
		message,
		&pending,
		record.ExpectedStatus,
	); err != nil {
		return err
	}
	return a.completeTerminalRetry(record)
}

func (a *Agent) completeTerminalRetry(
	record TerminalRetryRecord,
) error {
	if err := a.deleteTerminalRetry(
		record.SessionID,
		record.Generation,
	); err != nil {
		return err
	}
	a.indicatorSync.ForgetSession(context.Background(), record.SessionID)
	return nil
}

func (a *Agent) deleteTerminalRetry(
	sessionID string,
	generation uint64,
) error {
	key := terminalRetryKey(sessionID, generation)
	a.retryMu.Lock()
	store := a.retryStore
	_, retained := a.terminalRetries[key]
	a.retryMu.Unlock()
	if store != nil && retained {
		if err := store.Delete(
			sessionID,
			generation,
		); err != nil {
			return err
		}
	}
	a.retryMu.Lock()
	delete(a.terminalRetries, key)
	a.retryMu.Unlock()
	return nil
}

func (a *Agent) hasDurableTerminalRetry(
	sessionID string,
	generation uint64,
) bool {
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	_, exists := a.terminalRetries[terminalRetryKey(
		sessionID,
		generation,
	)]
	return exists &&
		a.retryStore != nil
}

func (a *Agent) terminalRetryRecord(
	sessionID string,
	generation uint64,
) (TerminalRetryRecord, bool) {
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	record, exists := a.terminalRetries[terminalRetryKey(
		sessionID,
		generation,
	)]
	return record, exists
}

func terminalRetryKey(sessionID string, generation uint64) string {
	return strings.TrimSpace(sessionID) + "\x00" +
		strconv.FormatUint(generation, 10)
}
