package runtimeagent

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrWorkerTokenMismatch = errors.New("worker token mismatch")
	ErrWorkerNotExpected   = errors.New("worker is not expected")
	ErrWorkerAlreadyExists = errors.New("worker already exists")
)

type SessionRegistry struct {
	mu       sync.Mutex
	expected map[string]string
	active   map[string]WorkerIdentity
}

type WorkerIdentity struct {
	SessionID string
	PID       int64
	token     string
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		expected: map[string]string{},
		active:   map[string]WorkerIdentity{},
	}
}

func (r *SessionRegistry) ExpectWorker(sessionID string, token string) error {
	sessionID = strings.TrimSpace(sessionID)
	token = strings.TrimSpace(token)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if token == "" {
		return fmt.Errorf("worker token is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.active[sessionID]; ok {
		return ErrWorkerAlreadyExists
	}
	if _, ok := r.expected[sessionID]; ok {
		return ErrWorkerAlreadyExists
	}
	r.expected[sessionID] = token
	return nil
}

func (r *SessionRegistry) AdmitWorker(sessionID string, token string, pid int64) error {
	sessionID = strings.TrimSpace(sessionID)
	token = strings.TrimSpace(token)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	expected, ok := r.expected[sessionID]
	if !ok {
		return ErrWorkerNotExpected
	}
	if expected != token {
		return ErrWorkerTokenMismatch
	}
	delete(r.expected, sessionID)
	r.active[sessionID] = WorkerIdentity{
		SessionID: sessionID,
		PID:       pid,
		token:     token,
	}
	return nil
}

func (r *SessionRegistry) AliasWorkerSession(existingSessionID string, sessionID string) error {
	existingSessionID = strings.TrimSpace(existingSessionID)
	sessionID = strings.TrimSpace(sessionID)
	if existingSessionID == "" || sessionID == "" {
		return fmt.Errorf("session_id is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	worker, ok := r.active[existingSessionID]
	if !ok {
		return ErrWorkerNotExpected
	}
	if existingSessionID == sessionID {
		return nil
	}
	if _, ok := r.expected[sessionID]; ok {
		return ErrWorkerAlreadyExists
	}
	if existing, ok := r.active[sessionID]; ok {
		if sameWorkerIdentity(existing, worker) {
			return nil
		}
		return ErrWorkerAlreadyExists
	}
	worker.SessionID = sessionID
	for key, active := range r.active {
		if sameWorkerIdentity(active, worker) {
			r.active[key] = worker
		}
	}
	r.active[sessionID] = worker
	return nil
}

func (r *SessionRegistry) ForgetWorker(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.expected, sessionID)
	worker, ok := r.active[sessionID]
	if !ok {
		delete(r.active, sessionID)
		return
	}
	for key, active := range r.active {
		if sameWorkerIdentity(active, worker) {
			delete(r.active, key)
		}
	}
}

func (r *SessionRegistry) ForgetWorkerIdentity(sessionID string, pid int64, token string) {
	sessionID = strings.TrimSpace(sessionID)
	token = strings.TrimSpace(token)
	if sessionID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if expected, ok := r.expected[sessionID]; ok && token != "" && expected == token {
		delete(r.expected, sessionID)
	}
	if pid <= 0 {
		return
	}
	for key, active := range r.active {
		if active.PID == pid && token != "" && active.token == token {
			delete(r.active, key)
		}
	}
}

func sameWorkerIdentity(left, right WorkerIdentity) bool {
	return left.PID == right.PID && left.token != "" && left.token == right.token
}

func (r *SessionRegistry) ActiveWorker(sessionID string) (WorkerIdentity, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return WorkerIdentity{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	worker, ok := r.active[sessionID]
	return worker, ok
}
