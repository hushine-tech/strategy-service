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
	}
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
	delete(r.active, sessionID)
}
