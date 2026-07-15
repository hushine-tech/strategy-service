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
	mu                 sync.Mutex
	expected           map[string]string
	expectedGeneration map[string]uint64
	nextGeneration     uint64
	active             map[string]WorkerIdentity
}

type WorkerIdentity struct {
	SessionID  string
	PID        int64
	Generation uint64
	token      string
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		expected:           map[string]string{},
		expectedGeneration: map[string]uint64{},
		active:             map[string]WorkerIdentity{},
	}
}

func (r *SessionRegistry) ExpectWorker(sessionID string, token string) error {
	_, err := r.ExpectWorkerGeneration(sessionID, token)
	return err
}

func (r *SessionRegistry) ExpectWorkerGeneration(sessionID string, token string) (uint64, error) {
	sessionID = strings.TrimSpace(sessionID)
	token = strings.TrimSpace(token)
	if sessionID == "" {
		return 0, fmt.Errorf("session_id is required")
	}
	if token == "" {
		return 0, fmt.Errorf("worker token is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.active[sessionID]; ok {
		return 0, ErrWorkerAlreadyExists
	}
	if _, ok := r.expected[sessionID]; ok {
		return 0, ErrWorkerAlreadyExists
	}
	r.nextGeneration++
	r.expected[sessionID] = token
	r.expectedGeneration[sessionID] = r.nextGeneration
	return r.nextGeneration, nil
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
	generation := r.expectedGeneration[sessionID]
	delete(r.expected, sessionID)
	delete(r.expectedGeneration, sessionID)
	r.active[sessionID] = WorkerIdentity{
		SessionID:  sessionID,
		PID:        pid,
		Generation: generation,
		token:      token,
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
	delete(r.expectedGeneration, sessionID)
	delete(r.active, sessionID)
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
		delete(r.expectedGeneration, sessionID)
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
