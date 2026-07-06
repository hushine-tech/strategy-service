package runtimeagent

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type WorkerManagerConfig struct {
	PythonExecutable string
	WorkerModule     string
	AgentAddr        string
	DebugpyBasePort  int
}

type WorkerStartSpec struct {
	SessionID   string
	Token       string
	AgentAddr   string
	DebugpyPort int
	Env         []string
}

type WorkerManager struct {
	cfg      WorkerManagerConfig
	registry *SessionRegistry
}

func NewWorkerManager(cfg WorkerManagerConfig) *WorkerManager {
	if strings.TrimSpace(cfg.PythonExecutable) == "" {
		cfg.PythonExecutable = "python3"
	}
	if strings.TrimSpace(cfg.WorkerModule) == "" {
		cfg.WorkerModule = "strategy_service.session_worker_entry"
	}
	if strings.TrimSpace(cfg.AgentAddr) == "" {
		cfg.AgentAddr = "127.0.0.1:0"
	}
	return &WorkerManager{
		cfg:      cfg,
		registry: NewSessionRegistry(),
	}
}

func (m *WorkerManager) PrepareSessionWorker(sessionID string) (WorkerStartSpec, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return WorkerStartSpec{}, fmt.Errorf("session_id is required")
	}
	token, err := randomToken()
	if err != nil {
		return WorkerStartSpec{}, err
	}
	if err := m.registry.ExpectWorker(sessionID, token); err != nil {
		return WorkerStartSpec{}, err
	}
	debugpyPort := 0
	if m.cfg.DebugpyBasePort > 0 {
		debugpyPort = m.cfg.DebugpyBasePort
	}
	spec := WorkerStartSpec{
		SessionID:   sessionID,
		Token:       token,
		AgentAddr:   m.cfg.AgentAddr,
		DebugpyPort: debugpyPort,
	}
	spec.Env = []string{
		"HUSHINE_AGENT_ADDR=" + spec.AgentAddr,
		"HUSHINE_WORKER_TOKEN=" + spec.Token,
		"HUSHINE_SESSION_ID=" + spec.SessionID,
		fmt.Sprintf("HUSHINE_DEBUGPY_PORT=%d", spec.DebugpyPort),
	}
	return spec, nil
}

func (m *WorkerManager) Registry() *SessionRegistry {
	return m.registry
}

func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate worker token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
