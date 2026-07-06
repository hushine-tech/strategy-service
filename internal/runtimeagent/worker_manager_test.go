package runtimeagent

import "testing"

func TestWorkerManagerAllocatesOneTokenPerSession(t *testing.T) {
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "strategy_service.session_worker_entry",
	})

	spec, err := m.PrepareSessionWorker("sess-1")
	if err != nil {
		t.Fatalf("PrepareSessionWorker: %v", err)
	}
	if spec.SessionID != "sess-1" || spec.Token == "" {
		t.Fatalf("spec = %+v, want session and token", spec)
	}
	if spec.AgentAddr == "" {
		t.Fatalf("AgentAddr is empty")
	}
}

func TestWorkerManagerRejectsDuplicateSession(t *testing.T) {
	m := NewWorkerManager(WorkerManagerConfig{
		PythonExecutable: "python3",
		WorkerModule:     "strategy_service.session_worker_entry",
	})
	if _, err := m.PrepareSessionWorker("sess-1"); err != nil {
		t.Fatalf("first PrepareSessionWorker: %v", err)
	}
	if _, err := m.PrepareSessionWorker("sess-1"); err == nil {
		t.Fatalf("duplicate session was accepted")
	}
}
