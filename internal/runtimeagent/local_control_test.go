package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestLocalControlSeparatesHealthFromRuntimeChannelReadiness(t *testing.T) {
	restarter := &fakeSessionRestarter{}
	readiness := &fakeRuntimeChannelReadiness{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, shutdown, err := StartLocalControlServer(
		ctx, "127.0.0.1:0", restarter, readiness,
	)
	if err != nil {
		t.Fatalf("StartLocalControlServer: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	assertLocalControlStatus(t, addr.String(), "/healthz", http.StatusOK)
	assertLocalControlStatus(t, addr.String(), "/readyz", http.StatusServiceUnavailable)
	readiness.ready.Store(true)
	assertLocalControlStatus(t, addr.String(), "/readyz", http.StatusOK)
	readiness.ready.Store(false)
	assertLocalControlStatus(t, addr.String(), "/healthz", http.StatusOK)
	assertLocalControlStatus(t, addr.String(), "/readyz", http.StatusServiceUnavailable)
}

func assertLocalControlStatus(t *testing.T, addr, path string, want int) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, want)
	}
}

type fakeRuntimeChannelReadiness struct{ ready atomic.Bool }

func (r *fakeRuntimeChannelReadiness) Ready() bool { return r.ready.Load() }

func TestLocalControlRestartWorkerSessionPostsRestartRequest(t *testing.T) {
	restarter := &fakeSessionRestarter{
		result: RestartSessionResult{
			OldSessionID: "sess-old",
			NewSessionID: "sess-new",
			RuntimeID:    "rt-1",
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, shutdown, err := StartLocalControlServer(ctx, "127.0.0.1:0", restarter)
	if err != nil {
		t.Fatalf("StartLocalControlServer: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	body := []byte(`{"session_id":"sess-old","max_loss_close_pct":0.25}`)
	resp, err := http.Post("http://"+addr.String()+"/restart-worker-session", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST restart-worker-session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got RestartSessionResult
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got != restarter.result {
		t.Fatalf("response = %+v", got)
	}
	if restarter.opts.SessionID != "sess-old" || restarter.opts.MaxLossClosePct != 0.25 {
		t.Fatalf("restart opts = %+v", restarter.opts)
	}
}

func TestLocalControlRestartWorkerSessionRejectsUnknownFields(t *testing.T) {
	restarter := &fakeSessionRestarter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, shutdown, err := StartLocalControlServer(ctx, "127.0.0.1:0", restarter)
	if err != nil {
		t.Fatalf("StartLocalControlServer: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	body := []byte(`{"session_id":"sess-old","leverage":3}`)
	resp, err := http.Post("http://"+addr.String()+"/restart-worker-session", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST restart-worker-session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

type fakeSessionRestarter struct {
	opts   RestartSessionOptions
	result RestartSessionResult
}

func (f *fakeSessionRestarter) RestartSession(_ context.Context, opts RestartSessionOptions) (RestartSessionResult, error) {
	f.opts = opts
	return f.result, nil
}
