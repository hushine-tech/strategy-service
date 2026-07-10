package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

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

	body := []byte(`{"session_id":"sess-old","max_loss_close_pct":0.25,"leverage":3}`)
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
	if restarter.opts.SessionID != "sess-old" || restarter.opts.MaxLossClosePct != 0.25 || restarter.opts.Leverage != 3 {
		t.Fatalf("restart opts = %+v", restarter.opts)
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
