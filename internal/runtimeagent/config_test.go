package runtimeagent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigReadsRuntimeChannelAndLogConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
dependencies:
  runtime_channel_grpc: "127.0.0.1:50055"
  control_panel_service_grpc: "127.0.0.1:50054"
runtime:
  source: "self_hosted"
  runtime_id: "rt-test"
  name: "runtime-test"
  heartbeat_interval_seconds: 3
runtime_channel_tls:
  enabled: true
  root_cert_file: "/tmp/ca.pem"
  server_name: "runtime-channel.local"
log:
  output_dir: "./logs"
  tracing:
    enabled: true
    endpoint: "http://127.0.0.1:4318"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.RuntimeChannelAddr != "127.0.0.1:50055" {
		t.Fatalf("runtime channel addr = %q", cfg.RuntimeChannelAddr)
	}
	if cfg.ControlPanelAddr != "127.0.0.1:50054" {
		t.Fatalf("control panel addr = %q", cfg.ControlPanelAddr)
	}
	if cfg.RuntimeID != "rt-test" || cfg.RuntimeName != "runtime-test" {
		t.Fatalf("runtime identity = %q/%q", cfg.RuntimeID, cfg.RuntimeName)
	}
	if cfg.HeartbeatSeconds != 3 {
		t.Fatalf("heartbeat seconds = %d", cfg.HeartbeatSeconds)
	}
	if !cfg.TLS.Enabled || cfg.TLS.RootCertFile != "/tmp/ca.pem" {
		t.Fatalf("tls config = %+v", cfg.TLS)
	}
	if cfg.Log.Tracing.ServiceName != RuntimeAgentServiceName {
		t.Fatalf("tracing service = %q", cfg.Log.Tracing.ServiceName)
	}
}

func TestLoadConfigAppliesRuntimeChannelEnvOverride(t *testing.T) {
	t.Setenv("RUNTIME_CHANNEL_GRPC_ADDR", "192.168.88.6:50055")
	t.Setenv("CONTROL_PANEL_SERVICE_GRPC_ADDR", "192.168.88.6:50054")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`dependencies: {runtime_channel_grpc: "127.0.0.1:50055"}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.RuntimeChannelAddr != "192.168.88.6:50055" {
		t.Fatalf("runtime channel override = %q", cfg.RuntimeChannelAddr)
	}
	if cfg.ControlPanelAddr != "192.168.88.6:50054" {
		t.Fatalf("control panel override = %q", cfg.ControlPanelAddr)
	}
}
