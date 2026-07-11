package runtimeagent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigReadsRuntimeOnlyConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
dependencies:
  runtime_channel_grpc: "127.0.0.1:50055"
runtime:
  source: "self_hosted"
  runtime_id: "rt-test"
  name: "runtime-test"
  worker_state_root: "/tmp/hushine-workers"
  heartbeat_interval_seconds: 3
runtime_channel_tls:
  enabled: true
  root_cert_file: "/tmp/ca.pem"
  server_name: "runtime-channel.local"
log:
  output_dir: "./logs"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.RuntimeChannelAddr != "127.0.0.1:50055" {
		t.Fatalf("RuntimeChannelAddr = %q", cfg.RuntimeChannelAddr)
	}
	if cfg.RuntimeID != "rt-test" || cfg.RuntimeName != "runtime-test" {
		t.Fatalf("runtime identity = %q/%q", cfg.RuntimeID, cfg.RuntimeName)
	}
	if cfg.WorkerStateRoot != "/tmp/hushine-workers" {
		t.Fatalf("WorkerStateRoot = %q", cfg.WorkerStateRoot)
	}
	if cfg.HeartbeatSeconds != 3 || !cfg.TLS.Enabled || cfg.TLS.RootCertFile != "/tmp/ca.pem" {
		t.Fatalf("runtime config = %+v", cfg)
	}
	if cfg.Log.OutputDir != "./logs" {
		t.Fatalf("Log.OutputDir = %q", cfg.Log.OutputDir)
	}
}

func TestLoadConfigRejectsForbiddenRuntimeFields(t *testing.T) {
	cases := map[string]string{
		"top-level kafka":          `kafka: {brokers: ["127.0.0.1:19092"]}`,
		"log kafka":                `log: {kafka: {enabled: true, brokers: ["127.0.0.1:19092"]}}`,
		"log elasticsearch":        `log: {elasticsearch: {enabled: true, addresses: ["http://127.0.0.1:9200"]}}`,
		"log tracing":              `log: {tracing: {enabled: true, endpoint: "http://127.0.0.1:4318"}}`,
		"control-panel dependency": `dependencies: {control_panel_service_grpc: "127.0.0.1:50054"}`,
		"core dependency":          `dependencies: {core_service_grpc: "127.0.0.1:50051"}`,
		"database":                 `database: {host: "127.0.0.1", password: "secret"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfig(path)
			if err == nil {
				t.Fatalf("LoadConfig accepted forbidden config: %s", body)
			}
			if !strings.Contains(err.Error(), "field") {
				t.Fatalf("error = %v, want strict unknown-field error", err)
			}
		})
	}
}

func TestLoadConfigRejectsMultipleYAMLDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte("dependencies:\n  runtime_channel_grpc: 127.0.0.1:50055\n---\ndatabase:\n  password: secret\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("LoadConfig error = %v, want multiple-document rejection", err)
	}
}

func TestLoadConfigAppliesRuntimeChannelEnvOverride(t *testing.T) {
	t.Setenv("RUNTIME_CHANNEL_GRPC_ADDR", "192.168.88.6:50055")

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
}
