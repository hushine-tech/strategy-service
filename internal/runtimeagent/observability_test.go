package runtimeagent

import (
	"testing"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

func TestNormalizeLogConfigDefaultsRuntimeAgentServiceName(t *testing.T) {
	cfg := elog.Config{
		OutputDir: "./logs",
		Tracing: elog.TracingConfig{
			Enabled:     true,
			Endpoint:    "http://127.0.0.1:4318",
			ServiceName: "strategy-service",
		},
	}

	NormalizeLogConfig(&cfg)

	if cfg.Tracing.ServiceName != "strategy-runtime-agent" {
		t.Fatalf("service_name = %q, want strategy-runtime-agent", cfg.Tracing.ServiceName)
	}
}

func TestRuntimeChannelDialOptionsUseGolangLibInterceptors(t *testing.T) {
	opts := RuntimeChannelDialOptions(nil)

	if len(opts) == 0 {
		t.Fatalf("RuntimeChannelDialOptions returned no options")
	}
}

func TestWorkerServerOptionsUseGolangLibInterceptors(t *testing.T) {
	opts := WorkerServerOptions(nil)

	if len(opts) == 0 {
		t.Fatalf("WorkerServerOptions returned no options")
	}
}
