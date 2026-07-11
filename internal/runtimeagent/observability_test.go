package runtimeagent

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestRuntimeLocalLogBackendConfigDisablesNetworkSinks(t *testing.T) {
	cfg := RuntimeLocalLogBackendConfig(RuntimeLogConfig{OutputDir: "/tmp/runtime-logs"})
	if cfg.OutputDir != "/tmp/runtime-logs" || !cfg.LocalFile.Enabled {
		t.Fatalf("local log config = %+v", cfg)
	}
	if cfg.Kafka.Enabled || len(cfg.Kafka.Brokers) != 0 {
		t.Fatalf("runtime Kafka must be disabled: %+v", cfg.Kafka)
	}
	if cfg.Elasticsearch.Enabled || len(cfg.Elasticsearch.Addresses) != 0 {
		t.Fatalf("runtime Elasticsearch must be disabled: %+v", cfg.Elasticsearch)
	}
	if cfg.Tracing.Enabled || cfg.Tracing.Endpoint != "" {
		t.Fatalf("runtime direct tracing must be disabled: %+v", cfg.Tracing)
	}
}

func TestRuntimeObservabilityKeepsW3CPropagationWithoutExporter(t *testing.T) {
	shutdown, err := InitObservability(context.Background(), RuntimeLogConfig{OutputDir: t.TempDir()})
	if err != nil {
		t.Fatalf("InitObservability: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	var traceID trace.TraceID
	var spanID trace.SpanID
	traceID[15] = 1
	spanID[7] = 1
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(
		trace.ContextWithSpanContext(context.Background(), spanContext),
		carrier,
	)
	if carrier.Get("traceparent") == "" {
		t.Fatal("W3C traceparent propagation was not installed")
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
