package runtimeagent

import (
	"context"
	"errors"
	"strings"

	grpcmw "github.com/hushine-tech/golang-lib/middleware/grpc"
	grpcclientmw "github.com/hushine-tech/golang-lib/middleware/grpcclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"google.golang.org/grpc"
)

const RuntimeAgentServiceName = "strategy-runtime-agent"

func RuntimeLocalLogBackendConfig(cfg RuntimeLogConfig) *elog.Config {
	outputDir := strings.TrimSpace(cfg.OutputDir)
	if outputDir == "" {
		outputDir = "./logs"
	}
	return &elog.Config{
		OutputDir:     outputDir,
		LocalFile:     elog.LocalFileConfig{Enabled: true},
		Kafka:         elog.KafkaConfig{Enabled: false, Brokers: []string{}},
		Elasticsearch: elog.ElasticsearchConfig{Enabled: false, Addresses: []string{}},
		Tracing:       elog.TracingConfig{Enabled: false, Endpoint: "", ServiceName: RuntimeAgentServiceName},
	}
}

func InitObservability(ctx context.Context, cfg RuntimeLogConfig) (func(context.Context) error, error) {
	local := RuntimeLocalLogBackendConfig(cfg)
	if err := elog.InitLogWithConfig(local); err != nil {
		return nil, err
	}
	tracerShutdown, err := elog.InitTracerFromConfig(local.Tracing)
	if err != nil {
		_ = elog.Close()
		return nil, err
	}
	elog.Info(ctx, "system", "strategy-runtime-agent local observability initialized")
	return func(shutdownCtx context.Context) error {
		return errors.Join(tracerShutdown(shutdownCtx), elog.Close())
	}, nil
}

func RuntimeChannelDialOptions(logger elog.Logger) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpcclientmw.UnaryClientInterceptor(logger)),
		grpc.WithStreamInterceptor(grpcclientmw.StreamClientInterceptor(logger)),
	}
}

func WorkerServerOptions(logger elog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcmw.UnaryServerInterceptor(logger)),
		grpc.StreamInterceptor(grpcmw.StreamServerInterceptor(logger)),
	}
}
