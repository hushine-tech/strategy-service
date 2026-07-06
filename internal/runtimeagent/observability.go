package runtimeagent

import (
	"context"

	grpcmw "github.com/hushine-tech/golang-lib/middleware/grpc"
	grpcclientmw "github.com/hushine-tech/golang-lib/middleware/grpcclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"google.golang.org/grpc"
)

const RuntimeAgentServiceName = "strategy-runtime-agent"

func NormalizeLogConfig(cfg *elog.Config) {
	if cfg == nil {
		return
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "./logs"
	}
	if cfg.Tracing.ServiceName == "" || cfg.Tracing.ServiceName == "strategy-service" {
		cfg.Tracing.ServiceName = RuntimeAgentServiceName
	}
}

func InitObservability(ctx context.Context, cfg *elog.Config) (func(context.Context) error, error) {
	if cfg == nil {
		cfg = elog.DefaultConfig()
	}
	NormalizeLogConfig(cfg)
	if err := elog.InitLogWithConfig(cfg); err != nil {
		return nil, err
	}
	shutdown, err := elog.InitTracerFromConfig(cfg.Tracing)
	if err != nil {
		return nil, err
	}
	elog.Info(ctx, "system", "strategy-runtime-agent observability initialized")
	return shutdown, nil
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
