package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	"github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	return runWithOps(args, defaultRuntimeBootstrapOps())
}

type runtimeBootstrapResolution struct {
	launchSpec runtimeagent.WorkerLaunchSpec
	facts      runtimeagent.EmbeddedRuntimeFacts
}

type runtimeBootstrapOps struct {
	loadConfig              func(string) (runtimeagent.Config, error)
	resolveWorkerLaunchSpec func(
		runtimeagent.Config,
		runtimeagent.RuntimeIdentity,
		string,
		[]string,
	) (runtimeBootstrapResolution, error)
	verifyProfile func(
		context.Context,
		runtimeagent.WorkerPythonInvocation,
		runtimeagent.EmbeddedRuntimeFacts,
	) (*strategyv1.RuntimeDependencyProfile, error)
	emitStartupFailure         func(io.Writer, runtimeagent.RuntimeIdentity, runtimeagent.EmbeddedRuntimeFacts, *runtimeagent.RuntimeDependencyProfileError)
	loadCredential             func(string) (*runtimeagent.RuntimeCredential, error)
	dialOptions                func(runtimeagent.TLSConfig) ([]grpc.DialOption, error)
	buildStartupFailureRequest func(
		runtimeagent.RuntimeIdentity,
		*runtimeagent.RuntimeCredential,
		*runtimeagent.RuntimeDependencyProfileError,
		time.Time,
		string,
	) (*controlpanelv1.ReportRuntimeStartupFailureRequest, error)
	reportStartupFailure func(context.Context, string, []grpc.DialOption, *controlpanelv1.ReportRuntimeStartupFailureRequest) error
	now                  func() time.Time
	nonce                func() (string, error)
}

func defaultRuntimeBootstrapOps() runtimeBootstrapOps {
	return runtimeBootstrapOps{
		loadConfig:              runtimeagent.LoadConfig,
		resolveWorkerLaunchSpec: resolveRuntimeWorkerLaunchSpec,
		verifyProfile: func(
			ctx context.Context,
			invocation runtimeagent.WorkerPythonInvocation,
			facts runtimeagent.EmbeddedRuntimeFacts,
		) (*strategyv1.RuntimeDependencyProfile, error) {
			return runtimeagent.VerifyRuntimeDependencyProfile(ctx, invocation, facts, nil)
		},
		emitStartupFailure:         emitRuntimeStartupFailure,
		loadCredential:             runtimeagent.LoadRuntimeCredential,
		dialOptions:                dialOptionsFromConfig,
		buildStartupFailureRequest: runtimeagent.BuildRuntimeStartupFailureRequest,
		reportStartupFailure:       runtimeagent.ReportRuntimeStartupFailure,
		now:                        time.Now,
		nonce:                      runtimeagentTestRandomToken,
	}
}

func runWithOps(args []string, ops runtimeBootstrapOps) int {
	fs := flag.NewFlagSet("runtime-agent", flag.ContinueOnError)
	configPath := fs.String("config", "config.yaml", "path to config.yaml")
	runtimeChannelAddr := fs.String("runtime-channel-addr", "", "control-panel RuntimeChannel gRPC address")
	userID := fs.Int64("user-id", 0, "debug bare runtime user id")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	cfg, err := ops.loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		return 1
	}
	if strings.TrimSpace(*runtimeChannelAddr) != "" {
		cfg.RuntimeChannelAddr = strings.TrimSpace(*runtimeChannelAddr)
	}
	coverageRoot, err := prepareRuntimeCoverageRoot(os.Getenv("HUSHINE_RUNTIME_COVERAGE_DIR"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "runtime coverage: %v\n", err)
		return 1
	}
	identity := runtimeIdentityFromConfig(cfg, *userID)
	if cfg.RuntimeChannelAddr == "" {
		fmt.Fprintln(os.Stderr, "runtime-channel address is required")
		return 1
	}
	resolution, err := ops.resolveWorkerLaunchSpec(cfg, identity, coverageRoot, os.Environ())
	if err != nil {
		dependencyErr := runtimeDependencyErrorFrom(err, "worker Python invocation is invalid")
		ops.emitStartupFailure(os.Stderr, identity, resolution.facts, dependencyErr)
		return reportSelfHostedStartupFailure(cfg, identity, resolution.facts, dependencyErr, ops)
	}
	verifiedProfile, err := ops.verifyProfile(
		context.Background(), resolution.launchSpec.Invocation, resolution.facts,
	)
	if err != nil {
		dependencyErr := runtimeDependencyErrorFrom(err, "runtime dependency startup probe failed")
		ops.emitStartupFailure(os.Stderr, identity, resolution.facts, dependencyErr)
		return reportSelfHostedStartupFailure(cfg, identity, resolution.facts, dependencyErr, ops)
	}
	identity.DependencyProfile = verifiedProfile

	coverageBootID := ""
	if coverageRoot != "" {
		coverageBootID, err = runtimeagent.InitializeCoverageFinalization(coverageRoot, identity.RuntimeID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "initialize runtime coverage finalization: failed")
			return 1
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownObservability, err := runtimeagent.InitObservability(ctx, cfg.Log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init observability: %v\n", err)
		return 1
	}
	defer func() { _ = shutdownObservability(context.Background()) }()

	workerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen worker ipc: %v\n", err)
		return 1
	}
	defer workerListener.Close()

	resolution.launchSpec.AgentAddr = workerListener.Addr().String()
	workerManager, err := runtimeagent.NewWorkerManager(resolution.launchSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "construct verified worker manager: failed")
		return 1
	}

	var credential *runtimeagent.RuntimeCredential
	if identity.Source != "bare" {
		credential, err = ops.loadCredential(cfg.CredentialPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load runtime credential: %v\n", err)
			return 1
		}
		applyCredentialTLS(&cfg.TLS, credential)
	}
	dialOptions, err := ops.dialOptions(cfg.TLS)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runtime channel TLS config: %v\n", err)
		return 1
	}

	return runAgent(ctx, cfg, identity, credential, workerManager, workerListener, dialOptions, coverageRoot, coverageBootID)
}

func resolveRuntimeWorkerLaunchSpec(
	cfg runtimeagent.Config,
	identity runtimeagent.RuntimeIdentity,
	coverageRoot string,
	processEnv []string,
) (runtimeBootstrapResolution, error) {
	var result runtimeBootstrapResolution
	facts, err := runtimeagent.LoadEmbeddedRuntimeFacts(identity.Source, processEnv)
	if err != nil {
		return result, err
	}
	result.facts = facts
	debugPort := 0
	if raw := strings.TrimSpace(os.Getenv("DEBUG_PORT")); raw != "" {
		debugPort, err = strconv.Atoi(raw)
		if err != nil || debugPort < 0 || debugPort > 65535 {
			return result, fmt.Errorf("invalid DEBUG_PORT")
		}
	}
	launchSpec, err := runtimeagent.ResolveWorkerLaunchSpec(runtimeagent.WorkerManagerConfig{
		PythonExecutable: workerPythonExecutable(debugPort),
		PythonArgsPrefix: (runtimeagent.CoverageConfig{RootDir: coverageRoot}).PythonArgsPrefix(),
		WorkerModule:     "strategy_service.session_worker_entry",
		AgentAddr:        "127.0.0.1:0",
		DebugpyBasePort:  debugPort,
		DebugpyWait:      parseDebugpyWait(os.Getenv("DEBUG_WAIT")),
		WorkDir:          ".",
		StateRoot:        cfg.WorkerStateRoot,
	}, identity.Source, processEnv)
	if err != nil {
		return result, err
	}
	result.launchSpec = launchSpec
	return result, nil
}

func runtimeDependencyErrorFrom(err error, fallback string) *runtimeagent.RuntimeDependencyProfileError {
	var dependencyErr *runtimeagent.RuntimeDependencyProfileError
	if errors.As(err, &dependencyErr) {
		return dependencyErr
	}
	return &runtimeagent.RuntimeDependencyProfileError{
		Code:    "RUNTIME_DEPENDENCY_PROFILE_INVALID",
		Module:  "strategy_service.runtime_startup_probe",
		Message: fallback,
	}
}

type runtimeStartupFailureLog struct {
	Code           string `json:"code"`
	Module         string `json:"module"`
	ProfileName    string `json:"profile_name"`
	ProfileVersion string `json:"profile_version"`
	ImageBuildID   string `json:"image_build_id"`
	Source         string `json:"source"`
	Reason         string `json:"reason"`
}

func emitRuntimeStartupFailure(
	output io.Writer,
	identity runtimeagent.RuntimeIdentity,
	facts runtimeagent.EmbeddedRuntimeFacts,
	dependencyErr *runtimeagent.RuntimeDependencyProfileError,
) {
	record := runtimeStartupFailureLog{
		Code:   "RUNTIME_DEPENDENCY_PROFILE_INVALID",
		Source: identity.Source,
		Reason: "runtime dependency profile verification failed",
	}
	if dependencyErr != nil {
		record.Module = dependencyErr.Module
	}
	if facts.Profile != nil {
		record.ProfileName = facts.Profile.GetProfileName()
		record.ProfileVersion = facts.Profile.GetProfileVersion()
		record.ImageBuildID = facts.Profile.GetImageBuildId()
	}
	_ = json.NewEncoder(output).Encode(record)
}

func reportSelfHostedStartupFailure(
	cfg runtimeagent.Config,
	identity runtimeagent.RuntimeIdentity,
	facts runtimeagent.EmbeddedRuntimeFacts,
	dependencyErr *runtimeagent.RuntimeDependencyProfileError,
	ops runtimeBootstrapOps,
) int {
	if identity.Source != "self_hosted" {
		return 1
	}
	credential, err := ops.loadCredential(cfg.CredentialPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime startup failure report credential unavailable")
		return 1
	}
	applyCredentialTLS(&cfg.TLS, credential)
	dialOptions, err := ops.dialOptions(cfg.TLS)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime startup failure report TLS unavailable")
		return 1
	}
	identity.DependencyProfile = facts.Profile
	nonce, err := ops.nonce()
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime startup failure report nonce unavailable")
		return 1
	}
	request, err := ops.buildStartupFailureRequest(identity, credential, dependencyErr, ops.now(), nonce)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime startup failure report could not be built")
		return 1
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ops.reportStartupFailure(reportCtx, cfg.RuntimeChannelAddr, dialOptions, request); err != nil {
		fmt.Fprintln(os.Stderr, "runtime startup failure report failed")
	}
	return 1
}

func runAgent(
	ctx context.Context,
	cfg runtimeagent.Config,
	identity runtimeagent.RuntimeIdentity,
	credential *runtimeagent.RuntimeCredential,
	workerManager *runtimeagent.WorkerManager,
	workerListener net.Listener,
	dialOptions []grpc.DialOption,
	coverageRoot string,
	coverageBootID string,
) int {
	agentCtx, cancelAgent := context.WithCancel(ctx)
	defer cancelAgent()
	var agent *runtimeagent.Agent
	runtimeClient := runtimeagent.NewRuntimeChannelClient(runtimeagent.RuntimeChannelClientConfig{
		Address:          cfg.RuntimeChannelAddr,
		Identity:         identity,
		Credential:       credential,
		HeartbeatSeconds: cfg.HeartbeatSeconds,
		DialOptions:      append(dialOptions, runtimeagent.RuntimeChannelDialOptions(nil)...),
		RequestHandler: func(ctx context.Context, frame *controlpanelv1.RuntimeFrame) *controlpanelv1.RuntimeFrame {
			return agent.HandleRuntimeRequest(ctx, frame)
		},
		DataHandler: func(ctx context.Context, frame *controlpanelv1.RuntimeFrame) error {
			return agent.HandleRuntimeData(ctx, frame)
		},
	})
	agent = runtimeagent.NewAgent(runtimeagent.AgentConfig{
		RuntimeID:       identity.RuntimeID,
		RuntimeSource:   identity.Source,
		RuntimeName:     identity.Name,
		UserID:          identity.UserID,
		WorkerStarter:   workerManager,
		WorkerStopper:   workerManager,
		PlatformInvoker: runtimeClient,
	})
	workerServer := runtimeagent.NewAuthenticatedWorkerIPCServer(
		workerManager.Registry(),
		agent.HandleAuthenticatedWorkerFrame,
		func(identity runtimeagent.WorkerIdentity, cause error) {
			_ = agent.HandleWorkerDisconnect(identity, cause)
		},
	)
	agent.SetWorkerSender(workerServer)

	if controlAddr := strings.TrimSpace(os.Getenv("RUNTIME_AGENT_CONTROL_ADDR")); controlAddr != "" && !strings.EqualFold(controlAddr, "off") && controlAddr != "0" {
		addr, shutdown, err := runtimeagent.StartLocalControlServer(agentCtx, controlAddr, agent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "start local control: %v\n", err)
			return 1
		}
		defer func() { _ = shutdown(context.Background()) }()
		fmt.Printf("runtime-agent local-control=http://%s\n", addr.String())
	}

	grpcServer := grpc.NewServer(runtimeagent.WorkerServerOptions(nil)...)
	runtimeworkerv1.RegisterRuntimeWorkerAgentServer(grpcServer, workerServer)
	go func() {
		_ = grpcServer.Serve(workerListener)
	}()
	shutdownDone := make(chan struct{})
	go func() {
		shutdownAgentOnContext(
			agentCtx,
			coverageRoot,
			identity.RuntimeID,
			coverageBootID,
			workerManager,
			grpcServer,
			10*time.Second,
			5*time.Second,
			defaultCoverageShutdownOps(),
		)
		close(shutdownDone)
	}()

	fmt.Printf(
		"runtime-agent started: runtime_id=%s name=%s source=%s runtime-channel=%s worker-ipc=%s\n",
		identity.RuntimeID,
		identity.Name,
		identity.Source,
		cfg.RuntimeChannelAddr,
		workerListener.Addr().String(),
	)
	startAgentBackgroundLoops(agentCtx, agent)
	runErr := runtimeClient.Run(agentCtx)
	cancelAgent()
	<-shutdownDone
	if runErr != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "runtime channel stopped: %v\n", runErr)
		return 1
	}
	return 0
}

type agentWorkerStopper interface {
	StopAll(context.Context, time.Duration) error
}

type agentWorkerShutdownReporter interface {
	ShutdownSummary() runtimeagent.WorkerShutdownSummary
}

type agentGRPCStopper interface {
	GracefulStop()
	Stop()
}

type coverageShutdownOps struct {
	writeSnapshot     func(string) error
	writeFinalization func(string, runtimeagent.CoverageFinalization) error
	now               func() time.Time
}

func defaultCoverageShutdownOps() coverageShutdownOps {
	return coverageShutdownOps{
		writeSnapshot:     runtimeagent.WriteGoCoverageSnapshot,
		writeFinalization: runtimeagent.WriteCoverageFinalization,
		now:               time.Now,
	}
}

func shutdownAgentOnContext(
	ctx context.Context,
	coverageRoot string,
	runtimeID string,
	coverageBootID string,
	workerManager agentWorkerStopper,
	grpcServer agentGRPCStopper,
	shutdownTimeout time.Duration,
	workerTimeout time.Duration,
	ops coverageShutdownOps,
) {
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	workerErr := workerManager.StopAll(shutdownCtx, workerTimeout)
	forcedWorkers := 0
	if reporter, ok := workerManager.(agentWorkerShutdownReporter); ok {
		forcedWorkers = reporter.ShutdownSummary().ForcedStops
	}
	var snapshotErr error
	if coverageRoot != "" {
		snapshotErr = ops.writeSnapshot(filepath.Join(coverageRoot, "go"))
		workerStatus := runtimeagent.CoverageFinalizationOK
		if workerErr != nil {
			workerStatus = runtimeagent.CoverageFinalizationError
		} else if forcedWorkers > 0 {
			workerStatus = runtimeagent.CoverageFinalizationForced
		}
		snapshotStatus := runtimeagent.CoverageFinalizationOK
		if snapshotErr != nil {
			snapshotStatus = runtimeagent.CoverageFinalizationError
		}
		state := runtimeagent.CoverageFinalizationComplete
		if workerErr != nil || forcedWorkers > 0 || snapshotErr != nil {
			state = runtimeagent.CoverageFinalizationIncomplete
		}
		record := runtimeagent.CoverageFinalization{
			SchemaVersion:  1,
			RuntimeID:      runtimeID,
			BootID:         coverageBootID,
			State:          state,
			WorkerShutdown: workerStatus,
			ForcedWorkers:  forcedWorkers,
			GoSnapshot:     snapshotStatus,
			CompletedAt:    ops.now().UTC().Format(time.RFC3339Nano),
		}
		if err := ops.writeFinalization(coverageRoot, record); err != nil {
			fmt.Fprintln(os.Stderr, "write runtime coverage finalization: failed")
		}
	}

	gracefulDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
}

func prepareRuntimeCoverageRoot(root string) (string, error) {
	if root == "" {
		return "", nil
	}
	if strings.TrimSpace(root) != root || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", fmt.Errorf("HUSHINE_RUNTIME_COVERAGE_DIR must be an absolute cleaned path")
	}
	for _, child := range []string{"go", "python"} {
		path := filepath.Join(root, child)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return "", fmt.Errorf("create %s coverage directory: %w", child, err)
		}
	}
	return root, nil
}

func startAgentBackgroundLoops(ctx context.Context, agent *runtimeagent.Agent) {
	go agent.RunSyncLoop(ctx)
}

func runtimeIdentityFromConfig(cfg runtimeagent.Config, userID int64) runtimeagent.RuntimeIdentity {
	source := strings.TrimSpace(cfg.RuntimeSource)
	runtimeID := strings.TrimSpace(cfg.RuntimeID)
	name := strings.TrimSpace(cfg.RuntimeName)
	if userID > 0 {
		source = "bare"
		if runtimeID == "" {
			runtimeID = fmt.Sprintf("bare-%d-%s", userID, mustTokenPrefix(8))
		}
		if name == "" {
			name = fmt.Sprintf("bare-debug-%d-%s", userID, mustTokenPrefix(6))
		}
	}
	return runtimeagent.RuntimeIdentity{
		Source:          source,
		UserID:          userID,
		RuntimeID:       runtimeID,
		Name:            name,
		Capabilities:    cfg.Capabilities,
		ResourceProfile: cfg.ResourceProfile,
		Version:         cfg.Version,
	}
}

func dialOptionsFromConfig(tlsCfg runtimeagent.TLSConfig) ([]grpc.DialOption, error) {
	if !tlsCfg.Enabled {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	if err := applyTLSBundleJSON(&tlsCfg); err != nil {
		return nil, err
	}
	rootPool := x509.NewCertPool()
	if tlsCfg.RootCertFile != "" {
		root, err := os.ReadFile(tlsCfg.RootCertFile)
		if err != nil {
			return nil, err
		}
		if !rootPool.AppendCertsFromPEM(root) {
			return nil, fmt.Errorf("failed to parse root cert file: %s", tlsCfg.RootCertFile)
		}
	}
	if tlsCfg.RootCertPEM != "" && !rootPool.AppendCertsFromPEM([]byte(tlsCfg.RootCertPEM)) {
		return nil, fmt.Errorf("failed to parse root cert PEM")
	}
	tlsConfig := &tls.Config{
		RootCAs:    rootPool,
		ServerName: tlsCfg.ServerName,
		MinVersion: tls.VersionTLS12,
	}
	if tlsCfg.ClientCertFile != "" || tlsCfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.ClientCertFile, tlsCfg.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	if tlsCfg.ClientCertPEM != "" || tlsCfg.ClientKeyPEM != "" {
		cert, err := tls.X509KeyPair([]byte(tlsCfg.ClientCertPEM), []byte(tlsCfg.ClientKeyPEM))
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}, nil
}

func applyCredentialTLS(tlsCfg *runtimeagent.TLSConfig, credential *runtimeagent.RuntimeCredential) {
	if tlsCfg == nil || credential == nil {
		return
	}
	if tlsCfg.RootCertPEM == "" {
		tlsCfg.RootCertPEM = credential.ServerCAPEM
	}
	if tlsCfg.ClientCertPEM == "" {
		tlsCfg.ClientCertPEM = credential.ClientCertPEM
	}
	if tlsCfg.ClientKeyPEM == "" {
		tlsCfg.ClientKeyPEM = credential.ClientKeyPEM
	}
}

func applyTLSBundleJSON(tlsCfg *runtimeagent.TLSConfig) error {
	if tlsCfg == nil || strings.TrimSpace(tlsCfg.BundleJSON) == "" {
		return nil
	}
	var raw struct {
		ClientCertPEM string `json:"client_cert_pem"`
		ClientKeyPEM  string `json:"client_key_pem"`
		ServerCAPEM   string `json:"server_ca_pem"`
	}
	if err := json.Unmarshal([]byte(tlsCfg.BundleJSON), &raw); err != nil {
		return fmt.Errorf("parse runtime_channel_tls.bundle_json: %w", err)
	}
	if tlsCfg.RootCertPEM == "" {
		tlsCfg.RootCertPEM = raw.ServerCAPEM
	}
	if tlsCfg.ClientCertPEM == "" {
		tlsCfg.ClientCertPEM = raw.ClientCertPEM
	}
	if tlsCfg.ClientKeyPEM == "" {
		tlsCfg.ClientKeyPEM = raw.ClientKeyPEM
	}
	return nil
}

func mustTokenPrefix(n int) string {
	token, err := runtimeagentTestRandomToken()
	if err != nil {
		panic(err)
	}
	if n > len(token) {
		return token
	}
	return token[:n]
}

func runtimeagentTestRandomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:]), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseDebugpyWait(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "0", "false", "no", "off":
		return false
	case "1", "true", "yes", "on":
		return true
	default:
		return true
	}
}

func workerPythonExecutable(debugPort int) string {
	_ = debugPort
	if value := strings.TrimSpace(os.Getenv("HUSHINE_WORKER_PYTHON")); value != "" {
		return value
	}
	return localVenvPythonExecutable()
}

func localVenvPythonExecutable() string {
	candidates := []string{
		".venv/bin/python",
		".venv/Scripts/python.exe",
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			if abs, err := filepath.Abs(candidate); err == nil {
				return abs
			}
			return candidate
		}
	}
	return ""
}
