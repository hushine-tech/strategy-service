package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	"github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
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
	cfg, err := runtimeagent.LoadConfig(*configPath)
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
	coverageBootID := ""
	if coverageRoot != "" {
		coverageBootID, err = runtimeagent.InitializeCoverageFinalization(coverageRoot, identity.RuntimeID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "initialize runtime coverage finalization: %v\n", err)
			return 1
		}
	}
	if cfg.RuntimeChannelAddr == "" {
		fmt.Fprintln(os.Stderr, "runtime-channel address is required")
		return 1
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

	debugPort := 0
	if raw := strings.TrimSpace(os.Getenv("DEBUG_PORT")); raw != "" {
		_, _ = fmt.Sscanf(raw, "%d", &debugPort)
	}
	debugpyWait := parseDebugpyWait(os.Getenv("DEBUG_WAIT"))
	pythonArgsPrefix := append([]string{}, workerPythonArgsPrefix(debugPort)...)
	pythonArgsPrefix = append(
		pythonArgsPrefix,
		(runtimeagent.CoverageConfig{RootDir: coverageRoot}).PythonArgsPrefix()...,
	)
	workerManager := runtimeagent.NewWorkerManager(runtimeagent.WorkerManagerConfig{
		PythonExecutable: workerPythonExecutable(debugPort),
		PythonArgsPrefix: pythonArgsPrefix,
		WorkerModule:     "strategy_service.session_worker_entry",
		AgentAddr:        workerListener.Addr().String(),
		DebugpyBasePort:  debugPort,
		DebugpyWait:      debugpyWait,
		WorkDir:          ".",
		StateRoot:        cfg.WorkerStateRoot,
		PythonPath: []string{
			".",
			"../strategy-library",
		},
	})

	var credential *runtimeagent.RuntimeCredential
	if identity.Source != "bare" {
		credential, err = runtimeagent.LoadRuntimeCredential(cfg.CredentialPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load runtime credential: %v\n", err)
			return 1
		}
		applyCredentialTLS(&cfg.TLS, credential)
	}
	dialOptions, err := dialOptionsFromConfig(cfg.TLS)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runtime channel TLS config: %v\n", err)
		return 1
	}

	return runAgent(ctx, cfg, identity, credential, workerManager, workerListener, dialOptions, coverageRoot, coverageBootID)
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
	workerServer := runtimeagent.NewWorkerIPCServer(workerManager.Registry(), agent.HandleWorkerFrame)
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
	if value := firstNonEmpty(os.Getenv("HUSHINE_WORKER_PYTHON"), os.Getenv("PYTHON")); value != "" {
		return value
	}
	if venvPython := localVenvPythonExecutable(); venvPython != "" {
		return venvPython
	}
	if _, err := exec.LookPath("uv"); err == nil {
		return "uv"
	}
	return "python3"
}

func workerPythonArgsPrefix(debugPort int) []string {
	if raw := strings.TrimSpace(os.Getenv("HUSHINE_WORKER_PYTHON_ARGS")); raw != "" {
		return strings.Fields(raw)
	}
	if localVenvPythonExecutable() == "" && firstNonEmpty(os.Getenv("HUSHINE_WORKER_PYTHON"), os.Getenv("PYTHON")) == "" {
		if _, err := exec.LookPath("uv"); err != nil {
			return nil
		}
		args := []string{"run"}
		if debugPort > 0 {
			args = append(args, "--with", "debugpy")
		}
		args = append(args, "python", "-Xfrozen_modules=off")
		return args
	}
	return nil
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
