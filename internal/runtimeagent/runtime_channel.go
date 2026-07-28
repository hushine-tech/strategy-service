package runtimeagent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type RuntimeIdentity struct {
	KeyID             string
	PrivateKeyPEM     string
	Source            string
	UserID            int64
	RuntimeID         string
	Name              string
	EndpointHost      string
	GRPCPort          int32
	DebugPort         int32
	Capabilities      []string
	ResourceProfile   string
	Version           string
	DependencyProfile *strategyv1.RuntimeDependencyProfile
}

type RuntimeCredential struct {
	KeyID         string
	PrivateKeyPEM string
	ClientCertPEM string
	ClientKeyPEM  string
	ServerCAPEM   string
}

type RuntimeRequestHandler func(context.Context, *cpv1.RuntimeFrame) *cpv1.RuntimeFrame
type RuntimeDataHandler func(context.Context, *cpv1.RuntimeFrame) error

type RuntimeChannelClientConfig struct {
	Address          string
	Identity         RuntimeIdentity
	Credential       *RuntimeCredential
	HeartbeatSeconds int
	DialOptions      []grpc.DialOption
	RequestHandler   RuntimeRequestHandler
	DataHandler      RuntimeDataHandler
}

type RuntimeChannelClient struct {
	cfg RuntimeChannelClientConfig

	mu          sync.Mutex
	runtimeID   string
	fingerprint string
	outbound    chan *cpv1.RuntimeFrame
	pending     map[string]chan *cpv1.RuntimeFrame
	connected   chan struct{}
	connectOnce sync.Once
}

func NewRuntimeChannelClient(cfg RuntimeChannelClientConfig) *RuntimeChannelClient {
	if cfg.HeartbeatSeconds <= 0 {
		cfg.HeartbeatSeconds = 10
	}
	if len(cfg.DialOptions) == 0 {
		cfg.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &RuntimeChannelClient{
		cfg:       cfg,
		pending:   map[string]chan *cpv1.RuntimeFrame{},
		connected: make(chan struct{}),
	}
}

func (c *RuntimeChannelClient) Run(ctx context.Context) error {
	address := normalizeRuntimeChannelAddress(c.cfg.Address)
	if address == "" {
		return fmt.Errorf("runtime channel address is required")
	}
	conn, err := grpc.DialContext(ctx, address, c.cfg.DialOptions...)
	if err != nil {
		return fmt.Errorf("dial runtime channel: %w", err)
	}
	defer conn.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := cpv1.NewControlPanelServiceClient(conn).RuntimeChannel(runCtx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("open runtime channel: %w", err)
	}

	outbound := make(chan *cpv1.RuntimeFrame, 128)
	c.mu.Lock()
	c.outbound = outbound
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.outbound == outbound {
			c.outbound = nil
		}
		c.mu.Unlock()
	}()

	sendDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-runCtx.Done():
				sendDone <- nil
				return
			case frame, ok := <-outbound:
				if !ok {
					sendDone <- nil
					return
				}
				if frame == nil {
					continue
				}
				if err := stream.Send(frame); err != nil {
					sendDone <- err
					return
				}
			}
		}
	}()

	initial, err := BuildInitialRuntimeFrame(c.cfg.Identity, c.cfg.Credential)
	if err != nil {
		return err
	}
	if err := c.sendOutbound(runCtx, outbound, initial); err != nil {
		return err
	}

	heartbeatStop := make(chan struct{})
	go c.heartbeatLoop(runCtx, outbound, heartbeatStop)
	defer close(heartbeatStop)

	recvErr := make(chan error, 1)
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			c.handleInboundFrame(runCtx, frame, outbound)
		}
	}()

	select {
	case <-ctx.Done():
		cancel()
		<-sendDone
		return nil
	case err := <-sendDone:
		cancel()
		if ctx.Err() != nil || err == io.EOF {
			return nil
		}
		return err
	case err := <-recvErr:
		cancel()
		<-sendDone
		if ctx.Err() != nil || err == io.EOF {
			return nil
		}
		return err
	}
}

func normalizeRuntimeChannelAddress(address string) string {
	address = strings.TrimSpace(address)
	if strings.HasPrefix(address, "ipv4:") && strings.Count(address, ":") == 2 {
		return strings.TrimPrefix(address, "ipv4:")
	}
	return address
}

func (c *RuntimeChannelClient) Send(frame *cpv1.RuntimeFrame) error {
	c.mu.Lock()
	outbound := c.outbound
	c.mu.Unlock()
	if outbound == nil {
		return fmt.Errorf("runtime channel is not connected")
	}
	select {
	case outbound <- frame:
		return nil
	default:
		return fmt.Errorf("runtime channel outbound queue is full")
	}
}

// WaitAuthenticated blocks until control-panel-service has accepted the
// initial HELLO and returned HELLO_ACK. Merely opening the transport or
// enqueueing HELLO is not sufficient proof that the runtime identity was
// admitted.
func (c *RuntimeChannelClient) WaitAuthenticated(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.connected:
		return nil
	}
}

func (c *RuntimeChannelClient) sendOutbound(
	ctx context.Context,
	outbound chan<- *cpv1.RuntimeFrame,
	frame *cpv1.RuntimeFrame,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("runtime channel outbound is closed")
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outbound <- frame:
		return nil
	}
}

func (c *RuntimeChannelClient) InvokePlatformAny(
	ctx context.Context,
	method string,
	request *anypb.Any,
	timeout time.Duration,
) (*anypb.Any, error) {
	method = strings.TrimSpace(method)
	if method == "" {
		return nil, fmt.Errorf("platform method is required")
	}
	if request == nil {
		return nil, fmt.Errorf("platform request payload is required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.connected:
	case <-time.After(timeout):
		return nil, fmt.Errorf("runtime channel is not connected")
	}
	correlationID, err := randomToken()
	if err != nil {
		return nil, err
	}
	reply := make(chan *cpv1.RuntimeFrame, 8)
	c.mu.Lock()
	c.pending[correlationID] = reply
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, correlationID)
		c.mu.Unlock()
	}()

	deadline := time.Now().Add(timeout)
	if err := c.Send(&cpv1.RuntimeFrame{
		CorrelationId:  correlationID,
		FrameType:      cpv1.FrameType_FRAME_TYPE_REQUEST,
		DeadlineUnixMs: deadline.UnixMilli(),
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  method,
			Request: request,
		}},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("runtime platform request timed out: %s", method)
	case frame := <-reply:
		switch frame.GetFrameType() {
		case cpv1.FrameType_FRAME_TYPE_RESPONSE:
			resp := frame.GetResponse()
			if resp == nil || resp.GetResponse() == nil {
				return nil, fmt.Errorf("runtime platform response payload is empty")
			}
			return resp.GetResponse(), nil
		case cpv1.FrameType_FRAME_TYPE_ERROR:
			errFrame := frame.GetError()
			if errFrame == nil {
				return nil, fmt.Errorf("runtime platform request failed")
			}
			return nil, fmt.Errorf("%s: %s", errFrame.GetCode(), errFrame.GetMessage())
		default:
			return nil, fmt.Errorf("unexpected runtime platform frame_type=%v", frame.GetFrameType())
		}
	}
}

func (c *RuntimeChannelClient) heartbeatLoop(
	ctx context.Context,
	outbound chan<- *cpv1.RuntimeFrame,
	stop <-chan struct{},
) {
	ticker := time.NewTicker(time.Duration(c.cfg.HeartbeatSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			_ = c.sendOutbound(ctx, outbound, c.heartbeatFrame())
		}
	}
}

func (c *RuntimeChannelClient) heartbeatFrame() *cpv1.RuntimeFrame {
	c.mu.Lock()
	fingerprint := c.fingerprint
	c.mu.Unlock()
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT,
		Payload: &cpv1.RuntimeFrame_Heartbeat{Heartbeat: &cpv1.Heartbeat{
			SentAtUnixMs: time.Now().UnixMilli(),
			Fingerprint:  fingerprint,
		}},
	}
}

func (c *RuntimeChannelClient) handleInboundFrame(
	ctx context.Context,
	frame *cpv1.RuntimeFrame,
	outbound chan<- *cpv1.RuntimeFrame,
) {
	if frame == nil {
		return
	}
	if c.deliverPending(frame) {
		return
	}
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_HELLO_ACK:
		ack := frame.GetHelloAck()
		if ack != nil {
			c.mu.Lock()
			c.runtimeID = strings.TrimSpace(ack.GetRuntimeId())
			c.fingerprint = strings.TrimSpace(firstNonEmpty(ack.GetFingerprint(), ack.GetResumeToken()))
			c.mu.Unlock()
			c.connectOnce.Do(func() { close(c.connected) })
		}
	case cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK:
		ack := frame.GetHeartbeatAck()
		if ack != nil {
			c.mu.Lock()
			if strings.TrimSpace(ack.GetRuntimeId()) != "" {
				c.runtimeID = strings.TrimSpace(ack.GetRuntimeId())
			}
			c.fingerprint = strings.TrimSpace(ack.GetFingerprint())
			c.mu.Unlock()
		}
	case cpv1.FrameType_FRAME_TYPE_REQUEST:
		handler := c.cfg.RequestHandler
		if handler == nil {
			_ = c.sendOutbound(ctx, outbound, runtimeErrorFrame(frame.GetCorrelationId(), "Unimplemented", "runtime request handler is not configured"))
			return
		}
		go func() {
			response := handler(ctx, frame)
			if response == nil {
				response = runtimeErrorFrame(frame.GetCorrelationId(), "Internal", "runtime request handler returned nil")
			}
			_ = c.sendOutbound(ctx, outbound, response)
		}()
	case cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH:
		var err error
		if c.cfg.DataHandler != nil {
			err = c.cfg.DataHandler(ctx, frame)
		} else {
			err = fmt.Errorf("runtime data handler is not configured")
		}
		if err != nil {
			if backpressure := dataBackpressureForFrame(frame, err); backpressure != nil {
				_ = c.sendOutbound(ctx, outbound, backpressure)
			}
			return
		}
		if ack := dataAckForFrame(frame); ack != nil {
			_ = c.sendOutbound(ctx, outbound, ack)
		}
	}
}

func (c *RuntimeChannelClient) deliverPending(frame *cpv1.RuntimeFrame) bool {
	correlationID := strings.TrimSpace(frame.GetCorrelationId())
	if correlationID == "" {
		return false
	}
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_RESPONSE,
		cpv1.FrameType_FRAME_TYPE_ERROR,
		cpv1.FrameType_FRAME_TYPE_PROGRESS:
	default:
		return false
	}
	c.mu.Lock()
	reply := c.pending[correlationID]
	c.mu.Unlock()
	if reply == nil {
		return false
	}
	select {
	case reply <- frame:
	default:
	}
	return true
}

func BuildInitialRuntimeFrame(identity RuntimeIdentity, credential *RuntimeCredential) (*cpv1.RuntimeFrame, error) {
	source := strings.TrimSpace(identity.Source)
	if strings.EqualFold(source, "bare") {
		hello, err := buildBareHello(identity)
		if err != nil {
			return nil, err
		}
		return &cpv1.RuntimeFrame{
			FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
			Payload:   &cpv1.RuntimeFrame_Hello{Hello: hello},
		}, nil
	}
	if credential == nil {
		return nil, fmt.Errorf("runtime credential is required for signed RuntimeChannel hello")
	}
	hello, err := buildSignedHello(identity, credential)
	if err != nil {
		return nil, err
	}
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: hello},
	}, nil
}

func buildBareHello(identity RuntimeIdentity) (*cpv1.RuntimeHello, error) {
	if !strings.EqualFold(strings.TrimSpace(identity.Source), "bare") {
		return nil, fmt.Errorf("bare RuntimeChannel hello requires source=bare")
	}
	if identity.UserID <= 0 {
		return nil, fmt.Errorf("bare RuntimeChannel hello requires user_id")
	}
	runtimeID := strings.TrimSpace(identity.RuntimeID)
	if runtimeID == "" {
		return nil, fmt.Errorf("bare RuntimeChannel hello requires runtime_id")
	}
	dependencyProfile, err := verifiedIdentityDependencyProfile(identity)
	if err != nil {
		return nil, err
	}
	return &cpv1.RuntimeHello{
		Source:            "bare",
		UserId:            identity.UserID,
		RuntimeId:         runtimeID,
		Name:              strings.TrimSpace(identity.Name),
		EndpointHost:      strings.TrimSpace(identity.EndpointHost),
		GrpcPort:          identity.GRPCPort,
		DebugPort:         identity.DebugPort,
		Capabilities:      normalizeCapabilities(identity.Capabilities),
		ResourceProfile:   defaultString(identity.ResourceProfile, "small"),
		Version:           defaultString(identity.Version, "0.1.0"),
		IssuedAtUnixMs:    time.Now().UnixMilli(),
		DependencyProfile: dependencyProfile,
	}, nil
}

func buildSignedHello(identity RuntimeIdentity, credential *RuntimeCredential) (*cpv1.RuntimeHello, error) {
	keyID := strings.TrimSpace(firstNonEmpty(credential.KeyID, identity.KeyID))
	if keyID == "" {
		return nil, fmt.Errorf("key_id is required")
	}
	privateKeyPEM := firstNonEmpty(credential.PrivateKeyPEM, identity.PrivateKeyPEM)
	privateKey, err := loadEd25519PrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	dependencyProfile, err := verifiedIdentityDependencyProfile(identity)
	if err != nil {
		return nil, err
	}
	hello := &cpv1.RuntimeHello{
		KeyId:             keyID,
		Source:            strings.TrimSpace(identity.Source),
		UserId:            identity.UserID,
		RuntimeId:         strings.TrimSpace(identity.RuntimeID),
		Name:              strings.TrimSpace(identity.Name),
		EndpointHost:      strings.TrimSpace(identity.EndpointHost),
		GrpcPort:          identity.GRPCPort,
		DebugPort:         identity.DebugPort,
		Capabilities:      normalizeCapabilities(identity.Capabilities),
		ResourceProfile:   defaultString(identity.ResourceProfile, "small"),
		Version:           defaultString(identity.Version, "0.1.0"),
		IssuedAtUnixMs:    time.Now().UnixMilli(),
		Nonce:             b64URLNoPad(randomBytes(16)),
		DependencyProfile: dependencyProfile,
	}
	signature := ed25519.Sign(privateKey, canonicalHelloPayload(hello))
	hello.Signature = b64URLNoPad(signature)
	return hello, nil
}

func verifiedIdentityDependencyProfile(identity RuntimeIdentity) (*strategyv1.RuntimeDependencyProfile, error) {
	profile := identity.DependencyProfile
	if profile == nil {
		return nil, fmt.Errorf("verified runtime dependency profile is required")
	}
	if err := validateEmbeddedRuntimeFacts(EmbeddedRuntimeFacts{
		Source:  strings.TrimSpace(identity.Source),
		Profile: profile,
	}); err != nil {
		return nil, fmt.Errorf("verified runtime dependency profile is invalid")
	}
	return proto.Clone(profile).(*strategyv1.RuntimeDependencyProfile), nil
}

func BuildResumeRuntimeFrame(
	identity RuntimeIdentity,
	resumeToken string,
	fingerprint string,
) (*cpv1.RuntimeFrame, error) {
	runtimeID := strings.TrimSpace(identity.RuntimeID)
	resumeToken = strings.TrimSpace(resumeToken)
	fingerprint = strings.TrimSpace(fingerprint)
	if runtimeID == "" || resumeToken == "" || fingerprint == "" {
		return nil, fmt.Errorf("runtime_id, resume_token, and fingerprint are required")
	}
	profile, err := verifiedIdentityDependencyProfile(identity)
	if err != nil {
		return nil, err
	}
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_RESUME,
		Payload: &cpv1.RuntimeFrame_Resume{Resume: &cpv1.RuntimeResume{
			RuntimeId:         runtimeID,
			ResumeToken:       resumeToken,
			Fingerprint:       fingerprint,
			DependencyProfile: profile,
		}},
	}, nil
}

func loadEd25519PrivateKey(privateKeyPEM string) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return nil, fmt.Errorf("private_key_pem must contain an Ed25519 private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private_key_pem must contain an Ed25519 private key")
	}
	return edKey, nil
}

func canonicalHelloPayload(hello *cpv1.RuntimeHello) []byte {
	profile := hello.GetDependencyProfile()
	publicImportRoots := append([]string(nil), profile.GetPublicImportRoots()...)
	sort.Strings(publicImportRoots)
	fields := []struct {
		name  string
		value any
	}{
		{"capabilities", hello.GetCapabilities()},
		{"dependency_contract_sha256", profile.GetContractSha256()},
		{"dependency_hosted_python", profile.GetHostedPython()},
		{"dependency_image_build_id", profile.GetImageBuildId()},
		{"dependency_profile_name", profile.GetProfileName()},
		{"dependency_profile_version", profile.GetProfileVersion()},
		{"dependency_public_import_roots", publicImportRoots},
		{"dependency_schema_version", profile.GetSchemaVersion()},
		{"dependency_strategy_library_commit", profile.GetStrategyLibraryCommit()},
		{"dependency_strategy_service_commit", profile.GetStrategyServiceCommit()},
		{"debug_port", int(hello.GetDebugPort())},
		{"endpoint_host", hello.GetEndpointHost()},
		{"grpc_port", int(hello.GetGrpcPort())},
		{"issued_at_unix_ms", hello.GetIssuedAtUnixMs()},
		{"key_id", hello.GetKeyId()},
		{"nonce", hello.GetNonce()},
		{"resource_profile", hello.GetResourceProfile()},
		{"runtime_id", hello.GetRuntimeId()},
		{"name", hello.GetName()},
		{"source", hello.GetSource()},
		{"user_id", hello.GetUserId()},
		{"version", hello.GetVersion()},
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		name, _ := json.Marshal(field.name)
		value, _ := json.Marshal(field.value)
		b.Write(name)
		b.WriteByte(':')
		b.Write(value)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

func randomBytes(n int) []byte {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return buf
}

func b64URLNoPad(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}

func runtimeErrorFrame(correlationID string, code string, message string) *cpv1.RuntimeFrame {
	return runtimeErrorFrameWithDependency(correlationID, code, message, nil)
}

func runtimeErrorFrameWithDependency(
	correlationID string,
	code string,
	message string,
	dependencyError *strategyv1.RuntimeDependencyError,
) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		CorrelationId: correlationID,
		FrameType:     cpv1.FrameType_FRAME_TYPE_ERROR,
		Payload: &cpv1.RuntimeFrame_Error{Error: &cpv1.StreamError{
			Code:            code,
			Message:         message,
			DependencyError: dependencyError,
		}},
	}
}

func dataAckForFrame(frame *cpv1.RuntimeFrame) *cpv1.RuntimeFrame {
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK:
		chunk := frame.GetDatasetChunk()
		if chunk == nil {
			return nil
		}
		return runtimeDataAckFrame(chunk.GetSessionId(), chunk.GetDatasetId(), chunk.GetSequence())
	case cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH:
		batch := frame.GetLiveKlineBatch()
		if batch == nil {
			return nil
		}
		return runtimeDataAckFrame(batch.GetSessionId(), batch.GetStreamKey(), batch.GetSequence())
	case cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH:
		batch := frame.GetOrderUpdateBatch()
		if batch == nil {
			return nil
		}
		streamKey := batch.GetStreamKey()
		if streamKey == "" {
			streamKey = "order_lifecycle"
		}
		return runtimeDataAckFrame(batch.GetSessionId(), streamKey, batch.GetSequence())
	default:
		return nil
	}
}

func runtimeDataAckFrame(sessionID, streamKey string, sequence int64) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_ACK,
		Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
			SessionId: sessionID,
			StreamKey: streamKey,
			Sequence:  sequence,
		}},
	}
}

func dataBackpressureForFrame(frame *cpv1.RuntimeFrame, cause error) *cpv1.RuntimeFrame {
	if frame == nil {
		return nil
	}
	sessionID := ""
	streamKey := ""
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK:
		chunk := frame.GetDatasetChunk()
		if chunk == nil {
			return nil
		}
		sessionID = chunk.GetSessionId()
		streamKey = chunk.GetDatasetId()
	case cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH:
		batch := frame.GetLiveKlineBatch()
		if batch == nil {
			return nil
		}
		sessionID = batch.GetSessionId()
		streamKey = batch.GetStreamKey()
	case cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH:
		batch := frame.GetOrderUpdateBatch()
		if batch == nil {
			return nil
		}
		sessionID = batch.GetSessionId()
		streamKey = firstNonEmpty(batch.GetStreamKey(), "order_lifecycle")
	default:
		return nil
	}
	reason := "runtime data delivery failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		reason = cause.Error()
	}
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{DataBackpressure: &cpv1.RuntimeDataBackpressure{
			SessionId:         sessionID,
			StreamKey:         streamKey,
			ResumeAfterUnixMs: time.Now().Add(time.Second).UnixMilli(),
			Reason:            reason,
		}},
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
