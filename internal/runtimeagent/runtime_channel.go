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
	mathrand "math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

type runtimeChannelPlatformError struct {
	code            string
	message         string
	detailJSON      string
	dependencyError *strategyv1.RuntimeDependencyError
}

func (e *runtimeChannelPlatformError) Error() string {
	if e == nil {
		return "runtime platform request failed"
	}
	if strings.TrimSpace(e.code) == "" {
		return e.message
	}
	return e.code + ": " + e.message
}

func (e *runtimeChannelPlatformError) PlatformErrorCode() string       { return e.code }
func (e *runtimeChannelPlatformError) PlatformErrorMessage() string    { return e.message }
func (e *runtimeChannelPlatformError) PlatformErrorDetailJSON() string { return e.detailJSON }
func (e *runtimeChannelPlatformError) PlatformDependencyError() *strategyv1.RuntimeDependencyError {
	return cloneDependencyError(e.dependencyError)
}

func streamErrorDetailJSON(detail *strategyv1.RuntimeDependencyError) string {
	if detail == nil {
		return "{}"
	}
	payload := map[string]string{
		"code": detail.GetCode(), "module": detail.GetModule(),
		"runtime_profile":         detail.GetRuntimeProfile(),
		"runtime_profile_version": detail.GetRuntimeProfileVersion(),
		"image_build_id":          detail.GetImageBuildId(), "message": detail.GetMessage(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
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
	HeartbeatTicks   <-chan time.Time
	DialOptions      []grpc.DialOption
	RequestHandler   RuntimeRequestHandler
	DataHandler      RuntimeDataHandler
	ReconnectJitter  func(time.Duration) time.Duration
	ReconnectWait    func(context.Context, time.Duration) error
}

type runtimeChannelOutbound struct {
	frame *cpv1.RuntimeFrame
	done  chan error
}

type runtimeChannelGeneration struct {
	id       uint64
	outbound chan *runtimeChannelOutbound
	ready    bool
}

type runtimeChannelPendingResult struct {
	frame *cpv1.RuntimeFrame
	err   error
}

type runtimeChannelPendingCall struct {
	generation uint64
	reply      chan runtimeChannelPendingResult
}

type retainedRuntimeDataACK struct {
	revision uint64
	frame    *cpv1.RuntimeFrame
}

type RuntimeChannelClient struct {
	cfg RuntimeChannelClientConfig

	mu          sync.Mutex
	runtimeID   string
	resumeToken string
	fingerprint string
	generation  uint64
	current     *runtimeChannelGeneration
	pending     map[string]*runtimeChannelPendingCall
	ackRevision uint64
	retainedACK map[string]*retainedRuntimeDataACK
	readyChange chan struct{}
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
	if cfg.ReconnectJitter == nil {
		cfg.ReconnectJitter = func(max time.Duration) time.Duration {
			if max <= 0 {
				return 0
			}
			return time.Duration(mathrand.Int64N(int64(max) + 1))
		}
	}
	if cfg.ReconnectWait == nil {
		cfg.ReconnectWait = waitRuntimeReconnect
	}
	return &RuntimeChannelClient{
		cfg:         cfg,
		pending:     map[string]*runtimeChannelPendingCall{},
		retainedACK: map[string]*retainedRuntimeDataACK{},
		readyChange: make(chan struct{}),
		connected:   make(chan struct{}),
	}
}

func (c *RuntimeChannelClient) Run(ctx context.Context) error {
	address := normalizeRuntimeChannelAddress(c.cfg.Address)
	if address == "" {
		return fmt.Errorf("runtime channel address is required")
	}
	attempt := 0
	for {
		authenticated, err := c.runConnection(ctx, address)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = status.Error(codes.Unavailable, "runtime channel disconnected")
		}
		if isPermanentRuntimeChannelError(err) {
			return err
		}
		if authenticated {
			attempt = 0
		} else {
			attempt++
		}
		maximum := runtimeReconnectMaximum(attempt)
		delay := c.cfg.ReconnectJitter(maximum)
		if delay < 0 {
			delay = 0
		}
		if delay > maximum {
			delay = maximum
		}
		if err := c.cfg.ReconnectWait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (c *RuntimeChannelClient) runConnection(ctx context.Context, address string) (bool, error) {
	conn, err := grpc.DialContext(ctx, address, c.cfg.DialOptions...)
	if err != nil {
		return false, fmt.Errorf("dial runtime channel: %w", err)
	}
	defer conn.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := cpv1.NewControlPanelServiceClient(conn).RuntimeChannel(runCtx)
	if err != nil {
		return false, fmt.Errorf("open runtime channel: %w", err)
	}

	c.mu.Lock()
	c.generation++
	generation := &runtimeChannelGeneration{
		id: c.generation, outbound: make(chan *runtimeChannelOutbound, 128),
	}
	c.current = generation
	c.signalReadyChangeLocked()
	resumeToken := c.resumeToken
	fingerprint := c.fingerprint
	c.mu.Unlock()
	defer c.finishGeneration(generation)

	var initial *cpv1.RuntimeFrame
	if resumeToken != "" || fingerprint != "" {
		initial, err = BuildResumeRuntimeFrame(c.cfg.Identity, resumeToken, fingerprint)
	} else {
		initial, err = BuildInitialRuntimeFrame(c.cfg.Identity, c.cfg.Credential)
	}
	if err != nil {
		return false, err
	}

	var loops sync.WaitGroup
	sendDone := make(chan error, 1)
	loops.Add(1)
	go func() {
		defer loops.Done()
		for {
			select {
			case <-runCtx.Done():
				sendDone <- nil
				return
			case item := <-generation.outbound:
				if item == nil || item.frame == nil {
					continue
				}
				sendErr := stream.Send(item.frame)
				if item.done != nil {
					item.done <- sendErr
				}
				if sendErr != nil {
					sendDone <- sendErr
					return
				}
			}
		}
	}()
	if err := c.enqueueGeneration(runCtx, generation, initial, nil); err != nil {
		cancel()
		loops.Wait()
		return false, err
	}

	recvDone := make(chan error, 1)
	authenticatedCh := make(chan struct{}, 1)
	loops.Add(1)
	go func() {
		defer loops.Done()
		for {
			frame, recvErr := stream.Recv()
			if recvErr != nil {
				recvDone <- recvErr
				return
			}
			if c.handleGenerationInboundFrame(runCtx, generation, frame) {
				select {
				case authenticatedCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	authenticated := false
	var result error
	select {
	case <-ctx.Done():
		result = nil
	case <-sendDone:
		result = waitForRuntimeChannelReceive(runCtx, recvDone)
	case result = <-recvDone:
	case <-authenticatedCh:
		authenticated = true
		result = c.runAuthenticatedGeneration(runCtx, generation, sendDone, recvDone, &loops)
	}
	c.markGenerationUnready(generation)
	cancel()
	_ = conn.Close()
	loops.Wait()
	if ctx.Err() != nil {
		return authenticated, nil
	}
	if result == nil || errorsIsEOF(result) {
		return authenticated, status.Error(codes.Unavailable, "runtime channel disconnected")
	}
	return authenticated, result
}

func (c *RuntimeChannelClient) runAuthenticatedGeneration(
	ctx context.Context,
	generation *runtimeChannelGeneration,
	sendDone <-chan error,
	recvDone <-chan error,
	loops *sync.WaitGroup,
) error {
	loops.Add(1)
	go func() {
		defer loops.Done()
		c.generationHeartbeatLoop(ctx, generation)
	}()
	if err := c.enqueueGeneration(ctx, generation, c.heartbeatFrame(), nil); err != nil {
		c.markGenerationUnready(generation)
		return waitForRuntimeChannelReceive(ctx, recvDone)
	}
	if err := c.replayRetainedACKs(ctx, generation); err != nil {
		c.markGenerationUnready(generation)
		return waitForRuntimeChannelReceive(ctx, recvDone)
	}
	select {
	case <-ctx.Done():
		return nil
	case <-sendDone:
		c.markGenerationUnready(generation)
		return waitForRuntimeChannelReceive(ctx, recvDone)
	case err := <-recvDone:
		return err
	}
}

// waitForRuntimeChannelReceive gives RecvMsg ownership of the stream's final
// RPC status after the send loop terminates. Root cancellation remains the only
// escape when the peer does not publish a final receive status.
func waitForRuntimeChannelReceive(ctx context.Context, recvDone <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-recvDone:
		return err
	}
}

func errorsIsEOF(err error) bool {
	return err == io.EOF || status.Code(err) == codes.OK
}

func runtimeReconnectMaximum(attempt int) time.Duration {
	maximum := 250 * time.Millisecond
	for i := 1; i < attempt && maximum < 5*time.Second; i++ {
		maximum *= 2
	}
	if maximum > 5*time.Second {
		return 5 * time.Second
	}
	return maximum
}

func waitRuntimeReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isPermanentRuntimeChannelError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied,
		codes.FailedPrecondition, codes.NotFound:
		return true
	default:
		return false
	}
}

func (c *RuntimeChannelClient) finishGeneration(generation *runtimeChannelGeneration) {
	unavailable := status.Error(codes.Unavailable, "runtime channel generation disconnected")
	c.mu.Lock()
	if c.current == generation {
		c.current = nil
		c.signalReadyChangeLocked()
	}
	failures := make([]chan runtimeChannelPendingResult, 0)
	for correlationID, pending := range c.pending {
		if pending.generation == generation.id {
			delete(c.pending, correlationID)
			failures = append(failures, pending.reply)
		}
	}
	c.mu.Unlock()
	for _, reply := range failures {
		reply <- runtimeChannelPendingResult{err: unavailable}
	}
}

func (c *RuntimeChannelClient) markGenerationUnready(generation *runtimeChannelGeneration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != generation || !generation.ready {
		return
	}
	generation.ready = false
	c.signalReadyChangeLocked()
}

func normalizeRuntimeChannelAddress(address string) string {
	address = strings.TrimSpace(address)
	if strings.HasPrefix(address, "ipv4:") && strings.Count(address, ":") == 2 {
		return strings.TrimPrefix(address, "ipv4:")
	}
	return address
}

func (c *RuntimeChannelClient) Send(frame *cpv1.RuntimeFrame) error {
	if frame == nil {
		return status.Error(codes.InvalidArgument, "runtime channel frame is required")
	}
	if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_DATA_ACK && frame.GetDataAck() != nil {
		return c.retainAndSendDataACK(frame)
	}
	c.mu.Lock()
	generation := c.current
	ready := generation != nil && generation.ready
	c.mu.Unlock()
	if !ready {
		return status.Error(codes.Unavailable, "runtime channel is not authenticated")
	}
	done := make(chan error, 1)
	select {
	case generation.outbound <- &runtimeChannelOutbound{frame: frame, done: done}:
	default:
		return status.Error(codes.ResourceExhausted, "runtime channel outbound queue is full")
	}
	select {
	case err := <-done:
		if err != nil {
			return status.Errorf(codes.Unavailable, "send runtime channel frame: %v", err)
		}
		return nil
	case <-c.generationDone(generation):
		return status.Error(codes.Unavailable, "runtime channel generation disconnected")
	}
}

func (c *RuntimeChannelClient) retainAndSendDataACK(frame *cpv1.RuntimeFrame) error {
	key := runtimeDataACKKey(frame.GetDataAck())
	c.mu.Lock()
	c.ackRevision++
	entry := &retainedRuntimeDataACK{
		revision: c.ackRevision,
		frame:    proto.Clone(frame).(*cpv1.RuntimeFrame),
	}
	c.retainedACK[key] = entry
	generation := c.current
	ready := generation != nil && generation.ready
	c.mu.Unlock()
	if !ready {
		return nil
	}
	if err := c.sendRetainedACK(context.Background(), generation, key, entry); err != nil {
		return nil
	}
	return nil
}

func runtimeDataACKKey(ack *cpv1.RuntimeDataAck) string {
	if ack == nil {
		return ""
	}
	return fmt.Sprintf("%s\x00%s\x00%d", ack.GetSessionId(), ack.GetStreamKey(), ack.GetSequence())
}

func (c *RuntimeChannelClient) sendRetainedACK(
	ctx context.Context,
	generation *runtimeChannelGeneration,
	key string,
	entry *retainedRuntimeDataACK,
) error {
	done := make(chan error, 1)
	if err := c.enqueueGeneration(ctx, generation, entry.frame, done); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.generationDone(generation):
		return status.Error(codes.Unavailable, "runtime channel generation disconnected")
	case err := <-done:
		if err != nil {
			return err
		}
		c.mu.Lock()
		if retained := c.retainedACK[key]; retained == entry && retained.revision == entry.revision {
			delete(c.retainedACK, key)
		}
		c.mu.Unlock()
		return nil
	}
}

func (c *RuntimeChannelClient) replayRetainedACKs(
	ctx context.Context,
	generation *runtimeChannelGeneration,
) error {
	c.mu.Lock()
	keys := make([]string, 0, len(c.retainedACK))
	entries := make(map[string]*retainedRuntimeDataACK, len(c.retainedACK))
	for key, entry := range c.retainedACK {
		keys = append(keys, key)
		entries[key] = entry
	}
	c.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.sendRetainedACK(ctx, generation, key, entries[key]); err != nil {
			return err
		}
	}
	return nil
}

func (c *RuntimeChannelClient) enqueueGeneration(
	ctx context.Context,
	generation *runtimeChannelGeneration,
	frame *cpv1.RuntimeFrame,
	done chan error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case generation.outbound <- &runtimeChannelOutbound{frame: frame, done: done}:
		return nil
	}
}

func (c *RuntimeChannelClient) generationDone(generation *runtimeChannelGeneration) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != generation {
		done := make(chan struct{})
		close(done)
		return done
	}
	return c.readyChange
}

func (c *RuntimeChannelClient) signalReadyChangeLocked() {
	close(c.readyChange)
	c.readyChange = make(chan struct{})
}

func (c *RuntimeChannelClient) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current != nil && c.current.ready
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
	deadline := time.Now().Add(timeout)
	if err := c.waitReady(ctx, deadline); err != nil {
		return nil, err
	}
	correlationID, err := randomToken()
	if err != nil {
		return nil, err
	}
	reply := make(chan runtimeChannelPendingResult, 8)
	c.mu.Lock()
	generation := c.current
	if generation == nil || !generation.ready {
		c.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "runtime channel is not authenticated")
	}
	c.pending[correlationID] = &runtimeChannelPendingCall{
		generation: generation.id,
		reply:      reply,
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, correlationID)
		c.mu.Unlock()
	}()

	requestFrame := &cpv1.RuntimeFrame{
		CorrelationId:  correlationID,
		FrameType:      cpv1.FrameType_FRAME_TYPE_REQUEST,
		DeadlineUnixMs: deadline.UnixMilli(),
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  method,
			Request: request,
		}},
	}
	select {
	case generation.outbound <- &runtimeChannelOutbound{frame: requestFrame}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "runtime channel outbound queue is full")
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("runtime platform request timed out: %s", method)
	case result := <-reply:
		if result.err != nil {
			return nil, result.err
		}
		frame := result.frame
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
			detailJSON := errFrame.GetErrorDetailJson()
			if strings.TrimSpace(detailJSON) == "" {
				detailJSON = streamErrorDetailJSON(errFrame.GetDependencyError())
			}
			return nil, &runtimeChannelPlatformError{
				code: errFrame.GetCode(), message: errFrame.GetMessage(),
				detailJSON:      detailJSON,
				dependencyError: cloneDependencyError(errFrame.GetDependencyError()),
			}
		default:
			return nil, fmt.Errorf("unexpected runtime platform frame_type=%v", frame.GetFrameType())
		}
	}
}

func (c *RuntimeChannelClient) waitReady(ctx context.Context, deadline time.Time) error {
	for {
		c.mu.Lock()
		ready := c.current != nil && c.current.ready
		changed := c.readyChange
		c.mu.Unlock()
		if ready {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return status.Error(codes.Unavailable, "runtime channel is not authenticated")
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return status.Error(codes.Unavailable, "runtime channel is not authenticated")
		}
	}
}

func (c *RuntimeChannelClient) heartbeatLoop(
	ctx context.Context,
	outbound chan<- *cpv1.RuntimeFrame,
	stop <-chan struct{},
) {
	ticks := c.cfg.HeartbeatTicks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(time.Duration(c.cfg.HeartbeatSeconds) * time.Second)
		ticks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			_ = c.sendOutbound(ctx, outbound, c.heartbeatFrame())
		}
	}
}

func (c *RuntimeChannelClient) generationHeartbeatLoop(
	ctx context.Context,
	generation *runtimeChannelGeneration,
) {
	ticks := c.cfg.HeartbeatTicks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(time.Duration(c.cfg.HeartbeatSeconds) * time.Second)
		ticks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			if err := c.enqueueGeneration(ctx, generation, c.heartbeatFrame(), nil); err != nil {
				return
			}
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

func (c *RuntimeChannelClient) handleGenerationInboundFrame(
	ctx context.Context,
	generation *runtimeChannelGeneration,
	frame *cpv1.RuntimeFrame,
) bool {
	if frame == nil {
		return false
	}
	if c.deliverPending(frame) {
		return false
	}
	send := func(outbound *cpv1.RuntimeFrame) {
		_ = c.enqueueGeneration(ctx, generation, outbound, nil)
	}
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_HELLO_ACK:
		ack := frame.GetHelloAck()
		if ack == nil {
			return false
		}
		c.mu.Lock()
		if c.current != generation || generation.ready {
			c.mu.Unlock()
			return false
		}
		if runtimeID := strings.TrimSpace(ack.GetRuntimeId()); runtimeID != "" {
			c.runtimeID = runtimeID
		}
		if token := strings.TrimSpace(ack.GetResumeToken()); token != "" {
			c.resumeToken = token
		}
		if fingerprint := strings.TrimSpace(firstNonEmpty(ack.GetFingerprint(), ack.GetResumeToken())); fingerprint != "" {
			c.fingerprint = fingerprint
		}
		generation.ready = true
		c.signalReadyChangeLocked()
		c.mu.Unlock()
		c.connectOnce.Do(func() { close(c.connected) })
		return true
	case cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK:
		ack := frame.GetHeartbeatAck()
		if ack != nil {
			c.mu.Lock()
			if c.current == generation {
				if runtimeID := strings.TrimSpace(ack.GetRuntimeId()); runtimeID != "" {
					c.runtimeID = runtimeID
				}
				if fingerprint := strings.TrimSpace(ack.GetFingerprint()); fingerprint != "" {
					c.fingerprint = fingerprint
					c.resumeToken = fingerprint
				}
			}
			c.mu.Unlock()
		}
	case cpv1.FrameType_FRAME_TYPE_REQUEST:
		handler := c.cfg.RequestHandler
		if handler == nil {
			send(runtimeErrorFrame(frame.GetCorrelationId(), "Unimplemented", "runtime request handler is not configured"))
			return false
		}
		go func() {
			response := handler(ctx, frame)
			if response == nil {
				response = runtimeErrorFrame(frame.GetCorrelationId(), "Internal", "runtime request handler returned nil")
			}
			send(response)
		}()
	case cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH,
		cpv1.FrameType_FRAME_TYPE_INCOME_BATCH:
		var err error
		if c.cfg.DataHandler != nil {
			err = c.cfg.DataHandler(ctx, frame)
		} else {
			err = fmt.Errorf("runtime data handler is not configured")
		}
		if err != nil {
			if backpressure := dataBackpressureForFrame(frame, err); backpressure != nil {
				send(backpressure)
			}
			return false
		}
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_INCOME_BATCH {
			if ack := dataAckForFrame(frame); ack != nil {
				send(ack)
			}
		}
	}
	return false
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
			c.resumeToken = strings.TrimSpace(firstNonEmpty(ack.GetResumeToken(), ack.GetFingerprint()))
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
		cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH,
		cpv1.FrameType_FRAME_TYPE_INCOME_BATCH:
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
		// Income remains pending in the Agent until the current authenticated
		// Worker generation confirms durable application. Queue admission alone
		// cannot advance the control-panel cursor across a Worker-only restart.
		if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_INCOME_BATCH {
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
	pending := c.pending[correlationID]
	c.mu.Unlock()
	if pending == nil {
		return false
	}
	select {
	case pending.reply <- runtimeChannelPendingResult{frame: frame}:
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
	return runtimeErrorFrameWithDetail(
		correlationID, code, message, "", dependencyError,
	)
}

func runtimeErrorFrameWithDetail(
	correlationID string,
	code string,
	message string,
	errorDetailJSON string,
	dependencyError *strategyv1.RuntimeDependencyError,
) *cpv1.RuntimeFrame {
	if strings.TrimSpace(errorDetailJSON) == "" {
		errorDetailJSON = "{}"
	}
	return &cpv1.RuntimeFrame{
		CorrelationId: correlationID,
		FrameType:     cpv1.FrameType_FRAME_TYPE_ERROR,
		Payload: &cpv1.RuntimeFrame_Error{Error: &cpv1.StreamError{
			Code:            code,
			Message:         message,
			DependencyError: dependencyError,
			ErrorDetailJson: errorDetailJSON,
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
	case cpv1.FrameType_FRAME_TYPE_INCOME_BATCH:
		batch := frame.GetIncomeBatch()
		if batch == nil {
			return nil
		}
		return runtimeDataAckFrame(batch.GetSessionId(), batch.GetStreamKey(), batch.GetSequence())
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
	case cpv1.FrameType_FRAME_TYPE_INCOME_BATCH:
		batch := frame.GetIncomeBatch()
		if batch == nil {
			return nil
		}
		sessionID = batch.GetSessionId()
		streamKey = batch.GetStreamKey()
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
