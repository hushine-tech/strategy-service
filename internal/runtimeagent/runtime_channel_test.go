package runtimeagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBuildInitialFrameUsesBareHelloForBareRuntime(t *testing.T) {
	frame, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source:            "bare",
		UserID:            6,
		RuntimeID:         "bare-6-test",
		Name:              "bare-debug-6-test",
		Capabilities:      []string{"strategy"},
		ResourceProfile:   "small",
		Version:           "0.1.0",
		DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
	}, nil)
	if err != nil {
		t.Fatalf("BuildInitialRuntimeFrame: %v", err)
	}

	hello := frame.GetHello()
	if hello == nil {
		t.Fatalf("hello frame is missing")
	}
	if hello.GetSource() != "bare" || hello.GetUserId() != 6 {
		t.Fatalf("hello identity = source:%q user:%d", hello.GetSource(), hello.GetUserId())
	}
	if hello.GetRuntimeId() != "bare-6-test" {
		t.Fatalf("runtime_id = %q", hello.GetRuntimeId())
	}
	if hello.GetDependencyProfile().GetContractSha256() != validEmbeddedRuntimeFacts("bare").Profile.GetContractSha256() {
		t.Fatalf("bare hello dependency profile = %+v", hello.GetDependencyProfile())
	}
}

func TestBuildInitialFrameRejectsBareRuntimeWithoutUser(t *testing.T) {
	_, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source:            "bare",
		RuntimeID:         "bare-missing-user",
		DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
	}, nil)
	if err == nil {
		t.Fatalf("bare hello without user id was accepted")
	}
}

func TestBuildInitialFrameSignsCredentialHello(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	raw, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pemBody := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))

	frame, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source:            "self_hosted",
		RuntimeID:         "rt-signed",
		Name:              "signed",
		Capabilities:      []string{"strategy"},
		ResourceProfile:   "small",
		Version:           "0.1.0",
		DependencyProfile: validEmbeddedRuntimeFacts("self_hosted").Profile,
	}, &RuntimeCredential{
		KeyID:         "key-1",
		PrivateKeyPEM: pemBody,
	})
	if err != nil {
		t.Fatalf("BuildInitialRuntimeFrame: %v", err)
	}

	hello := frame.GetHello()
	if hello.GetKeyId() != "key-1" || hello.GetSignature() == "" || hello.GetNonce() == "" {
		t.Fatalf("signed hello missing credential material: %+v", hello)
	}
	if hello.GetSource() != "self_hosted" {
		t.Fatalf("source = %q", hello.GetSource())
	}
	signature, err := base64.RawURLEncoding.DecodeString(hello.GetSignature())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(privateKey.Public().(ed25519.PublicKey), canonicalHelloPayload(hello), signature) {
		t.Fatal("signed hello does not cover dependency profile")
	}
	hello.DependencyProfile.ProfileName = "mutated"
	if ed25519.Verify(privateKey.Public().(ed25519.PublicKey), canonicalHelloPayload(hello), signature) {
		t.Fatal("hello signature remained valid after dependency profile mutation")
	}
}

func TestBuildInitialFrameRequiresCompleteDependencyProfile(t *testing.T) {
	_, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source: "bare", UserID: 6, RuntimeID: "bare-6-test",
	}, nil)
	if err == nil {
		t.Fatal("hello without verified dependency profile was accepted")
	}
}

func TestCanonicalHelloPayloadCoversEveryDependencyProfileFact(t *testing.T) {
	base := &cpv1.RuntimeHello{
		RuntimeId: "rt-1", Source: "self_hosted",
		DependencyProfile: validEmbeddedRuntimeFacts("self_hosted").Profile,
	}
	want := canonicalHelloPayload(base)
	mutations := map[string]func(*strategyv1.RuntimeDependencyProfile){
		"schema":  func(profile *strategyv1.RuntimeDependencyProfile) { profile.SchemaVersion++ },
		"name":    func(profile *strategyv1.RuntimeDependencyProfile) { profile.ProfileName = "mutated" },
		"version": func(profile *strategyv1.RuntimeDependencyProfile) { profile.ProfileVersion = "2.0.0" },
		"digest": func(profile *strategyv1.RuntimeDependencyProfile) {
			profile.ContractSha256 = "f" + profile.ContractSha256[1:]
		},
		"python": func(profile *strategyv1.RuntimeDependencyProfile) { profile.HostedPython = "3.14" },
		"roots":  func(profile *strategyv1.RuntimeDependencyProfile) { profile.PublicImportRoots[0] = "mutated" },
		"service": func(profile *strategyv1.RuntimeDependencyProfile) {
			profile.StrategyServiceCommit = "f" + profile.StrategyServiceCommit[1:]
		},
		"library": func(profile *strategyv1.RuntimeDependencyProfile) {
			profile.StrategyLibraryCommit = "f" + profile.StrategyLibraryCommit[1:]
		},
		"image": func(profile *strategyv1.RuntimeDependencyProfile) { profile.ImageBuildId += "-mutated" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(base).(*cpv1.RuntimeHello)
			mutate(changed.DependencyProfile)
			if bytes.Equal(canonicalHelloPayload(changed), want) {
				t.Fatal("canonical hello did not change")
			}
		})
	}
}

func TestBuildResumeRuntimeFrameCarriesImmutableDependencyProfile(t *testing.T) {
	profile := validEmbeddedRuntimeFacts("bare").Profile
	frame, err := BuildResumeRuntimeFrame(RuntimeIdentity{
		Source: "bare", RuntimeID: "bare-6-test", DependencyProfile: profile,
	}, "resume-token", "fingerprint")
	if err != nil {
		t.Fatalf("BuildResumeRuntimeFrame: %v", err)
	}
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESUME ||
		frame.GetResume().GetRuntimeId() != "bare-6-test" ||
		frame.GetResume().GetDependencyProfile().GetImageBuildId() != profile.GetImageBuildId() {
		t.Fatalf("resume frame = %+v", frame)
	}
	profile.ImageBuildId = "mutated"
	if frame.GetResume().GetDependencyProfile().GetImageBuildId() == "mutated" {
		t.Fatal("resume profile aliases caller-owned profile")
	}
}

func TestNormalizeRuntimeChannelAddressAcceptsLegacyIPv4Target(t *testing.T) {
	got := normalizeRuntimeChannelAddress("ipv4:192.168.65.254:50055")
	if got != "192.168.65.254:50055" {
		t.Fatalf("normalized address = %q, want 192.168.65.254:50055", got)
	}
}

func TestRuntimeChannelClientSendsInitialHello(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &captureRuntimeChannelServer{firstFrame: make(chan *cpv1.RuntimeFrame, 1)}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         "bare-6-test",
			Name:              "bare-debug-6-test",
			Capabilities:      []string{"strategy"},
			ResourceProfile:   "small",
			Version:           "0.1.0",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		HeartbeatSeconds: 1,
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Run(ctx)
	}()

	select {
	case frame := <-capture.firstFrame:
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HELLO {
			t.Fatalf("first frame type = %v", frame.GetFrameType())
		}
		if frame.GetHello().GetRuntimeId() != "bare-6-test" {
			t.Fatalf("runtime id = %q", frame.GetHello().GetRuntimeId())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("runtime channel did not send hello")
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("runtime channel did not stop after context cancel")
	}
}

func TestRuntimeChannelClientWaitAuthenticatedRequiresHelloAck(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &delayedHelloAckRuntimeChannelServer{
		firstFrame: make(chan *cpv1.RuntimeFrame, 1),
		sendAck:    make(chan struct{}),
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         "bare-6-auth-gate",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	runDone := make(chan error, 1)
	go func() {
		runDone <- client.Run(ctx)
	}()

	select {
	case <-capture.firstFrame:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime channel did not send HELLO")
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWait()
	if err := client.WaitAuthenticated(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAuthenticated before HELLO_ACK = %v, want deadline exceeded", err)
	}

	close(capture.sendAck)
	ackCtx, cancelAck := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAck()
	if err := client.WaitAuthenticated(ackCtx); err != nil {
		t.Fatalf("WaitAuthenticated after HELLO_ACK: %v", err)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime channel did not stop after context cancel")
	}
}

func TestRuntimeChannelClientInvokesPlatformRequest(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &platformRequestRuntimeChannelServer{requestFrame: make(chan *cpv1.RuntimeFrame, 1)}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         "bare-6-test",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Run(ctx)
	}()

	request, err := anypb.New(&strategyv1.GetStrategyStatusRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatalf("pack request: %v", err)
	}
	response, err := client.InvokePlatformAny(ctx, "GetStrategyStatus", request, time.Second)
	if err != nil {
		t.Fatalf("InvokePlatformAny: %v", err)
	}
	var status strategyv1.GetStrategyStatusResponse
	if err := response.UnmarshalTo(&status); err != nil {
		t.Fatalf("unpack response: %v", err)
	}
	if status.GetStatus() != "running" {
		t.Fatalf("status = %q", status.GetStatus())
	}

	select {
	case frame := <-capture.requestFrame:
		if frame.GetRequest().GetMethod() != "GetStrategyStatus" {
			t.Fatalf("method = %q", frame.GetRequest().GetMethod())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not receive platform request")
	}
	cancel()
	<-errCh
}

func TestRuntimeChannelHeartbeatIndependentOfBlockedRequestHandler(
	t *testing.T,
) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &blockedRequestRuntimeChannelServer{
		heartbeats: make(chan time.Time, 4),
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         "bare-6-heartbeat",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(
				context.Context,
				string,
			) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		HeartbeatSeconds: 1,
		RequestHandler: func(
			context.Context,
			*cpv1.RuntimeFrame,
		) *cpv1.RuntimeFrame {
			close(handlerStarted)
			<-releaseHandler
			return &cpv1.RuntimeFrame{
				CorrelationId: "blocked-request",
				FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
			}
		},
	})
	runDone := make(chan error, 1)
	go func() {
		runDone <- client.Run(ctx)
	}()
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked request handler did not start")
	}
	timestamps := make([]time.Time, 0, 3)
	for len(timestamps) < 3 {
		select {
		case timestamp := <-capture.heartbeats:
			timestamps = append(timestamps, timestamp)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"received %d heartbeats while request handler was blocked",
				len(timestamps),
			)
		}
	}
	select {
	case <-releaseHandler:
		t.Fatal("blocked request handler was unexpectedly released")
	default:
	}
	for index := 1; index < len(timestamps); index++ {
		if gap := timestamps[index].Sub(timestamps[index-1]); gap > 2*time.Second {
			t.Fatalf("heartbeat gap = %v, want <= 2s", gap)
		}
	}
	close(releaseHandler)
	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime channel did not stop")
	}
}

func TestRuntimeChannelDataHandlerErrorDoesNotAckDroppedData(t *testing.T) {
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		DataHandler: func(context.Context, *cpv1.RuntimeFrame) error {
			return context.Canceled
		},
	})
	outbound := make(chan *cpv1.RuntimeFrame, 1)

	client.handleInboundFrame(context.Background(), &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		Payload: &cpv1.RuntimeFrame_LiveKlineBatch{LiveKlineBatch: &cpv1.RuntimeLiveKlineBatch{
			SessionId: "sess-1",
			StreamKey: "futures:ZECUSDT:1m",
			Sequence:  42,
		}},
	}, outbound)

	select {
	case frame := <-outbound:
		if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_DATA_ACK {
			t.Fatalf("sent DATA_ACK for dropped data: %+v", frame.GetDataAck())
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRuntimeChannelIncomeWaitsForWorkerAckInsteadOfAckingOnQueue(t *testing.T) {
	called := false
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		DataHandler: func(_ context.Context, frame *cpv1.RuntimeFrame) error {
			called = frame.GetIncomeBatch() != nil
			return nil
		},
	})
	outbound := make(chan *cpv1.RuntimeFrame, 1)

	client.handleInboundFrame(context.Background(), runtimeIncomeFrameForTest("sess-income", 10), outbound)

	if !called {
		t.Fatal("Income frame did not enter the RuntimeChannel data handler")
	}
	select {
	case frame := <-outbound:
		t.Fatalf("Income queue admission emitted premature frame: %+v", frame)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRuntimeChannelIncomeDeliveryFailureEmitsBackpressureWithoutAck(t *testing.T) {
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		DataHandler: func(context.Context, *cpv1.RuntimeFrame) error {
			return errors.New("worker outbound queue is full")
		},
	})
	outbound := make(chan *cpv1.RuntimeFrame, 1)

	client.handleInboundFrame(context.Background(), runtimeIncomeFrameForTest("sess-income", 10), outbound)

	select {
	case frame := <-outbound:
		backpressure := frame.GetDataBackpressure()
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE ||
			backpressure.GetSessionId() != "sess-income" ||
			backpressure.GetStreamKey() != "income/sess-income" ||
			!strings.Contains(backpressure.GetReason(), "queue is full") {
			t.Fatalf("Income failure frame = %+v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("Income delivery failure emitted no backpressure")
	}
}

func TestRuntimeChannelIncomeBackpressureKeepsFakeTenMinuteHeartbeatAndAckFlow(t *testing.T) {
	ticks := make(chan time.Time, 600)
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		HeartbeatTicks: ticks,
		DataHandler: func(context.Context, *cpv1.RuntimeFrame) error {
			return errors.New("Worker has been blocked for ten logical minutes")
		},
	})
	outbound := make(chan *cpv1.RuntimeFrame, 602)
	client.handleInboundFrame(context.Background(), runtimeIncomeFrameForTest("sess-income", 10), outbound)
	if frame := <-outbound; frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE {
		t.Fatalf("blocked Worker frame = %+v, want backpressure", frame)
	}
	client.handleInboundFrame(context.Background(), &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK,
		Payload: &cpv1.RuntimeFrame_HeartbeatAck{HeartbeatAck: &cpv1.RuntimeHeartbeatAck{
			RuntimeId: "rt-income", Fingerprint: "after-ten-minutes",
		}},
	}, outbound)
	if frame := client.heartbeatFrame(); frame.GetHeartbeat().GetFingerprint() != "after-ten-minutes" {
		t.Fatalf("heartbeat ACK was not processed while Income was blocked: %+v", frame)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		client.heartbeatLoop(ctx, outbound, stop)
		close(done)
	}()
	for logicalSecond := 0; logicalSecond < 600; logicalSecond++ {
		ticks <- time.Unix(int64(logicalSecond), 0)
	}
	close(ticks)
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("fake ten-minute heartbeat loop did not finish")
	}
	cancel()
	close(stop)
	heartbeats := 0
	for len(outbound) > 0 {
		if frame := <-outbound; frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_HEARTBEAT {
			heartbeats++
		}
	}
	if heartbeats != 600 {
		t.Fatalf("fake ten-minute heartbeats = %d, want 600", heartbeats)
	}
}

func runtimeIncomeFrameForTest(sessionID string, sequence int64) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_INCOME_BATCH,
		Payload: &cpv1.RuntimeFrame_IncomeBatch{IncomeBatch: &cpv1.RuntimeIncomeBatch{
			SessionId: sessionID,
			StreamKey: "income/" + sessionID,
			Sequence:  sequence,
			Entries: []*portfoliov1.VenueIncomeEntry{{
				IncomeEntryId: sequence,
				SessionId:     sessionID,
				VenueId:       23,
				IncomeType:    "FUNDING_FEE",
				Source:        "exchange",
				Status:        "confirmed",
			}},
		}},
	}
}

func TestRuntimeChannelSafeSendAfterOutboundClosedDoesNotPanic(t *testing.T) {
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{})
	outbound := make(chan *cpv1.RuntimeFrame)
	close(outbound)

	if err := client.sendOutbound(context.Background(), outbound, client.heartbeatFrame()); err == nil {
		t.Fatalf("sendOutbound on closed channel succeeded")
	}
}

type captureRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	firstFrame chan *cpv1.RuntimeFrame
}

type delayedHelloAckRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	firstFrame chan *cpv1.RuntimeFrame
	sendAck    chan struct{}
}

func (s *delayedHelloAckRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	s.firstFrame <- frame
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.sendAck:
	}
	if err := stream.Send(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{HelloAck: &cpv1.RuntimeHelloAck{
			RuntimeId: "bare-6-auth-gate",
		}},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *captureRuntimeChannelServer) RuntimeChannel(stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame]) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	s.firstFrame <- frame
	_ = stream.Send(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{HelloAck: &cpv1.RuntimeHelloAck{
			RuntimeId: "bare-6-test",
		}},
	})
	<-stream.Context().Done()
	return stream.Context().Err()
}

type platformRequestRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	requestFrame chan *cpv1.RuntimeFrame
}

type blockedRequestRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	heartbeats chan time.Time
}

func (s *blockedRequestRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[
		cpv1.RuntimeFrame,
		cpv1.RuntimeFrame,
	],
) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HELLO {
		return nil
	}
	if err := stream.Send(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{
			HelloAck: &cpv1.RuntimeHelloAck{
				RuntimeId: "bare-6-heartbeat",
			},
		},
	}); err != nil {
		return err
	}
	if err := stream.Send(&cpv1.RuntimeFrame{
		CorrelationId: "blocked-request",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{
			Request: &cpv1.StrategyRequest{
				Method: "RunStrategy",
			},
		},
	}); err != nil {
		return err
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.GetFrameType() ==
			cpv1.FrameType_FRAME_TYPE_HEARTBEAT {
			s.heartbeats <- time.Now()
		}
	}
}

func (s *platformRequestRuntimeChannelServer) RuntimeChannel(stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HELLO {
		return nil
	}
	_ = stream.Send(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{HelloAck: &cpv1.RuntimeHelloAck{
			RuntimeId: "bare-6-test",
		}},
	})
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_REQUEST {
			continue
		}
		s.requestFrame <- frame
		response, _ := anypb.New(&strategyv1.GetStrategyStatusResponse{Status: "running"})
		return stream.Send(&cpv1.RuntimeFrame{
			CorrelationId: frame.GetCorrelationId(),
			FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
			Payload: &cpv1.RuntimeFrame_Response{Response: &cpv1.StrategyResponse{
				Response: response,
			}},
		})
	}
}
