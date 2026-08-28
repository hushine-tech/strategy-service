package runtimeagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

func TestRuntimeChannelSupervisorResumesSeriallyAndDoesNotReplayPendingCalls(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &resumeSupervisorRuntimeChannelServer{
		firstAuthenticated:  make(chan struct{}),
		firstRequest:        make(chan struct{}),
		secondAuthenticated: make(chan struct{}),
		secondFrames:        make(chan *cpv1.RuntimeFrame, 8),
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := newSupervisorTestRuntimeClient(listener)
	client.cfg.ReconnectJitter = func(time.Duration) time.Duration { return 0 }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()
	receiveRuntimeSignal(t, capture.firstAuthenticated, "first authentication")
	waitForRuntimeReady(t, client, true)

	request, err := anypb.New(&strategyv1.GetStrategyStatusRequest{SessionId: "sess-pending"})
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		_, callErr := client.InvokePlatformAny(context.Background(), "GetStrategyStatus", request, 5*time.Second)
		callDone <- callErr
	}()
	receiveRuntimeSignal(t, capture.firstRequest, "first-generation request")

	select {
	case callErr := <-callDone:
		if status.Code(callErr) != codes.Unavailable {
			t.Fatalf("pending call error = %v, want Unavailable", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call did not fail immediately on disconnect")
	}
	receiveRuntimeSignal(t, capture.secondAuthenticated, "RESUME authentication")
	waitForRuntimeReady(t, client, true)

	if got := capture.maxActive.Load(); got != 1 {
		t.Fatalf("simultaneous RuntimeChannel streams = %d, want 1", got)
	}
	if got := capture.firstTypesSnapshot(); len(got) < 2 ||
		got[0] != cpv1.FrameType_FRAME_TYPE_HELLO || got[1] != cpv1.FrameType_FRAME_TYPE_RESUME {
		t.Fatalf("generation first frames = %v, want HELLO then RESUME", got)
	}
	select {
	case frame := <-capture.secondFrames:
		if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_REQUEST {
			t.Fatalf("old pending request replayed on RESUME: %+v", frame)
		}
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("supervisor did not stop after cancellation")
	}
}

func TestRuntimeChannelSupervisorDoesNotRetryPermanentStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			capture := &permanentRuntimeChannelServer{code: code}
			cpv1.RegisterControlPanelServiceServer(server, capture)
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()

			client := newSupervisorTestRuntimeClient(listener)
			client.cfg.ReconnectJitter = func(time.Duration) time.Duration { return 0 }
			err := client.Run(context.Background())
			if status.Code(err) != code {
				t.Fatalf("Run error = %v, want %s", err, code)
			}
			if got := capture.calls.Load(); got != 1 {
				t.Fatalf("RuntimeChannel attempts = %d, want 1", got)
			}
		})
	}
}

func TestRuntimeChannelSupervisorBackoffStartsAt250MillisecondsAndCapsAt5Seconds(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &alwaysUnavailableRuntimeChannelServer{}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := newSupervisorTestRuntimeClient(listener)
	client.cfg.ReconnectJitter = func(max time.Duration) time.Duration { return max }
	var waits []time.Duration
	client.cfg.ReconnectWait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		if len(waits) == 6 {
			return status.Error(codes.PermissionDenied, "stop test")
		}
		return nil
	}
	if err := client.Run(context.Background()); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Run error = %v, want test stop", err)
	}
	want := []time.Duration{
		250 * time.Millisecond, 500 * time.Millisecond, time.Second,
		2 * time.Second, 4 * time.Second, 5 * time.Second,
	}
	if !slices.Equal(waits, want) {
		t.Fatalf("reconnect waits = %v, want %v", waits, want)
	}
}

func TestRuntimeChannelSupervisorResetsBackoffAfterAuthenticatedResumeableGeneration(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &backoffResetRuntimeChannelServer{}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := newSupervisorTestRuntimeClient(listener)
	client.cfg.ReconnectJitter = func(max time.Duration) time.Duration { return max }
	var waits []time.Duration
	client.cfg.ReconnectWait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	err := client.Run(context.Background())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Run error = %v, want rejected RESUME", err)
	}
	if want := []time.Duration{250 * time.Millisecond, 250 * time.Millisecond}; !slices.Equal(waits, want) {
		t.Fatalf("reconnect waits = %v, want reset %v", waits, want)
	}
	if got := capture.firstTypesSnapshot(); len(got) != 3 ||
		got[0] != cpv1.FrameType_FRAME_TYPE_HELLO ||
		got[1] != cpv1.FrameType_FRAME_TYPE_HELLO ||
		got[2] != cpv1.FrameType_FRAME_TYPE_RESUME {
		t.Fatalf("generation first frames = %v, want HELLO, HELLO, RESUME", got)
	}
}

func TestRuntimeChannelReadyDropsDuringReconnectAndReturnsAfterResumeAck(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &gatedResumeRuntimeChannelServer{
		firstAuthenticated: make(chan struct{}),
		disconnectFirst:    make(chan struct{}),
		allowResumeAck:     make(chan struct{}),
		resumeReceived:     make(chan struct{}),
		resumeAckSent:      make(chan struct{}),
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := newSupervisorTestRuntimeClient(listener)
	blockedSend := make(chan struct{})
	releaseSend := make(chan struct{})
	var releaseOnce sync.Once
	releaseBlockedSend := func() { releaseOnce.Do(func() { close(releaseSend) }) }
	defer releaseBlockedSend()
	client.cfg.DialOptions = append(client.cfg.DialOptions, grpc.WithStreamInterceptor(
		blockingFirstRequestStreamInterceptor(blockedSend, releaseSend),
	))
	client.cfg.ReconnectJitter = func(time.Duration) time.Duration { return 0 }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()
	receiveRuntimeSignal(t, capture.firstAuthenticated, "first authentication")
	waitForRuntimeReady(t, client, true)
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- client.Send(&cpv1.RuntimeFrame{
			FrameType: cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method: "blocked-old-generation-send",
			}},
		})
	}()
	receiveRuntimeSignal(t, blockedSend, "blocked old-generation physical send")
	close(capture.disconnectFirst)
	waitForRuntimeReady(t, client, false)
	client.mu.Lock()
	currentPreserved := client.current != nil && !client.current.ready
	client.mu.Unlock()
	if !currentPreserved {
		t.Fatal("disconnect detection did not preserve unready generation ownership")
	}
	select {
	case <-capture.resumeReceived:
		t.Fatal("supervisor opened RESUME before the old send loop exited")
	default:
	}
	releaseBlockedSend()
	select {
	case err := <-sendDone:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("blocked old-generation send error = %v, want Unavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked old-generation send did not exit")
	}
	receiveRuntimeSignal(t, capture.resumeReceived, "RESUME frame")
	close(capture.allowResumeAck)
	receiveRuntimeSignal(t, capture.resumeAckSent, "RESUME ACK send")
	waitForRuntimeReady(t, client, true)
}

func TestRuntimeChannelSupervisorRetainsOnlyDataAckAcrossReconnect(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &ackReplayRuntimeChannelServer{
		firstAuthenticated: make(chan struct{}),
		resumeReceived:     make(chan struct{}),
		allowResumeAck:     make(chan struct{}),
		ackFrames:          make(chan *cpv1.RuntimeFrame, 2),
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := newSupervisorTestRuntimeClient(listener)
	physicalFailure := make(chan struct{})
	client.cfg.DialOptions = append(client.cfg.DialOptions, grpc.WithStreamInterceptor(
		failFirstGenerationDataACKStreamInterceptor(physicalFailure),
	))
	client.cfg.ReconnectJitter = func(time.Duration) time.Duration { return 0 }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()
	receiveRuntimeSignal(t, capture.firstAuthenticated, "first authentication")
	waitForRuntimeReady(t, client, true)

	ack := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_ACK,
		Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
			SessionId: "sess-income", StreamKey: "income/sess-income", Sequence: 11,
		}},
	}
	if err := client.Send(ack); err != nil {
		t.Fatalf("retain physically failed DATA_ACK: %v", err)
	}
	receiveRuntimeSignal(t, physicalFailure, "old-generation DATA_ACK physical failure")
	receiveRuntimeSignal(t, capture.resumeReceived, "RESUME frame")
	waitForRuntimeReady(t, client, false)
	requestFrame := &cpv1.RuntimeFrame{FrameType: cpv1.FrameType_FRAME_TYPE_REQUEST}
	if err := client.Send(requestFrame); status.Code(err) != codes.Unavailable {
		t.Fatalf("disconnected non-ACK Send error = %v, want Unavailable", err)
	}
	close(capture.allowResumeAck)
	select {
	case got := <-capture.ackFrames:
		if !proto.Equal(got, ack) {
			t.Fatalf("replayed DATA_ACK = %+v, want %+v", got, ack)
		}
	case <-time.After(time.Second):
		t.Fatal("retained DATA_ACK was not replayed after RESUME")
	}
	select {
	case duplicate := <-capture.ackFrames:
		t.Fatalf("DATA_ACK replayed more than once: %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRuntimeChannelRetainedDataACKCompletionCannotDeleteNewerSameKeyEntry(t *testing.T) {
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{})
	oldGeneration := &runtimeChannelGeneration{
		id: 1, outbound: make(chan *runtimeChannelOutbound, 2), ready: true,
	}
	client.mu.Lock()
	client.current = oldGeneration
	client.mu.Unlock()
	ack := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_ACK,
		Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
			SessionId: "sess-income", StreamKey: "income/sess-income", Sequence: 11,
		}},
	}

	oldSendDone := make(chan error, 1)
	go func() { oldSendDone <- client.Send(proto.Clone(ack).(*cpv1.RuntimeFrame)) }()
	oldPhysical := <-oldGeneration.outbound
	newSendDone := make(chan error, 1)
	go func() { newSendDone <- client.Send(proto.Clone(ack).(*cpv1.RuntimeFrame)) }()
	newPhysical := <-oldGeneration.outbound

	newPhysical.done <- status.Error(codes.Unavailable, "newer physical send failed")
	if err := <-newSendDone; err != nil {
		t.Fatalf("newer retained ACK ownership: %v", err)
	}
	key := runtimeDataACKKey(ack.GetDataAck())
	client.mu.Lock()
	newerEntry := client.retainedACK[key]
	client.mu.Unlock()
	if newerEntry == nil {
		t.Fatal("newer failed ACK was not retained")
	}
	oldPhysical.done <- nil
	if err := <-oldSendDone; err != nil {
		t.Fatalf("older physical ACK completion: %v", err)
	}

	client.mu.Lock()
	retained := client.retainedACK[key]
	client.mu.Unlock()
	if retained == nil {
		t.Fatal("older protobuf-equal completion deleted newer failed ACK")
	}
	if retained != newerEntry || retained.revision != newerEntry.revision {
		t.Fatalf(
			"older protobuf-equal completion changed newer failed ACK: retained=%p rev=%d want=%p rev=%d",
			retained, retained.revision, newerEntry, newerEntry.revision,
		)
	}

	resumeGeneration := &runtimeChannelGeneration{
		id: 2, outbound: make(chan *runtimeChannelOutbound, 1), ready: true,
	}
	client.mu.Lock()
	client.current = resumeGeneration
	client.mu.Unlock()
	replayDone := make(chan error, 1)
	go func() { replayDone <- client.replayRetainedACKs(context.Background(), resumeGeneration) }()
	replayed := <-resumeGeneration.outbound
	if !proto.Equal(replayed.frame, ack) {
		t.Fatalf("RESUME replay = %+v, want retained ACK %+v", replayed.frame, ack)
	}
	replayed.done <- nil
	if err := <-replayDone; err != nil {
		t.Fatalf("replay retained newer ACK: %v", err)
	}
	client.mu.Lock()
	retained = client.retainedACK[key]
	client.mu.Unlock()
	if retained != nil {
		t.Fatal("successful RESUME replay did not clear exact retained ACK version")
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

func TestRuntimeChannelClientPreservesStreamErrorFields(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dependency := &strategyv1.RuntimeDependencyError{
		Code: "STRATEGY_DEPENDENCY_UNAVAILABLE", Module: "google.cloud",
		RuntimeProfile: "platform-python-3.13", Message: "dependency unavailable",
	}
	capture := &platformRequestRuntimeChannelServer{
		requestFrame: make(chan *cpv1.RuntimeFrame, 1),
		errorFrame: &cpv1.StreamError{
			Code: "FailedPrecondition", Message: "platform route unavailable",
			DependencyError: dependency, ErrorDetailJson: `{"route":"portfolio.GetSession"}`,
		},
	}
	cpv1.RegisterControlPanelServiceServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{Source: "bare", UserID: 6, RuntimeID: "bare-6-test",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	errCh := make(chan error, 1)
	go func() { errCh <- client.Run(ctx) }()
	request, err := anypb.New(&strategyv1.GetStrategyStatusRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.InvokePlatformAny(ctx, "GetStrategyStatus", request, time.Second)
	if err == nil {
		t.Fatal("InvokePlatformAny error is nil")
	}
	typed, ok := err.(interface {
		PlatformErrorCode() string
		PlatformErrorMessage() string
		PlatformErrorDetailJSON() string
		PlatformDependencyError() *strategyv1.RuntimeDependencyError
	})
	if !ok {
		t.Fatalf("error type = %T, want typed platform error", err)
	}
	if typed.PlatformErrorCode() != "FailedPrecondition" ||
		typed.PlatformErrorMessage() != "platform route unavailable" ||
		typed.PlatformErrorDetailJSON() != `{"route":"portfolio.GetSession"}` ||
		typed.PlatformDependencyError().GetModule() != "google.cloud" {
		t.Fatalf("typed error = %#v", typed)
	}
	cancel()
	<-errCh
}

func TestStreamErrorDetailJSONPreservesDependencyFallback(t *testing.T) {
	detailJSON := streamErrorDetailJSON(&strategyv1.RuntimeDependencyError{
		Code: "STRATEGY_DEPENDENCY_UNAVAILABLE", Module: "google.cloud",
		RuntimeProfile: "platform-python-3.13", Message: "dependency unavailable",
	})
	var detail map[string]any
	if err := json.Unmarshal([]byte(detailJSON), &detail); err != nil {
		t.Fatalf("detail JSON = %q: %v", detailJSON, err)
	}
	if detail["code"] != "STRATEGY_DEPENDENCY_UNAVAILABLE" ||
		detail["module"] != "google.cloud" ||
		detail["runtime_profile"] != "platform-python-3.13" ||
		detail["message"] != "dependency unavailable" {
		t.Fatalf("detail = %#v", detail)
	}
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
	errorFrame   *cpv1.StreamError
}

type blockedRequestRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	heartbeats chan time.Time
}

func newSupervisorTestRuntimeClient(listener *bufconn.Listener) *RuntimeChannelClient {
	return NewRuntimeChannelClient(RuntimeChannelClientConfig{
		Address: "bufnet",
		Identity: RuntimeIdentity{
			Source:            "bare",
			UserID:            6,
			RuntimeID:         "bare-6-supervisor",
			DependencyProfile: validEmbeddedRuntimeFacts("bare").Profile,
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
}

func receiveRuntimeSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForRuntimeReady(t *testing.T, client *RuntimeChannelClient, want bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if client.Ready() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RuntimeChannel ready = %v, want %v", client.Ready(), want)
}

type resumeSupervisorRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	firstAuthenticated  chan struct{}
	firstRequest        chan struct{}
	secondAuthenticated chan struct{}
	secondFrames        chan *cpv1.RuntimeFrame
	active              atomic.Int64
	maxActive           atomic.Int64
	mu                  sync.Mutex
	firstTypes          []cpv1.FrameType
}

func (s *resumeSupervisorRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maxActive.Load()
		if active <= maximum || s.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.firstTypes = append(s.firstTypes, first.GetFrameType())
	generation := len(s.firstTypes)
	s.mu.Unlock()
	if generation == 1 {
		if first.GetHello() == nil {
			return status.Error(codes.InvalidArgument, "first generation requires HELLO")
		}
		if err := stream.Send(runtimeChannelAckForTest("resume-token")); err != nil {
			return err
		}
		close(s.firstAuthenticated)
		for {
			frame, recvErr := stream.Recv()
			if recvErr != nil {
				return recvErr
			}
			if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_REQUEST {
				close(s.firstRequest)
				return nil
			}
		}
	}
	if first.GetResume() == nil || first.GetResume().GetFingerprint() != "resume-token" {
		return status.Error(codes.PermissionDenied, "resume token mismatch")
	}
	if err := stream.Send(runtimeChannelAckForTest("resume-token")); err != nil {
		return err
	}
	close(s.secondAuthenticated)
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		s.secondFrames <- frame
	}
}

func (s *resumeSupervisorRuntimeChannelServer) firstTypesSnapshot() []cpv1.FrameType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]cpv1.FrameType(nil), s.firstTypes...)
}

type permanentRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	code  codes.Code
	calls atomic.Int64
}

type alwaysUnavailableRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
}

func (s *alwaysUnavailableRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "transient test failure")
}

type backoffResetRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	calls      atomic.Int64
	mu         sync.Mutex
	firstTypes []cpv1.FrameType
}

func (s *backoffResetRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	call := s.calls.Add(1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.firstTypes = append(s.firstTypes, first.GetFrameType())
	s.mu.Unlock()
	switch call {
	case 1:
		return status.Error(codes.Unavailable, "pre-auth transient")
	case 2:
		if err := stream.Send(runtimeChannelAckForTest("reset-token")); err != nil {
			return err
		}
		for {
			frame, recvErr := stream.Recv()
			if recvErr != nil {
				return recvErr
			}
			if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_HEARTBEAT {
				return nil
			}
		}
	default:
		return status.Error(codes.PermissionDenied, "RESUME rejected")
	}
}

func (s *backoffResetRuntimeChannelServer) firstTypesSnapshot() []cpv1.FrameType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]cpv1.FrameType(nil), s.firstTypes...)
}

func (s *permanentRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	s.calls.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(s.code, "terminal admission")
}

type gatedResumeRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	firstAuthenticated chan struct{}
	disconnectFirst    chan struct{}
	resumeReceived     chan struct{}
	allowResumeAck     chan struct{}
	resumeAckSent      chan struct{}
	calls              atomic.Int64
}

func blockingFirstRequestStreamInterceptor(
	blocked chan<- struct{},
	release <-chan struct{},
) grpc.StreamClientInterceptor {
	var streams atomic.Int64
	var blockedOnce sync.Once
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &runtimeChannelTestClientStream{
			ClientStream: stream,
			generation:   streams.Add(1),
			onSend: func(generation int64, frame *cpv1.RuntimeFrame) (bool, error) {
				if generation != 1 || frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_REQUEST {
					return false, nil
				}
				blockedOnce.Do(func() { close(blocked) })
				<-release
				return true, status.Error(codes.Unavailable, "old-generation physical send released after disconnect")
			},
		}, nil
	}
}

func failFirstGenerationDataACKStreamInterceptor(
	failed chan<- struct{},
) grpc.StreamClientInterceptor {
	var streams atomic.Int64
	var failedOnce sync.Once
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &runtimeChannelTestClientStream{
			ClientStream: stream,
			generation:   streams.Add(1),
			onSend: func(generation int64, frame *cpv1.RuntimeFrame) (bool, error) {
				if generation != 1 || frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_DATA_ACK {
					return false, nil
				}
				failedOnce.Do(func() { close(failed) })
				return true, status.Error(codes.Unavailable, "injected old-generation DATA_ACK send failure")
			},
		}, nil
	}
}

type runtimeChannelTestClientStream struct {
	grpc.ClientStream
	generation int64
	onSend     func(int64, *cpv1.RuntimeFrame) (bool, error)
}

func (s *runtimeChannelTestClientStream) SendMsg(message any) error {
	if frame, ok := message.(*cpv1.RuntimeFrame); ok && s.onSend != nil {
		if handled, err := s.onSend(s.generation, frame); handled {
			return err
		}
	}
	return s.ClientStream.SendMsg(message)
}

func (s *gatedResumeRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	call := s.calls.Add(1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if call == 1 {
		if err := stream.Send(runtimeChannelAckForTest("ready-token")); err != nil {
			return err
		}
		close(s.firstAuthenticated)
		<-s.disconnectFirst
		return nil
	}
	if first.GetResume() == nil {
		return status.Error(codes.InvalidArgument, "resume required")
	}
	close(s.resumeReceived)
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.allowResumeAck:
	}
	if err := stream.Send(runtimeChannelAckForTest("ready-token")); err != nil {
		return err
	}
	close(s.resumeAckSent)
	<-stream.Context().Done()
	return stream.Context().Err()
}

type ackReplayRuntimeChannelServer struct {
	cpv1.UnimplementedControlPanelServiceServer
	firstAuthenticated chan struct{}
	resumeReceived     chan struct{}
	allowResumeAck     chan struct{}
	ackFrames          chan *cpv1.RuntimeFrame
	calls              atomic.Int64
}

func (s *ackReplayRuntimeChannelServer) RuntimeChannel(
	stream grpc.BidiStreamingServer[cpv1.RuntimeFrame, cpv1.RuntimeFrame],
) error {
	call := s.calls.Add(1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if call == 1 {
		if err := stream.Send(runtimeChannelAckForTest("ack-token")); err != nil {
			return err
		}
		close(s.firstAuthenticated)
		for {
			if _, recvErr := stream.Recv(); recvErr != nil {
				return recvErr
			}
		}
	}
	if first.GetResume() == nil {
		return status.Error(codes.InvalidArgument, "resume required")
	}
	close(s.resumeReceived)
	<-s.allowResumeAck
	if err := stream.Send(runtimeChannelAckForTest("ack-token")); err != nil {
		return err
	}
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return nil
			}
			return recvErr
		}
		if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_DATA_ACK {
			s.ackFrames <- frame
		}
	}
}

func runtimeChannelAckForTest(token string) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{HelloAck: &cpv1.RuntimeHelloAck{
			RuntimeId: "bare-6-supervisor", ResumeToken: token, Fingerprint: token,
		}},
	}
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
		if s.errorFrame != nil {
			return stream.Send(&cpv1.RuntimeFrame{
				CorrelationId: frame.GetCorrelationId(),
				FrameType:     cpv1.FrameType_FRAME_TYPE_ERROR,
				Payload:       &cpv1.RuntimeFrame_Error{Error: s.errorFrame},
			})
		}
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
