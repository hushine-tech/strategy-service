package runtimeagent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBuildInitialFrameUsesBareHelloForBareRuntime(t *testing.T) {
	frame, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source:          "bare",
		UserID:          6,
		RuntimeID:       "bare-6-test",
		Name:            "bare-debug-6-test",
		Capabilities:    []string{"strategy"},
		ResourceProfile: "small",
		Version:         "0.1.0",
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
}

func TestBuildInitialFrameRejectsBareRuntimeWithoutUser(t *testing.T) {
	_, err := BuildInitialRuntimeFrame(RuntimeIdentity{
		Source:    "bare",
		RuntimeID: "bare-missing-user",
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
		Source:          "self_hosted",
		RuntimeID:       "rt-signed",
		Name:            "signed",
		Capabilities:    []string{"strategy"},
		ResourceProfile: "small",
		Version:         "0.1.0",
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
			Source:          "bare",
			UserID:          6,
			RuntimeID:       "bare-6-test",
			Name:            "bare-debug-6-test",
			Capabilities:    []string{"strategy"},
			ResourceProfile: "small",
			Version:         "0.1.0",
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
			Source:    "bare",
			UserID:    6,
			RuntimeID: "bare-6-test",
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
