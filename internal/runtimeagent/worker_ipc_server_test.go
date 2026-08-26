package runtimeagent

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerIPCServerAdmitsExpectedWorkerHello(t *testing.T) {
	registry := NewSessionRegistry()
	if err := registry.ExpectWorker("sess-1", "token-1"); err != nil {
		t.Fatalf("ExpectWorker: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	workerServer := NewWorkerIPCServer(registry, nil)
	rwv1.RegisterRuntimeWorkerAgentServer(server, workerServer)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial worker ipc: %v", err)
	}
	defer conn.Close()

	stream, err := rwv1.NewRuntimeWorkerAgentClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(&rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
			SessionId:     "sess-1",
			Token:         "token-1",
			WorkerVersion: "test",
			Pid:           123,
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if worker, ok := registry.ActiveWorker("sess-1"); ok && worker.PID == 123 {
			if err := workerServer.SendToWorker("sess-1", &rwv1.AgentFrame{
				Payload: &rwv1.AgentFrame_StartSession{StartSession: &rwv1.StartSession{
					SessionId: "sess-1",
					UserId:    6,
				}},
			}); err != nil {
				t.Fatalf("SendToWorker: %v", err)
			}
			frame, err := stream.Recv()
			if err != nil {
				t.Fatalf("recv start session: %v", err)
			}
			if frame.GetStartSession().GetUserId() != 6 {
				t.Fatalf("start session user_id = %d", frame.GetStartSession().GetUserId())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker was not admitted")
}

func TestWorkerIPCServerPassesImmutableGenerationAndDisconnectIdentity(t *testing.T) {
	registry := NewSessionRegistry()
	generation, err := registry.ExpectWorkerGeneration("sess-generation", "token-generation")
	if err != nil {
		t.Fatal(err)
	}
	frames := make(chan WorkerIdentity, 1)
	disconnects := make(chan WorkerIdentity, 1)
	workerServer := NewAuthenticatedWorkerIPCServer(
		registry,
		func(_ context.Context, identity WorkerIdentity, _ *rwv1.WorkerFrame, _ func(*rwv1.AgentFrame) error) error {
			frames <- identity
			return nil
		},
		func(identity WorkerIdentity, _ error) { disconnects <- identity },
	)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	rwv1.RegisterRuntimeWorkerAgentServer(server, workerServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := rwv1.NewRuntimeWorkerAgentClient(conn).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&rwv1.WorkerFrame{Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
		SessionId: "sess-generation", Token: "token-generation", Pid: 321,
	}}}); err != nil {
		t.Fatal(err)
	}
	identity := <-frames
	if identity.SessionID != "sess-generation" || identity.PID != 321 || identity.Generation != generation {
		t.Fatalf("frame identity = %+v, generation=%d", identity, generation)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want EOF", err)
	}
	disconnected := <-disconnects
	if disconnected.SessionID != identity.SessionID || disconnected.PID != identity.PID || disconnected.Generation != identity.Generation {
		t.Fatalf("disconnect identity = %+v, frame identity = %+v", disconnected, identity)
	}
}

func TestWorkerIPCServerIncomeDeliveryIsGenerationBound(t *testing.T) {
	server := NewAuthenticatedWorkerIPCServer(NewSessionRegistry(), nil, nil)
	outbound := make(chan *rwv1.AgentFrame, 1)
	server.outbound["sess-income"] = workerOutbound{
		identity: WorkerIdentity{SessionID: "sess-income", Generation: 7},
		frames:   outbound,
	}
	frame := &rwv1.AgentFrame{Payload: &rwv1.AgentFrame_IncomeBatch{IncomeBatch: &rwv1.IncomeBatch{
		SessionId: "sess-income", StreamKey: "income/sess-income", Sequence: 10,
	}}}

	if err := server.SendToWorkerGeneration(WorkerIdentity{
		SessionID: "sess-income", Generation: 6,
	}, frame); err == nil || !strings.Contains(err.Error(), "stale worker generation") {
		t.Fatalf("stale Worker generation error = %v", err)
	}
	if len(outbound) != 0 {
		t.Fatal("stale Worker generation received Income")
	}
	if err := server.SendToWorkerGeneration(WorkerIdentity{
		SessionID: "sess-income", Generation: 7,
	}, frame); err != nil {
		t.Fatalf("current Worker generation Income delivery: %v", err)
	}
	if got := <-outbound; got != frame {
		t.Fatalf("delivered frame = %+v, want original frame", got)
	}
}

func TestWorkerIPCDisconnectPreservesManagerLifecycleReservation(t *testing.T) {
	manager := newLegacyWorkerManager(WorkerManagerConfig{})
	if err := manager.registry.ExpectWorker("sess-owned", "token-owned"); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	workerServer := NewWorkerIPCServer(manager.registry, nil)
	rwv1.RegisterRuntimeWorkerAgentServer(server, workerServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial worker ipc: %v", err)
	}
	defer conn.Close()
	stream, err := rwv1.NewRuntimeWorkerAgentClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(&rwv1.WorkerFrame{
		Payload: &rwv1.WorkerFrame_Hello{Hello: &rwv1.WorkerHello{
			SessionId: "sess-owned", Token: "token-owned", Pid: 123,
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := manager.registry.ActiveWorker("sess-owned"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after CloseSend = %v, want EOF", err)
	}
	if identity, ok := manager.registry.ActiveWorker("sess-owned"); !ok || identity.PID != 123 {
		t.Fatalf("disconnect released manager lifecycle reservation: (%+v, %v)", identity, ok)
	}
	if _, err := manager.PrepareSessionWorker("sess-owned"); !errors.Is(err, ErrWorkerAlreadyExists) {
		t.Fatalf("replacement reservation error = %v, want ErrWorkerAlreadyExists", err)
	}
	manager.registry.ForgetWorkerIdentity("sess-owned", 123, "token-owned")
}
