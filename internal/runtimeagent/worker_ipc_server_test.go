package runtimeagent

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestWorkerIPCServerAliasesRealSessionID(t *testing.T) {
	registry := NewSessionRegistry()
	if err := registry.ExpectWorker("pending-1", "token-1"); err != nil {
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
			SessionId:     "pending-1",
			Token:         "token-1",
			WorkerVersion: "test",
			Pid:           123,
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := registry.ActiveWorker("pending-1"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := registry.ActiveWorker("pending-1"); !ok {
		t.Fatalf("worker was not admitted")
	}
	if err := workerServer.AliasWorkerSession("pending-1", "sess-real"); err != nil {
		t.Fatalf("AliasWorkerSession: %v", err)
	}
	if worker, ok := registry.ActiveWorker("sess-real"); !ok || worker.PID != 123 {
		t.Fatalf("real session worker = %+v ok=%v", worker, ok)
	}
	if err := workerServer.SendToWorker("sess-real", &rwv1.AgentFrame{
		Payload: &rwv1.AgentFrame_StopSession{StopSession: &rwv1.StopSession{
			SessionId: "sess-real",
			Reason:    "test",
		}},
	}); err != nil {
		t.Fatalf("SendToWorker real session: %v", err)
	}
	frame, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv real session frame: %v", err)
	}
	if frame.GetStopSession().GetSessionId() != "sess-real" {
		t.Fatalf("stop session id = %q", frame.GetStopSession().GetSessionId())
	}
}

func TestWorkerIPCDisconnectPreservesManagerLifecycleReservation(t *testing.T) {
	manager := NewWorkerManager(WorkerManagerConfig{})
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
