package runtimeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	rwv1 "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	"google.golang.org/grpc"
)

type WorkerFrameHandler func(
	ctx context.Context,
	sessionID string,
	frame *rwv1.WorkerFrame,
	send func(*rwv1.AgentFrame) error,
) error

type AuthenticatedWorkerFrameHandler func(
	ctx context.Context,
	identity WorkerIdentity,
	frame *rwv1.WorkerFrame,
	send func(*rwv1.AgentFrame) error,
) error

type WorkerDisconnectHandler func(identity WorkerIdentity, cause error)

type WorkerIPCServer struct {
	rwv1.UnimplementedRuntimeWorkerAgentServer

	registry   *SessionRegistry
	handler    AuthenticatedWorkerFrameHandler
	disconnect WorkerDisconnectHandler
	mu         sync.Mutex
	outbound   map[string]workerOutbound
}

type workerOutbound struct {
	identity WorkerIdentity
	frames   chan *rwv1.AgentFrame
}

func NewWorkerIPCServer(registry *SessionRegistry, handler WorkerFrameHandler) *WorkerIPCServer {
	var authenticated AuthenticatedWorkerFrameHandler
	if handler != nil {
		authenticated = func(ctx context.Context, identity WorkerIdentity, frame *rwv1.WorkerFrame, send func(*rwv1.AgentFrame) error) error {
			return handler(ctx, identity.SessionID, frame, send)
		}
	}
	return NewAuthenticatedWorkerIPCServer(registry, authenticated, nil)
}

func NewAuthenticatedWorkerIPCServer(
	registry *SessionRegistry,
	handler AuthenticatedWorkerFrameHandler,
	disconnect WorkerDisconnectHandler,
) *WorkerIPCServer {
	if registry == nil {
		registry = NewSessionRegistry()
	}
	return &WorkerIPCServer{
		registry:   registry,
		handler:    handler,
		disconnect: disconnect,
		outbound:   map[string]workerOutbound{},
	}
}

func (s *WorkerIPCServer) Connect(stream grpc.BidiStreamingServer[rwv1.WorkerFrame, rwv1.AgentFrame]) (returnErr error) {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("first worker frame must be WorkerHello")
	}
	sessionID := strings.TrimSpace(hello.GetSessionId())
	if err := s.registry.AdmitWorker(sessionID, hello.GetToken(), hello.GetPid()); err != nil {
		return err
	}
	identity, ok := s.registry.ActiveWorker(sessionID)
	if !ok {
		return fmt.Errorf("authenticated worker identity is unavailable: %s", sessionID)
	}
	if s.disconnect != nil {
		defer func() { s.disconnect(identity, returnErr) }()
	}
	outbound := make(chan *rwv1.AgentFrame, 128)
	s.mu.Lock()
	s.outbound[sessionID] = workerOutbound{identity: identity, frames: outbound}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		for key, candidate := range s.outbound {
			if candidate.frames == outbound {
				delete(s.outbound, key)
			}
		}
		close(outbound)
		s.mu.Unlock()
	}()

	sendErr := make(chan error, 1)
	go func() {
		for frame := range outbound {
			if frame == nil {
				continue
			}
			if err := stream.Send(frame); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	if s.handler != nil {
		if err := s.handler(stream.Context(), identity, first, func(frame *rwv1.AgentFrame) error {
			return s.SendToWorker(sessionID, frame)
		}); err != nil {
			return err
		}
	}

	for {
		select {
		case err := <-sendErr:
			return err
		default:
		}
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || stream.Context().Err() != nil {
				return nil
			}
			return err
		}
		if s.handler == nil {
			continue
		}
		if err := s.handler(stream.Context(), identity, frame, func(frame *rwv1.AgentFrame) error {
			return s.SendToWorker(sessionID, frame)
		}); err != nil {
			return err
		}
	}
}

func (s *WorkerIPCServer) SendToWorker(sessionID string, frame *rwv1.AgentFrame) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outbound, ok := s.outbound[sessionID]
	if !ok || outbound.frames == nil {
		return fmt.Errorf("worker is not connected: %s", sessionID)
	}
	select {
	case outbound.frames <- frame:
		return nil
	default:
		return fmt.Errorf("worker outbound queue is full: %s", sessionID)
	}
}

func (s *WorkerIPCServer) SendToWorkerGeneration(identity WorkerIdentity, frame *rwv1.AgentFrame) error {
	sessionID := strings.TrimSpace(identity.SessionID)
	if sessionID == "" || identity.Generation == 0 {
		return fmt.Errorf("worker Session identity and generation are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outbound, ok := s.outbound[sessionID]
	if !ok || outbound.frames == nil {
		return fmt.Errorf("worker is not connected: %s", sessionID)
	}
	if outbound.identity.Generation != identity.Generation {
		return fmt.Errorf("stale worker generation: %s", sessionID)
	}
	select {
	case outbound.frames <- frame:
		return nil
	default:
		return fmt.Errorf("worker outbound queue is full: %s", sessionID)
	}
}
