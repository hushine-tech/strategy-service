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

type WorkerIPCServer struct {
	rwv1.UnimplementedRuntimeWorkerAgentServer

	registry *SessionRegistry
	handler  WorkerFrameHandler
	mu       sync.Mutex
	outbound map[string]chan *rwv1.AgentFrame
}

func NewWorkerIPCServer(registry *SessionRegistry, handler WorkerFrameHandler) *WorkerIPCServer {
	if registry == nil {
		registry = NewSessionRegistry()
	}
	return &WorkerIPCServer{
		registry: registry,
		handler:  handler,
		outbound: map[string]chan *rwv1.AgentFrame{},
	}
}

func (s *WorkerIPCServer) Connect(stream grpc.BidiStreamingServer[rwv1.WorkerFrame, rwv1.AgentFrame]) error {
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
	outbound := make(chan *rwv1.AgentFrame, 128)
	s.mu.Lock()
	s.outbound[sessionID] = outbound
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		for key, ch := range s.outbound {
			if ch == outbound {
				delete(s.outbound, key)
			}
		}
		close(outbound)
		s.mu.Unlock()
		s.registry.ForgetWorker(sessionID)
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
		if err := s.handler(stream.Context(), sessionID, first, func(frame *rwv1.AgentFrame) error {
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
		if err := s.handler(stream.Context(), sessionID, frame, func(frame *rwv1.AgentFrame) error {
			return s.SendToWorker(sessionID, frame)
		}); err != nil {
			return err
		}
	}
}

func (s *WorkerIPCServer) AliasWorkerSession(existingSessionID string, sessionID string) error {
	existingSessionID = strings.TrimSpace(existingSessionID)
	sessionID = strings.TrimSpace(sessionID)
	if existingSessionID == "" || sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if existingSessionID == sessionID {
		return nil
	}
	if err := s.registry.AliasWorkerSession(existingSessionID, sessionID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outbound := s.outbound[existingSessionID]
	if outbound == nil {
		return fmt.Errorf("worker is not connected: %s", existingSessionID)
	}
	s.outbound[sessionID] = outbound
	return nil
}

func (s *WorkerIPCServer) SendToWorker(sessionID string, frame *rwv1.AgentFrame) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outbound := s.outbound[sessionID]
	if outbound == nil {
		return fmt.Errorf("worker is not connected: %s", sessionID)
	}
	select {
	case outbound <- frame:
		return nil
	default:
		return fmt.Errorf("worker outbound queue is full: %s", sessionID)
	}
}
