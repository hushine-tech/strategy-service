package runtimeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type SessionRestarter interface {
	RestartSession(ctx context.Context, opts RestartSessionOptions) (RestartSessionResult, error)
}

type restartSessionHTTPRequest struct {
	SessionID       string  `json:"session_id"`
	MaxLossClosePct float64 `json:"max_loss_close_pct"`
	Leverage        float64 `json:"leverage"`
}

func StartLocalControlServer(ctx context.Context, listenAddr string, restarter SessionRestarter) (net.Addr, func(context.Context) error, error) {
	if restarter == nil {
		return nil, nil, fmt.Errorf("session restarter is required")
	}
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	normalized, err := normalizeLocalControlAddr(listenAddr)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", normalized)
	if err != nil {
		return nil, nil, fmt.Errorf("listen local control: %w", err)
	}

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeLocalControlError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/restart-worker-session", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeLocalControlError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		defer r.Body.Close()
		var req restartSessionHTTPRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeLocalControlError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		result, err := restarter.RestartSession(r.Context(), RestartSessionOptions{
			SessionID:       req.SessionID,
			MaxLossClosePct: req.MaxLossClosePct,
			Leverage:        req.Leverage,
		})
		if err != nil {
			writeLocalControlError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("runtime-agent local control stopped: %v\n", err)
		}
	}()
	return listener.Addr(), server.Shutdown, nil
}

func normalizeLocalControlAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid local control address %q: %w", addr, err)
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	if !isLoopbackHost(host) {
		return "", fmt.Errorf("local control address must bind to loopback, got %s", host)
	}
	return net.JoinHostPort(host, port), nil
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func writeLocalControlError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
