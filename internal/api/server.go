// Package api owns the HTTP surface of meristem.
//
// v0 wires health/readiness plus the authenticated inbox, feed, and
// work-item routes. Command routes run through auth before idempotency so
// every replay cache entry and derived subject id is scoped to the caller.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/workitems"
)

// EnvHTTPAddr is the listen address. Defaults to :8080 when unset.
const EnvHTTPAddr = "MERISTEM_HTTP_ADDR"

// Defaults chosen to be reasonable behind a reverse proxy (Caddy/nginx). The
// spec calls for TLS termination at that layer, not in-process.
const (
	defaultAddr            = ":8080"
	defaultReadHeaderLimit = 5 * time.Second
	defaultReadTimeout     = 15 * time.Second
	defaultWriteTimeout    = 30 * time.Second
	defaultIdleTimeout     = 120 * time.Second
	readinessPingTimeout   = 2 * time.Second
)

// Server bundles the HTTP handler with its dependencies. Construct it with
// New, then drive it with Run.
type Server struct {
	pool                  *pgxpool.Pool
	logger                *slog.Logger
	addr                  string
	mux                   *http.ServeMux
	writer                *events.Writer
	authService           *auth.Service
	authMiddleware        *auth.Middleware
	idempotencyMiddleware *idempotency.Middleware
	inbox                 *inbox.Service
	signals               *signals.Service
	workItems             *workitems.Service
	deterministicErrors   *errorreporting.Service
	feed                  *feed.Service
	policy                safety.Policy
}

// New constructs a Server. The pool must already be open; the API does not
// own its lifecycle.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	addr := os.Getenv(EnvHTTPAddr)
	if addr == "" {
		addr = defaultAddr
	}
	s := &Server{
		pool:   pool,
		logger: logger,
		addr:   addr,
		mux:    http.NewServeMux(),
		policy: safety.DefaultPolicy(),
	}
	if pool != nil {
		s.writer = app.NewEventWriter()
		s.authService = auth.NewService(pool, s.writer)
		s.authMiddleware = auth.NewMiddleware(s.authService)
		s.idempotencyMiddleware = idempotency.NewMiddleware(pool, s.writer)
		s.inbox = inbox.NewService(pool, s.writer)
		s.signals = signals.NewService(pool, s.writer)
		s.workItems = workitems.NewService(pool, s.writer)
		s.deterministicErrors = errorreporting.NewService(pool, s.writer)
		s.feed = feed.NewService(pool)
	}
	s.routes()
	return s
}

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.addr }

// Handler exposes the underlying mux so tests can hit handlers without going
// through the network.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleLiveness)
	s.mux.HandleFunc("GET /readyz", s.handleReadiness)
	s.mux.Handle("POST /v1/inbox/messages", s.command(http.HandlerFunc(s.handleCaptureMessage)))
	s.mux.Handle("POST /v1/signals", s.command(http.HandlerFunc(s.handleReceiveSignal)))
	s.mux.Handle("GET /v1/feed", s.protected(http.HandlerFunc(s.handleFeed)))
	s.mux.Handle("GET /v1/feed/stream", s.protected(http.HandlerFunc(s.handleFeedStream)))
	s.mux.Handle("GET /v1/deterministic-errors", s.protected(http.HandlerFunc(s.handleListDeterministicErrors)))
	s.mux.Handle("GET /v1/deterministic-errors/{id}", s.protected(http.HandlerFunc(s.handleGetDeterministicError)))
	s.mux.Handle("GET /v1/work-items", s.protected(http.HandlerFunc(s.handleListWorkItems)))
	s.mux.Handle("POST /v1/work-items", s.command(http.HandlerFunc(s.handleCreateWorkItem)))
	s.mux.Handle("GET /v1/work-items/{id}", s.protected(http.HandlerFunc(s.handleGetWorkItem)))
	s.mux.Handle("POST /v1/work-items/{id}/children", s.command(http.HandlerFunc(s.handleSpawnChild)))
	s.mux.Handle("POST /v1/work-items/{id}/events", s.command(http.HandlerFunc(s.handleAppendWorkItemEvent)))
	s.mux.Handle("POST /v1/work-items/{id}/metadata", s.command(http.HandlerFunc(s.handleUpdateWorkItemMetadata)))
	s.mux.Handle("POST /v1/work-items/{id}/transition", s.command(http.HandlerFunc(s.handleTransitionWorkItem)))
}

func (s *Server) protected(next http.Handler) http.Handler {
	if s.authMiddleware == nil {
		return serviceUnavailableHandler()
	}
	return s.authMiddleware.Wrap(next)
}

func (s *Server) command(next http.Handler) http.Handler {
	if s.authMiddleware == nil || s.idempotencyMiddleware == nil {
		return serviceUnavailableHandler()
	}
	return s.authMiddleware.Wrap(s.idempotencyMiddleware.Wrap(next))
}

// Run starts the HTTP server and blocks until ctx is cancelled or the
// underlying server returns an error. Shutdown is graceful: in-flight
// requests get up to 10 seconds to complete.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.mux,
		ReadHeaderTimeout: defaultReadHeaderLimit,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		policyID, _ := s.policy.Fingerprint()
		s.logger.Info("api listening",
			slog.String("addr", s.addr),
			slog.String("safety_policy", policyID),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.logger.Info("api shutting down")
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
}

// handleLiveness returns 200 if the process is alive enough to answer HTTP.
// It deliberately does not touch Postgres: a flaky DB should not cause an
// orchestrator to kill an otherwise-healthy process and lose in-flight work.
func (s *Server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadiness reports whether the process is fit to serve real traffic
// right now. We require Postgres connectivity because every meaningful
// endpoint will need it. A 503 here tells the load balancer to stop sending
// requests until the next probe succeeds.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessPingTimeout)
	defer cancel()

	if s.pool == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"reason": "database",
		})
		return
	}
	if err := s.pool.Ping(ctx); err != nil {
		s.logger.Warn("readiness check failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"reason": "database",
		})
		return
	}
	policyID, _ := s.policy.Fingerprint()
	writeJSON(w, http.StatusOK, map[string]string{
		"status":        "ok",
		"database":      "ok",
		"safety":        "ok",
		"safety_policy": policyID,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func serviceUnavailableHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
	})
}
