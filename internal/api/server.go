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
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/escalations"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/workitems"
)

// EnvHTTPAddr is the listen address. Defaults to :8080 when unset.
const EnvHTTPAddr = "MERISTEM_HTTP_ADDR"

// EnvNodeID is this node's stable, DNS-safe node_id (docs/network-layer-spec.md
// §2 "Naming"). It lets the cross-node command route tell a command bound for
// this node (execute locally) from one to be durably queued for a peer. When
// unset a bare command is still treated as local and any X-Meristem-Queue-For
// naming a peer is still queued, so a single-node deploy needs no config.
const EnvNodeID = "MERISTEM_NODE_ID"

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
	nodeID                string
	publicBaseURL         string
	mux                   *http.ServeMux
	writer                *events.Writer
	authService           *auth.Service
	authenticator         auth.Authenticator
	authMiddleware        *auth.Middleware
	idempotencyMiddleware *idempotency.Middleware
	inbox                 *inbox.Service
	signals               *signals.Service
	workItems             *workitems.Service
	access                *access.Service
	escalations           *escalations.Service
	approvals             *approvals.Service
	httpConnector         *httpconnector.Service
	grants                *grants.IssuanceService
	deterministicErrors   *errorreporting.Service
	checkProposals        *convergence.ChecksProposalService
	cultivarActivations   *cultivaractivation.Service
	feed                  *feed.Service
	mcpServer             *mcp.Server
	policyProfiles        *policyprofile.Service
	projections           *projectiondefs.Service
	registry              *registry.Service
	crossnode             *crossnode.QueueService
	policy                safety.Policy
}

// New constructs a Server. The pool must already be open; the API does not
// own its lifecycle.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Server {
	return NewWithPolicy(pool, logger, safety.DefaultPolicy())
}

// NewWithPolicy constructs a Server with the already-resolved startup safety
// policy so readiness/logging reflect the active profile in force.
func NewWithPolicy(pool *pgxpool.Pool, logger *slog.Logger, policy safety.Policy) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	addr := os.Getenv(EnvHTTPAddr)
	if addr == "" {
		addr = defaultAddr
	}
	s := &Server{
		pool:          pool,
		logger:        logger,
		addr:          addr,
		nodeID:        strings.TrimSpace(os.Getenv(EnvNodeID)),
		publicBaseURL: normalizePublicBaseURL(os.Getenv(EnvPublicBaseURL)),
		mux:           http.NewServeMux(),
		policy:        policy,
	}
	if pool != nil {
		s.writer = app.NewEventWriter()
		s.authService = auth.NewService(pool, s.writer)
		s.authenticator = s.authService
		s.authMiddleware = auth.NewMiddleware(s.authService)
		s.idempotencyMiddleware = idempotency.NewMiddleware(pool, s.writer)
		s.inbox = inbox.NewService(pool, s.writer)
		s.signals = signals.NewService(pool, s.writer)
		s.workItems = workitems.NewService(pool, s.writer)
		s.access = access.NewService(pool)
		s.escalations = escalations.NewService(pool, s.writer)
		s.approvals = approvals.NewService(pool, s.writer)
		s.httpConnector = httpconnector.NewService(pool, s.writer, s.approvals, nil)
		s.grants = grants.NewIssuanceService(pool, s.writer, s.authService, s.escalations)
		s.deterministicErrors = errorreporting.NewService(pool, s.writer)
		s.checkProposals = convergence.NewChecksProposalService(pool, s.writer)
		s.cultivarActivations = cultivaractivation.NewService(pool, s.writer)
		s.feed = feed.NewService(pool)
		s.policyProfiles = policyprofile.NewService(pool, s.writer)
		s.projections = projectiondefs.NewService(pool, s.writer)
		s.registry = registry.NewService(pool, s.writer)
		s.crossnode = crossnode.NewQueueService(pool, s.writer)
		s.mcpServer = mcp.New(mcp.Deps{
			Auth:                s.authService,
			Access:              s.access,
			Idempotency:         s.idempotencyMiddleware,
			Inbox:               s.inbox,
			WorkItems:           s.workItems,
			Approvals:           s.approvals,
			HTTPConnector:       s.httpConnector,
			CheckProposals:      s.checkProposals,
			CultivarActivations: s.cultivarActivations,
			DeterministicErrors: s.deterministicErrors,
			Feed:                s.feed,
			PolicyProfiles:      s.policyProfiles,
			Projections:         s.projections,
			Registry:            s.registry,
			MaxFeedWait:         s.policy.MaxFeedWait,
		}, mcp.ServerInfo{Name: "meristem", Version: "dev"}, logger)
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
	s.mux.HandleFunc("GET /.well-known/oauth-protected-resource/mcp", s.handleOAuthProtectedResourceMetadata)
	s.mux.HandleFunc("GET /.well-known/oauth-authorization-server", s.handleOAuthAuthorizationServerMetadata)
	s.mux.HandleFunc("GET /oauth/authorize", s.handleOAuthFlowUnavailable)
	s.mux.HandleFunc("POST /oauth/token", s.handleOAuthFlowUnavailable)
	s.mux.Handle("GET /mcp", s.mcpProtected(http.HandlerFunc(s.handleMCP)))
	s.mux.Handle("POST /mcp", s.mcpProtected(http.HandlerFunc(s.handleMCP)))
	s.mux.Handle("POST /v1/inbox/messages", s.commandWithAccess(s.canCaptureInbox, http.HandlerFunc(s.handleCaptureMessage)))
	s.mux.Handle("POST /v1/signals", s.command(http.HandlerFunc(s.handleReceiveSignal)))
	s.mux.Handle("POST /v1/crossnode/commands", s.command(http.HandlerFunc(s.handleCrossnodeCommand)))
	s.mux.Handle("GET /v1/crossnode/commands", s.protected(http.HandlerFunc(s.handleCrossnodeCommandsList)))
	s.mux.Handle("POST /v1/crossnode/commands/{event_id}/ack", s.command(http.HandlerFunc(s.handleCrossnodeCommandAck)))
	s.mux.Handle("POST /v1/subactor-grants", s.command(http.HandlerFunc(s.handleCreateSubactorGrant)))
	s.mux.Handle("POST /v1/policy-profile", s.commandWithAccess(s.canSwitchPolicyProfile, http.HandlerFunc(s.handleSwitchPolicyProfile)))
	s.mux.Handle("POST /v1/tokens/revoke-all", s.commandWithAccess(s.canPanicRevokeTokens, http.HandlerFunc(s.handlePanicRevokeTokens)))
	s.mux.Handle("GET /v1/feed", s.protected(http.HandlerFunc(s.handleFeed)))
	s.mux.Handle("GET /v1/feed/stream", s.protected(http.HandlerFunc(s.handleFeedStream)))
	s.mux.Handle("GET /v1/deterministic-errors", s.protected(http.HandlerFunc(s.handleListDeterministicErrors)))
	s.mux.Handle("GET /v1/deterministic-errors/{id}", s.protected(http.HandlerFunc(s.handleGetDeterministicError)))
	s.mux.Handle("GET /v1/backlog/readiness", s.protected(http.HandlerFunc(s.handleBacklogReadiness)))
	s.mux.Handle("GET /v1/registry", s.protected(http.HandlerFunc(s.handleRegistryList)))
	s.mux.Handle("GET /v1/projections", s.protected(http.HandlerFunc(s.handleProjectionsList)))
	s.mux.Handle("GET /v1/projections/{name}", s.protected(http.HandlerFunc(s.handleProjectionsGet)))
	s.mux.Handle("POST /v1/registry/projections", s.commandWithAccess(s.canWriteRegistry("projections.define"), http.HandlerFunc(s.handleProjectionsDefine)))
	s.mux.Handle("GET /v1/registry/tropisms/{name}", s.protected(http.HandlerFunc(s.handleRegistryGetTropism)))
	s.mux.Handle("POST /v1/registry/tropisms", s.commandWithAccess(s.canWriteRegistry("registry.define_tropism"), http.HandlerFunc(s.handleRegistryDefineTropism)))
	s.mux.Handle("GET /v1/registry/cultivars/{name}", s.protected(http.HandlerFunc(s.handleRegistryGetCultivar)))
	s.mux.Handle("POST /v1/registry/cultivars", s.commandWithAccess(s.canWriteRegistry("registry.define_cultivar"), http.HandlerFunc(s.handleRegistryDefineCultivar)))
	s.mux.Handle("GET /v1/work-items", s.protected(http.HandlerFunc(s.handleListWorkItems)))
	s.mux.Handle("POST /v1/work-items", s.commandWithAccess(s.canCreateWorkItem, http.HandlerFunc(s.handleCreateWorkItem)))
	s.mux.Handle("GET /v1/work-items/{id}", s.protected(http.HandlerFunc(s.handleGetWorkItem)))
	s.mux.Handle("GET /v1/work-items/{id}/approvals", s.protected(http.HandlerFunc(s.handleListApprovalsForWorkItem)))
	s.mux.Handle("POST /v1/work-items/{id}/approvals", s.commandWithAccess(s.canWriteWorkItemPath("approvals.request"), http.HandlerFunc(s.handleCreateApproval)))
	s.mux.Handle("POST /v1/work-items/{id}/http-connector/actions", s.commandWithAccess(s.canWriteWorkItemPath("connectors.http_request"), http.HandlerFunc(s.handleHTTPConnectorAction)))
	s.mux.Handle("GET /v1/approvals/{id}", s.protected(http.HandlerFunc(s.handleGetApproval)))
	s.mux.Handle("POST /v1/approvals/{id}/decision", s.commandWithAccess(s.canDecideApprovalPath, http.HandlerFunc(s.handleDecideApproval)))
	s.mux.Handle("POST /v1/work-items/{id}/children", s.commandWithAccess(s.canWriteWorkItemPath("work_items.spawn_child"), http.HandlerFunc(s.handleSpawnChild)))
	s.mux.Handle("POST /v1/work-items/{id}/events", s.commandWithAccess(s.canWriteWorkItemPath("work_items.append_event"), http.HandlerFunc(s.handleAppendWorkItemEvent)))
	s.mux.Handle("POST /v1/work-items/{id}/convergence-proposal", s.commandWithAccess(s.canWriteWorkItemPath("convergence.propose_checks"), http.HandlerFunc(s.handleProposeConvergenceChecks)))
	s.mux.Handle("POST /v1/work-items/{id}/cultivar-activations", s.commandWithAccess(s.canWriteWorkItemPath("registry.activate_cultivar"), http.HandlerFunc(s.handleActivateCultivar)))
	s.mux.Handle("POST /v1/work-items/{id}/metadata", s.commandWithAccess(s.canWriteWorkItemPath("work_items.update_metadata"), http.HandlerFunc(s.handleUpdateWorkItemMetadata)))
	s.mux.Handle("POST /v1/work-items/{id}/transition", s.commandWithAccess(s.canWriteWorkItemPath("work_items.transition"), http.HandlerFunc(s.handleTransitionWorkItem)))
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

type accessGate func(http.ResponseWriter, *http.Request) bool

func (s *Server) commandWithAccess(gate accessGate, next http.Handler) http.Handler {
	if s.authMiddleware == nil || s.idempotencyMiddleware == nil {
		return serviceUnavailableHandler()
	}
	inner := s.idempotencyMiddleware.Wrap(next)
	return s.authMiddleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !gate(w, r) {
			return
		}
		inner.ServeHTTP(w, r)
	}))
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
	profileName := safety.ProfileSteady
	policyID, _ := s.policy.Fingerprint()
	if s.policyProfiles != nil {
		if active, err := s.policyProfiles.Active(ctx); err == nil {
			profileName = active.Name
			policyID = active.Fingerprint
		} else {
			s.logger.Warn("resolve active policy profile failed", slog.String("error", err.Error()))
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":         "ok",
		"database":       "ok",
		"safety":         "ok",
		"safety_policy":  policyID,
		"policy_profile": profileName,
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
