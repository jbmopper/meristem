// Package api owns the HTTP surface of meristem.
//
// v0 wires health/readiness plus the authenticated inbox, feed, and
// work-item routes. Command routes run through auth before idempotency so
// every replay cache entry and derived subject id is scoped to the caller.
package api

import (
	"bytes"
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
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/escalations"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/workitems"
)

// EnvHTTPAddr is the listen address. Defaults to :8080 when unset.
const EnvHTTPAddr = "MERISTEM_HTTP_ADDR"

// EnvMCPAllowedOrigins configures the exact-match Origin allowlist for /mcp
// (comma-separated). Default empty: requests without an Origin header are
// accepted (non-browser MCP clients send none); any present Origin is
// rejected 403 unless listed here. Per the 2026-07-28 spec there is no
// log-only mode — validation is always enforced.
const EnvMCPAllowedOrigins = "MERISTEM_MCP_ALLOWED_ORIGINS"

// EnvNodeID is this node's stable, DNS-safe node_id (docs/network-layer-spec.md
// §2 "Naming"). It lets the cross-node command route tell a command bound for
// this node (execute locally) from one to be durably queued for a peer. When
// unset a bare command is still treated as local and any X-Meristem-Queue-For
// naming a peer is still queued, so a single-node deploy needs no config.
const EnvNodeID = "MERISTEM_NODE_ID"

// EnvRegistryHomeNodeID pins the only source identity accepted by the local
// registry snapshot observation endpoint.
const EnvRegistryHomeNodeID = "MERISTEM_REGISTRY_HOME_NODE_ID"

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
	registryHomeNodeID    string
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
	listeners             *listeners.Service
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
	mcpAllowedOrigins     map[string]bool
	policyProfiles        *policyprofile.Service
	projections           *projectiondefs.Service
	registry              *registry.Service
	crossnode             *crossnode.QueueService
	nodeSnapshots         *nodes.SnapshotService
	oauthClients          *oauth.RegistrationService
	oauthAuthorization    *oauth.AuthorizationService
	oauthTokens           *oauth.TokenService
	oauthClientAdmin      *oauth.ClientAdminService
	oauthRuntime          oauthRuntimeConfig
	oauthActorLookup      oauthActorLookup
	policy                safety.Policy
	build                 buildguard.StatusProvider
}

// New constructs a Server. The pool must already be open; the API does not
// own its lifecycle.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Server {
	return NewWithPolicyAndBuildGuard(pool, logger, safety.DefaultPolicy(), buildguard.Disabled())
}

// NewWithPolicy constructs a Server with the already-resolved startup safety
// policy so readiness/logging reflect the active profile in force.
func NewWithPolicy(pool *pgxpool.Pool, logger *slog.Logger, policy safety.Policy) *Server {
	return NewWithPolicyAndBuildGuard(pool, logger, policy, buildguard.Disabled())
}

// NewWithPolicyAndBuildGuard constructs the runtime API with a dynamically
// checked build-consistency provider. Existing embedding/test constructors are
// explicitly unmanaged; the meristem runtime supplies its process guard here.
func NewWithPolicyAndBuildGuard(pool *pgxpool.Pool, logger *slog.Logger, policy safety.Policy, build buildguard.StatusProvider) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if build == nil {
		build = buildguard.Disabled()
	}
	addr := os.Getenv(EnvHTTPAddr)
	if addr == "" {
		addr = defaultAddr
	}
	rawPublicBaseURL := os.Getenv(EnvPublicBaseURL)
	oauthRuntime := resolveOAuthRuntimeConfig(rawPublicBaseURL, os.Getenv(EnvOAuthSystemActorID))
	publicBaseURL := normalizePublicBaseURL(rawPublicBaseURL)
	if oauthRuntime.mode == oauthRuntimeEnabled {
		publicBaseURL = oauthRuntime.publicBaseURL
	}
	s := &Server{
		pool:               pool,
		logger:             logger,
		addr:               addr,
		nodeID:             strings.TrimSpace(os.Getenv(EnvNodeID)),
		registryHomeNodeID: strings.TrimSpace(os.Getenv(EnvRegistryHomeNodeID)),
		publicBaseURL:      publicBaseURL,
		mux:                http.NewServeMux(),
		oauthRuntime:       oauthRuntime,
		policy:             policy,
		build:              build,
		mcpAllowedOrigins:  parseMCPAllowedOrigins(os.Getenv(EnvMCPAllowedOrigins)),
	}
	if pool != nil {
		s.writer = app.NewGuardedEventWriter(build)
		s.authService = auth.NewService(pool, s.writer)
		s.authenticator = s.authService
		s.authMiddleware = auth.NewMiddleware(s.authService)
		s.idempotencyMiddleware = idempotency.NewMiddlewareWithGuard(pool, s.writer, func() error {
			return buildguard.RequireNonBlocking(build)
		})
		s.inbox = inbox.NewService(pool, s.writer)
		s.signals = signals.NewService(pool, s.writer)
		s.workItems = workitems.NewService(pool, s.writer)
		s.listeners = listeners.NewService(pool, s.writer)
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
		s.nodeSnapshots = nodes.NewSnapshotService(pool, s.writer)
		systemActorID := oauthRuntime.systemActorID
		if oauthRuntime.mode == oauthRuntimeEnabled {
			s.oauthClients = oauth.NewRegistrationServiceWithSystemActor(pool, s.writer, systemActorID)
			s.oauthAuthorization = oauth.NewAuthorizationService(pool, s.writer, s.workItems, s.approvals, systemActorID)
			s.oauthTokens = oauth.NewTokenService(pool, s.writer, systemActorID)
		}
		s.oauthClientAdmin = oauth.NewClientAdminService(pool, s.writer)
		s.oauthActorLookup = s.authService.Get
		mcpVersion := build.Status().Version()
		if mcpVersion == "unknown" {
			mcpVersion = "dev"
		}
		s.mcpServer = mcp.New(mcp.Deps{
			Auth:                s.authService,
			Access:              s.access,
			Idempotency:         s.idempotencyMiddleware,
			Inbox:               s.inbox,
			OAuthClientAdmin:    s.oauthClientAdmin,
			WorkItems:           s.workItems,
			Listeners:           s.listeners,
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
		}, mcp.ServerInfo{Name: "meristem", Version: mcpVersion, BuildStatus: build}, logger)
	}
	s.routes()
	return s
}

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.addr }

// Handler exposes the target-bound mux so tests and the network server enforce
// identical cross-node metadata checks.
func (s *Server) Handler() http.Handler { return s.buildGuarded(s.targetBound(s.mux)) }

// buildGuarded refuses every authoritative REST/OAuth surface before auth,
// idempotency, or domain handlers when the running process no longer matches
// its reviewed-v1 pin. Liveness, readiness, discovery metadata, and the MCP
// envelope remain reachable so callers can diagnose the failure; tools/call
// performs its own dynamic guard before any tool handler or replay lookup.
func (s *Server) buildGuarded(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if buildGuardExemptPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		status := s.buildStatus()
		if status.Blocking() {
			writeAPIError(w, http.StatusServiceUnavailable, "build_pin",
				"served build is not current; inspect /readyz for build status")
			return
		}
		// SSE performs a finer-grained dynamic check before every tail/frame and
		// must retain streaming interfaces. Buffer auth/preflight responses until
		// the first Flush so a pin change during authentication cannot leak a stale
		// 401 before the stream handler reaches its own dynamic checks.
		if r.URL.Path == "/v1/feed/stream" {
			if _, ok := w.(http.Flusher); ok {
				stream := newBuildGuardStreamResponse(w, s)
				next.ServeHTTP(stream, r)
				stream.finish()
				return
			}
			// Preserve the historical stream_unsupported response for synthetic
			// writers without Flusher while still applying the ordinary postcheck.
			buffered := newBuildGuardResponse()
			next.ServeHTTP(buffered, r)
			if s.buildStatus().Blocking() {
				writeAPIError(w, http.StatusServiceUnavailable, "build_pin",
					"served build is not current; inspect /readyz for build status")
				return
			}
			buffered.flushTo(w)
			return
		}

		// Delay ordinary authoritative response headers until the handler has
		// completed and the dynamic pin has been checked again. This covers all
		// REST reads—not only known long polls—and also suppresses a mutation
		// response if an advisory-lock wait or handler outlives the reviewed pin.
		buffered := newBuildGuardResponse()
		next.ServeHTTP(buffered, r)
		if buffered.overflow {
			writeAPIError(w, http.StatusInternalServerError, "response_too_large",
				"response exceeds the deterministic API buffering limit")
			return
		}
		// A mutation admitted while the pin was current must be allowed to
		// return its committed result. Suppressing it can strand one-time
		// credentials (OAuth token rotation and subactor token issuance). Reads
		// remain post-handler guarded because they have no such commit boundary.
		if completesAdmittedMutation(r, buffered) {
			buffered.flushTo(w)
			return
		}
		if s.buildStatus().Blocking() {
			writeAPIError(w, http.StatusServiceUnavailable, "build_pin",
				"served build is not current; inspect /readyz for build status")
			return
		}
		buffered.flushTo(w)
	})
}

func completesAdmittedMutation(r *http.Request, response *buildGuardResponse) bool {
	if response == nil || response.status < http.StatusOK || response.status >= http.StatusBadRequest {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		// OAuth authorization is a stateful GET, but a pending continuation is
		// read-only. Its handler marks only responses whose service call actually
		// committed an authorization request/outcome (including a one-time code).
		return r.Method == http.MethodGet && r.URL.Path == "/oauth/authorize" && response.admittedMutation
	default:
		return true
	}
}

type buildGuardResponse struct {
	header           http.Header
	body             bytes.Buffer
	status           int
	wroteHeader      bool
	overflow         bool
	admittedMutation bool
}

func newBuildGuardResponse() *buildGuardResponse {
	return &buildGuardResponse{header: make(http.Header), status: http.StatusOK}
}

func (r *buildGuardResponse) Header() http.Header { return r.header }

func (r *buildGuardResponse) markAdmittedMutation() { r.admittedMutation = true }

type admittedMutationMarker interface {
	markAdmittedMutation()
}

func markAdmittedMutationResponse(w http.ResponseWriter) {
	if marker, ok := w.(admittedMutationMarker); ok {
		marker.markAdmittedMutation()
	}
}

func (r *buildGuardResponse) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
}

func (r *buildGuardResponse) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	if r.overflow {
		return len(body), nil
	}
	if len(body) > safety.MaxBufferedAuthoritativeResponseBytes-r.body.Len() {
		// Retain no partial authoritative response once the deterministic cap is
		// crossed. Report the write as consumed so a handler cannot replace the
		// fail-closed outer response with a transport-specific encoding error.
		r.overflow = true
		r.body.Reset()
		return len(body), nil
	}
	return r.body.Write(body)
}

func (r *buildGuardResponse) flushTo(w http.ResponseWriter) {
	for key, values := range r.header {
		w.Header()[key] = append([]string(nil), values...)
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body.Bytes())
}

// buildGuardStreamResponse delays the SSE response head until authentication
// and cursor/projection preflight finish. On the first Flush it rechecks the
// reviewed pin, commits the buffered head, and then becomes a passthrough for
// the long-lived stream. Later query/frame checks live in handleFeedStream.
type buildGuardStreamResponse struct {
	target      http.ResponseWriter
	server      *Server
	buffer      *buildGuardResponse
	passthrough bool
	finished    bool
}

func newBuildGuardStreamResponse(target http.ResponseWriter, server *Server) *buildGuardStreamResponse {
	return &buildGuardStreamResponse{target: target, server: server, buffer: newBuildGuardResponse()}
}

func (r *buildGuardStreamResponse) Header() http.Header {
	if r.passthrough {
		return r.target.Header()
	}
	return r.buffer.Header()
}

func (r *buildGuardStreamResponse) WriteHeader(status int) {
	if r.finished {
		return
	}
	if r.passthrough {
		r.target.WriteHeader(status)
		return
	}
	r.buffer.WriteHeader(status)
}

func (r *buildGuardStreamResponse) Write(body []byte) (int, error) {
	if r.finished {
		return len(body), nil
	}
	if r.passthrough {
		return r.target.Write(body)
	}
	return r.buffer.Write(body)
}

func (r *buildGuardStreamResponse) Flush() {
	if r.finished {
		return
	}
	if !r.passthrough {
		if !r.commitPreflight() {
			return
		}
	}
	if flusher, ok := r.target.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach the real connection for the SSE
// write-deadline calls without exposing its response headers before Flush.
func (r *buildGuardStreamResponse) Unwrap() http.ResponseWriter { return r.target }

func (r *buildGuardStreamResponse) finish() {
	if r.finished || r.passthrough {
		return
	}
	_ = r.commitPreflight()
}

func (r *buildGuardStreamResponse) commitPreflight() bool {
	if r.buffer.overflow {
		writeAPIError(r.target, http.StatusInternalServerError, "response_too_large",
			"response exceeds the deterministic API buffering limit")
		r.finished = true
		return false
	}
	if r.server.buildStatus().Blocking() {
		writeAPIError(r.target, http.StatusServiceUnavailable, "build_pin",
			"served build is not current; inspect /readyz for build status")
		r.finished = true
		return false
	}
	r.buffer.flushTo(r.target)
	r.passthrough = true
	return true
}

func buildGuardExemptPath(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/mcp",
		"/.well-known/oauth-protected-resource/mcp",
		"/.well-known/oauth-authorization-server":
		return true
	default:
		return false
	}
}

func (s *Server) buildStatus() buildguard.Status {
	if s.build == nil {
		return buildguard.Disabled().Status()
	}
	return s.build.Status()
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleLiveness)
	s.mux.HandleFunc("GET /readyz", s.handleReadiness)
	s.mux.HandleFunc("GET /.well-known/oauth-protected-resource/mcp", s.oauthPublicRoute(s.handleOAuthProtectedResourceMetadata))
	s.mux.HandleFunc("GET /.well-known/oauth-authorization-server", s.oauthPublicRoute(s.handleOAuthAuthorizationServerMetadata))
	s.mux.HandleFunc("POST /oauth/register", s.oauthPublicRoute(s.handleOAuthClientRegistration))
	s.mux.HandleFunc("GET /oauth/authorize", s.oauthPublicRoute(s.handleOAuthAuthorize))
	s.mux.HandleFunc("POST /oauth/token", s.oauthPublicRoute(s.handleOAuthToken))
	s.mux.Handle("POST /v1/oauth/clients/{client_id}/actor", s.command(http.HandlerFunc(s.handleOAuthBindActor)))
	s.mux.Handle("POST /v1/oauth/clients/{client_id}/revoke", s.command(http.HandlerFunc(s.handleOAuthRevokeClient)))
	s.mux.Handle("POST /v1/oauth/grants/{grant_id}/revoke", s.command(http.HandlerFunc(s.handleOAuthRevokeGrant)))
	// Origin validation wraps outermost so it runs before any credential work
	// (2026-07-28: servers MUST validate Origin; present-and-invalid MUST 403).
	s.mux.Handle("GET /mcp", s.mcpOriginGuard(s.oauthAccessRoute(s.mcpProtected(http.HandlerFunc(s.handleMCP)))))
	s.mux.Handle("POST /mcp", s.mcpOriginGuard(s.oauthAccessRoute(s.mcpProtected(http.HandlerFunc(s.handleMCP)))))
	s.mux.Handle("DELETE /mcp", s.mcpOriginGuard(http.HandlerFunc(handleMCPDelete)))
	s.mux.Handle("POST /v1/inbox/messages", s.commandWithAccess(s.canCaptureInbox, http.HandlerFunc(s.handleCaptureMessage)))
	s.mux.Handle("POST /v1/signals", s.command(http.HandlerFunc(s.handleReceiveSignal)))
	s.mux.Handle("POST /v1/crossnode/commands", s.crossnodeQueueCommand(http.HandlerFunc(s.handleCrossnodeCommand)))
	s.mux.Handle("GET /v1/crossnode/commands", s.protected(http.HandlerFunc(s.handleCrossnodeCommandsList)))
	s.mux.Handle("GET /v1/crossnode/outcomes", s.protected(http.HandlerFunc(s.handleCrossnodeOutcomesList)))
	s.mux.Handle("POST /v1/crossnode/commands/{event_id}/attempt", s.crossnodeAckCommand(http.HandlerFunc(s.handleCrossnodeCommandAttempt)))
	s.mux.Handle("POST /v1/crossnode/commands/{event_id}/ack", s.crossnodeAckCommand(http.HandlerFunc(s.handleCrossnodeCommandAck)))
	s.mux.Handle("GET /v1/nodes/registry-snapshot", s.protected(http.HandlerFunc(s.handleRegistrySnapshotRead)))
	s.mux.Handle("POST /v1/nodes/registry-snapshot/observe", s.commandWithAccess(s.canObserveRegistrySnapshot, http.HandlerFunc(s.handleRegistrySnapshotObserve)))
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
	s.mux.Handle("POST /v1/work-items/{id}/claim", s.commandWithAccess(s.canWriteWorkItemPath("work_items.claim"), http.HandlerFunc(s.handleClaimWorkItem)))
	s.mux.Handle("GET /v1/work-items/{id}/assignment", s.protected(http.HandlerFunc(s.handleGetWorkItemAssignment)))
	s.mux.Handle("GET /v1/assignments/held", s.protected(http.HandlerFunc(s.handleListHeldAssignments)))
	s.mux.Handle("POST /v1/work-items/{id}/yield", s.commandWithAccess(s.canWriteWorkItemPath("work_items.yield"), http.HandlerFunc(s.handleYieldWorkItem)))
	s.mux.Handle("POST /v1/listeners", s.commandWithAccess(s.canUseListenerTool("listeners.create"), http.HandlerFunc(s.handleCreateListener)))
	s.mux.Handle("GET /v1/listeners", s.protected(http.HandlerFunc(s.handleListListeners)))
	s.mux.Handle("GET /v1/listeners/{id}", s.protected(http.HandlerFunc(s.handleGetListener)))
	s.mux.Handle("GET /v1/listeners/by-name/{name}", s.protected(http.HandlerFunc(s.handleGetListenerByName)))
	s.mux.Handle("GET /v1/listeners/{id}/demand/candidates", s.protected(http.HandlerFunc(s.handleListDemandCandidates)))
	s.mux.Handle("POST /v1/listeners/{id}/claim", s.commandWithAccess(s.canUseListenerTool("listeners.claim"), http.HandlerFunc(s.handleClaimListenerDemand)))
	s.mux.Handle("POST /v1/listeners/{id}/policy", s.commandWithAccess(s.canUseListenerTool("listeners.set_policy"), http.HandlerFunc(s.handleSetListenerPolicy)))
	s.mux.Handle("POST /v1/listeners/{id}/credential-bindings", s.commandWithAccess(s.canUseListenerTool("listeners.bind_credential"), http.HandlerFunc(s.handleBindListenerCredential)))
	s.mux.Handle("POST /v1/listeners/{id}/retire", s.commandWithAccess(s.canUseListenerTool("listeners.retire"), http.HandlerFunc(s.handleRetireListener)))
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
		if !s.bindRemoteProvenance(w, r) {
			return
		}
		if !gate(w, r) {
			return
		}
		inner.ServeHTTP(w, r)
	}))
}

// targetBound validates optional peer-routing metadata before authentication,
// idempotency, or a domain handler can append events. Ordinary local requests
// omit the metadata and continue unchanged.
func (s *Server) targetBound(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			target := strings.TrimSpace(r.Header.Get(crossnode.HeaderTargetNode))
			if target != "" {
				if !domain.ValidNodeID(target) || target != s.nodeID {
					writeAPIError(w, http.StatusConflict, "target_node_mismatch",
						"X-Meristem-Target-Node does not match this node")
					return
				}
				origin := strings.TrimSpace(r.Header.Get(crossnode.HeaderOriginNode))
				if !domain.ValidNodeID(origin) {
					writeAPIError(w, http.StatusBadRequest, "invalid_origin_node",
						"X-Meristem-Origin-Node must name a DNS-safe originating node")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Run starts the HTTP server and blocks until ctx is cancelled or the
// underlying server returns an error. Shutdown is graceful: in-flight
// requests get up to 10 seconds to complete.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: defaultReadHeaderLimit,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		policyID, _ := s.policy.Fingerprint()
		buildStatus := s.buildStatus()
		s.logger.Info("api listening",
			slog.String("addr", s.addr),
			slog.String("safety_policy", policyID),
			slog.String("build_state", string(buildStatus.State)),
			slog.String("compiled_commit", buildStatus.CompiledCommit),
			slog.String("compiled_metadata", string(buildStatus.CompiledMetadata)),
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
	buildStatus := s.buildStatus()
	if buildStatus.Blocking() {
		s.writeReadiness(w, http.StatusServiceUnavailable, buildStatus, map[string]string{
			"status": "unavailable",
			"reason": "build_pin",
		})
		return
	}
	if s.oauthRuntime.mode == oauthRuntimeInvalid {
		s.writeReadiness(w, http.StatusServiceUnavailable, buildStatus, map[string]string{
			"status": "unavailable",
			"reason": "oauth_configuration",
		})
		return
	}

	if s.pool == nil {
		s.writeReadiness(w, http.StatusServiceUnavailable, buildStatus, map[string]string{
			"status": "unavailable",
			"reason": "database",
		})
		return
	}
	if err := s.pool.Ping(ctx); err != nil {
		s.logger.Warn("readiness check failed", slog.String("error", err.Error()))
		s.writeReadiness(w, http.StatusServiceUnavailable, buildStatus, map[string]string{
			"status": "unavailable",
			"reason": "database",
		})
		return
	}
	if s.oauthRuntime.mode == oauthRuntimeEnabled {
		if err := s.checkOAuthRuntime(ctx); err != nil {
			s.logger.Warn("oauth readiness check failed", slog.String("error", err.Error()))
			s.writeReadiness(w, http.StatusServiceUnavailable, buildStatus, map[string]string{
				"status": "unavailable",
				"reason": "oauth_system_actor",
			})
			return
		}
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
	oauthStatus := "disabled"
	if s.oauthRuntime.mode == oauthRuntimeEnabled {
		oauthStatus = "ok"
	}
	s.writeReadiness(w, http.StatusOK, buildStatus, map[string]string{
		"status":         "ok",
		"database":       "ok",
		"oauth":          oauthStatus,
		"safety":         "ok",
		"safety_policy":  policyID,
		"policy_profile": profileName,
	})
}

func (s *Server) writeReadiness(w http.ResponseWriter, statusCode int, buildStatus buildguard.Status, body map[string]string) {
	// Readiness probes can spend most of their lifetime waiting on Postgres and
	// OAuth/profile lookups. Re-read the independently published pin at the
	// response boundary so an old process cannot return a stale 200 after the
	// reviewed tip advances during those probes.
	latest := s.buildStatus()
	if latest.Blocking() {
		statusCode = http.StatusServiceUnavailable
		body = map[string]string{
			"status": "unavailable",
			"reason": "build_pin",
		}
		buildStatus = latest
	} else if !buildStatus.Blocking() {
		buildStatus = latest
	}
	body["build_state"] = string(buildStatus.State)
	body["build_compiled_commit"] = buildStatus.CompiledCommit
	body["build_pinned_commit"] = buildStatus.ExpectedCommit
	body["build_compiled_metadata"] = string(buildStatus.CompiledMetadata)
	body["build_reason"] = buildStatus.Reason
	writeJSON(w, statusCode, body)
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
