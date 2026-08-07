package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
)

const (
	apiBuildCommitA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	apiBuildCommitB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type pinFlipAuthenticator struct {
	flip func()
}

func (a pinFlipAuthenticator) Authenticate(context.Context, string) (domain.Token, error) {
	a.flip()
	return domain.Token{}, errors.New("synthetic authentication failure")
}

func currentBuildStatus() buildguard.Status {
	return buildguard.Status{
		State:            buildguard.StateCurrent,
		CompiledCommit:   apiBuildCommitA,
		ExpectedCommit:   apiBuildCommitA,
		CompiledMetadata: buildguard.CompiledValid,
		Reason:           "compiled commit matches the reviewed v1 pin",
	}
}

func mismatchedBuildStatus() buildguard.Status {
	return buildguard.Status{
		State:            buildguard.StateMismatch,
		CompiledCommit:   apiBuildCommitA,
		ExpectedCommit:   apiBuildCommitB,
		CompiledMetadata: buildguard.CompiledValid,
		Reason:           "compiled commit does not match the reviewed v1 pin",
	}
}

func TestBuildGuardReadinessIsDynamicAndLivenessSurvives(t *testing.T) {
	status := currentBuildStatus()
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	s := NewWithPolicyAndBuildGuard(nil, nil, safety.DefaultPolicy(), provider)

	ready := httptest.NewRecorder()
	s.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable || !strings.Contains(ready.Body.String(), `"reason":"database"`) {
		t.Fatalf("current build must preserve database readiness failure: status=%d body=%s", ready.Code, ready.Body.String())
	}
	assertReadinessBuildFields(t, ready, buildguard.StateCurrent, apiBuildCommitA, apiBuildCommitA)

	status = mismatchedBuildStatus()
	ready = httptest.NewRecorder()
	s.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable || !strings.Contains(ready.Body.String(), `"reason":"build_pin"`) {
		t.Fatalf("dynamic mismatch must take readiness out of service: status=%d body=%s", ready.Code, ready.Body.String())
	}
	assertReadinessBuildFields(t, ready, buildguard.StateMismatch, apiBuildCommitA, apiBuildCommitB)

	live := httptest.NewRecorder()
	s.Handler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if live.Code != http.StatusOK || !strings.Contains(live.Body.String(), `"status":"ok"`) {
		t.Fatalf("liveness must survive build mismatch: status=%d body=%s", live.Code, live.Body.String())
	}
}

func TestBuildGuardReadinessRechecksAtResponseBoundary(t *testing.T) {
	statusCalls := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		statusCalls++
		if statusCalls == 1 {
			return currentBuildStatus()
		}
		return mismatchedBuildStatus()
	})
	s := &Server{build: provider}
	initial := s.buildStatus()
	rec := httptest.NewRecorder()

	s.writeReadiness(rec, http.StatusOK, initial, map[string]string{
		"status":   "ok",
		"database": "ok",
	})

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"reason":"build_pin"`) {
		t.Fatalf("readiness returned a stale success: status=%d body=%s calls=%d", rec.Code, rec.Body.String(), statusCalls)
	}
	assertReadinessBuildFields(t, rec, buildguard.StateMismatch, apiBuildCommitA, apiBuildCommitB)
}

func TestBuildGuardBlocksAuthoritativeRoutesButLeavesDiagnostics(t *testing.T) {
	provider := buildguard.ProviderFunc(func() buildguard.Status { return mismatchedBuildStatus() })
	authn := &mcpRouteAuth{
		wantSecret: "mrs_guard_test",
		tok:        domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
	}
	s := &Server{
		authenticator: authn,
		mux:           http.NewServeMux(),
		policy:        safety.DefaultPolicy(),
		oauthRuntime:  oauthRuntimeConfig{mode: oauthRuntimeDisabled},
		build:         provider,
	}
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{
		Name:        "meristem-test",
		Version:     "fallback",
		BuildStatus: provider,
	}, nil)
	allowMCPWriteDeadlines(s)
	s.routes()

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/feed", nil),
		// OAuth authorization is a mutating GET and must not escape the guard.
		httptest.NewRequest(http.MethodGet, "/oauth/authorize", nil),
		httptest.NewRequest(http.MethodPost, "/oauth/token", nil),
	} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, request)
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
			t.Fatalf("%s %s escaped build guard: status=%d body=%s", request.Method, request.URL.Path, rec.Code, rec.Body.String())
		}
	}

	metadata := httptest.NewRecorder()
	s.Handler().ServeHTTP(metadata, httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil))
	if strings.Contains(metadata.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("well-known metadata was incorrectly build-blocked: status=%d body=%s", metadata.Code, metadata.Body.String())
	}

	initializeRequest := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	initializeRequest.Header.Set("Accept", "application/json, text/event-stream")
	initializeRequest.Header.Set("Authorization", "Bearer mrs_guard_test")
	initialize := httptest.NewRecorder()
	s.Handler().ServeHTTP(initialize, initializeRequest)
	if initialize.Code != http.StatusOK || !strings.Contains(initialize.Body.String(), `"state":"mismatch"`) || !strings.Contains(initialize.Body.String(), "ALL MCP TOOL CALLS ARE DISABLED") {
		t.Fatalf("MCP initialize could not explain build mismatch: status=%d body=%s", initialize.Code, initialize.Body.String())
	}

	callRequest := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"feed.read","arguments":{}}}`))
	callRequest.Header.Set("Accept", "application/json, text/event-stream")
	callRequest.Header.Set("Authorization", "Bearer mrs_guard_test")
	call := httptest.NewRecorder()
	s.Handler().ServeHTTP(call, callRequest)
	if call.Code != http.StatusOK || !strings.Contains(call.Body.String(), `"isError":true`) || !strings.Contains(call.Body.String(), "build_pin") {
		t.Fatalf("MCP tool call was not build-blocked: status=%d body=%s", call.Code, call.Body.String())
	}
}

func TestBuildGuardSuppressesGenericRESTResponseAfterHandlerPinChange(t *testing.T) {
	status := currentBuildStatus()
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	s := &Server{mux: http.NewServeMux(), build: provider}
	s.mux.HandleFunc("GET /v1/slow-read", func(w http.ResponseWriter, _ *http.Request) {
		status = mismatchedBuildStatus()
		w.Header().Set("X-Stale-Handler", "must-not-escape")
		writeJSON(w, http.StatusOK, map[string]string{"value": "must-not-escape"})
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/slow-read", nil))

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("post-handler pin change was not refused: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "must-not-escape") || rec.Header().Get("X-Stale-Handler") != "" {
		t.Fatalf("buffered stale REST response escaped: headers=%v body=%s", rec.Header(), rec.Body.String())
	}
}

func TestBuildGuardReplacesMutationErrorAfterHandlerPinChange(t *testing.T) {
	status := currentBuildStatus()
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	s := &Server{mux: http.NewServeMux(), build: provider}
	s.mux.HandleFunc("POST /v1/failing-mutation", func(w http.ResponseWriter, _ *http.Request) {
		status = mismatchedBuildStatus()
		w.Header().Set("X-Stale-Handler", "must-not-escape")
		writeAPIError(w, http.StatusInternalServerError, "must_not_escape", "must-not-escape")
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/failing-mutation", strings.NewReader(`{}`)))

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("pin change did not replace mutation error: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "must-not-escape") || rec.Header().Get("X-Stale-Handler") != "" {
		t.Fatalf("stale mutation error escaped: headers=%v body=%s", rec.Header(), rec.Body.String())
	}
}

func TestOAuthAuthorizeOnlyPreservesCommittedGETResponses(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/oauth/authorize?request_id=test", nil)
	response := newBuildGuardResponse()
	response.WriteHeader(http.StatusAccepted)
	if completesAdmittedMutation(req, response) {
		t.Fatal("read-only pending continuation was classified as a committed mutation")
	}
	markAdmittedMutationResponse(response)
	if !completesAdmittedMutation(req, response) {
		t.Fatal("committed OAuth authorization response was not preserved")
	}

	ordinaryRead := httptest.NewRequest(http.MethodGet, "/v1/feed", nil)
	if completesAdmittedMutation(ordinaryRead, response) {
		t.Fatal("ordinary GET inherited the OAuth mutation exception")
	}
}

func TestBuildGuardResponseBufferFailsClosedAtDeterministicCap(t *testing.T) {
	provider := buildguard.ProviderFunc(func() buildguard.Status { return currentBuildStatus() })
	s := &Server{mux: http.NewServeMux(), build: provider}
	chunk := bytes.Repeat([]byte{'x'}, safety.MaxBufferedAuthoritativeResponseBytes/2+1)
	s.mux.HandleFunc("GET /v1/oversized", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Oversized-Handler", "must-not-escape")
		_, _ = w.Write(chunk)
		_, _ = w.Write(chunk)
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/oversized", nil))

	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"code":"response_too_large"`) {
		t.Fatalf("oversized response did not fail closed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() >= safety.MaxBufferedAuthoritativeResponseBytes || rec.Header().Get("X-Oversized-Handler") != "" {
		t.Fatalf("oversized handler response escaped: bytes=%d headers=%v", rec.Body.Len(), rec.Header())
	}

	buffered := newBuildGuardResponse()
	_, _ = buffered.Write(chunk)
	_, _ = buffered.Write(chunk)
	if !buffered.overflow || buffered.body.Len() != 0 {
		t.Fatalf("overflow buffer retained response bytes: overflow=%t bytes=%d", buffered.overflow, buffered.body.Len())
	}
}

func TestIdempotencyRecorderFailsClosedAtDeterministicCap(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	tokenResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "idempotency-response-cap", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	chunk := bytes.Repeat([]byte{'x'}, safety.MaxBufferedAuthoritativeResponseBytes/2+1)
	handler := idempotency.NewMiddleware(pool, writer).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Oversized-Handler", "must-not-escape")
		_, _ = w.Write(chunk)
		_, _ = w.Write(chunk)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/oversized-mutation", strings.NewReader(`{}`))
	req.Header.Set("Idempotency-Key", "oversized-mutation")
	req = req.WithContext(auth.WithToken(req.Context(), tokenResult.Token))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"code":"response_too_large"`) {
		t.Fatalf("idempotency recorder did not fail closed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() >= safety.MaxBufferedAuthoritativeResponseBytes || rec.Header().Get("X-Oversized-Handler") != "" {
		t.Fatalf("oversized idempotent response escaped: bytes=%d headers=%v", rec.Body.Len(), rec.Header())
	}
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 0)
}

func TestBuildGuardReadinessCurrentIntegration(t *testing.T) {
	pool := newIntegrationPool(t)
	provider := buildguard.ProviderFunc(func() buildguard.Status { return currentBuildStatus() })
	s := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("current build readiness = %d body=%s", rec.Code, rec.Body.String())
	}
	assertReadinessBuildFields(t, rec, buildguard.StateCurrent, apiBuildCommitA, apiBuildCommitA)
}

func TestBuildGuardRechecksBeforeCachedIdempotencyResponse(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "build-guard-idempotency", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	body := signalRequestBody(t, "build-guard-idempotency-replay")
	const key = "build-guard-idempotency-replay"
	prime := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(),
		buildguard.ProviderFunc(func() buildguard.Status { return currentBuildStatus() }))
	first := postSignal(t, prime.Handler(), tokenResult.Secret, key, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("prime response = %d body=%s", first.Code, first.Body.String())
	}

	armed := false
	statusCalls := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		if !armed {
			return currentBuildStatus()
		}
		statusCalls++
		// Request entry and idempotency entry are current. The check directly
		// before the cached response observes the pin advance.
		if statusCalls <= 2 {
			return currentBuildStatus()
		}
		return mismatchedBuildStatus()
	})
	replayServer := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)
	armed = true
	replay := postSignal(t, replayServer.Handler(), tokenResult.Secret, key, body)

	if replay.Code != http.StatusServiceUnavailable || !strings.Contains(replay.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("cached response escaped after pin change: status=%d body=%s calls=%d", replay.Code, replay.Body.String(), statusCalls)
	}
	if replay.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("blocked replay retained idempotency response header: %v", replay.Header())
	}
	if statusCalls < 3 {
		t.Fatalf("idempotency response was not dynamically guarded: calls=%d", statusCalls)
	}
}

func TestBuildGuardFeedReadRechecksBeforeResponse(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "build-guard-feed", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "snapshot", path: "/v1/feed"},
		{name: "long-poll", path: "/v1/feed?cursor=" + feed.EncodeCursor(0) + "&wait=0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statusCalls := 0
			provider := buildguard.ProviderFunc(func() buildguard.Status {
				statusCalls++
				if statusCalls == 1 {
					return currentBuildStatus()
				}
				return mismatchedBuildStatus()
			})
			s := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
			rec := httptest.NewRecorder()

			s.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
				t.Fatalf("post-read pin change was not refused: status=%d body=%s calls=%d", rec.Code, rec.Body.String(), statusCalls)
			}
			if strings.Contains(rec.Body.String(), `"items"`) {
				t.Fatalf("feed payload escaped after pin change: %s", rec.Body.String())
			}
			if statusCalls < 2 {
				t.Fatalf("build status was not rechecked after feed read: calls=%d", statusCalls)
			}
		})
	}
}

func TestBuildGuardSSEPinChangeAfterTailSuppressesNextFrame(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "build-guard-sse", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	statusCalls := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		statusCalls++
		if statusCalls <= 2 {
			// The preflight check permits response headers, then the first loop
			// check permits the database tail. The third check immediately before
			// the first frame observes the new pin.
			return currentBuildStatus()
		}
		return mismatchedBuildStatus()
	})
	s := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)
	req := httptest.NewRequest(http.MethodGet, "/v1/feed/stream?cursor="+feed.EncodeCursor(0), nil)
	req = req.WithContext(auth.WithToken(req.Context(), tokenResult.Token))
	rec := httptest.NewRecorder()

	// Invoke the handler directly so the first status read is the loop's
	// pre-tail check. It returns synchronously when the post-tail check blocks;
	// no timing or polling sleep is involved.
	s.handleFeedStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("stream did not open before pin changed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("SSE frame escaped after post-tail pin change: %q", rec.Body.String())
	}
	if statusCalls != 3 {
		t.Fatalf("SSE loop did not stop at post-tail recheck: status calls=%d, want 3", statusCalls)
	}
}

func TestBuildGuardSSEPinChangeDuringPreflightReturnsBuildPin(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "build-guard-sse-preflight", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	statusCalls := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		statusCalls++
		if statusCalls == 1 {
			return currentBuildStatus()
		}
		return mismatchedBuildStatus()
	})
	s := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)
	req := httptest.NewRequest(http.MethodGet, "/v1/feed/stream?cursor="+feed.EncodeCursor(0), nil)
	req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("SSE preflight pin change was not refused: status=%d body=%s calls=%d", rec.Code, rec.Body.String(), statusCalls)
	}
	if rec.Header().Get("Content-Type") == "text/event-stream; charset=utf-8" {
		t.Fatalf("SSE headers committed before preflight pin check: %v", rec.Header())
	}
}

func TestBuildGuardSSEPinChangeDuringAuthenticationSuppressesAuthError(t *testing.T) {
	status := currentBuildStatus()
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	s := &Server{
		mux:   http.NewServeMux(),
		build: provider,
	}
	s.authMiddleware = auth.NewMiddleware(pinFlipAuthenticator{flip: func() {
		status = mismatchedBuildStatus()
	}})
	s.mux.Handle("GET /v1/feed/stream", s.protected(http.HandlerFunc(s.handleFeedStream)))

	req := httptest.NewRequest(http.MethodGet, "/v1/feed/stream", nil)
	req.Header.Set("Authorization", "Bearer synthetic")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"build_pin"`) {
		t.Fatalf("SSE auth-boundary pin change was not refused: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "invalid_bearer_token") || rec.Header().Get("Content-Type") == "text/event-stream; charset=utf-8" {
		t.Fatalf("stale authentication response escaped: headers=%v body=%s", rec.Header(), rec.Body.String())
	}
}

func assertReadinessBuildFields(t *testing.T, rec *httptest.ResponseRecorder, state buildguard.State, compiled, pinned string) {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode readiness: %v", err)
	}
	if body["build_state"] != string(state) || body["build_compiled_commit"] != compiled || body["build_pinned_commit"] != pinned {
		t.Fatalf("readiness build fields = %+v", body)
	}
}
