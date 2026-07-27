package api

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/mcp"
)

// parseMCPAllowedOrigins builds the exact-match Origin allowlist from
// EnvMCPAllowedOrigins. Empty input yields an empty (deny-all-present) set.
func parseMCPAllowedOrigins(raw string) map[string]bool {
	allowed := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			allowed[part] = true
		}
	}
	return allowed
}

// mcpOriginGuard enforces 2026-07-28 Origin validation ahead of any
// credential handling: absent Origin is accepted (non-browser MCP clients
// send none); a present Origin must exactly match the configured allowlist or
// the request is rejected 403. There is deliberately no log-only mode.
func (s *Server) mcpOriginGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Presence, not value, is the gate: an explicitly empty Origin header
		// is PRESENT and must match the allowlist (it never can — the parser
		// drops empty entries), so only truly absent Origin passes freely.
		origin, present := headerValue(r, "Origin")
		if present && !s.mcpAllowedOrigins[origin] {
			writeAPIError(w, http.StatusForbidden, "origin_forbidden", "Origin is not allowed for /mcp")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleMCPDelete exists so DELETE gets the spec-required 405 (the modern
// transport defines no DELETE semantics; sessions do not exist to delete).
func handleMCPDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "POST")
	writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func (s *Server) mcpProtected(next http.Handler) http.Handler {
	if s.authenticator == nil {
		return serviceUnavailableHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" || !strings.HasPrefix(header, "Bearer ") {
			s.writeMCPAuthError(w, r, "missing_bearer_token", "missing bearer token", "")
			return
		}
		secret := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
		var tok domain.Token
		var err error
		if strings.HasPrefix(secret, "mcpat_") && s.oauthTokens != nil {
			tok, err = s.oauthTokens.AuthenticateAccess(r.Context(), secret, s.oauthPublicBaseURL(r)+"/mcp")
		} else {
			tok, err = s.authenticator.Authenticate(r.Context(), secret)
		}
		if err != nil {
			code := "invalid_bearer_token"
			message := "invalid bearer token"
			if errors.Is(err, auth.ErrTokenRevoked) {
				code = "token_revoked"
				message = "token revoked"
			}
			s.writeMCPAuthError(w, r, code, message, "invalid_token")
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithToken(r.Context(), tok)))
	})
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	if s.mcpServer == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "mcp_unavailable", "mcp server is not configured")
		return
	}
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Allow", "POST")
		writeAPIError(w, http.StatusMethodNotAllowed, "mcp_sse_unavailable", "mcp server-initiated SSE is not implemented")
		return
	case http.MethodPost:
		s.handleMCPPost(w, r, actor)
	default:
		w.Header().Set("Allow", "POST")
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
}

func (s *Server) handleMCPPost(w http.ResponseWriter, r *http.Request, actor domain.Token) {
	if !mcp.AcceptsStreamableHTTPPost(r.Header.Get("Accept")) {
		writeAPIError(w, http.StatusNotAcceptable, "invalid_accept", "mcp POST requires Accept: application/json, text/event-stream")
		return
	}
	defer func() { _ = r.Body.Close() }()
	body := http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)
	raw, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
			return
		}
		writeAPIError(w, http.StatusBadRequest, "request_read_failed", "could not read request body")
		return
	}
	profile, err := providerHTTPProfile(actor)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, "provider_authority_denied", "bearer token does not carry one exact sealed provider authority profile")
		return
	}
	protocolHeader, hasProtocolHeader := headerValue(r, mcp.HeaderProtocolVersion)
	mcpMethod, hasMcpMethod := headerValue(r, "Mcp-Method")
	mcpName, hasMcpName := headerValue(r, "Mcp-Name")
	resp := s.mcpServer.HandleHTTPMessageWithOptions(r.Context(), raw, actor, mcp.HTTPOptions{
		Profile: profile,
		Transport: &mcp.HTTPTransportContext{
			ProtocolVersion:    protocolHeader,
			HasProtocolVersion: hasProtocolHeader,
			McpMethod:          mcpMethod,
			HasMcpMethod:       hasMcpMethod,
			McpName:            mcpName,
			HasMcpName:         hasMcpName,
		},
	})
	responseVersion := resp.ProtocolVersion
	if responseVersion == "" {
		responseVersion = "2025-06-18"
	}
	w.Header().Set(mcp.HeaderProtocolVersion, responseVersion)
	if resp.ContentType != "" {
		w.Header().Set("Content-Type", resp.ContentType)
	}
	w.WriteHeader(resp.Status)
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}

// headerValue distinguishes an absent header from an empty one; the era
// classifier needs the difference for the required-header rules.
func headerValue(r *http.Request, name string) (string, bool) {
	values, ok := r.Header[http.CanonicalHeaderKey(name)]
	if !ok || len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func providerHTTPProfile(actor domain.Token) (*mcp.HTTPToolProfile, error) {
	profile, err := access.ProviderAuthorityProfileFromScopes(actor.Scopes)
	if err != nil {
		if access.HasProviderAuthorityMarker(actor.Scopes) {
			return nil, err
		}
		// Static internal/debug bearers predate provider profiles. Preserve
		// their provider-safe read-only HTTP surface; ordinary access policy
		// still filters tools and objects from the token's actual scopes.
		return mcp.ProviderSafeReadHTTPProfile(), nil
	}
	switch profile {
	case access.ProviderOwnerTrackerReadV1, access.ProviderDelegatedTreeReadV1:
		return mcp.ProviderSafeReadHTTPProfile(), nil
	case access.ProviderOwnerTrackerWriteV1, access.ProviderDelegatedTreeWriteV1:
		return mcp.ProviderTrackerHTTPProfile(), nil
	default:
		return nil, access.ErrInvalidProviderAuthority
	}
}
