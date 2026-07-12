package api

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/mcp"
)

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
		w.Header().Set("Allow", "POST, GET")
		writeAPIError(w, http.StatusMethodNotAllowed, "mcp_sse_unavailable", "mcp server-initiated SSE is not implemented")
		return
	case http.MethodPost:
		s.handleMCPPost(w, r, actor)
	default:
		w.Header().Set("Allow", "POST, GET")
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
	resp := s.mcpServer.HandleHTTPMessageWithOptions(r.Context(), raw, actor, mcp.HTTPOptions{
		AllowedTools: mcp.ReadOnlyHTTPTools(),
	})
	w.Header().Set(mcp.HeaderProtocolVersion, "2025-06-18")
	if resp.ContentType != "" {
		w.Header().Set("Content-Type", resp.ContentType)
	}
	w.WriteHeader(resp.Status)
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}
