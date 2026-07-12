package api

import (
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/oauth"
)

func (s *Server) handleOAuthAuthorize(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if s.oauthAuthorization == nil {
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "oauth authorization is not configured")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("request_id")); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "request_id is invalid")
			return
		}
		result, err := s.oauthAuthorization.Continue(r.Context(), id)
		if err != nil {
			writeOAuthAuthorizationError(w, err)
			return
		}
		if result.Pending {
			s.writeOAuthPending(w, r, id, result.WorkItemID)
			return
		}
		u, err := url.Parse(result.RedirectURI)
		if err != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "stored redirect is invalid")
			return
		}
		q := u.Query()
		if result.OAuthError != "" {
			q.Set("error", result.OAuthError)
		} else {
			q.Set("code", result.Code)
		}
		if result.State != "" {
			q.Set("state", result.State)
		}
		u.RawQuery = q.Encode()
		http.Redirect(w, r, u.String(), http.StatusFound)
		return
	}
	q := r.URL.Query()
	result, err := s.oauthAuthorization.Begin(r.Context(), oauth.AuthorizationInput{ClientID: q.Get("client_id"), RedirectURI: q.Get("redirect_uri"), ResponseType: q.Get("response_type"), State: q.Get("state"), CodeChallenge: q.Get("code_challenge"), CodeChallengeMethod: q.Get("code_challenge_method"), Scope: q.Get("scope"), Resource: q.Get("resource"), ExpectedResource: s.oauthPublicBaseURL(r) + "/mcp"})
	if err != nil {
		writeOAuthAuthorizationError(w, err)
		return
	}
	s.writeOAuthPending(w, r, result.ID, result.WorkItemID)
}

var pendingTemplate = template.Must(template.New("oauth-pending").Parse(`<!doctype html><html><head><meta charset="utf-8"><title>Meristem authorization pending</title></head><body><h1>Authorization pending</h1><p>Approve or deny work item <code>{{.WorkItem}}</code> from a trusted Meristem owner client, then continue.</p><p><a href="{{.Continue}}">Continue authorization</a></p></body></html>`))

func (s *Server) writeOAuthPending(w http.ResponseWriter, r *http.Request, id, workItem uuid.UUID) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_ = pendingTemplate.Execute(w, map[string]string{"WorkItem": workItem.String(), "Continue": "/oauth/authorize?request_id=" + url.QueryEscape(id.String())})
}

func (s *Server) handleOAuthToken(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if s.oauthTokens == nil {
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "oauth token service is not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)
	if mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0])); mediaType != "application/x-www-form-urlencoded" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "token request must use application/x-www-form-urlencoded body")
		return
	}
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form body")
		return
	}
	var pair oauth.TokenPair
	var err error
	switch r.PostForm.Get("grant_type") {
	case oauth.GrantAuthorizationCode:
		pair, err = s.oauthTokens.ExchangeCode(r.Context(), oauth.RedeemInput{Code: r.PostForm.Get("code"), ClientID: r.PostForm.Get("client_id"), RedirectURI: r.PostForm.Get("redirect_uri"), CodeVerifier: r.PostForm.Get("code_verifier")})
	case "refresh_token":
		pair, err = s.oauthTokens.Refresh(r.Context(), r.PostForm.Get("refresh_token"), r.PostForm.Get("client_id"))
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type is unsupported")
		return
	}
	if err != nil {
		writeOAuthTokenError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"access_token": pair.AccessToken, "refresh_token": pair.RefreshToken, "token_type": pair.TokenType, "expires_in": pair.ExpiresIn, "scope": pair.Scope})
}

func writeOAuthError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]any{"error": code, "error_description": description})
}

func writeOAuthAuthorizationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, oauth.ErrInvalidAuthorizationRequest), errors.Is(err, oauth.ErrInvalidGrant):
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, oauth.ErrProviderActorUnavailable), errors.Is(err, oauth.ErrSystemActorUnavailable):
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "oauth authorization is temporarily unavailable")
	default:
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "oauth authorization failed")
	}
}

func writeOAuthTokenError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, oauth.ErrInvalidGrant), errors.Is(err, oauth.ErrRefreshReuse):
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "authorization grant is invalid")
	case errors.Is(err, oauth.ErrSystemActorUnavailable), errors.Is(err, oauth.ErrProviderActorUnavailable):
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "oauth token service is temporarily unavailable")
	default:
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "oauth token request failed")
	}
}

type bindActorRequest struct {
	ActorTokenID     uuid.UUID `json:"actor_token_id"`
	AuthorityProfile string    `json:"authority_profile"`
}

func (s *Server) handleOAuthBindActor(w http.ResponseWriter, r *http.Request) {
	tok, ok := auth.TokenFromContext(r.Context())
	if !ok || !access.CanBindOAuthClient(tok) {
		writeAPIError(w, http.StatusForbidden, "oauth_client_admin_denied", "explicit non-root human oauth_clients.bind scope required")
		return
	}
	if s.oauthClientAdmin == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth client administration is not configured")
		return
	}
	var req bindActorRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	clientID := r.PathValue("client_id")
	if err := s.oauthClientAdmin.BindActor(r.Context(), clientID, req.ActorTokenID, req.AuthorityProfile, tok); err != nil {
		writeOAuthClientAdminError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"client_id": clientID, "actor_token_id": req.ActorTokenID, "authority_profile": req.AuthorityProfile})
}

type revokeClientRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleOAuthRevokeClient(w http.ResponseWriter, r *http.Request) {
	tok, ok := auth.TokenFromContext(r.Context())
	if !ok || !access.CanRevokeOAuthClient(tok) {
		writeAPIError(w, http.StatusForbidden, "oauth_client_admin_denied", "explicit non-root human oauth_clients.revoke scope required")
		return
	}
	if s.oauthClientAdmin == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth client administration is not configured")
		return
	}
	var req revokeClientRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	clientID := r.PathValue("client_id")
	if err := s.oauthClientAdmin.Revoke(r.Context(), clientID, req.Reason, tok); err != nil {
		writeOAuthClientAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeOAuthClientAdminError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, oauth.ErrOAuthClientAdminDenied):
		writeAPIError(w, http.StatusForbidden, "oauth_client_admin_denied", "oauth client administration denied")
	case errors.Is(err, oauth.ErrClientNotFound):
		writeAPIError(w, http.StatusNotFound, "oauth_client_not_found", "oauth client not found")
	case errors.Is(err, oauth.ErrInvalidClientAdminInput):
		writeAPIError(w, http.StatusBadRequest, "invalid_oauth_client_admin_request", err.Error())
	case errors.Is(err, oauth.ErrOAuthClientConflict):
		writeAPIError(w, http.StatusConflict, "oauth_client_conflict", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "oauth_client_admin_failed", "oauth client administration failed")
	}
}
