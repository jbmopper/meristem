package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/oauth"
)

func (s *Server) handleOAuthAuthorize(w http.ResponseWriter, r *http.Request) {
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
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", err.Error())
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
		code := "invalid_request"
		if errors.Is(err, oauth.ErrProviderActorUnavailable) {
			code = "temporarily_unavailable"
		}
		writeOAuthError(w, http.StatusBadRequest, code, err.Error())
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
	if s.oauthTokens == nil {
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "oauth token service is not configured")
		return
	}
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form body")
		return
	}
	var pair oauth.TokenPair
	var err error
	switch r.Form.Get("grant_type") {
	case oauth.GrantAuthorizationCode:
		pair, err = s.oauthTokens.ExchangeCode(r.Context(), oauth.RedeemInput{Code: r.Form.Get("code"), ClientID: r.Form.Get("client_id"), RedirectURI: r.Form.Get("redirect_uri"), CodeVerifier: r.Form.Get("code_verifier")})
	case "refresh_token":
		pair, err = s.oauthTokens.Refresh(r.Context(), r.Form.Get("refresh_token"), r.Form.Get("client_id"))
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type is unsupported")
		return
	}
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"access_token": pair.AccessToken, "refresh_token": pair.RefreshToken, "token_type": pair.TokenType, "expires_in": pair.ExpiresIn, "scope": pair.Scope})
}

func writeOAuthError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]any{"error": code, "error_description": description})
}

type bindActorRequest struct {
	ActorTokenID     uuid.UUID `json:"actor_token_id"`
	AuthorityProfile string    `json:"authority_profile"`
}

func (s *Server) handleOAuthBindActor(w http.ResponseWriter, r *http.Request) {
	tok, ok := auth.TokenFromContext(r.Context())
	if !ok || !tok.IsRoot {
		writeAPIError(w, http.StatusForbidden, "root_required", "root token required")
		return
	}
	var req bindActorRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	clientID := r.PathValue("client_id")
	if err := s.oauthClientAdmin.BindActor(r.Context(), clientID, req.ActorTokenID, req.AuthorityProfile, tok); err != nil {
		writeAPIError(w, http.StatusBadRequest, "oauth_actor_binding_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"client_id": clientID, "actor_token_id": req.ActorTokenID, "authority_profile": req.AuthorityProfile})
}

type revokeClientRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleOAuthRevokeClient(w http.ResponseWriter, r *http.Request) {
	tok, ok := auth.TokenFromContext(r.Context())
	if !ok || !tok.IsRoot {
		writeAPIError(w, http.StatusForbidden, "root_required", "root token required")
		return
	}
	var req revokeClientRequest
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, s.policy.MaxRequestBodyBytes)).Decode(&req)
	clientID := r.PathValue("client_id")
	if err := s.oauthClientAdmin.Revoke(r.Context(), clientID, req.Reason, tok); err != nil {
		writeAPIError(w, http.StatusBadRequest, "oauth_client_revoke_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var _ = fmt.Sprintf
