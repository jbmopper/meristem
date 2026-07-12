package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/jbmopper/meristem/internal/oauth"
)

// oauthRegistrationRequest is the RFC 7591 dynamic client registration request
// subset the gateway accepts. Unknown fields are ignored; only public clients
// (token_endpoint_auth_method "none" / omitted) using PKCE are supported.
type oauthRegistrationRequest struct {
	ClientName              string   `json:"client_name"`
	RedirectURIs            []string `json:"redirect_uris"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	Scope                   string   `json:"scope"`
	GrantTypes              []string `json:"grant_types"`
	ResponseTypes           []string `json:"response_types"`
}

// handleOAuthClientRegistration implements RFC 7591 dynamic client
// registration for provider MCP clients. It is intentionally unauthenticated:
// public providers (Claude, ChatGPT) self-register before any owner consent,
// and no secret is issued — the client_id is a non-secret identifier and PKCE
// binds the later authorization-code exchange.
func (s *Server) handleOAuthClientRegistration(w http.ResponseWriter, r *http.Request) {
	if s.oauthClients == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth registration is not configured")
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

	var req oauthRegistrationRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			// RFC 7591 §3.2.2 uses invalid_client_metadata for a bad request body.
			writeOAuthRegistrationError(w, http.StatusBadRequest, "invalid_client_metadata", "request body is not valid JSON")
			return
		}
	}

	// grant_types/response_types, when supplied, must be consistent with the
	// only flow the gateway supports; anything else is rejected rather than
	// silently narrowed, so a client learns its expectation is unmet.
	if !grantTypesSupported(req.GrantTypes) {
		writeOAuthRegistrationError(w, http.StatusBadRequest, "invalid_client_metadata", "only the authorization_code grant is supported")
		return
	}
	if !responseTypesSupported(req.ResponseTypes) {
		writeOAuthRegistrationError(w, http.StatusBadRequest, "invalid_client_metadata", "only the code response_type is supported")
		return
	}

	registered, err := s.oauthClients.Register(r.Context(), oauth.RegisterInput{
		ClientName:              req.ClientName,
		RedirectURIs:            req.RedirectURIs,
		TokenEndpointAuthMethod: req.TokenEndpointAuthMethod,
		Scope:                   req.Scope,
	})
	if err != nil {
		if errors.Is(err, oauth.ErrInvalidRegistration) {
			// A bad redirect_uri is invalid_redirect_uri; other metadata is
			// invalid_client_metadata. Both are 400 per RFC 7591 §3.2.2.
			code := "invalid_client_metadata"
			if isRedirectURIError(err) {
				code = "invalid_redirect_uri"
			}
			writeOAuthRegistrationError(w, http.StatusBadRequest, code, err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "oauth_registration_failed", "could not register client")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"client_id":                  registered.ClientID,
		"client_name":                registered.ClientName,
		"redirect_uris":              registered.RedirectURIs,
		"grant_types":                registered.GrantTypes,
		"response_types":             registered.ResponseTypes,
		"token_endpoint_auth_method": registered.TokenEndpointAuthMethod,
		"scope":                      registered.Scope,
	})
}

// grantTypesSupported accepts an omitted list (the server returns its
// authorization_code + refresh_token contract) or that exact supported set.
func grantTypesSupported(grants []string) bool {
	if len(grants) == 0 {
		return true
	}
	seen := make(map[string]bool, len(grants))
	for _, g := range grants {
		if seen[g] || (g != oauth.GrantAuthorizationCode && g != oauth.GrantRefreshToken) {
			return false
		}
		seen[g] = true
	}
	return seen[oauth.GrantAuthorizationCode] && seen[oauth.GrantRefreshToken]
}

func responseTypesSupported(responses []string) bool {
	for _, rt := range responses {
		if rt != oauth.ResponseTypeCode {
			return false
		}
	}
	return true
}

func isRedirectURIError(err error) bool {
	return err != nil && containsRedirectURI(err.Error())
}

func containsRedirectURI(msg string) bool {
	// The registration validator prefixes redirect-uri failures with
	// "redirect_uri"; use that to pick the RFC 7591 error code.
	const marker = "redirect_uri"
	for i := 0; i+len(marker) <= len(msg); i++ {
		if msg[i:i+len(marker)] == marker {
			return true
		}
	}
	return false
}

func writeOAuthRegistrationError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]any{
		"error":             code,
		"error_description": description,
	})
}
