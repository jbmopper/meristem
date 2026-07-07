package api

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// EnvPublicBaseURL pins the externally reachable scheme and host used in
// provider OAuth metadata. When unset, API requests derive it from forwarded
// proxy headers and the request host.
const EnvPublicBaseURL = "MERISTEM_PUBLIC_BASE_URL"

const mcpReadScope = "mcp:read"

func (s *Server) handleOAuthProtectedResourceMetadata(w http.ResponseWriter, r *http.Request) {
	base := s.oauthPublicBaseURL(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"resource":                 base + "/mcp",
		"authorization_servers":    []string{base},
		"scopes_supported":         []string{mcpReadScope},
		"bearer_methods_supported": []string{"header"},
		"resource_name":            "meristem MCP",
	})
}

func (s *Server) handleOAuthAuthorizationServerMetadata(w http.ResponseWriter, r *http.Request) {
	base := s.oauthPublicBaseURL(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                base,
		"authorization_endpoint":                base + "/oauth/authorize",
		"token_endpoint":                        base + "/oauth/token",
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none"},
		"scopes_supported":                      []string{mcpReadScope},
	})
}

func (s *Server) handleOAuthFlowUnavailable(w http.ResponseWriter, _ *http.Request) {
	writeAPIError(w, http.StatusServiceUnavailable, "oauth_flow_unavailable", "oauth authorization flow is not implemented yet")
}

func (s *Server) writeMCPAuthError(w http.ResponseWriter, r *http.Request, code, message, oauthError string) {
	w.Header().Set("WWW-Authenticate", s.mcpBearerChallenge(r, oauthError))
	writeAPIError(w, http.StatusUnauthorized, code, message)
}

func (s *Server) mcpBearerChallenge(r *http.Request, oauthError string) string {
	params := make([]string, 0, 2)
	if oauthError != "" {
		params = append(params, "error="+strconv.Quote(oauthError))
	}
	params = append(params, "resource_metadata="+strconv.Quote(s.oauthPublicBaseURL(r)+"/.well-known/oauth-protected-resource/mcp"))
	return "Bearer " + strings.Join(params, ", ")
}

func (s *Server) oauthPublicBaseURL(r *http.Request) string {
	if s.publicBaseURL != "" {
		return s.publicBaseURL
	}
	return requestPublicBaseURL(r)
}

func normalizePublicBaseURL(raw string) string {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

func requestPublicBaseURL(r *http.Request) string {
	host := firstForwardedValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	if host == "" {
		host = "127.0.0.1"
	}

	scheme := firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))
	if scheme != "http" && scheme != "https" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + host
}

func firstForwardedValue(raw string) string {
	if raw == "" {
		return ""
	}
	first, _, _ := strings.Cut(raw, ",")
	return strings.TrimSpace(first)
}
