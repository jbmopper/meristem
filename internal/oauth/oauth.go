// Package oauth implements the provider-facing OAuth surface for the
// meristem MCP gateway: dynamic client registration (RFC 7591) and, on top of
// it, the authorization-code + PKCE flow that lets vanilla providers (Claude,
// ChatGPT) register and obtain an access token scoped to /mcp.
//
// Truth is event-backed. oauth_client.registered events fold into the
// oauth_clients projection; the authorization and token endpoints read that
// projection to validate a client and its redirect_uri. Provider clients are
// public clients (RFC 8252): they authenticate with PKCE, never a client
// secret, so no secret material is ever issued, stored, or logged here.
package oauth

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

// subjectNamespace is a fixed v5 namespace so ClientSubjectID is pure and
// stable across processes and rebuilds.
var subjectNamespace = uuid.MustParse("a1c4e7d2-9b30-5f18-8e6a-2d4b6c8f0a13")

// ClientSubjectID derives the deterministic event subject id for an OAuth
// client from its non-secret client_id. Every event about one client shares
// this subject; the projection keys on the client_id text.
func ClientSubjectID(clientID string) uuid.UUID {
	return uuid.NewSHA1(subjectNamespace, []byte("oauth_client|"+clientID))
}

// AuthMethodNone is the only token-endpoint auth method accepted for provider
// registration: public clients authenticating with PKCE. It matches what the
// authorization-server metadata advertises
// (token_endpoint_auth_methods_supported: ["none"]).
const AuthMethodNone = "none"

// GrantAuthorizationCode and ResponseTypeCode are the only grant/response
// types the gateway supports, matching the advertised metadata.
const (
	GrantAuthorizationCode = "authorization_code"
	GrantRefreshToken      = "refresh_token"
	ResponseTypeCode       = "code"
)

// MaxRedirectURIs caps how many redirect URIs a single client may register,
// bounding projection row size and the authorize-time allowlist scan.
const MaxRedirectURIs = 10
const MaxRedirectURILength = 2048
const MaxClientNameLength = 256
const MaxScopeLength = 256

// registeredPayload is the field-minimal structural payload of an
// oauth_client.registered event. Every field here is read by deterministic
// code (the projector and the authorize endpoint); no secrets appear.
type registeredPayload struct {
	PayloadVersion          int      `json:"payload_version"`
	ClientID                string   `json:"client_id"`
	ClientName              string   `json:"client_name,omitempty"`
	RedirectURIs            []string `json:"redirect_uris"`
	GrantTypes              []string `json:"grant_types"`
	ResponseTypes           []string `json:"response_types"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	Scope                   string   `json:"scope,omitempty"`
}

// generateClientID returns a non-secret random client identifier. It is a
// public value echoed back to the client and stored in the projection.
func generateClientID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("oauth: generate client_id: %w", err)
	}
	return "mcpc_" + hex.EncodeToString(b[:]), nil
}

// validateRedirectURIs enforces the redirect_uri safety rules RFC 7591/8252
// require of a public client: at least one URI, each an absolute URL with no
// fragment, and either https or an http loopback (127.0.0.1 / [::1] /
// localhost) for native/CLI redirect. Anything else is rejected so a malformed
// or open-redirect-prone URI can never enter the allowlist.
func validateRedirectURIs(uris []string) ([]string, error) {
	if len(uris) == 0 {
		return nil, fmt.Errorf("%w: at least one redirect_uri is required", ErrInvalidRegistration)
	}
	if len(uris) > MaxRedirectURIs {
		return nil, fmt.Errorf("%w: at most %d redirect_uris allowed", ErrInvalidRegistration, MaxRedirectURIs)
	}
	out := make([]string, 0, len(uris))
	for _, raw := range uris {
		if len(raw) > MaxRedirectURILength {
			return nil, fmt.Errorf("%w: redirect_uri exceeds %d bytes", ErrInvalidRegistration, MaxRedirectURILength)
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("%w: empty redirect_uri", ErrInvalidRegistration)
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: redirect_uri %q is not a valid URL", ErrInvalidRegistration, raw)
		}
		if !u.IsAbs() || u.Host == "" {
			return nil, fmt.Errorf("%w: redirect_uri %q must be an absolute URL", ErrInvalidRegistration, raw)
		}
		if u.Fragment != "" || strings.Contains(raw, "#") {
			return nil, fmt.Errorf("%w: redirect_uri %q must not contain a fragment", ErrInvalidRegistration, raw)
		}
		switch u.Scheme {
		case "https":
		case "http":
			if !isLoopbackHost(u.Hostname()) {
				return nil, fmt.Errorf("%w: http redirect_uri %q is only allowed for loopback hosts", ErrInvalidRegistration, raw)
			}
		default:
			return nil, fmt.Errorf("%w: redirect_uri %q must use https (or http loopback)", ErrInvalidRegistration, raw)
		}
		out = append(out, raw)
	}
	return out, nil
}

func isLoopbackHost(host string) bool {
	switch host {
	case "127.0.0.1", "::1", "localhost":
		return true
	default:
		return false
	}
}

// payloadVersion reads the payload_version field, treating absence as 1 per
// docs/payload-versioning.md.
func payloadVersion(raw any) int {
	b, err := json.Marshal(raw)
	if err != nil {
		return 1
	}
	var probe struct {
		PayloadVersion int `json:"payload_version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil || probe.PayloadVersion == 0 {
		return 1
	}
	return probe.PayloadVersion
}

func decode(raw any, dst any) error {
	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
