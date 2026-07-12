package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

type oauthRuntimeMode uint8

const (
	// oauthRuntimeUnmanaged is the zero value reserved for package tests that
	// construct Server literals. Production construction always resolves an
	// explicit disabled, enabled, or invalid state in NewWithPolicy.
	oauthRuntimeUnmanaged oauthRuntimeMode = iota
	oauthRuntimeDisabled
	oauthRuntimeEnabled
	oauthRuntimeInvalid
)

var (
	errOAuthRuntimeDisabled = errors.New("oauth runtime is disabled")
	errOAuthRuntimeInvalid  = errors.New("oauth runtime configuration is invalid")
	errOAuthSystemActor     = errors.New("oauth system actor is unavailable")
)

type oauthRuntimeConfig struct {
	mode          oauthRuntimeMode
	publicBaseURL string
	systemActorID uuid.UUID
}

// oauthActorLookup is an explicit test seam as well as the narrow dependency
// used by readiness and the public OAuth route gate. Production uses
// auth.Service.Get; tests need not construct a real pool merely to exercise
// configuration behavior.
type oauthActorLookup func(context.Context, uuid.UUID) (domain.Token, error)

func resolveOAuthRuntimeConfig(rawBaseURL, rawSystemActorID string) oauthRuntimeConfig {
	baseRaw := strings.TrimSpace(rawBaseURL)
	actorRaw := strings.TrimSpace(rawSystemActorID)
	if baseRaw == "" && actorRaw == "" {
		return oauthRuntimeConfig{mode: oauthRuntimeDisabled}
	}
	if baseRaw == "" || actorRaw == "" {
		return oauthRuntimeConfig{mode: oauthRuntimeInvalid}
	}

	base, ok := explicitHTTPSBaseURL(baseRaw)
	if !ok {
		return oauthRuntimeConfig{mode: oauthRuntimeInvalid}
	}
	actorID, err := uuid.Parse(actorRaw)
	if err != nil || actorID == uuid.Nil {
		return oauthRuntimeConfig{mode: oauthRuntimeInvalid}
	}
	return oauthRuntimeConfig{
		mode:          oauthRuntimeEnabled,
		publicBaseURL: base,
		systemActorID: actorID,
	}
}

func explicitHTTPSBaseURL(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", false
	}
	// The public issuer is an exact operator-set origin/base path. A trailing
	// slash is harmless and normalized; all other components are preserved.
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = strings.TrimRight(u.RawPath, "/")
	return strings.TrimRight(u.String(), "/"), true
}

func (s *Server) checkOAuthRuntime(ctx context.Context) error {
	switch s.oauthRuntime.mode {
	case oauthRuntimeUnmanaged:
		return nil
	case oauthRuntimeDisabled:
		return errOAuthRuntimeDisabled
	case oauthRuntimeInvalid:
		return errOAuthRuntimeInvalid
	case oauthRuntimeEnabled:
		if s.oauthActorLookup == nil {
			return errOAuthSystemActor
		}
		tok, err := s.oauthActorLookup(ctx, s.oauthRuntime.systemActorID)
		if err != nil || tok.ID != s.oauthRuntime.systemActorID || tok.RevokedAt != nil || tok.IsRoot || tok.Source != domain.SourceSystem {
			return errOAuthSystemActor
		}
		return nil
	default:
		return errOAuthRuntimeInvalid
	}
}

func (s *Server) oauthPublicRoute(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.checkOAuthRuntime(r.Context()); err != nil {
			// All disabled/configuration/actor failures use the same public
			// response. Discovery and DCR must not expose deployment details.
			writeAPIError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth is unavailable")
			return
		}
		next(w, r)
	}
}

// oauthAccessRoute keeps local/static bearer MCP available while ensuring an
// OAuth access token cannot outlive the runtime enable switch or the dedicated
// system actor. It runs before mcpProtected so a disabled runtime never asks
// the OAuth token service to authenticate an old grant.
func (s *Server) oauthAccessRoute(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		secret := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
		if strings.HasPrefix(secret, "mcpat_") {
			if err := s.checkOAuthRuntime(r.Context()); err != nil {
				writeAPIError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth is unavailable")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
