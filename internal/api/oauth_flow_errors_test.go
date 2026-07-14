package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/oauth"
)

func TestOAuthTypedErrorsDoNotExposeInternalFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		write      func(http.ResponseWriter, error)
		err        error
		wantStatus int
		wantCode   string
	}{
		{"authorization validation", writeOAuthAuthorizationError, oauth.ErrInvalidAuthorizationRequest, http.StatusBadRequest, "invalid_request"},
		{"authorization internal", writeOAuthAuthorizationError, errors.New("database password secret"), http.StatusInternalServerError, "server_error"},
		{"token validation", writeOAuthTokenError, oauth.ErrInvalidGrant, http.StatusBadRequest, "invalid_grant"},
		{"token internal", writeOAuthTokenError, errors.New("database password secret"), http.StatusInternalServerError, "server_error"},
		{"admin validation", writeOAuthClientAdminError, oauth.ErrInvalidClientAdminInput, http.StatusBadRequest, "invalid_oauth_client_admin_request"},
		{"admin grant not found", writeOAuthClientAdminError, oauth.ErrGrantNotFound, http.StatusNotFound, "oauth_grant_not_found"},
		{"admin internal", writeOAuthClientAdminError, errors.New("database password secret"), http.StatusInternalServerError, "oauth_client_admin_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.write(rec, tc.err)
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantCode) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if tc.wantStatus >= 500 && strings.Contains(rec.Body.String(), "password secret") {
				t.Fatalf("internal error leaked: %s", rec.Body.String())
			}
		})
	}
}

func TestOAuthClientAdminHTTPRejectsRootAndAgent(t *testing.T) {
	for _, actor := range []domain.Token{
		{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind}},
		{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeOAuthClientsBind}},
	} {
		s := New(nil, nil)
		req := httptest.NewRequest(http.MethodPost, "/v1/oauth/clients/example/actor", strings.NewReader(`{"actor_token_id":"`+uuid.NewString()+`","authority_profile":"owner_tracker_read_v1"}`))
		req.SetPathValue("client_id", "example")
		req = req.WithContext(auth.WithToken(req.Context(), actor))
		rec := httptest.NewRecorder()
		s.handleOAuthBindActor(rec, req)
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "oauth_client_admin_denied") {
			t.Fatalf("actor=%+v status=%d body=%s", actor, rec.Code, rec.Body.String())
		}
	}
}

func TestOAuthGrantRevokeHTTPRejectsRootAndAgent(t *testing.T) {
	for _, actor := range []domain.Token{
		{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsRevoke}},
		{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeOAuthClientsRevoke}},
	} {
		s := New(nil, nil)
		req := httptest.NewRequest(http.MethodPost, "/v1/oauth/grants/"+uuid.NewString()+"/revoke", strings.NewReader(`{"reason":"compromised"}`))
		req.SetPathValue("grant_id", uuid.NewString())
		req = req.WithContext(auth.WithToken(req.Context(), actor))
		rec := httptest.NewRecorder()
		s.handleOAuthRevokeGrant(rec, req)
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "oauth_client_admin_denied") {
			t.Fatalf("actor=%+v status=%d body=%s", actor, rec.Code, rec.Body.String())
		}
	}
}
