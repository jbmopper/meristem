package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOAuthResponsesAreNotCacheable(t *testing.T) {
	s := &Server{}
	for name, tc := range map[string]struct {
		h http.HandlerFunc
		r *http.Request
	}{
		"token":     {s.handleOAuthToken, httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader("grant_type=authorization_code"))},
		"authorize": {s.handleOAuthAuthorize, httptest.NewRequest(http.MethodGet, "/oauth/authorize", nil)},
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.h(rec, tc.r)
			if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
				t.Fatalf("cache headers=%v", rec.Header())
			}
		})
	}
}
