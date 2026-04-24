package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestLivenessNeedsNoDatabase asserts that the liveness probe answers 200
// even when no Postgres pool is attached. This is deliberate: a flaky DB
// must not cause an orchestrator to kill the process and lose work.
func TestLivenessNeedsNoDatabase(t *testing.T) {
	s := New(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: want 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("healthz: unexpected content type %q", got)
	}
}
