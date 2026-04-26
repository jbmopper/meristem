package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/safety"
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

func TestDecodeJSONRequestRejectsOversizedBody(t *testing.T) {
	body := strings.NewReader(`{"text":"` + strings.Repeat("x", int(safety.DefaultPolicy().MaxRequestBodyBytes)) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/inbox/messages", body)
	rec := httptest.NewRecorder()

	var out struct {
		Text string `json:"text"`
	}
	if decodeJSONRequest(rec, req, &out) {
		t.Fatal("decodeJSONRequest succeeded for oversized body")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 for oversized body, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "request_too_large") {
		t.Fatalf("expected request_too_large error, got body=%s", rec.Body.String())
	}
}

func TestHandleFeedRejectsWaitAboveSafetyLimit(t *testing.T) {
	s := New(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/feed?wait=61s", nil)
	rec := httptest.NewRecorder()

	s.handleFeed(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for wait above safety limit, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "wait_too_large") {
		t.Fatalf("expected wait_too_large error, got body=%s", rec.Body.String())
	}
}
