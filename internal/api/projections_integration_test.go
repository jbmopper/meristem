package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestProjectionDefinitionFeedsAndCursorMismatchIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	authSvc := auth.NewService(pool, app.NewEventWriter())
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "projection-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	// Registry/projection defines deny root by design (access.ToolVisible);
	// the definer is an ordinary non-root human operator token.
	tokenResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "projection-api",
		Source: domain.SourceHuman,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create operator token: %v", err)
	}
	server := New(pool, nil)

	defineProjection(t, server.Handler(), tokenResult.Secret, "signal-view", `{
		"name":"signal-view",
		"version":1,
		"type":"feed",
		"filter":{"kinds":["signal.received"]},
		"description":"signals only"
	}`)
	defineProjection(t, server.Handler(), tokenResult.Secret, "work-view", `{
		"name":"work-view",
		"version":1,
		"type":"feed",
		"filter":{"kinds":["work_item.created"]},
		"description":"work item creations only"
	}`)

	rec := postSignal(t, server.Handler(), tokenResult.Secret, "projection-signal-1", signalRequestBody(t, "integration:projection-feed:signal"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("signal create: %d %s", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/feed?projection=signal-view&limit=20", nil)
	req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
	feedRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(feedRec, req)
	if feedRec.Code != http.StatusOK {
		t.Fatalf("projected feed: %d %s", feedRec.Code, feedRec.Body.String())
	}
	var snapshot struct {
		Items []struct {
			Kind string `json:"kind"`
		} `json:"items"`
	}
	if err := json.Unmarshal(feedRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode projected feed: %v", err)
	}
	if len(snapshot.Items) == 0 {
		t.Fatalf("projected feed returned no signal items: %s", feedRec.Body.String())
	}
	for _, item := range snapshot.Items {
		if item.Kind != domain.EventSignalReceived {
			t.Fatalf("signal-view leaked kind %q: body=%s", item.Kind, feedRec.Body.String())
		}
	}

	cursor := projectedHeadCursor(t, server.Handler(), tokenResult.Secret, "signal-view")
	req = httptest.NewRequest(http.MethodGet, "/v1/feed?projection=work-view&cursor="+cursor, nil)
	req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
	mismatchRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(mismatchRec, req)
	if mismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("cursor mismatch status = %d, want 400 body=%s", mismatchRec.Code, mismatchRec.Body.String())
	}
	if !strings.Contains(mismatchRec.Body.String(), "cursor_projection_mismatch") {
		t.Fatalf("expected cursor_projection_mismatch, got %s", mismatchRec.Body.String())
	}
}

func defineProjection(t *testing.T, handler http.Handler, token, key, raw string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/registry/projections", bytes.NewReader([]byte(raw)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", "projection-"+key)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("define projection %s: %d %s", key, rec.Code, rec.Body.String())
	}
}

func projectedHeadCursor(t *testing.T, h http.Handler, token, projection string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/feed?projection="+projection+"&wait=0s", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("projection head fetch: %d %s", rec.Code, rec.Body.String())
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode projection head: %v", err)
	}
	if page.NextCursor == "" {
		t.Fatal("projection head cursor empty")
	}
	return page.NextCursor
}
