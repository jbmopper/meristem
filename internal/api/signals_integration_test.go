package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestSignalsEndpointIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "integration-human",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	body := signalRequestBody(t, "integration:signal:retry-budget")

	first := postSignal(t, server.Handler(), tokenResult.Secret, "signal-first", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first signal: want 201, got %d body=%s", first.Code, first.Body.String())
	}
	var firstResponse signalResponse
	decodeResponse(t, first, &firstResponse)
	if !firstResponse.Dedupe.CreatedWorkItem {
		t.Fatalf("first signal should create a work item: %+v", firstResponse.Dedupe)
	}
	if firstResponse.Resource.Kind != "signal" || firstResponse.Resource.ID == uuid.Nil {
		t.Fatalf("unexpected resource block: %+v", firstResponse.Resource)
	}
	if firstResponse.WorkItem.ID == uuid.Nil {
		t.Fatal("expected work item id")
	}
	if firstResponse.Events.SignalReceived == uuid.Nil || firstResponse.Events.WorkItemCreated == nil {
		t.Fatalf("expected both event ids, got %+v", firstResponse.Events)
	}
	if !strings.HasPrefix(firstResponse.Fingerprint, "sha256:") {
		t.Fatalf("expected sha256-prefixed fingerprint, got %q", firstResponse.Fingerprint)
	}

	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "signals", 1)
	assertTableCount(t, pool, "work_items", 1)

	feedResponse := getFeed(t, server.Handler(), tokenResult.Secret)
	if !feedContainsKind(feedResponse.Items, domain.EventSignalReceived) {
		t.Fatalf("feed did not include signal.received; items=%+v", feedResponse.Items)
	}

	replay := postSignal(t, server.Handler(), tokenResult.Secret, "signal-first", body)
	if replay.Code != http.StatusCreated {
		t.Fatalf("replay: want original 201, got %d body=%s", replay.Code, replay.Body.String())
	}
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("expected replay header, got headers=%v", replay.Header())
	}
	var replayResponse signalResponse
	decodeResponse(t, replay, &replayResponse)
	if replayResponse.Resource.ID != firstResponse.Resource.ID {
		t.Fatalf("replay should return same signal id: %s != %s", replayResponse.Resource.ID, firstResponse.Resource.ID)
	}
	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)

	second := postSignal(t, server.Handler(), tokenResult.Secret, "signal-second", body)
	if second.Code != http.StatusCreated {
		t.Fatalf("second signal: want 201, got %d body=%s", second.Code, second.Body.String())
	}
	var secondResponse signalResponse
	decodeResponse(t, second, &secondResponse)
	if secondResponse.Dedupe.CreatedWorkItem {
		t.Fatalf("second signal with same dedupe_key should link to existing live work item: %+v", secondResponse.Dedupe)
	}
	if secondResponse.WorkItem.ID != firstResponse.WorkItem.ID {
		t.Fatalf("semantic dedupe should keep work_item id: %s != %s", secondResponse.WorkItem.ID, firstResponse.WorkItem.ID)
	}
	if secondResponse.Resource.ID == firstResponse.Resource.ID {
		t.Fatalf("distinct idempotency key should create a distinct signal row")
	}
	assertEventCount(t, pool, domain.EventSignalReceived, 2)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 2)
	assertTableCount(t, pool, "signals", 2)
	assertTableCount(t, pool, "work_items", 1)
}

func TestSignalsEndpointRejectsOversizedBody(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "integration-large-body-human",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	body := []byte(`{"padding":"` + strings.Repeat("x", int(safety.DefaultPolicy().MaxRequestBodyBytes)+1) + `"}`)
	rec := postSignal(t, server.Handler(), tokenResult.Secret, "signal-too-large", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized signal: want 413, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "request_too_large") {
		t.Fatalf("expected request_too_large body, got %s", rec.Body.String())
	}
	assertEventCount(t, pool, domain.EventSignalReceived, 0)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 0)
}

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgtest.NewPool(t, "meristem_itest")
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func signalRequestBody(t *testing.T, dedupeKey string) []byte {
	t.Helper()
	body := fmt.Sprintf(`{
		"kind": "repairable_failure",
		"dedupe_key": %q,
		"source": {
			"kind": "system_event",
			"identifier": "integration-001",
			"external_ref": "integration-test"
		},
		"work_spec": %s
	}`, dedupeKey, validSignalWorkSpec())
	return []byte(body)
}

func postSignal(t *testing.T, handler http.Handler, token string, key string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/signals", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

type feedEnvelope struct {
	Items []feed.Item `json:"items"`
}

func getFeed(t *testing.T, handler http.Handler, token string) feedEnvelope {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/feed?limit=20", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("feed: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out feedEnvelope
	decodeResponse(t, rec, &out)
	return out
}

func feedContainsKind(items []feed.Item, kind string) bool {
	for _, item := range items {
		if item.Kind == kind {
			return true
		}
	}
	return false
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
}

func assertEventCount(t *testing.T, pool *pgxpool.Pool, kind string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&got); err != nil {
		t.Fatalf("count events %s: %v", kind, err)
	}
	if got != want {
		t.Fatalf("events %s: want %d, got %d", kind, want, got)
	}
}

func assertTableCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+quoteIdentifier(table)).Scan(&got); err != nil {
		t.Fatalf("count table %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("table %s: want %d, got %d", table, want, got)
	}
}
