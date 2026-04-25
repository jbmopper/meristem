package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jbmopper/wayline/internal/app"
	"github.com/jbmopper/wayline/internal/auth"
	"github.com/jbmopper/wayline/internal/domain"
	"github.com/jbmopper/wayline/internal/storage"
)

// TestFeedWatcherWakesUpOnNewEvent pins the e1625848 wake-up contract:
// a long-poll started before a writer appends an event must return
// within ~1s of that append. The bound is the 250ms poll tick plus
// handler/network overhead; we allow 1.5s in the assertion to keep the
// test stable on busy CI but if it ever takes >1.5s in practice that's
// a real regression in the bounded-poll loop.
//
// Why this test matters: the alternative implementation (LISTEN/NOTIFY)
// was rejected for portability; this test is what catches the bounded-
// poll path silently degrading to "next time we get around to it."
func TestFeedWatcherWakesUpOnNewEvent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "feed-watcher",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)

	type pollResult struct {
		status int
		body   []byte
		dur    time.Duration
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/feed?wait=5s", nil)
		req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
		rec := httptest.NewRecorder()
		start := time.Now()
		server.Handler().ServeHTTP(rec, req)
		pollDone <- pollResult{rec.Code, rec.Body.Bytes(), time.Since(start)}
	}()

	// Let the poller capture head before we write.
	time.Sleep(300 * time.Millisecond)

	signalRec := postSignal(t, server.Handler(), tokenResult.Secret, "feed-watcher-wake-1", signalRequestBody(t, "integration:feed-watcher:wake"))
	if signalRec.Code != http.StatusCreated {
		t.Fatalf("signal create: status=%d body=%s", signalRec.Code, signalRec.Body.String())
	}
	signalLanded := time.Now()

	select {
	case res := <-pollDone:
		if res.status != http.StatusOK {
			t.Fatalf("poll status: %d body=%s", res.status, string(res.body))
		}
		// Wake-up bound is poll-tick (250ms) + handler overhead. If the
		// loop ever sleeps without checking time.Now().Before(deadline),
		// or the tick gets longer, this assertion catches it.
		woken := time.Since(signalLanded)
		if woken > 1500*time.Millisecond {
			t.Errorf("wake-up took %v after signal landed; want <1.5s (250ms tick budget)", woken)
		}

		var page struct {
			Items      []map[string]any `json:"items"`
			NextCursor string           `json:"next_cursor"`
			HasMore    bool             `json:"has_more"`
		}
		if err := json.Unmarshal(res.body, &page); err != nil {
			t.Fatalf("decode body: %v (raw=%s)", err, res.body)
		}
		if len(page.Items) == 0 {
			t.Errorf("expected at least one item from the signal write, got none (body=%s)", res.body)
		}
		if page.NextCursor == "" {
			t.Errorf("next_cursor must be non-empty when items are present")
		}
	case <-time.After(7 * time.Second):
		t.Fatal("long-poll did not return within 7s of signal landing")
	}
}

// TestFeedWatcherResumesFromCursor verifies the at-least-once + no-skip
// contract for restart resumption: a consumer that records its cursor,
// disconnects, and resumes must see every event that arrived during the
// gap and exactly the events after the cursor (no replay of pre-cursor
// events).
//
// Without this test it'd be possible to silently break the resume path
// by switching to "from now" semantics on every cursored call, which
// would lose events between disconnect and reconnect.
func TestFeedWatcherResumesFromCursor(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "feed-resume",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)

	for i := 0; i < 3; i++ {
		rec := postSignal(t, server.Handler(), tokenResult.Secret, mustKey(t, i), signalRequestBody(t, mustDedupe(t, "pre", i)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("pre-signal %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}

	// Snapshot the head as our cursor.
	cursor := fetchHeadCursor(t, server.Handler(), tokenResult.Secret)

	// Three more signals after the cursor.
	for i := 0; i < 3; i++ {
		rec := postSignal(t, server.Handler(), tokenResult.Secret, mustKey(t, i+10), signalRequestBody(t, mustDedupe(t, "post", i)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("post-signal %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}

	page := fetchPage(t, server.Handler(), tokenResult.Secret, "/v1/feed?cursor="+cursor)
	if len(page.Items) < 3 {
		t.Fatalf("expected at least 3 items after cursor, got %d", len(page.Items))
	}
	// Every returned item's payload must be from the post-cursor batch.
	// signal.received payloads include dedupe_key; we asserted the prefix
	// "post" on those when we wrote them.
	for _, item := range page.Items {
		if !looksLikePostBatch(item) {
			t.Errorf("item leaked from pre-cursor batch into resume page: %v", item)
		}
	}
}

func fetchHeadCursor(t *testing.T, h http.Handler, token string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/feed?wait=0s", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("head fetch: %d %s", rec.Code, rec.Body.String())
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode head: %v", err)
	}
	if page.NextCursor == "" {
		t.Fatal("head cursor empty")
	}
	return page.NextCursor
}

func fetchPage(t *testing.T, h http.Handler, token, path string) struct {
	Items      []map[string]any `json:"items"`
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
} {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch %s: %d %s", path, rec.Code, rec.Body.String())
	}
	var page struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
		HasMore    bool             `json:"has_more"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v (raw=%s)", err, rec.Body.String())
	}
	return page
}

func mustKey(t *testing.T, i int) string {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("feed-resume-")
	buf.WriteString(itoa(i))
	return buf.String()
}

func mustDedupe(t *testing.T, prefix string, i int) string {
	t.Helper()
	return "integration:feed-resume:" + prefix + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func looksLikePostBatch(item map[string]any) bool {
	payload, ok := item["payload"].(map[string]any)
	if !ok {
		return true // not a signal; not part of the assertion target
	}
	dedupe, ok := payload["dedupe_key"].(string)
	if !ok {
		return true
	}
	return !contains(dedupe, "pre-")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
