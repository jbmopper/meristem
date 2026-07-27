package main

// Watch-ergonomics behavior of `meristem feed --watch`: lens flags become
// query params, the cursor file is durable and torn-write-proof, and the
// --exec wake hook has redelivery semantics (a failed hook must never
// advance the cursor past the undelivered event).

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildFeedQueryTranslatesLensFlags(t *testing.T) {
	q := buildFeedQuery(feedQueryFlags{
		scope:         "assigned",
		listenFor:     "self",
		actors:        []string{"self", "9a1c2b3d-0000-4000-8000-000000000001"},
		excludeActors: []string{"self"},
		kinds:         []string{"work_item.event_appended", "work_item.created"},
		excludeKinds:  []string{"patience.breached"},
		workItem:      "9a1c2b3d-0000-4000-8000-000000000002",
		tree:          "9a1c2b3d-0000-4000-8000-000000000003",
	})
	want := url.Values{
		"scope":          {"assigned"},
		"listen_for":     {"self"},
		"actor":          {"self", "9a1c2b3d-0000-4000-8000-000000000001"},
		"exclude_actor":  {"self"},
		"kind":           {"work_item.event_appended", "work_item.created"},
		"exclude_kind":   {"patience.breached"},
		"work_item":      {"9a1c2b3d-0000-4000-8000-000000000002"},
		"work_item_tree": {"9a1c2b3d-0000-4000-8000-000000000003"},
	}
	if got, wanted := q.Encode(), want.Encode(); got != wanted {
		t.Fatalf("query mismatch:\ngot  %s\nwant %s", got, wanted)
	}

	if got := buildFeedQuery(feedQueryFlags{}); len(got) != 0 {
		t.Fatalf("empty flags must add no params, got %s", got.Encode())
	}
}

func TestSplitCommaList(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", ""},
		{"  ", ""},
		{"a", "a"},
		{"a,b", "a|b"},
		{" a , b ,,c", "a|b|c"},
	} {
		got := strings.Join(splitCommaList(tc.in), "|")
		if got != tc.want {
			t.Fatalf("splitCommaList(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCursorFileRoundTripAndFreshStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor")

	// Missing file: fresh watcher, no error.
	got, err := loadCursorFile(path)
	if err != nil || got != "" {
		t.Fatalf("missing cursor file: got %q err=%v", got, err)
	}

	if err := saveCursorFile(path, "cursor-one"); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err = loadCursorFile(path)
	if err != nil || got != "cursor-one" {
		t.Fatalf("round trip: got %q err=%v", got, err)
	}

	// Clearing writes an empty cursor, which loads as a fresh start.
	if err := saveCursorFile(path, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	got, err = loadCursorFile(path)
	if err != nil || got != "" {
		t.Fatalf("cleared cursor: got %q err=%v", got, err)
	}

	// No temp debris left behind by the atomic write.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".cursor-") {
			t.Fatalf("atomic save leaked temp file %s", entry.Name())
		}
	}
}

// sseTestServer serves a fixed set of SSE frames to every connection and
// records the Last-Event-ID and query each connection arrived with.
func sseWatchTestServer(t *testing.T, frames []string, connections chan<- *http.Request) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connections != nil {
			connections <- r.Clone(context.Background())
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		for _, frame := range frames {
			fmt.Fprint(w, frame)
		}
	}))
}

func sseFrameFor(id, kind, marker string) string {
	return fmt.Sprintf("id: %s\nevent: feed\ndata: {\"kind\":%q,\"subject_id\":\"s\",\"payload\":{\"marker\":%q}}\n\n", id, kind, marker)
}

func TestWatchWakeHookDeliversEventAndPersistsCursor(t *testing.T) {
	hookOut := filepath.Join(t.TempDir(), "delivered")
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	connections := make(chan *http.Request, 4)
	srv := sseWatchTestServer(t, []string{
		sseFrameFor("cursor-1", "work_item.event_appended", "wake-me"),
	}, connections)
	defer srv.Close()

	client := &feedClient{
		baseURL:    srv.URL,
		token:      "test-token",
		query:      url.Values{"kind": {"work_item.event_appended"}},
		http:       &http.Client{Timeout: 5 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}

	// One connection serves one frame then closes; cancel after the reconnect
	// window so the loop exits via ctx instead of reconnecting forever.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFeedWatch(ctx, nil, client, watchOptions{
			retryBackoff: 5 * time.Second,
			cursorFile:   cursorFile,
			execCmd:      "cat >> " + hookOut,
			ndjson:       true,
		}, os.Stderr)
	}()

	first := <-connections
	if got := first.URL.Query().Get("kind"); got != "work_item.event_appended" {
		t.Fatalf("stream connection missing lens param, query=%s", first.URL.RawQuery)
	}
	if got := first.Header.Get("Last-Event-ID"); got != "" {
		t.Fatalf("fresh watcher sent Last-Event-ID %q", got)
	}

	deadline := time.After(3 * time.Second)
	for {
		data, _ := os.ReadFile(hookOut)
		if strings.Contains(string(data), "wake-me") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("wake hook never received the event; hook file: %q", string(data))
		case <-time.After(20 * time.Millisecond):
		}
	}

	// The cursor persisted only after successful delivery.
	if cursor, err := loadCursorFile(cursorFile); err != nil || cursor != "cursor-1" {
		t.Fatalf("persisted cursor = %q err=%v, want cursor-1", cursor, err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}
}

func TestWatchFailingWakeHookStopsWithoutAdvancingCursor(t *testing.T) {
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	if err := saveCursorFile(cursorFile, "cursor-before"); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	connections := make(chan *http.Request, 4)
	srv := sseWatchTestServer(t, []string{
		sseFrameFor("cursor-after", "work_item.event_appended", "will-fail"),
	}, connections)
	defer srv.Close()

	client := &feedClient{
		baseURL:    srv.URL,
		token:      "test-token",
		http:       &http.Client{Timeout: 5 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runFeedWatch(ctx, nil, client, watchOptions{
		retryBackoff: 10 * time.Millisecond,
		cursorFile:   cursorFile,
		execCmd:      "exit 7",
	}, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "wake hook failed") {
		t.Fatalf("failing hook should stop the watcher loudly, got err=%v", err)
	}

	// The watcher resumed from the seeded cursor and must not have advanced
	// past the undelivered event — redelivery is the contract.
	first := <-connections
	if got := first.Header.Get("Last-Event-ID"); got != "cursor-before" {
		t.Fatalf("watcher resumed from %q, want seeded cursor-before", got)
	}
	if cursor, loadErr := loadCursorFile(cursorFile); loadErr != nil || cursor != "cursor-before" {
		t.Fatalf("failed delivery advanced the cursor to %q (err=%v)", cursor, loadErr)
	}
}

func TestWatchRejectedCursorRebootstrapsAndClearsFile(t *testing.T) {
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	if err := saveCursorFile(cursorFile, "stale-identity-cursor"); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	connections := make(chan *http.Request, 8)
	var rejected bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case connections <- r.Clone(context.Background()):
		default:
		}
		// Only the stale filter identity is rejected — a real server accepts
		// cursors minted under the current identity. Accepted streams are
		// held open until the client goes away, like real SSE, so the
		// watcher does not churn through reconnects.
		if r.Header.Get("Last-Event-ID") == "stale-identity-cursor" {
			rejected = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"code":"cursor_filter_mismatch","message":"cursor was issued under a different filter identity; reconnect without Last-Event-ID"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if r.Header.Get("Last-Event-ID") == "" {
			fmt.Fprint(w, sseFrameFor("fresh-cursor", "work_item.event_appended", "post-rebootstrap"))
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := &feedClient{
		baseURL:    srv.URL,
		token:      "test-token",
		http:       &http.Client{Timeout: 5 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFeedWatch(ctx, nil, client, watchOptions{
			retryBackoff: 10 * time.Millisecond,
			cursorFile:   cursorFile,
		}, os.Stderr)
	}()

	// First connection carries the stale cursor and is rejected; the second
	// must arrive with no Last-Event-ID (re-bootstrap from now).
	first := <-connections
	if first.Header.Get("Last-Event-ID") != "stale-identity-cursor" {
		t.Fatalf("first connection did not resume from the persisted cursor")
	}
	second := <-connections
	if got := second.Header.Get("Last-Event-ID"); got != "" {
		t.Fatalf("re-bootstrap still sent Last-Event-ID %q", got)
	}
	if !rejected {
		t.Fatalf("server never exercised the rejection path")
	}

	// The fresh frame's cursor eventually replaces the cleared file.
	deadline := time.After(3 * time.Second)
	for {
		cursor, err := loadCursorFile(cursorFile)
		if err != nil {
			t.Fatalf("load cursor: %v", err)
		}
		if cursor == "fresh-cursor" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("cursor file was not replaced after re-bootstrap, still %q", cursor)
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}
}
