package main

// Watch-ergonomics behavior of `meristem feed --watch`: lens flags become
// query params, the cursor file is bootstrapped/durable/torn-write-proof,
// the --exec wake hook has redelivery semantics (a failed hook must never
// advance the cursor past the undelivered event), and the reconnect loop
// classifies server rejections instead of retrying everything.

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
		projection:    "owner-attention",
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
		"projection":     {"owner-attention"},
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

func TestClassifyWatchError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want watchErrorClass
	}{
		{"transport", fmt.Errorf("feed: stream connect: connection refused"), watchErrTransient},
		{"stream closed", fmt.Errorf("feed: stream closed by server"), watchErrTransient},
		{"http 500", fmt.Errorf("wrap: %w", &apiRequestError{Status: 500, Code: "feed_read_failed"}), watchErrTransient},
		{"invalid cursor", fmt.Errorf("wrap: %w", &apiRequestError{Status: 400, Code: "invalid_cursor"}), watchErrCursorIdentity},
		{"filter mismatch", fmt.Errorf("wrap: %w", &apiRequestError{Status: 400, Code: "cursor_filter_mismatch"}), watchErrCursorIdentity},
		{"projection mismatch", fmt.Errorf("wrap: %w", &apiRequestError{Status: 400, Code: "cursor_projection_mismatch"}), watchErrCursorIdentity},
		{"invalid filter", fmt.Errorf("wrap: %w", &apiRequestError{Status: 400, Code: "invalid_feed_predicate"}), watchErrPermanent},
		{"auth denied", fmt.Errorf("wrap: %w", &apiRequestError{Status: 403, Code: "insufficient_scope"}), watchErrPermanent},
		{"unauthenticated", fmt.Errorf("wrap: %w", &apiRequestError{Status: 401, Code: "missing_authenticated_token"}), watchErrPermanent},
		{"request timeout", fmt.Errorf("wrap: %w", &apiRequestError{Status: 408, Code: "request_timeout"}), watchErrTransient},
		{"rate limited", fmt.Errorf("wrap: %w", &apiRequestError{Status: 429, Code: "rate_limited"}), watchErrTransient},
	} {
		if got := classifyWatchError(tc.err); got != tc.want {
			t.Fatalf("%s: classified %d, want %d", tc.name, got, tc.want)
		}
	}
}

// watchFakeServer speaks both surfaces the durable watcher uses: the page
// endpoint (/v1/feed) for cursor bootstrap and the SSE stream
// (/v1/feed/stream). Behavior is driven by the stream func; the page always
// mints bootCursor. All requests are recorded for assertions (non-blocking,
// so runaway loops cannot deadlock server shutdown).
func watchFakeServer(t *testing.T, bootCursor string, stream func(w http.ResponseWriter, r *http.Request), requests chan<- *http.Request) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests != nil {
			select {
			case requests <- r.Clone(context.Background()):
			default:
			}
		}
		if r.URL.Path == "/v1/feed" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"items":[],"next_cursor":%q,"has_more":false}`, bootCursor)
			return
		}
		stream(w, r)
	}))
}

func serveSSE(w http.ResponseWriter, frames ...string) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, frame := range frames {
		fmt.Fprint(w, frame)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func sseFrameFor(id, kind, marker string) string {
	return fmt.Sprintf("id: %s\nevent: feed\ndata: {\"kind\":%q,\"subject_id\":\"s\",\"payload\":{\"marker\":%q}}\n\n", id, kind, marker)
}

func watchClientFor(srv *httptest.Server, lens url.Values) *feedClient {
	return &feedClient{
		baseURL:    srv.URL,
		token:      "test-token",
		query:      lens,
		http:       &http.Client{Timeout: 5 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}
}

// waitForFileContent polls until want appears via read, or fails at the
// deadline. Used for assertions on asynchronously-written files.
func waitForFileContent(t *testing.T, what string, read func() string, want string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		got := read()
		if strings.Contains(got, want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s never reached %q; last value %q", what, want, got)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestWatchBootstrapsCursorThenDeliversAndPersists(t *testing.T) {
	hookOut := filepath.Join(t.TempDir(), "delivered")
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	requests := make(chan *http.Request, 8)
	srv := watchFakeServer(t, "boot-cursor", func(w http.ResponseWriter, r *http.Request) {
		serveSSE(w, sseFrameFor("cursor-1", "work_item.event_appended", "wake-me"))
	}, requests)
	defer srv.Close()

	client := watchClientFor(srv, url.Values{"kind": {"work_item.event_appended"}})
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

	// First request is the atomic bootstrap read; it must carry the lens
	// and ask for the head cursor rather than a consuming page.
	boot := <-requests
	if boot.URL.Path != "/v1/feed" || boot.URL.Query().Get("kind") != "work_item.event_appended" {
		t.Fatalf("expected lens-carrying bootstrap read, got %s?%s", boot.URL.Path, boot.URL.RawQuery)
	}
	if got := boot.URL.Query().Get("bootstrap"); got != "head" {
		t.Fatalf("bootstrap read must use bootstrap=head, got %q", got)
	}
	// The stream then resumes from the minted cursor — no from-now gap.
	stream := <-requests
	if stream.URL.Path != "/v1/feed/stream" {
		t.Fatalf("expected stream connect after bootstrap, got %s", stream.URL.Path)
	}
	if got := stream.Header.Get("Last-Event-ID"); got != "boot-cursor" {
		t.Fatalf("stream did not resume from the bootstrap cursor: %q", got)
	}
	if got := stream.URL.Query().Get("kind"); got != "work_item.event_appended" {
		t.Fatalf("stream connection missing lens param, query=%s", stream.URL.RawQuery)
	}

	waitForFileContent(t, "wake hook output", func() string {
		data, _ := os.ReadFile(hookOut)
		return string(data)
	}, "wake-me")

	// The cursor advance is asynchronous relative to the hook write; wait
	// for it instead of asserting immediately (WE-T1).
	waitForFileContent(t, "cursor file", func() string {
		cursor, _ := loadCursorFile(cursorFile)
		return cursor
	}, "cursor-1")

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
	requests := make(chan *http.Request, 8)
	srv := watchFakeServer(t, "unused-boot", func(w http.ResponseWriter, r *http.Request) {
		serveSSE(w, sseFrameFor("cursor-after", "work_item.event_appended", "will-fail"))
	}, requests)
	defer srv.Close()

	client := watchClientFor(srv, nil)
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

	// A seeded cursor skips bootstrap; the one connection is the stream,
	// resuming from the seed. The failed delivery must not advance it.
	first := <-requests
	if first.URL.Path != "/v1/feed/stream" || first.Header.Get("Last-Event-ID") != "cursor-before" {
		t.Fatalf("expected stream resume from seeded cursor, got %s Last-Event-ID=%q", first.URL.Path, first.Header.Get("Last-Event-ID"))
	}
	if cursor, loadErr := loadCursorFile(cursorFile); loadErr != nil || cursor != "cursor-before" {
		t.Fatalf("failed delivery advanced the cursor to %q (err=%v)", cursor, loadErr)
	}
}

func TestWatchCursorMismatchWithoutOptInExitsAndPreservesCursor(t *testing.T) {
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	if err := saveCursorFile(cursorFile, "stale-identity-cursor"); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	srv := watchFakeServer(t, "unused-boot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"code":"cursor_filter_mismatch","message":"cursor was issued under a different filter identity"}}`)
	}, nil)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runFeedWatch(ctx, nil, watchClientFor(srv, nil), watchOptions{
		retryBackoff: 10 * time.Millisecond,
		cursorFile:   cursorFile,
	}, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "cursor_filter_mismatch") || !strings.Contains(err.Error(), "--reset-cursor-on-mismatch") {
		t.Fatalf("mismatch without opt-in should exit loudly naming the flag, got err=%v", err)
	}
	if cursor, loadErr := loadCursorFile(cursorFile); loadErr != nil || cursor != "stale-identity-cursor" {
		t.Fatalf("mismatch without opt-in must preserve the cursor file, got %q err=%v", cursor, loadErr)
	}
}

func TestWatchCursorMismatchWithOptInRemintsAndResumes(t *testing.T) {
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	if err := saveCursorFile(cursorFile, "stale-identity-cursor"); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	requests := make(chan *http.Request, 8)
	srv := watchFakeServer(t, "fresh-boot-cursor", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Last-Event-ID") == "stale-identity-cursor" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"code":"cursor_filter_mismatch","message":"cursor was issued under a different filter identity"}}`)
			return
		}
		// Post-reset stream: accepted, delivers one frame, then holds open
		// like real SSE so the watcher does not churn reconnects.
		serveSSE(w, sseFrameFor("post-reset-cursor", "work_item.event_appended", "post-reset"))
		<-r.Context().Done()
	}, requests)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFeedWatch(ctx, nil, watchClientFor(srv, nil), watchOptions{
			retryBackoff:          10 * time.Millisecond,
			cursorFile:            cursorFile,
			resetCursorOnMismatch: true,
		}, os.Stderr)
	}()

	// stale stream reject -> page re-mint -> stream resume from fresh cursor.
	stale := <-requests
	if stale.URL.Path != "/v1/feed/stream" || stale.Header.Get("Last-Event-ID") != "stale-identity-cursor" {
		t.Fatalf("expected stale stream attempt first, got %s %q", stale.URL.Path, stale.Header.Get("Last-Event-ID"))
	}
	remint := <-requests
	if remint.URL.Path != "/v1/feed" {
		t.Fatalf("expected page re-mint after opted-in reset, got %s", remint.URL.Path)
	}
	resumed := <-requests
	if resumed.URL.Path != "/v1/feed/stream" || resumed.Header.Get("Last-Event-ID") != "fresh-boot-cursor" {
		t.Fatalf("expected stream resume from re-minted cursor, got %s %q", resumed.URL.Path, resumed.Header.Get("Last-Event-ID"))
	}

	waitForFileContent(t, "cursor file", func() string {
		cursor, _ := loadCursorFile(cursorFile)
		return cursor
	}, "post-reset-cursor")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}
}

func TestWatchBootstrapRetriesTransientFailureThenConnects(t *testing.T) {
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	requests := make(chan *http.Request, 8)
	var bootAttempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- r.Clone(context.Background()):
		default:
		}
		if r.URL.Path == "/v1/feed" {
			bootAttempts++
			if bootAttempts == 1 {
				// Transient outage on the first mint: must be retried, not fatal.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"error":{"code":"database_unavailable","message":"temporarily down"}}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"items":[],"next_cursor":"boot-after-503","has_more":false}`)
			return
		}
		serveSSE(w, sseFrameFor("cursor-live", "work_item.event_appended", "post-outage"))
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFeedWatch(ctx, nil, watchClientFor(srv, nil), watchOptions{
			retryBackoff: 10 * time.Millisecond,
			cursorFile:   cursorFile,
		}, os.Stderr)
	}()

	// 503 mint -> retried mint -> stream resumes from the minted cursor.
	first := <-requests
	if first.URL.Path != "/v1/feed" {
		t.Fatalf("expected bootstrap attempt first, got %s", first.URL.Path)
	}
	second := <-requests
	if second.URL.Path != "/v1/feed" {
		t.Fatalf("expected bootstrap retry after 503, got %s", second.URL.Path)
	}
	stream := <-requests
	if stream.URL.Path != "/v1/feed/stream" || stream.Header.Get("Last-Event-ID") != "boot-after-503" {
		t.Fatalf("expected stream resume from retried bootstrap cursor, got %s %q", stream.URL.Path, stream.Header.Get("Last-Event-ID"))
	}

	waitForFileContent(t, "cursor file", func() string {
		cursor, _ := loadCursorFile(cursorFile)
		return cursor
	}, "cursor-live")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}
}

func TestWatchPermanentRequestErrorsExitWithoutRetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{"invalid filter", http.StatusBadRequest, "invalid_feed_predicate"},
		{"auth denied", http.StatusForbidden, "insufficient_scope"},
		{"unauthenticated", http.StatusUnauthorized, "missing_authenticated_token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursorFile := filepath.Join(t.TempDir(), "cursor")
			if err := saveCursorFile(cursorFile, "cursor-keep"); err != nil {
				t.Fatalf("seed cursor: %v", err)
			}
			var hits int
			srv := watchFakeServer(t, "unused-boot", func(w http.ResponseWriter, r *http.Request) {
				hits++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"error":{"code":%q,"message":"permanent"}}`, tc.code)
			}, nil)
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := runFeedWatch(ctx, nil, watchClientFor(srv, nil), watchOptions{
				retryBackoff: time.Millisecond,
				cursorFile:   cursorFile,
			}, os.Stderr)
			if err == nil || !strings.Contains(err.Error(), tc.code) {
				t.Fatalf("permanent error should exit loudly with the code, got err=%v", err)
			}
			if hits != 1 {
				t.Fatalf("permanent error was retried: %d stream attempts", hits)
			}
			if cursor, loadErr := loadCursorFile(cursorFile); loadErr != nil || cursor != "cursor-keep" {
				t.Fatalf("permanent error must not touch the cursor file, got %q err=%v", cursor, loadErr)
			}
		})
	}
}
