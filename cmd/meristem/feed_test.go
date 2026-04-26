package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"", 5, ""},
		{"abc", 5, "abc"},
		{"abcdef", 5, "abcd…"},
		{"abcdef", 6, "abcdef"},
		{"abcdef", 0, "abcdef"},
		{"abcdef", 1, "abcdef"},
	}
	for _, c := range cases {
		if got := truncate(c.in, c.n); got != c.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

func TestPadRight(t *testing.T) {
	if got := padRight("hi", 5); got != "hi   " {
		t.Errorf("padRight(\"hi\", 5) = %q, want %q", got, "hi   ")
	}
	if got := padRight("hello", 5); got != "hello" {
		t.Errorf("padRight equal-length pass-through; got %q", got)
	}
	if got := padRight("longstring", 3); got != "longstring" {
		t.Errorf("padRight should not truncate; got %q", got)
	}
}

func TestShortIDAndSafeSource(t *testing.T) {
	if got := shortID("12345678-aaaa-bbbb-cccc-dddddddddddd"); got != "12345678" {
		t.Errorf("shortID UUID prefix wrong: %q", got)
	}
	if got := shortID("abc"); got != "abc" {
		t.Errorf("shortID short input pass-through: %q", got)
	}
	if got := safeSource(""); got != "?" {
		t.Errorf("safeSource(\"\") = %q, want %q", got, "?")
	}
	if got := safeSource("system"); got != "system" {
		t.Errorf("safeSource passthrough: %q", got)
	}
}

func TestFmtDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30s"},
		{60 * time.Second, "1m"},
		{90 * time.Second, "1m"},
		{time.Hour, "1h"},
		{25 * time.Hour, "1d"},
	}
	for _, c := range cases {
		if got := fmtDuration(c.d); got != c.want {
			t.Errorf("fmtDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestSummarizePerKind(t *testing.T) {
	cases := []struct {
		name string
		kind string
		raw  string
		want string
	}{
		{
			name: "message.captured trims long text",
			kind: "message.captured",
			raw:  `{"text":"hello world"}`,
			want: `inbox    "hello world"`,
		},
		{
			name: "work_item.created surfaces title and state",
			kind: "work_item.created",
			raw:  `{"title":"Pick the next slice","state":"captured"}`,
			want: `created  [captured] Pick the next slice`,
		},
		{
			name: "work_item.transitioned with from + reason",
			kind: "work_item.transitioned",
			raw:  `{"from":"triaged","to":"done","reason":"shipped"}`,
			want: `triaged → done         "shipped"`,
		},
		{
			name: "work_item.transitioned without from",
			kind: "work_item.transitioned",
			raw:  `{"to":"triaged"}`,
			want: `→ triaged`,
		},
		{
			name: "work_item.event_appended unwraps envelope and pulls excerpt",
			kind: "work_item.event_appended",
			raw:  `{"inner_kind":"work_item.note_added","inner":{"text":"left a note","author":"agent-A"}}`,
			want: `fact     work_item.note_added  "left a note"`,
		},
		{
			name: "work_item.event_appended without inner-text shows just inner_kind",
			kind: "work_item.event_appended",
			raw:  `{"inner_kind":"work_item.note_added","inner":{"author":"agent-A"}}`,
			want: `fact     work_item.note_added`,
		},
		{
			name: "patience.breached compact format",
			kind: "patience.breached",
			raw:  `{"state":"captured","budget_seconds":60}`,
			want: `BREACH   state=captured budget=1m`,
		},
		{
			name: "signal.received with dedupe key",
			kind: "signal.received",
			raw:  `{"signal_kind":"repairable_failure","dedupe_key":"repo:jay:retry"}`,
			want: `signal   repairable_failure  dedupe=repo:jay:retry`,
		},
		{
			name: "unknown kind falls back to bare kind string",
			kind: "novel.kind",
			raw:  `{}`,
			want: `novel.kind`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := feedItem{Kind: c.kind, Payload: json.RawMessage(c.raw)}
			if got := summarize(it); got != c.want {
				t.Errorf("summarize:\n  got:  %q\n  want: %q", got, c.want)
			}
		})
	}
}

// TestFeedClientFetchHonorsAuthAndLimit pins the wire shape the CLI sends:
// Bearer header, ?limit query, and a tolerant decode of the canonical
// {"items":[...]} envelope. Done with httptest so the test does not depend
// on a running API.
func TestFeedClientFetchHonorsAuthAndLimit(t *testing.T) {
	gotAuth := ""
	gotLimit := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotLimit = r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[
			{"event_id":"e1","occurred_at":"2026-04-24T12:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11111111-aaaa-bbbb-cccc-dddddddddddd","kind":"patience.breached","payload":{"state":"captured","budget_seconds":60}}
		]}`))
	}))
	defer srv.Close()

	c := &feedClient{baseURL: srv.URL, token: "mrs_secret", http: srv.Client()}
	items, err := c.fetch(context.Background(), 33)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if gotAuth != "Bearer mrs_secret" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer mrs_secret")
	}
	if gotLimit != "33" {
		t.Errorf("limit query = %q, want %q", gotLimit, "33")
	}
	if len(items) != 1 || items[0].Kind != "patience.breached" {
		t.Errorf("unexpected items: %+v", items)
	}
}

// TestFeedClientFetchSurfacesNon200 confirms a non-200 response becomes a
// returned error rather than a silent empty list.
func TestFeedClientFetchSurfacesNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`unauthorized`))
	}))
	defer srv.Close()

	c := &feedClient{baseURL: srv.URL, token: "bad", http: srv.Client()}
	_, err := c.fetch(context.Background(), 5)
	if err == nil {
		t.Fatal("expected fetch to surface 401 as error, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected error to mention 401, got: %v", err)
	}
}

// TestPrintItemsSortsOldestFirst pins the chronological-reading property:
// regardless of the order the API returns items in, the renderer prints
// oldest-first so reading top-to-bottom matches the timeline.
func TestPrintItemsSortsOldestFirst(t *testing.T) {
	t1 := time.Date(2026, 4, 24, 14, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 4, 24, 14, 5, 0, 0, time.UTC)
	t3 := time.Date(2026, 4, 24, 14, 10, 0, 0, time.UTC)

	items := []feedItem{
		{EventID: "c", OccurredAt: t3, Kind: "work_item.created", Payload: json.RawMessage(`{"title":"third","state":"captured"}`)},
		{EventID: "a", OccurredAt: t1, Kind: "work_item.created", Payload: json.RawMessage(`{"title":"first","state":"captured"}`)},
		{EventID: "b", OccurredAt: t2, Kind: "work_item.created", Payload: json.RawMessage(`{"title":"second","state":"captured"}`)},
	}

	var buf bytes.Buffer
	printItems(&buf, items)
	out := buf.String()

	idxFirst := strings.Index(out, "first")
	idxSecond := strings.Index(out, "second")
	idxThird := strings.Index(out, "third")
	if idxFirst < 0 || idxSecond < 0 || idxThird < 0 {
		t.Fatalf("expected all three titles in output, got:\n%s", out)
	}
	if !(idxFirst < idxSecond && idxSecond < idxThird) {
		t.Errorf("expected oldest-first order; got positions first=%d second=%d third=%d\noutput:\n%s",
			idxFirst, idxSecond, idxThird, out)
	}
}

// TestResolveFeedTokenEnvWins pins the precedence rule: even when a
// .meristem/ on disk has tokens that would otherwise be picked up,
// MERISTEM_TOKEN takes precedence and source is reported as "MERISTEM_TOKEN".
func TestResolveFeedTokenEnvWins(t *testing.T) {
	dir := t.TempDir()
	wln := filepath.Join(dir, ".meristem")
	if err := os.MkdirAll(wln, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "root.token"), []byte("file-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	withCwd(t, dir)

	t.Setenv("MERISTEM_TOKEN", "env-token")
	tok, src, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "env-token" {
		t.Errorf("token = %q, want %q (env should win over file)", tok, "env-token")
	}
	if src != "MERISTEM_TOKEN" {
		t.Errorf("source = %q, want %q", src, "MERISTEM_TOKEN")
	}
}

// TestResolveFeedTokenWalksUp pins the walk-upward discovery: a .meristem/
// at the project root is found from a deep subdirectory.
func TestResolveFeedTokenWalksUp(t *testing.T) {
	root := t.TempDir()
	wln := filepath.Join(root, ".meristem")
	if err := os.MkdirAll(wln, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "root.token"), []byte("found-it"), 0o600); err != nil {
		t.Fatal(err)
	}
	deep := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	withCwd(t, deep)
	t.Setenv("MERISTEM_TOKEN", "")

	tok, src, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "found-it" {
		t.Errorf("token = %q, want %q", tok, "found-it")
	}
	if !strings.HasSuffix(src, ".meristem/root.token") {
		t.Errorf("source = %q, want suffix .meristem/root.token", src)
	}
}

// TestResolveFeedTokenPriorityOrder pins that root.token is preferred over
// agent-A.token when both exist, matching the documented order.
func TestResolveFeedTokenPriorityOrder(t *testing.T) {
	dir := t.TempDir()
	wln := filepath.Join(dir, ".meristem")
	if err := os.MkdirAll(wln, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "agent-A.token"), []byte("agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "root.token"), []byte("root"), 0o600); err != nil {
		t.Fatal(err)
	}
	withCwd(t, dir)
	t.Setenv("MERISTEM_TOKEN", "")

	tok, _, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "root" {
		t.Errorf("token = %q, want %q (root should win over agent)", tok, "root")
	}
}

// TestResolveFeedTokenFallsThroughEmptyFiles pins that a present-but-empty
// token file is skipped and the next priority candidate is tried, rather
// than returning an empty token to the API.
func TestResolveFeedTokenFallsThroughEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	wln := filepath.Join(dir, ".meristem")
	if err := os.MkdirAll(wln, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "root.token"), []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "agent-A.token"), []byte("agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	withCwd(t, dir)
	t.Setenv("MERISTEM_TOKEN", "")

	tok, _, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "agent" {
		t.Errorf("token = %q, want %q (whitespace-only root.token should fall through)", tok, "agent")
	}
}

// TestResolveFeedTokenNoSources pins the error path: with no env var and
// no .meristem directory anywhere up the tree, the resolver returns a clear
// error rather than a usable empty token.
func TestResolveFeedTokenNoSources(t *testing.T) {
	dir := t.TempDir()
	withCwd(t, dir)
	t.Setenv("MERISTEM_TOKEN", "")

	_, _, err := resolveFeedToken()
	if err == nil {
		t.Fatal("expected error when no env and no .meristem, got nil")
	}
	if !strings.Contains(err.Error(), "no .meristem/") {
		t.Errorf("error should mention missing .meristem directory; got: %v", err)
	}
}

// withCwd chdirs to dir for the duration of the test and restores the
// original cwd on cleanup. Resolves dir to its real path so symlinks (e.g.
// macOS /private/var vs /var) do not confuse the assertion.
func withCwd(t *testing.T, dir string) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		resolved = dir
	}
	if err := os.Chdir(resolved); err != nil {
		t.Fatalf("chdir %s: %v", resolved, err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })
}

// TestRunFeedWatchAdvancesCursorAndDoesNotRepeat is the cursor-mode
// equivalent of the old "no dedupe re-render" test. It pins three things
// at once because they are the same property viewed from three angles:
//
//  1. Bootstrap call sends no cursor and wait=0s.
//  2. Each subsequent call sends the cursor returned by the previous call.
//  3. Each item appears in output exactly once (the cursor advanced past
//     it; the server has no reason to send it again).
//
// If we ever regress to "repaginate the same window each tick," this
// test catches it because the recorded cursor sequence flattens into
// repeats and the output gains duplicate lines.
// sseTestServer constructs an httptest.Server that speaks SSE. Each
// connection runs handler with a frame writer; the test owns when to
// emit and when to close the connection by returning from handler.
//
// The test server flushes after each call to writeFrame so the client
// sees frames immediately, mirroring how the real server pushes.
func sseTestServer(t *testing.T, handler func(t *testing.T, r *http.Request, write func(id, data string), close func())) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		closed := false
		write := func(id, data string) {
			if closed {
				return
			}
			fmt.Fprintf(w, "id: %s\nevent: feed\ndata: %s\n\n", id, data)
			flusher.Flush()
		}
		closeFn := func() { closed = true }
		handler(t, r, write, closeFn)
	}))
}

// TestRunFeedWatchSSEDeliversInOrderAndAdvancesLastID pins the core
// happy path of the SSE watcher. Two SSE frames are pushed on a single
// connection that stays open until ctx cancels — no reconnect, so each
// event is delivered exactly once. The first-connect Last-Event-ID is
// captured to confirm a fresh watcher boots "from now" with no cursor.
func TestRunFeedWatchSSEDeliversInOrderAndAdvancesLastID(t *testing.T) {
	var (
		mu        sync.Mutex
		firstLEI  string
		firstAuth string
		captured  bool
	)
	srv := sseTestServer(t, func(t *testing.T, r *http.Request, write func(id, data string), closeFn func()) {
		mu.Lock()
		if !captured {
			firstLEI = r.Header.Get("Last-Event-ID")
			firstAuth = r.Header.Get("Authorization")
			captured = true
		}
		mu.Unlock()
		write("AAAAAAAAAAB",
			`{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11111111","kind":"work_item.created","payload":{"title":"alpha","state":"captured"}}`)
		write("AAAAAAAAAAC",
			`{"event_id":"e2","occurred_at":"2026-04-24T14:00:01Z","source":"system","subject_kind":"work_item","subject_id":"22222222","kind":"work_item.transitioned","payload":{"to":"triaged"}}`)
		// Block until the client closes the connection (ctx cancellation
		// in the watcher closes it via http.Client). This prevents the
		// watcher from reconnecting and replaying frames as duplicates.
		<-r.Context().Done()
	})
	defer srv.Close()

	client := &feedClient{
		baseURL: srv.URL, token: "tok",
		http:       srv.Client(),
		streamHTTP: srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	_ = runFeedWatch(ctx, nil, client, watchOptions{
		retryBackoff: 5 * time.Millisecond,
	}, &buf)

	mu.Lock()
	defer mu.Unlock()
	if firstLEI != "" {
		t.Errorf("first connect should have empty Last-Event-ID, got %q", firstLEI)
	}
	if firstAuth != "Bearer tok" {
		t.Errorf("Authorization header = %q, want %q", firstAuth, "Bearer tok")
	}
	out := buf.String()
	if strings.Count(out, "alpha") != 1 {
		t.Errorf("expected exactly one alpha line, output:\n%s", out)
	}
	if strings.Count(out, "→ triaged") != 1 {
		t.Errorf("expected exactly one triaged line, output:\n%s", out)
	}
}

// TestRunFeedWatchSSEReconnectsWithLastEventID pins the resume-after-
// disconnect contract. First connection emits one event then closes;
// the watcher must reconnect carrying that event's id as Last-Event-ID
// so the server can replay anything that landed during the gap.
func TestRunFeedWatchSSEReconnectsWithLastEventID(t *testing.T) {
	var (
		mu             sync.Mutex
		connectCount   int
		secondLEI      string
		secondLEIOnce  sync.Once
		secondReached  = make(chan struct{})
	)
	srv := sseTestServer(t, func(t *testing.T, r *http.Request, write func(id, data string), closeFn func()) {
		mu.Lock()
		connectCount++
		idx := connectCount
		mu.Unlock()

		if idx == 1 {
			write("FIRSTEVENTID",
				`{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11","kind":"work_item.created","payload":{"title":"first","state":"captured"}}`)
			// Give the client time to receive and process the frame,
			// then drop the connection by returning. The watcher's
			// outer loop will reconnect carrying FIRSTEVENTID.
			time.Sleep(60 * time.Millisecond)
			return
		}

		secondLEIOnce.Do(func() {
			lei := r.Header.Get("Last-Event-ID")
			mu.Lock()
			secondLEI = lei
			mu.Unlock()
			close(secondReached)
		})
		// Hold the second connection open so the watcher doesn't loop
		// past it and clobber what we recorded.
		<-r.Context().Done()
	})
	defer srv.Close()

	client := &feedClient{
		baseURL: srv.URL, token: "tok",
		http:       srv.Client(),
		streamHTTP: srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	_ = runFeedWatch(ctx, nil, client, watchOptions{
		retryBackoff: 5 * time.Millisecond,
	}, &buf)

	select {
	case <-secondReached:
	case <-time.After(400 * time.Millisecond):
		t.Fatal("server never received the reconnect")
	}

	mu.Lock()
	defer mu.Unlock()
	if secondLEI != "FIRSTEVENTID" {
		t.Errorf("reconnect Last-Event-ID = %q, want %q (the cursor of the last delivered event)", secondLEI, "FIRSTEVENTID")
	}
	out := buf.String()
	if !strings.Contains(out, "first") {
		t.Errorf("first event should be printed, output:\n%s", out)
	}
}

// TestRunFeedWatchSSERecoversFromInvalidCursor pins the recovery
// contract. A 400 invalid_cursor response on connect must NOT kill the
// watcher session: the loop drops its lastID and reconnects from now,
// so a stale cursor surviving an encoding bump or a server restart
// causes a short gap, not a crash.
//
// Connection sequence:
//   1: stream one event with id=STALECURSOR, drop
//   2: receive STALECURSOR, return 400 invalid_cursor
//   3: receive empty LEI (recovery dropped it), stream post-recovery event
func TestRunFeedWatchSSERecoversFromInvalidCursor(t *testing.T) {
	var (
		mu               sync.Mutex
		connects         int
		thirdLEI         string
		thirdLEIOnce     sync.Once
		thirdReached     = make(chan struct{})
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connects++
		idx := connects
		mu.Unlock()
		lei := r.Header.Get("Last-Event-ID")

		switch idx {
		case 1:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			flusher.Flush()
			fmt.Fprintf(w, "id: STALECURSOR\nevent: feed\ndata: %s\n\n",
				`{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11","kind":"work_item.created","payload":{"title":"pre","state":"captured"}}`)
			flusher.Flush()
			time.Sleep(60 * time.Millisecond)
		case 2:
			if lei != "STALECURSOR" {
				t.Errorf("connect 2 LEI = %q, want STALECURSOR", lei)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_cursor","message":"cursor is malformed"}}`))
		default:
			thirdLEIOnce.Do(func() {
				mu.Lock()
				thirdLEI = lei
				mu.Unlock()
				close(thirdReached)
			})
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			flusher.Flush()
			fmt.Fprintf(w, "id: POSTRECVCURSR\nevent: feed\ndata: %s\n\n",
				`{"event_id":"e2","occurred_at":"2026-04-24T14:00:05Z","source":"system","subject_kind":"work_item","subject_id":"33","kind":"work_item.created","payload":{"title":"post-recovery","state":"captured"}}`)
			flusher.Flush()
			<-r.Context().Done()
		}
	}))
	defer srv.Close()

	client := &feedClient{
		baseURL: srv.URL, token: "tok",
		http:       srv.Client(),
		streamHTTP: srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	_ = runFeedWatch(ctx, nil, client, watchOptions{
		retryBackoff: 5 * time.Millisecond,
	}, &buf)

	select {
	case <-thirdReached:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watcher never reached the post-recovery connect")
	}

	mu.Lock()
	defer mu.Unlock()
	if thirdLEI != "" {
		t.Errorf("connect 3 LEI = %q, want empty (recovery dropped stale cursor)", thirdLEI)
	}
	out := buf.String()
	if !strings.Contains(out, "post-recovery") {
		t.Errorf("expected post-recovery item in output, got:\n%s", out)
	}
}

// TestRunFeedWatchFiltersMentions pins the --mentions filter behavior:
// items that mention the named recipient are printed; everything else
// is dropped silently. lastID still advances past dropped items
// (otherwise dropped events on a noisy feed would re-deliver on every
// reconnect).
func TestRunFeedWatchFiltersMentions(t *testing.T) {
	srv := sseTestServer(t, func(t *testing.T, r *http.Request, write func(id, data string), closeFn func()) {
		write("ID-OTHER",
			`{"event_id":"e-other","occurred_at":"2026-04-24T14:00:00Z","source":"agent","subject_kind":"work_item","subject_id":"11","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-B","text":"unrelated chatter"}}}`)
		write("ID-DIRECT",
			`{"event_id":"e-direct","occurred_at":"2026-04-24T14:00:01Z","source":"agent","subject_kind":"work_item","subject_id":"22","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-A","text":"a note authored by A"}}}`)
		write("ID-MENTION",
			`{"event_id":"e-mention","occurred_at":"2026-04-24T14:00:02Z","source":"agent","subject_kind":"work_item","subject_id":"33","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-B","text":"hey @agent-A please look"}}}`)
		// Hold the connection until ctx cancels so the watcher doesn't
		// reconnect and re-deliver these events.
		_ = closeFn
		<-r.Context().Done()
	})
	defer srv.Close()

	client := &feedClient{
		baseURL: srv.URL, token: "tok",
		http:       srv.Client(),
		streamHTTP: srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	_ = runFeedWatch(ctx, nil, client, watchOptions{
		mentions:     []string{"agent-A"},
		retryBackoff: 5 * time.Millisecond,
	}, &buf)

	out := buf.String()
	if strings.Contains(out, "unrelated chatter") {
		t.Errorf("unrelated note leaked through filter, output:\n%s", out)
	}
	if !strings.Contains(out, "a note authored by A") {
		t.Errorf("authored-by-A note should match, output:\n%s", out)
	}
	if !strings.Contains(out, "hey @agent-A please look") {
		t.Errorf("@agent-A mention should match, output:\n%s", out)
	}
}

// TestMatchesMentions covers the small matching matrix at unit level
// independent of the watch loop. Direct payload, mentions array,
// @-syntax in text/note, and recursion into the event_appended inner
// envelope are all exercised.
func TestMatchesMentions(t *testing.T) {
	cases := []struct {
		name     string
		payload  string
		mentions []string
		want     bool
	}{
		{
			name:     "no mentions filter matches everything",
			payload:  `{"text":"random"}`,
			mentions: nil,
			want:     true,
		},
		{
			name:     "author equality matches",
			payload:  `{"author":"agent-A","text":"x"}`,
			mentions: []string{"agent-A"},
			want:     true,
		},
		{
			name:     "author mismatch does not match",
			payload:  `{"author":"agent-B","text":"x"}`,
			mentions: []string{"agent-A"},
			want:     false,
		},
		{
			name:     "mentions array hit",
			payload:  `{"author":"agent-B","mentions":["agent-A","agent-C"]}`,
			mentions: []string{"agent-A"},
			want:     true,
		},
		{
			name:     "@name in text body",
			payload:  `{"author":"agent-B","text":"poking @agent-A on this"}`,
			mentions: []string{"agent-A"},
			want:     true,
		},
		{
			name:     "@name in note field also matches",
			payload:  `{"note":"see @agent-A"}`,
			mentions: []string{"agent-A"},
			want:     true,
		},
		{
			name:     "recurses into event_appended inner",
			payload:  `{"inner_kind":"work_item.note_added","inner":{"author":"agent-A","text":"hi"}}`,
			mentions: []string{"agent-A"},
			want:     true,
		},
		{
			name:     "bare-name (no @) does NOT match (avoids false positives)",
			payload:  `{"text":"agent-A is fine"}`,
			mentions: []string{"agent-A"},
			want:     false,
		},
		{
			name:     "any-of: matches if at least one name hits",
			payload:  `{"author":"agent-B"}`,
			mentions: []string{"agent-A", "agent-B"},
			want:     true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := feedItem{Payload: json.RawMessage(c.payload)}
			if got := matchesMentions(it, c.mentions); got != c.want {
				t.Errorf("matchesMentions(%s, %v) = %v, want %v", c.payload, c.mentions, got, c.want)
			}
		})
	}
}

// TestFeedClientFetchPageHonorsCursorAndWait pins the wire shape for the
// TestConsumeStreamSendsLastEventIDAndAuth pins the stream-connect
// contract independent of the watcher loop: Authorization carries the
// Bearer, Last-Event-ID rides on the request when supplied, frames are
// parsed into sseEvent, and the cursor of the last frame is returned
// for the watcher's resume bookkeeping.
func TestConsumeStreamSendsLastEventIDAndAuth(t *testing.T) {
	var (
		gotAuth string
		gotLEI  string
		gotAcc  string
	)
	srv := sseTestServer(t, func(t *testing.T, r *http.Request, write func(id, data string), _ func()) {
		gotAuth = r.Header.Get("Authorization")
		gotLEI = r.Header.Get("Last-Event-ID")
		gotAcc = r.Header.Get("Accept")
		write("CURFINAL",
			`{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11","kind":"work_item.created","payload":{"title":"x","state":"captured"}}`)
		<-r.Context().Done()
	})
	defer srv.Close()

	c := &feedClient{
		baseURL: srv.URL, token: "mrs_secret",
		http:       srv.Client(),
		streamHTTP: srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var got []sseEvent
	lastID, _ := c.consumeStream(ctx, "PRIORCURSOR", func(ev sseEvent) error {
		got = append(got, ev)
		return nil
	})

	if gotAuth != "Bearer mrs_secret" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer mrs_secret")
	}
	if gotLEI != "PRIORCURSOR" {
		t.Errorf("Last-Event-ID = %q, want %q", gotLEI, "PRIORCURSOR")
	}
	if gotAcc != "text/event-stream" {
		t.Errorf("Accept = %q, want %q", gotAcc, "text/event-stream")
	}
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].ID != "CURFINAL" {
		t.Errorf("event id = %q, want %q", got[0].ID, "CURFINAL")
	}
	if got[0].Item.EventID != "e1" {
		t.Errorf("event_id = %q, want %q", got[0].Item.EventID, "e1")
	}
	if lastID != "CURFINAL" {
		t.Errorf("returned lastID = %q, want %q", lastID, "CURFINAL")
	}
}

func TestParseMentions(t *testing.T) {
	cases := map[string][]string{
		"":                       nil,
		"agent-A":                {"agent-A"},
		"agent-A,agent-B":        {"agent-A", "agent-B"},
		" agent-A , agent-B ":    {"agent-A", "agent-B"},
		"a,,b":                   {"a", "b"},
		strings.Repeat(",", 10):  nil,
	}
	for in, want := range cases {
		got := parseMentions(in)
		if len(got) != len(want) {
			t.Errorf("parseMentions(%q) = %v, want %v", in, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("parseMentions(%q)[%d] = %q, want %q", in, i, got[i], want[i])
			}
		}
	}
}
