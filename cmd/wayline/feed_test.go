package main

import (
	"bytes"
	"context"
	"encoding/json"
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

	c := &feedClient{baseURL: srv.URL, token: "wln_secret", http: srv.Client()}
	items, err := c.fetch(context.Background(), 33)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if gotAuth != "Bearer wln_secret" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer wln_secret")
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
// .wayline/ on disk has tokens that would otherwise be picked up,
// WAYLINE_TOKEN takes precedence and source is reported as "WAYLINE_TOKEN".
func TestResolveFeedTokenEnvWins(t *testing.T) {
	dir := t.TempDir()
	wln := filepath.Join(dir, ".wayline")
	if err := os.MkdirAll(wln, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wln, "root.token"), []byte("file-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	withCwd(t, dir)

	t.Setenv("WAYLINE_TOKEN", "env-token")
	tok, src, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "env-token" {
		t.Errorf("token = %q, want %q (env should win over file)", tok, "env-token")
	}
	if src != "WAYLINE_TOKEN" {
		t.Errorf("source = %q, want %q", src, "WAYLINE_TOKEN")
	}
}

// TestResolveFeedTokenWalksUp pins the walk-upward discovery: a .wayline/
// at the project root is found from a deep subdirectory.
func TestResolveFeedTokenWalksUp(t *testing.T) {
	root := t.TempDir()
	wln := filepath.Join(root, ".wayline")
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
	t.Setenv("WAYLINE_TOKEN", "")

	tok, src, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "found-it" {
		t.Errorf("token = %q, want %q", tok, "found-it")
	}
	if !strings.HasSuffix(src, ".wayline/root.token") {
		t.Errorf("source = %q, want suffix .wayline/root.token", src)
	}
}

// TestResolveFeedTokenPriorityOrder pins that root.token is preferred over
// agent-A.token when both exist, matching the documented order.
func TestResolveFeedTokenPriorityOrder(t *testing.T) {
	dir := t.TempDir()
	wln := filepath.Join(dir, ".wayline")
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
	t.Setenv("WAYLINE_TOKEN", "")

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
	wln := filepath.Join(dir, ".wayline")
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
	t.Setenv("WAYLINE_TOKEN", "")

	tok, _, err := resolveFeedToken()
	if err != nil {
		t.Fatalf("resolveFeedToken: %v", err)
	}
	if tok != "agent" {
		t.Errorf("token = %q, want %q (whitespace-only root.token should fall through)", tok, "agent")
	}
}

// TestResolveFeedTokenNoSources pins the error path: with no env var and
// no .wayline directory anywhere up the tree, the resolver returns a clear
// error rather than a usable empty token.
func TestResolveFeedTokenNoSources(t *testing.T) {
	dir := t.TempDir()
	withCwd(t, dir)
	t.Setenv("WAYLINE_TOKEN", "")

	_, _, err := resolveFeedToken()
	if err == nil {
		t.Fatal("expected error when no env and no .wayline, got nil")
	}
	if !strings.Contains(err.Error(), "no .wayline/") {
		t.Errorf("error should mention missing .wayline directory; got: %v", err)
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
func TestRunFeedWatchAdvancesCursorAndDoesNotRepeat(t *testing.T) {
	var (
		mu          sync.Mutex
		callCursors []string
		callWaits   []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCursors = append(callCursors, r.URL.Query().Get("cursor"))
		callWaits = append(callWaits, r.URL.Query().Get("wait"))
		callIdx := len(callCursors)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch callIdx {
		case 1:
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":"cur-after-bootstrap","has_more":false}`))
		case 2:
			_, _ = w.Write([]byte(`{"items":[
				{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11111111","kind":"work_item.created","payload":{"title":"alpha","state":"captured"}}
			],"next_cursor":"cur-after-e1","has_more":false}`))
		case 3:
			_, _ = w.Write([]byte(`{"items":[
				{"event_id":"e2","occurred_at":"2026-04-24T14:00:01Z","source":"system","subject_kind":"work_item","subject_id":"22222222","kind":"work_item.transitioned","payload":{"to":"triaged"}}
			],"next_cursor":"cur-after-e2","has_more":false}`))
		default:
			// Empty page with cursor unchanged: this is what the real
			// server returns when the long-poll deadline fires with no
			// new events. The watcher must just go around again with the
			// same cursor, NOT retreat to the bootstrap path.
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":"cur-after-e2","has_more":false}`))
		}
	}))
	defer srv.Close()

	client := &feedClient{baseURL: srv.URL, token: "tok", http: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	if err := runFeedWatch(ctx, nil, client, watchOptions{
		limit:        50,
		wait:         10 * time.Millisecond,
		retryBackoff: 5 * time.Millisecond,
	}, &buf); err != nil {
		t.Fatalf("runFeedWatch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(callCursors) < 3 {
		t.Fatalf("expected at least 3 calls (bootstrap + 2 polls), got %d (cursors=%v)", len(callCursors), callCursors)
	}
	if callCursors[0] != "" {
		t.Errorf("call 1 (bootstrap) cursor = %q, want empty", callCursors[0])
	}
	if callWaits[0] != "0s" {
		t.Errorf("call 1 wait = %q, want %q", callWaits[0], "0s")
	}
	if callCursors[1] != "cur-after-bootstrap" {
		t.Errorf("call 2 cursor = %q, want %q", callCursors[1], "cur-after-bootstrap")
	}
	if callCursors[2] != "cur-after-e1" {
		t.Errorf("call 3 cursor = %q, want %q (cursor must advance after a non-empty page)", callCursors[2], "cur-after-e1")
	}

	out := buf.String()
	if strings.Count(out, "alpha") != 1 {
		t.Errorf("expected exactly one alpha line, output:\n%s", out)
	}
	if strings.Count(out, "→ triaged") != 1 {
		t.Errorf("expected exactly one triaged line, output:\n%s", out)
	}
}

// TestRunFeedWatchRecoversFromInvalidCursor pins the recovery contract.
// A 400 invalid_cursor response from the server (cursor decode failure,
// encoding rolled over, etc.) must NOT kill the watcher session. The
// loop re-bootstraps a fresh head and continues; the user sees a
// short gap, not a crash.
//
// Why it matters: cursor opacity is contractual but the encoding can
// change. Without this recovery path, every encoding bump would break
// every running --watch session.
func TestRunFeedWatchRecoversFromInvalidCursor(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		idx := calls
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch idx {
		case 1:
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":"stale-cursor","has_more":false}`))
		case 2:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_cursor","message":"cursor is malformed"}}`))
		case 3:
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":"fresh-cursor","has_more":false}`))
		default:
			_, _ = w.Write([]byte(`{"items":[
				{"event_id":"after-recovery","occurred_at":"2026-04-24T14:00:05Z","source":"system","subject_kind":"work_item","subject_id":"33333333","kind":"work_item.created","payload":{"title":"post-recovery","state":"captured"}}
			],"next_cursor":"cur-after-recovery","has_more":false}`))
		}
	}))
	defer srv.Close()

	client := &feedClient{baseURL: srv.URL, token: "tok", http: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	if err := runFeedWatch(ctx, nil, client, watchOptions{
		limit:        50,
		wait:         10 * time.Millisecond,
		retryBackoff: 5 * time.Millisecond,
	}, &buf); err != nil {
		t.Fatalf("runFeedWatch returned error instead of recovering: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls < 4 {
		t.Errorf("expected at least 4 calls (bootstrap, 400, re-bootstrap, success), got %d", calls)
	}
	if !strings.Contains(buf.String(), "post-recovery") {
		t.Errorf("expected post-recovery item in output, got:\n%s", buf.String())
	}
}

// TestRunFeedWatchFiltersMentions pins the --mentions filter behavior:
// items that mention the named recipient are printed; everything else
// is dropped silently. The cursor still advances past dropped items
// (otherwise dropped events would prevent the watcher from making
// progress at all on a noisy feed).
func TestRunFeedWatchFiltersMentions(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		idx := calls
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch idx {
		case 1:
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":"c0","has_more":false}`))
		default:
			_, _ = w.Write([]byte(`{"items":[
				{"event_id":"e-other","occurred_at":"2026-04-24T14:00:00Z","source":"agent","subject_kind":"work_item","subject_id":"11","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-B","text":"unrelated chatter"}}},
				{"event_id":"e-direct","occurred_at":"2026-04-24T14:00:01Z","source":"agent","subject_kind":"work_item","subject_id":"22","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-A","text":"a note authored by A"}}},
				{"event_id":"e-mention","occurred_at":"2026-04-24T14:00:02Z","source":"agent","subject_kind":"work_item","subject_id":"33","kind":"work_item.event_appended","payload":{"inner_kind":"work_item.note_added","inner":{"author":"agent-B","text":"hey @agent-A please look"}}}
			],"next_cursor":"c1","has_more":false}`))
		}
	}))
	defer srv.Close()

	client := &feedClient{baseURL: srv.URL, token: "tok", http: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	if err := runFeedWatch(ctx, nil, client, watchOptions{
		limit:        50,
		wait:         10 * time.Millisecond,
		mentions:     []string{"agent-A"},
		retryBackoff: 5 * time.Millisecond,
	}, &buf); err != nil {
		t.Fatalf("runFeedWatch: %v", err)
	}

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
// watcher-mode endpoint: cursor and wait both ride as query params,
// Authorization carries the Bearer, and the response decodes into the
// feedPage struct (items + next_cursor + has_more).
func TestFeedClientFetchPageHonorsCursorAndWait(t *testing.T) {
	var (
		gotCursor string
		gotWait   string
		gotLimit  string
		gotAuth   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCursor = r.URL.Query().Get("cursor")
		gotWait = r.URL.Query().Get("wait")
		gotLimit = r.URL.Query().Get("limit")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[
			{"event_id":"e1","occurred_at":"2026-04-24T14:00:00Z","source":"system","subject_kind":"work_item","subject_id":"11","kind":"work_item.created","payload":{"title":"x","state":"captured"}}
		],"next_cursor":"abc123","has_more":true}`))
	}))
	defer srv.Close()

	c := &feedClient{baseURL: srv.URL, token: "wln_secret", http: srv.Client()}
	page, err := c.fetchPage(context.Background(), "prev-cursor", 30*time.Second, 25)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if gotAuth != "Bearer wln_secret" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer wln_secret")
	}
	if gotCursor != "prev-cursor" {
		t.Errorf("cursor = %q, want %q", gotCursor, "prev-cursor")
	}
	if gotWait != "30s" {
		t.Errorf("wait = %q, want %q", gotWait, "30s")
	}
	if gotLimit != "25" {
		t.Errorf("limit = %q, want %q", gotLimit, "25")
	}
	if page.NextCursor != "abc123" {
		t.Errorf("next_cursor = %q, want %q", page.NextCursor, "abc123")
	}
	if !page.HasMore {
		t.Errorf("has_more = false, want true")
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(page.Items))
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
