// `wayline feed` is the human-facing read of the activity log: a tiny
// terminal renderer for the same events /v1/feed serves to integrators.
//
// The subcommand exists because the system was JSON-API-first by design,
// which left no comfortable seat for the owner. Until the web UI substrate
// item ships, this gives a person at a terminal a readable view of what
// the system has been up to. It owns no state, has no special privileges,
// and runs nowhere persistent.
//
// Two modes:
//
//   wayline feed                snapshot of the last --limit items, exit
//   wayline feed --watch        consume the SSE push stream at /v1/feed/stream;
//                               server pushes each new event as it lands,
//                               client prints (after optional --mentions filter).
//                               Reconnects with Last-Event-ID on disconnect
//                               so events that landed during the gap are
//                               replayed deterministically.
//
// Watch mode uses Server-Sent Events (a11dd7d5). The model is push, not
// pull: the client opens one long-lived connection, the server writes
// SSE frames as events happen, and the client never re-asks for events
// it has already seen. Reconnects send Last-Event-ID (the SSE-standard
// header) carrying the last cursor we processed, so the server replays
// from there before resuming live push. No polling loop, no client-side
// dedupe map, no per-poll re-read of payload state already in context.
//
// Optional client-side filter:
//
//   --mentions=agent-A         only print items that mention any of the
//   --mentions=agent-A,me      named recipients. Matches on payload.author
//                              equality, payload.mentions array membership,
//                              and "@name" appearing in text/note fields.
//                              Recurses into work_item.event_appended.inner
//                              so notes posted as appended sub-events are
//                              filtered the same as direct payloads.
//                              The cursor still advances past dropped events,
//                              so reconnects don't re-deliver them.
//
// Output format is one line per event, in arrival order (which is
// chronologically equivalent — the SSE stream emits oldest-first by
// events.seq). Per-kind summaries (work_item.created, transitioned,
// patience.breached, signal.received, message.captured, etc.) are
// formatted in summarize(); unknown kinds fall back to the raw kind
// string so a new event kind shipping without renderer support is loud-but-
// not-broken.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func runFeed(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("feed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	api := fs.String("api", "http://127.0.0.1:8080", "wayline API base URL")
	limit := fs.Int("limit", 20, "number of feed items to fetch (snapshot mode)")
	watch := fs.Bool("watch", false, "consume the SSE push stream and append new items (Ctrl-C to exit)")
	mentions := fs.String("mentions", "", "comma-separated names; in --watch mode, only print items that mention any of them")
	// retryBackoff caps how often we reconnect on transient SSE failures.
	// The healthy --watch path holds one long-lived connection and never
	// touches this; only network blips and server restarts reach it.
	retryBackoff := fs.Duration("interval", 2*time.Second, "reconnect backoff when the SSE stream drops in --watch mode")
	if err := fs.Parse(args); err != nil {
		feedUsage(os.Stderr)
		return err
	}

	token, source, err := resolveFeedToken()
	if err != nil {
		return err
	}
	if source != "" && source != "WAYLINE_TOKEN" {
		fmt.Fprintf(os.Stderr, "feed: using token from %s\n", source)
	}

	// Two clients with different timeout disciplines:
	//   - snapshot HTTP: bounded 30s; a snapshot read should complete fast.
	//   - stream HTTP: NO Timeout; SSE connections are designed to be long-
	//     lived. Cancellation comes from ctx, not from a wallclock cap.
	//     A timeout would silently kill healthy streams the moment they
	//     went idle past the threshold.
	client := &feedClient{
		baseURL:    strings.TrimRight(*api, "/"),
		token:      token,
		http:       &http.Client{Timeout: 30 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}

	if !*watch {
		items, err := client.fetch(ctx, *limit)
		if err != nil {
			return err
		}
		printItems(os.Stdout, items)
		return nil
	}

	return runFeedWatch(ctx, logger, client, watchOptions{
		mentions:     parseMentions(*mentions),
		retryBackoff: *retryBackoff,
	}, os.Stdout)
}

func parseMentions(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if name := strings.TrimSpace(p); name != "" {
			out = append(out, name)
		}
	}
	return out
}

type watchOptions struct {
	mentions     []string
	retryBackoff time.Duration
}

// runFeedWatch consumes /v1/feed/stream over SSE. Sequence:
//
//  1. Open the stream with no Last-Event-ID. The server's
//     ResolveStreamStart treats the absence of a cursor as "from now"
//     (its current MAX(seq)), so a fresh watcher does not dump history.
//  2. Read SSE frames as they arrive: each event has an `id:` line
//     (the v1 opaque cursor) and a `data:` line (the JSON envelope).
//     Apply the mentions filter, print, and advance lastID. Heartbeat
//     comments (lines starting with `:`) reset our liveness timer but
//     produce no output.
//  3. On disconnect or error: backoff, then reconnect with the saved
//     lastID as the Last-Event-ID header. The server replays events
//     strictly after that cursor, so anything that landed during the
//     gap is delivered before live push resumes. A 400 invalid_cursor
//     (server forgot the seq, or encoding rolled over) is recovered by
//     reconnecting without lastID rather than crashing the session.
//
// The mentions filter is intentionally client-side: the server sends
// every visible event, the client decides which to print. This keeps
// the stream's framing identical for all consumers (so a future shared
// broadcaster can fan one stream to many filters) and keeps dropped
// events from creating gaps in the cursor (the client still advances
// past them so reconnects don't redeliver).
func runFeedWatch(ctx context.Context, logger *slog.Logger, client *feedClient, opts watchOptions, out io.Writer) error {
	var lastID string
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		newLastID, err := client.consumeStream(ctx, lastID, func(ev sseEvent) error {
			if matchesMentions(ev.Item, opts.mentions) {
				fmt.Fprintln(out, formatItem(ev.Item))
			}
			return nil
		})
		// Always preserve the last cursor we processed, even if the
		// connection ended in error. The next reconnect will resume
		// from there. Empty newLastID means we reconnect "from now".
		if newLastID != "" {
			lastID = newLastID
		}

		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}

		if isInvalidCursorErr(err) {
			if logger != nil {
				logger.Warn("feed --watch: server rejected resume cursor, restarting from now",
					slog.String("last_id", lastID))
			}
			lastID = ""
		} else if logger != nil {
			logger.Warn("feed --watch: stream ended, reconnecting",
				slog.String("error", err.Error()),
				slog.String("backoff", opts.retryBackoff.String()))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.retryBackoff):
		}
	}
}

// matchesMentions is the client-side filter for --mentions. The empty
// list short-circuits to "everything matches" so unfiltered watchers
// keep their current behavior.
//
// Match sources, in order of preference:
//
//   - payload.author == name (note attribution; current convention used by
//     coord-thread notes posted by agent-A and agent-B)
//   - payload.mentions contains name (future-proof for the @mentions
//     payload field convention sketched in d56a0bc3)
//   - payload.text or payload.note contains "@name" (explicit mention
//     syntax in free-text bodies)
//   - For work_item.event_appended, recurse into payload.inner so notes
//     posted via the appended-event path filter the same as direct payloads
//
// The recursion is intentional and bounded: event_appended envelopes
// only nest one level (there's no event_appended-of-event_appended path
// in the model), so this terminates.
func matchesMentions(it feedItem, mentions []string) bool {
	if len(mentions) == 0 {
		return true
	}
	var probe map[string]any
	if err := json.Unmarshal(it.Payload, &probe); err != nil {
		return false
	}
	return matchesMentionsInPayload(probe, mentions)
}

func matchesMentionsInPayload(probe map[string]any, mentions []string) bool {
	if author, ok := probe["author"].(string); ok {
		for _, m := range mentions {
			if author == m {
				return true
			}
		}
	}
	if list, ok := probe["mentions"].([]any); ok {
		for _, raw := range list {
			if name, ok := raw.(string); ok {
				for _, m := range mentions {
					if name == m {
						return true
					}
				}
			}
		}
	}
	for _, key := range []string{"text", "note"} {
		if v, ok := probe[key].(string); ok {
			for _, m := range mentions {
				if strings.Contains(v, "@"+m) {
					return true
				}
			}
		}
	}
	if inner, ok := probe["inner"].(map[string]any); ok {
		if matchesMentionsInPayload(inner, mentions) {
			return true
		}
	}
	return false
}

func isInvalidCursorErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "invalid_cursor") || strings.Contains(msg, "400")
}

func printItems(out io.Writer, items []feedItem) {
	sort.Slice(items, func(i, j int) bool { return items[i].OccurredAt.Before(items[j].OccurredAt) })
	for _, it := range items {
		fmt.Fprintln(out, formatItem(it))
	}
}

// feedItem is the wire shape of one /v1/feed entry, narrowed to the
// fields the renderer cares about. The full envelope lives server-side.
type feedItem struct {
	EventID     string          `json:"event_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Source      string          `json:"source"`
	SubjectKind string          `json:"subject_kind"`
	SubjectID   string          `json:"subject_id"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
}

// feedClient is the smallest possible HTTP client for /v1/feed. Inlined
// (rather than going through pkg/wayline) because the SDK currently only
// covers the write paths integrators most need; growing it to cover GETs
// and streaming is its own slice. If that slice lands, lift this code
// into the SDK and have the CLI call it.
//
// http is for short-lived snapshot reads (Timeout enforced).
// streamHTTP is for the long-lived SSE connection (no Timeout — only
// ctx cancellation can end it). Splitting them keeps the snapshot path
// safe from a hung server while letting the stream live indefinitely.
type feedClient struct {
	baseURL    string
	token      string
	http       *http.Client
	streamHTTP *http.Client
}

func (c *feedClient) fetch(ctx context.Context, limit int) ([]feedItem, error) {
	u, err := url.Parse(c.baseURL + "/v1/feed")
	if err != nil {
		return nil, fmt.Errorf("feed: parse api URL: %w", err)
	}
	q := u.Query()
	q.Set("limit", fmt.Sprintf("%d", limit))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("feed: request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("feed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var envelope struct {
		Items []feedItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("feed: decode response: %w", err)
	}
	return envelope.Items, nil
}

// sseEvent is one frame from /v1/feed/stream after parsing.
type sseEvent struct {
	ID   string
	Item feedItem
}

// consumeStream opens GET /v1/feed/stream and calls onEvent for each
// frame the server pushes. lastID, if non-empty, is sent as the
// SSE-standard Last-Event-ID header so the server replays from that
// point before resuming live push.
//
// Returns the cursor of the most recent successfully-handled event
// (so the caller can resume there even when the connection ended in
// error) and the terminal error, if any. A clean disconnect (server
// shutdown) returns a non-nil error too — at this layer there is no
// "the stream is supposed to end."
//
// SSE wire parsing per https://html.spec.whatwg.org/multipage/server-sent-events.html
// (stripped to what our server actually emits):
//
//	id: <opaque cursor>     # last-event-id for this event
//	event: feed             # event type (we ignore; assume "feed")
//	data: <one-line json>   # payload
//	(blank line)            # dispatch
//
// Comment lines start with `:` and serve as keepalive only. Lines we
// don't recognize are tolerated (forward-compat with future SSE fields
// the server might emit, e.g. retry:).
func (c *feedClient) consumeStream(ctx context.Context, lastID string, onEvent func(sseEvent) error) (string, error) {
	u, err := url.Parse(c.baseURL + "/v1/feed/stream")
	if err != nil {
		return lastID, fmt.Errorf("feed: parse api URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return lastID, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "text/event-stream")
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}

	resp, err := c.streamHTTP.Do(req)
	if err != nil {
		return lastID, fmt.Errorf("feed: stream connect: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return lastID, fmt.Errorf("feed: stream %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	// Buffer sized for an event payload comfortably larger than typical
	// (most events are <2KB; cap at 1MB to bound damage from a runaway
	// payload while still tolerating large work-item bodies).
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var (
		curID    string
		curData  strings.Builder
		curEvent string
	)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			// blank line = dispatch
			if curData.Len() == 0 {
				curEvent = ""
				continue
			}
			ev, ok := parseSSEDispatch(curID, curEvent, curData.String())
			curData.Reset()
			curEvent = ""
			if !ok {
				continue
			}
			if ev.ID != "" {
				lastID = ev.ID
			}
			if err := onEvent(ev); err != nil {
				return lastID, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			// Comment / keepalive — ignore.
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		// SSE allows " " padding after the colon; strip exactly one space.
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			curID = value
		case "event":
			curEvent = value
		case "data":
			if curData.Len() > 0 {
				curData.WriteByte('\n')
			}
			curData.WriteString(value)
		default:
			// Tolerate unknown fields (forward-compat).
		}
	}
	if err := scanner.Err(); err != nil {
		return lastID, fmt.Errorf("feed: stream read: %w", err)
	}
	// Scanner returned cleanly (server closed). Treat as a connection
	// drop the caller should reconnect from. If ctx is the cause, the
	// outer loop checks ctx.Err() and exits gracefully.
	if ctx.Err() != nil {
		return lastID, ctx.Err()
	}
	return lastID, fmt.Errorf("feed: stream closed by server")
}

// parseSSEDispatch turns one accumulated SSE frame into a typed event.
// We ignore curEvent (the server only emits "feed"); a future kind
// dimension would key off it. Returns ok=false if the data isn't a
// recognizable feed envelope, which is logged-and-skipped rather than
// fatal so a malformed event from a future server version doesn't kill
// the watcher.
func parseSSEDispatch(id, _, data string) (sseEvent, bool) {
	var item feedItem
	if err := json.Unmarshal([]byte(data), &item); err != nil {
		return sseEvent{}, false
	}
	return sseEvent{ID: id, Item: item}, true
}

// formatItem renders one feed entry as a single fixed-shape line. Columns:
// "MM-DD HH:MM:SS  source  subject8  summary". The date prefix is included
// so an event from yesterday is unambiguous when the feed spans days.
func formatItem(it feedItem) string {
	ts := it.OccurredAt.Local().Format("01-02 15:04:05")
	src := padRight(safeSource(it.Source), 6)
	subj := shortID(it.SubjectID)
	return fmt.Sprintf("%s  %s  %s  %s", ts, src, subj, summarize(it))
}

// summarize is the per-kind renderer. New event kinds that ship without a
// case here render as their bare kind string; that is by design (loud-but-
// not-broken). When a new kind lands and stays around, add a case here.
func summarize(it feedItem) string {
	switch it.Kind {
	case "message.captured":
		var p struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		return fmt.Sprintf("inbox    \"%s\"", truncate(p.Text, 96))

	case "work_item.created":
		var p struct {
			Title string `json:"title"`
			State string `json:"state"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		state := p.State
		if state == "" {
			state = "captured"
		}
		return fmt.Sprintf("created  [%s] %s", state, truncate(p.Title, 90))

	case "work_item.transitioned":
		var p struct {
			From   string `json:"from"`
			To     string `json:"to"`
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		arrow := "→ " + p.To
		if p.From != "" {
			arrow = fmt.Sprintf("%s → %s", p.From, p.To)
		}
		if p.Reason != "" {
			return fmt.Sprintf("%-22s \"%s\"", arrow, truncate(p.Reason, 90))
		}
		return arrow

	case "work_item.event_appended":
		// Server wraps appended sub-events in an envelope:
		//   {"inner_kind": "...", "inner": {...}}
		// Surface inner_kind as the primary tag; if inner has a text/note
		// field, include a short excerpt so the reader gets at the gist.
		var env struct {
			InnerKind string          `json:"inner_kind"`
			Inner     json.RawMessage `json:"inner"`
		}
		_ = json.Unmarshal(it.Payload, &env)
		excerpt := excerptFromInner(env.Inner)
		if excerpt != "" {
			return fmt.Sprintf("fact     %s  \"%s\"", env.InnerKind, truncate(excerpt, 80))
		}
		return fmt.Sprintf("fact     %s", env.InnerKind)

	case "work_item.relation_added":
		var p struct {
			Kind  string `json:"kind"`
			Other string `json:"other_id"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		return fmt.Sprintf("rel      %s -> %s", p.Kind, shortID(p.Other))

	case "signal.received":
		var p struct {
			SignalKind string `json:"signal_kind"`
			DedupeKey  string `json:"dedupe_key"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		if p.DedupeKey != "" {
			return fmt.Sprintf("signal   %s  dedupe=%s", p.SignalKind, truncate(p.DedupeKey, 60))
		}
		return fmt.Sprintf("signal   %s", p.SignalKind)

	case "patience.breached":
		var p struct {
			State         string `json:"state"`
			BudgetSeconds int64  `json:"budget_seconds"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		return fmt.Sprintf("BREACH   state=%s budget=%s", p.State, fmtDuration(time.Duration(p.BudgetSeconds)*time.Second))

	default:
		return it.Kind
	}
}

// excerptFromInner pulls a short human excerpt out of the inner payload of a
// work_item.event_appended envelope. Probes the common gist-bearing fields in
// order; returns "" if none are present, in which case the renderer omits
// the excerpt entirely rather than printing an empty quoted string.
func excerptFromInner(inner json.RawMessage) string {
	if len(inner) == 0 {
		return ""
	}
	var probe map[string]any
	if err := json.Unmarshal(inner, &probe); err != nil {
		return ""
	}
	for _, key := range []string{"text", "note", "title", "summary", "reason"} {
		if v, ok := probe[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// fmtDuration prints a Duration in a compact human form: "60s", "5m",
// "2h", "3d". Used in BREACH summaries where the precise wall time is not
// what the human cares about.
func fmtDuration(d time.Duration) string {
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
}

func shortID(s string) string {
	if len(s) >= 8 {
		return s[:8]
	}
	return s
}

func safeSource(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func truncate(s string, n int) string {
	if n <= 1 || len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// resolveFeedToken returns a bearer to send on /v1/feed requests, plus a
// human-readable source string describing where the token came from (so the
// caller can announce it on stderr). Resolution order:
//
//  1. WAYLINE_TOKEN env var (explicit always wins; CI and ephemeral shells
//     should never have a file silently take precedence).
//  2. The first existing token file in priorityTokens, searched in the
//     nearest .wayline directory found by walking up from cwd. The walk is
//     bounded by filesystem-root convergence so it terminates on every OS.
//
// For /v1/feed any valid bearer works (no source restriction), so the
// priority list leads with root.token (highest authority, also source=human
// so it can also be reused for inbox capture). agent and seed follow as
// fallbacks for setups that ran bootstrap with --no-root or similar.
func resolveFeedToken() (token, source string, err error) {
	if t := strings.TrimSpace(os.Getenv("WAYLINE_TOKEN")); t != "" {
		return t, "WAYLINE_TOKEN", nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("feed: getwd: %w", err)
	}
	dir := findWaylineDir(cwd)
	if dir == "" {
		return "", "", fmt.Errorf("feed: WAYLINE_TOKEN not set and no .wayline/ directory found walking up from %s", cwd)
	}
	for _, name := range priorityTokens {
		path := filepath.Join(dir, name)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		t := strings.TrimSpace(string(data))
		if t == "" {
			continue
		}
		return t, path, nil
	}
	return "", "", fmt.Errorf("feed: WAYLINE_TOKEN not set and no usable token found in %s (looked for %s)",
		dir, strings.Join(priorityTokens, ", "))
}

// priorityTokens is the file-discovery order resolveFeedToken uses inside a
// .wayline/ directory. Order is most-permissive-first: root can do anything,
// agent-A is the standard non-root bearer minted by bootstrap, seed is the
// system token used by `wayline seed v1` and `wayline worker --once`.
var priorityTokens = []string{
	"root.token",
	"agent-A.token",
	"seed.token",
}

// findWaylineDir walks upward from start looking for a directory named
// `.wayline`. Returns the absolute path of the .wayline directory itself,
// or "" if none is found before the filesystem root. Termination is
// guaranteed by the parent==current convergence check (true on POSIX and
// Windows for the root path).
func findWaylineDir(start string) string {
	dir := start
	for {
		candidate := filepath.Join(dir, ".wayline")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func feedUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  wayline feed [--limit=N]
  wayline feed --watch [--mentions=NAME[,NAME...]]

Prints recent activity from the wayline event feed in a human-readable
form. Without --watch, fetches the last --limit items and exits. With
--watch, holds one long-lived connection to /v1/feed/stream (SSE) and
prints each event the server pushes, reconnecting with Last-Event-ID
on disconnect so events that landed during the gap are replayed.

Token resolution (first match wins):
  1. WAYLINE_TOKEN env var
  2. .wayline/{root.token, agent-A.token, seed.token} in the nearest
     .wayline directory found walking up from the current directory.
     The chosen path is announced on stderr.

Flags:
  --limit=N            items per snapshot fetch (default 20, server caps at 200)
  --watch              consume the SSE push stream (Ctrl-C to exit)
  --mentions=A,B       in --watch mode, only print items mentioning any of
                       these names (matched on payload.author, the
                       payload.mentions array, or "@name" in text/note)
  --interval=DURATION  reconnect backoff when the SSE stream drops in --watch
                       (default 2s); only network blips and server restarts
                       reach this — the healthy path holds one connection
  --api=URL            wayline API base URL (default http://127.0.0.1:8080)

Output is one line per event, in arrival order (oldest-first by events.seq).
Format:
  MM-DD HH:MM:SS  source  subj8  per-kind summary
`)
}
