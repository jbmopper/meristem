// `wayline feed` is the human-facing read of the activity log: a tiny
// terminal renderer for the same events /v1/feed serves to integrators.
//
// The subcommand exists because the system was JSON-API-first by design,
// which left no comfortable seat for the owner. Until the web UI substrate
// item ships, this gives a person at a terminal a readable view of what
// the system has been up to. It owns no state, has no special privileges,
// and runs nowhere persistent: it is just a polling client of /v1/feed
// formatted for eyes.
//
// Two modes:
//
//   wayline feed                snapshot of the last --limit items, exit
//   wayline feed --watch        bootstrap "from now" via opaque cursor,
//                               then long-poll /v1/feed?cursor=...&wait=...
//                               and append matching new items as they arrive
//
// Watch mode uses the resumable cursor + bounded long-poll endpoint
// (e1625848): the server returns oldest-first events strictly after the
// cursor with at-least-once delivery, and the client round-trips
// next_cursor verbatim. No client-side dedupe is needed (cursor advances
// past every printed item), and no in-memory event-id set grows
// unboundedly across long sessions.
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
//
// Output format is one line per event, sorted oldest-first so reading
// top-to-bottom matches the timeline. Per-kind summaries (work_item.created,
// transitioned, patience.breached, signal.received, message.captured, etc.)
// are formatted in summarize(); unknown kinds fall back to the raw kind
// string so a new event kind shipping without renderer support is loud-but-
// not-broken.
package main

import (
	"context"
	"encoding/json"
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
	limit := fs.Int("limit", 20, "number of feed items to fetch per poll (max 200)")
	watch := fs.Bool("watch", false, "long-poll for new items and append (Ctrl-C to exit)")
	wait := fs.Duration("wait", 30*time.Second, "long-poll cap when --watch (server caps at 60s)")
	mentions := fs.String("mentions", "", "comma-separated names; in --watch mode, only print items that mention any of them")
	// retainedForCompat: --interval is preserved as a backoff knob for the
	// transient-error retry path. The healthy --watch loop drives its
	// cadence off the server's bounded long-poll, not this duration.
	retryBackoff := fs.Duration("interval", 2*time.Second, "retry backoff when a poll fails in --watch mode")
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

	// HTTP timeout must outlast the longest expected long-poll. The server
	// caps wait at 60s; we add headroom for handler overhead and the
	// bootstrap call. Snapshot mode (no --watch) reuses this client and is
	// not harmed by the longer ceiling.
	client := &feedClient{
		baseURL: strings.TrimRight(*api, "/"),
		token:   token,
		http:    &http.Client{Timeout: 90 * time.Second},
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
		limit:        *limit,
		wait:         *wait,
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
	limit        int
	wait         time.Duration
	mentions     []string
	retryBackoff time.Duration
}

// runFeedWatch implements the long-poll watcher against the resumable
// /v1/feed cursor (e1625848). Sequence:
//
//  1. Bootstrap: GET /v1/feed?wait=0s with no cursor → server snapshots
//     the current head and returns an empty page + next_cursor. This
//     gives the watcher a "start from now" cursor without dumping history.
//  2. Loop: GET /v1/feed?cursor=<cursor>&wait=<wait> → server long-polls
//     up to `wait` for events strictly after the cursor. On items, print
//     and advance the cursor. On HasMore, immediately drain (next call
//     uses the new cursor with wait=0) before going back to long-poll.
//  3. Recover: a 400 invalid_cursor means the server forgot or the
//     encoding rolled over; re-bootstrap a head and continue rather than
//     crash the session. Any other error is soft-failed with a backoff.
//
// No client-side dedupe map is needed — the cursor advances past every
// item before the next call, so the server never re-sends them. This is
// the property pre-cursor watchers paid for with an unbounded `seen`
// set; replacing one with the other is the whole point of e1625848.
func runFeedWatch(ctx context.Context, logger *slog.Logger, client *feedClient, opts watchOptions, out io.Writer) error {
	cursor, err := bootstrapWatchCursor(ctx, client, opts.limit)
	if err != nil {
		return fmt.Errorf("feed --watch: bootstrap: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		page, err := client.fetchPage(ctx, cursor, opts.wait, opts.limit)
		if err != nil {
			if isInvalidCursorErr(err) {
				if logger != nil {
					logger.Warn("feed --watch: cursor invalid, re-bootstrapping head")
				}
				newCursor, bootErr := bootstrapWatchCursor(ctx, client, opts.limit)
				if bootErr != nil {
					return fmt.Errorf("feed --watch: re-bootstrap: %w", bootErr)
				}
				cursor = newCursor
				continue
			}
			if logger != nil {
				logger.Warn("feed --watch: poll failed", slog.String("error", err.Error()))
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(opts.retryBackoff):
			}
			continue
		}

		for _, it := range page.Items {
			if matchesMentions(it, opts.mentions) {
				fmt.Fprintln(out, formatItem(it))
			}
		}
		if page.NextCursor != "" {
			cursor = page.NextCursor
		}
		// HasMore drains immediately (no long-poll between drained pages)
		// so a burst of events doesn't get split across two long-poll cycles.
		if !page.HasMore {
			continue
		}
	}
}

func bootstrapWatchCursor(ctx context.Context, client *feedClient, limit int) (string, error) {
	page, err := client.fetchPage(ctx, "", 0, limit)
	if err != nil {
		return "", err
	}
	return page.NextCursor, nil
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
// is its own slice. If that slice lands, lift this code into the SDK and
// have the CLI call it.
type feedClient struct {
	baseURL string
	token   string
	http    *http.Client
}

// feedPage is the watcher-mode response shape from /v1/feed when either
// ?cursor= or ?wait= is present. NextCursor is opaque and round-tripped
// verbatim. HasMore signals an immediate drain rather than a long-poll.
type feedPage struct {
	Items      []feedItem `json:"items"`
	NextCursor string     `json:"next_cursor"`
	HasMore    bool       `json:"has_more"`
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

// fetchPage hits /v1/feed in watcher mode. cursor empty + wait=0 is the
// bootstrap shape (server returns the head). cursor non-empty + wait>0 is
// the normal long-poll shape. cursor non-empty + wait=0 is the drain
// shape used when HasMore is true on the previous response.
func (c *feedClient) fetchPage(ctx context.Context, cursor string, wait time.Duration, limit int) (feedPage, error) {
	u, err := url.Parse(c.baseURL + "/v1/feed")
	if err != nil {
		return feedPage{}, fmt.Errorf("feed: parse api URL: %w", err)
	}
	q := u.Query()
	q.Set("limit", fmt.Sprintf("%d", limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	// wait=0s is sent explicitly so the server distinguishes "I want
	// watcher response shape with no long-poll" from "I want snapshot
	// response shape." The server selects by presence of either param.
	q.Set("wait", wait.String())
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return feedPage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return feedPage{}, fmt.Errorf("feed: request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return feedPage{}, fmt.Errorf("feed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var page feedPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return feedPage{}, fmt.Errorf("feed: decode page: %w", err)
	}
	return page, nil
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
  wayline feed --watch [--wait=DURATION] [--mentions=NAME[,NAME...]]

Prints recent activity from the wayline event feed in a human-readable
form. Without --watch, fetches the last --limit items and exits. With
--watch, bootstraps a "from now" cursor and long-polls /v1/feed for
events strictly after that cursor. No client-side dedupe; the cursor
advances past every printed item.

Token resolution (first match wins):
  1. WAYLINE_TOKEN env var
  2. .wayline/{root.token, agent-A.token, seed.token} in the nearest
     .wayline directory found walking up from the current directory.
     The chosen path is announced on stderr.

Flags:
  --limit=N            items per fetch (default 20, server caps at 200)
  --watch              long-poll, append new items (Ctrl-C to exit)
  --wait=DURATION      long-poll cap when --watch (default 30s, server caps at 60s)
  --mentions=A,B       in --watch mode, only print items mentioning any of
                       these names (matched on payload.author, the
                       payload.mentions array, or "@name" in text/note)
  --interval=DURATION  retry backoff when a poll fails in --watch (default 2s);
                       the healthy loop drives off the server long-poll
  --api=URL            wayline API base URL (default http://127.0.0.1:8080)

Output is one line per event, sorted oldest-first. Format:
  MM-DD HH:MM:SS  source  subj8  per-kind summary
`)
}
