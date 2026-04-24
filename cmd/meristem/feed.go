// `meristem feed` is the human-facing read of the activity log: a tiny
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
//   meristem feed                snapshot of the last --limit items, exit
//   meristem feed --watch        snapshot, then poll --interval and append
//                               only newly-arrived items (dedupe by event_id)
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
	api := fs.String("api", "http://127.0.0.1:8080", "meristem API base URL")
	limit := fs.Int("limit", 20, "number of feed items to fetch per poll (max 200)")
	watch := fs.Bool("watch", false, "poll for new items and append (Ctrl-C to exit)")
	interval := fs.Duration("interval", 2*time.Second, "poll interval when --watch")
	if err := fs.Parse(args); err != nil {
		feedUsage(os.Stderr)
		return err
	}

	token, source, err := resolveFeedToken()
	if err != nil {
		return err
	}
	if source != "" && source != "MERISTEM_TOKEN" {
		// Loud-by-default: when a token is picked off disk, the user should
		// see which one. Quietly using a file would be surprising in CI or
		// when several .meristem directories sit on a workstation.
		fmt.Fprintf(os.Stderr, "feed: using token from %s\n", source)
	}

	client := &feedClient{
		baseURL: strings.TrimRight(*api, "/"),
		token:   token,
		http:    &http.Client{Timeout: 10 * time.Second},
	}

	if !*watch {
		items, err := client.fetch(ctx, *limit)
		if err != nil {
			return err
		}
		printItems(os.Stdout, items)
		return nil
	}

	return runFeedWatch(ctx, logger, client, *limit, *interval, os.Stdout)
}

// runFeedWatch is split out so it is callable from tests with a fake
// context and a buffer in place of stdout. The polling loop dedupes by
// event_id, so the API returning overlapping windows on each poll is fine
// (and expected).
func runFeedWatch(ctx context.Context, logger *slog.Logger, client *feedClient, limit int, interval time.Duration, out io.Writer) error {
	seen := make(map[string]bool)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	emit := func() {
		items, err := client.fetch(ctx, limit)
		if err != nil {
			// Soft-fail in watch mode: a transient API blip should not
			// kill the session. Log to stderr-via-slog and keep polling.
			if logger != nil {
				logger.Warn("feed --watch: poll failed", slog.String("error", err.Error()))
			}
			return
		}
		sort.Slice(items, func(i, j int) bool { return items[i].OccurredAt.Before(items[j].OccurredAt) })
		for _, it := range items {
			if seen[it.EventID] {
				continue
			}
			seen[it.EventID] = true
			fmt.Fprintln(out, formatItem(it))
		}
	}

	emit()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			emit()
		}
	}
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
// (rather than going through pkg/meristem) because the SDK currently only
// covers the write paths integrators most need; growing it to cover GETs
// is its own slice. If that slice lands, lift this code into the SDK and
// have the CLI call it.
type feedClient struct {
	baseURL string
	token   string
	http    *http.Client
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
//  1. MERISTEM_TOKEN env var (explicit always wins; CI and ephemeral shells
//     should never have a file silently take precedence).
//  2. The first existing token file in priorityTokens, searched in the
//     nearest .meristem directory found by walking up from cwd. The walk is
//     bounded by filesystem-root convergence so it terminates on every OS.
//
// For /v1/feed any valid bearer works (no source restriction), so the
// priority list leads with root.token (highest authority, also source=human
// so it can also be reused for inbox capture). agent and seed follow as
// fallbacks for setups that ran bootstrap with --no-root or similar.
func resolveFeedToken() (token, source string, err error) {
	if t := strings.TrimSpace(os.Getenv("MERISTEM_TOKEN")); t != "" {
		return t, "MERISTEM_TOKEN", nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("feed: getwd: %w", err)
	}
	dir := findmeristemDir(cwd)
	if dir == "" {
		return "", "", fmt.Errorf("feed: MERISTEM_TOKEN not set and no .meristem/ directory found walking up from %s", cwd)
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
	return "", "", fmt.Errorf("feed: MERISTEM_TOKEN not set and no usable token found in %s (looked for %s)",
		dir, strings.Join(priorityTokens, ", "))
}

// priorityTokens is the file-discovery order resolveFeedToken uses inside a
// .meristem/ directory. Order is most-permissive-first: root can do anything,
// agent-A is the standard non-root bearer minted by bootstrap, seed is the
// system token used by `meristem seed v1` and `meristem worker --once`.
var priorityTokens = []string{
	"root.token",
	"agent-A.token",
	"seed.token",
}

// findmeristemDir walks upward from start looking for a directory named
// `.meristem`. Returns the absolute path of the .meristem directory itself,
// or "" if none is found before the filesystem root. Termination is
// guaranteed by the parent==current convergence check (true on POSIX and
// Windows for the root path).
func findmeristemDir(start string) string {
	dir := start
	for {
		candidate := filepath.Join(dir, ".meristem")
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
  meristem feed [--limit=N] [--watch]

Prints recent activity from the meristem event feed in a human-readable
form. Without --watch, fetches the last --limit items and exits. With
--watch, polls every --interval and appends only newly-arrived items.

Token resolution (first match wins):
  1. MERISTEM_TOKEN env var
  2. .meristem/{root.token, agent-A.token, seed.token} in the nearest
     .meristem directory found walking up from the current directory.
     The chosen path is announced on stderr.

Flags:
  --limit=N            items to fetch per poll (default 20, server caps at 200)
  --watch              poll continuously, append new items (Ctrl-C to exit)
  --interval=DURATION  poll interval when --watch (default 2s)
  --api=URL            meristem API base URL (default http://127.0.0.1:8080)

Output is one line per event, sorted oldest-first. Format:
  MM-DD HH:MM:SS  source  subj8  per-kind summary
`)
}
