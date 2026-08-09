// `meristem feed` is the human-facing read of the activity log: a tiny
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
//	meristem feed                snapshot of the last --limit items, exit
//	meristem feed --watch        consume the SSE push stream at /v1/feed/stream;
//	                            server pushes each new event as it lands,
//	                            client prints (after optional --mentions filter).
//	                            Reconnects with Last-Event-ID on disconnect
//	                            so events that landed during the gap are
//	                            replayed deterministically.
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
//	--mentions=agent-A         only print items that mention any of the
//	--mentions=agent-A,me      named recipients. Matches on payload.author
//	                           equality, payload.mentions array membership,
//	                           and "@name" appearing in text/note fields.
//	                           Recurses into work_item.event_appended.inner
//	                           so notes posted as appended sub-events are
//	                           filtered the same as direct payloads.
//	                           The cursor still advances past dropped events,
//	                           so reconnects don't re-deliver them.
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
	"bytes"
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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func runFeed(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("feed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	api := fs.String("api", "http://127.0.0.1:8080", "meristem API base URL")
	limit := fs.Int("limit", 20, "number of feed items to fetch (snapshot mode)")
	watch := fs.Bool("watch", false, "consume the SSE push stream and append new items (Ctrl-C to exit)")
	mentions := fs.String("mentions", "", "comma-separated names; in --watch mode, only print items that mention any of them")
	// retryBackoff caps how often we reconnect on transient SSE failures.
	// The healthy --watch path holds one long-lived connection and never
	// touches this; only network blips and server restarts reach it.
	retryBackoff := fs.Duration("interval", 2*time.Second, "reconnect backoff when the SSE stream drops in --watch mode")

	// Channel lens flags: translated into query params the server folds into
	// the same normalized filter contract every surface shares. The server
	// owns validation (fail-closed); the CLI just carries the request.
	projection := fs.String("projection", "", "named feed projection (activity, owner-attention, dispatch, ...); part of the cursor identity")
	scope := fs.String("scope", "", "feed scope; \"assigned\" selects the assigned/addressed lane")
	listenFor := fs.String("listen-for", "", "delegated lane: \"self\" or a token id whose assigned/addressed lane to read")
	actors := fs.String("actor", "", "comma-separated author filter: \"self\" or token ids; only their events are delivered")
	excludeActors := fs.String("exclude-actor", "", "comma-separated author exclusions: \"self\" or token ids")
	kinds := fs.String("kind", "", "comma-separated event kinds to include")
	excludeKinds := fs.String("exclude-kind", "", "comma-separated event kinds to exclude")
	workItem := fs.String("work-item", "", "only events anchored to this work item id")
	tree := fs.String("tree", "", "only events anchored to the subtree rooted at this work item id")

	// Watch-ergonomics flags: durable resume and the wake hook.
	cursorFile := fs.String("cursor-file", "", "persist the last delivered cursor here and resume from it on restart; bootstrapped at the current head under this lens before the first stream connect")
	execCmd := fs.String("exec", "", "run this command (via sh -c) for each delivered event; event JSON on stdin. A failing command stops the watcher without advancing the cursor, so the event is redelivered")
	ndjson := fs.Bool("ndjson", false, "print raw event envelopes as NDJSON instead of the human-readable rendering")
	resetOnMismatch := fs.Bool("reset-cursor-on-mismatch", false, "when the server rejects the persisted cursor's identity (filter/projection changed), clear it and restart from now instead of exiting; queued history under the old identity is deliberately abandoned")
	if err := fs.Parse(args); err != nil {
		feedUsage(os.Stderr)
		return err
	}

	token, source, err := resolveFeedToken()
	if err != nil {
		return err
	}
	if source != "" && source != "MERISTEM_TOKEN" {
		fmt.Fprintf(os.Stderr, "feed: using token from %s\n", source)
	}

	query := buildFeedQuery(feedQueryFlags{
		projection:    *projection,
		scope:         *scope,
		listenFor:     *listenFor,
		actors:        splitCommaList(*actors),
		excludeActors: splitCommaList(*excludeActors),
		kinds:         splitCommaList(*kinds),
		excludeKinds:  splitCommaList(*excludeKinds),
		workItem:      *workItem,
		tree:          *tree,
	})

	// Two clients with different timeout disciplines:
	//   - snapshot HTTP: bounded 30s; a snapshot read should complete fast.
	//   - stream HTTP: NO Timeout; SSE connections are designed to be long-
	//     lived. Cancellation comes from ctx, not from a wallclock cap.
	//     A timeout would silently kill healthy streams the moment they
	//     went idle past the threshold.
	client := &feedClient{
		baseURL:    strings.TrimRight(*api, "/"),
		token:      token,
		query:      query,
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
		mentions:              parseMentions(*mentions),
		retryBackoff:          *retryBackoff,
		cursorFile:            *cursorFile,
		execCmd:               *execCmd,
		ndjson:                *ndjson,
		resetCursorOnMismatch: *resetOnMismatch,
	}, os.Stdout)
}

// feedQueryFlags is the CLI-side shape of the channel lens before it becomes
// query params. Kept as a struct so tests can exercise the translation
// without a flag.FlagSet.
type feedQueryFlags struct {
	projection    string
	scope         string
	listenFor     string
	actors        []string
	excludeActors []string
	kinds         []string
	excludeKinds  []string
	workItem      string
	tree          string
}

// buildFeedQuery translates lens flags into the query params GET /v1/feed and
// /v1/feed/stream accept. Values are passed through verbatim — the server is
// the authority on identity shape and kind vocabulary and fails closed on
// anything malformed — but structurally empty values are dropped here so an
// unset flag adds nothing to the filter identity.
func buildFeedQuery(f feedQueryFlags) url.Values {
	q := url.Values{}
	if p := strings.TrimSpace(f.projection); p != "" {
		q.Set("projection", p)
	}
	if s := strings.TrimSpace(f.scope); s != "" {
		q.Set("scope", s)
	}
	if lf := strings.TrimSpace(f.listenFor); lf != "" {
		q.Set("listen_for", lf)
	}
	for _, a := range f.actors {
		q.Add("actor", a)
	}
	for _, a := range f.excludeActors {
		q.Add("exclude_actor", a)
	}
	for _, k := range f.kinds {
		q.Add("kind", k)
	}
	for _, k := range f.excludeKinds {
		q.Add("exclude_kind", k)
	}
	if wi := strings.TrimSpace(f.workItem); wi != "" {
		q.Set("work_item", wi)
	}
	if t := strings.TrimSpace(f.tree); t != "" {
		q.Set("work_item_tree", t)
	}
	return q
}

// splitCommaList splits a comma-separated flag value, trimming whitespace and
// dropping empty entries so "a,,b" and "a, b" both mean [a b].
func splitCommaList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
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
	// cursorFile, when set, makes the watch durable: the cursor of the last
	// successfully delivered event is persisted there and a restarted watcher
	// resumes from it instead of "from now". A missing/empty file is
	// bootstrapped BEFORE the first stream connect by minting an
	// identity-bound cursor at the current head through the page surface, so
	// an event landing between watcher start and stream connect (or across a
	// crash before the first delivery) is never silently skipped.
	cursorFile string
	// execCmd, when set, is the wake hook: run via sh -c for each delivered
	// event with the event JSON on stdin. Delivery means the command exited
	// zero; a failure stops the watcher WITHOUT advancing the cursor, so the
	// event is redelivered on the next connection rather than dropped.
	execCmd string
	ndjson  bool
	// resetCursorOnMismatch opts into recovering from a cursor identity
	// rejection (the persisted cursor was minted under a different filter or
	// projection) by clearing the cursor and restarting from now. Off by
	// default: silently abandoning queued history is a data-loss decision
	// the operator must make explicitly; without it the watcher exits loudly
	// and leaves the cursor file untouched.
	resetCursorOnMismatch bool
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
	if opts.cursorFile != "" {
		saved, err := loadCursorFile(opts.cursorFile)
		if err != nil {
			return fmt.Errorf("feed --watch: read cursor file: %w", err)
		}
		if saved != "" {
			lastID = saved
		} else {
			// Fresh durable watcher: mint the identity-bound head cursor
			// via the server's atomic bootstrap and persist it BEFORE
			// opening the stream. From here on, every event after this
			// point is either delivered or still ahead of the durable
			// cursor — a crash or a slow first connect cannot silently
			// skip a wake. Transient mint failures retry like the stream.
			lastID, err = mintCursorWithRetry(ctx, logger, client, opts.retryBackoff)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return fmt.Errorf("feed --watch: bootstrap cursor: %w", err)
			}
			if err := saveCursorFile(opts.cursorFile, lastID); err != nil {
				return fmt.Errorf("feed --watch: persist bootstrap cursor: %w", err)
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		newLastID, err := client.consumeStream(ctx, lastID, func(ev sseEvent) error {
			if matchesMentions(ev.Item, opts.mentions) {
				if opts.ndjson {
					if err := printNDJSON(out, ev.Item); err != nil {
						return err
					}
				} else {
					fmt.Fprintln(out, formatItem(ev.Item))
				}
				if opts.execCmd != "" {
					if err := runWakeHook(ctx, opts.execCmd, ev); err != nil {
						return fmt.Errorf("wake hook failed (event will be redelivered): %w", err)
					}
				}
			}
			// Delivery succeeded (or the event was filtered out); the cursor
			// may durably advance past it.
			if opts.cursorFile != "" && ev.ID != "" {
				if err := saveCursorFile(opts.cursorFile, ev.ID); err != nil {
					return fmt.Errorf("persist cursor: %w", err)
				}
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

		if isWakeHookErr(err) {
			// The hook is the point of the watch; if it cannot run, exit
			// loudly instead of spinning delivery attempts forever. The
			// cursor was not advanced past the failed event.
			return err
		}

		switch classifyWatchError(err) {
		case watchErrCursorIdentity:
			// The cursor was minted under a different filter or projection
			// identity, or the server no longer recognizes it.
			//
			// Ephemeral watcher (no cursor file): there is no durability
			// contract to protect — recover by restarting from now, exactly
			// as a human tailing a terminal expects.
			if opts.cursorFile == "" {
				if logger != nil {
					logger.Warn("feed --watch: server rejected resume cursor, restarting from now",
						slog.String("last_id", lastID),
						slog.String("error", err.Error()))
				}
				lastID = ""
				break
			}
			// Durable watcher: resetting means abandoning whatever queued
			// history the old identity still held — a data-loss decision
			// the operator must opt into. Without the opt-in, exit loudly
			// and leave the cursor file untouched for inspection.
			if !opts.resetCursorOnMismatch {
				return fmt.Errorf("feed --watch: server rejected the resume cursor (%w); rerun with --reset-cursor-on-mismatch to discard it and restart from now", err)
			}
			if logger != nil {
				logger.Warn("feed --watch: server rejected resume cursor, resetting and restarting from now",
					slog.String("last_id", lastID),
					slog.String("error", err.Error()))
			}
			lastID = ""
			if opts.cursorFile != "" {
				// Re-mint under the current identity rather than leaving the
				// file empty: the durable no-silent-skip property resumes
				// immediately at the reset point instead of lapsing until
				// the first post-reset delivery. Transient failures retry.
				fresh, mintErr := mintCursorWithRetry(ctx, logger, client, opts.retryBackoff)
				if mintErr != nil {
					return fmt.Errorf("feed --watch: re-bootstrap cursor after reset: %w", mintErr)
				}
				lastID = fresh
				if err := saveCursorFile(opts.cursorFile, fresh); err != nil {
					return fmt.Errorf("persist re-bootstrapped cursor: %w", err)
				}
			}
		case watchErrPermanent:
			// Config or auth is wrong (unknown kind, malformed id,
			// insufficient scope, bad token). Retrying cannot fix it and
			// each retry would just hammer the server; the cursor file is
			// left exactly as it was.
			return fmt.Errorf("feed --watch: %w", err)
		default:
			// Transient: network blip, server restart, 5xx, stream close.
			// Keep the cursor and reconnect after backoff.
			if logger != nil {
				logger.Warn("feed --watch: stream ended, reconnecting",
					slog.String("error", err.Error()),
					slog.String("backoff", opts.retryBackoff.String()))
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.retryBackoff):
		}
	}
}

// watchErrorClass is the reconnect-loop decision for one stream failure.
type watchErrorClass int

const (
	watchErrTransient watchErrorClass = iota
	watchErrCursorIdentity
	watchErrPermanent
)

// classifyWatchError sorts a stream error into the three behaviors the loop
// supports. Only API responses carry a decidable class; anything without a
// typed apiRequestError (transport errors, stream teardown) is transient.
func classifyWatchError(err error) watchErrorClass {
	var apiErr *apiRequestError
	if !errors.As(err, &apiErr) {
		return watchErrTransient
	}
	switch apiErr.Code {
	case "invalid_cursor", "cursor_filter_mismatch", "cursor_projection_mismatch":
		return watchErrCursorIdentity
	}
	switch apiErr.Status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		// Retryable by definition; treating them as permanent would kill a
		// healthy watcher during a load spike.
		return watchErrTransient
	}
	if apiErr.Status >= 400 && apiErr.Status < 500 {
		return watchErrPermanent
	}
	return watchErrTransient
}

// mintCursorWithRetry runs the bootstrap mint under the same error policy as
// the stream loop: transient failures (network, 5xx, 408/429) back off and
// retry until ctx cancels; permanent config/auth errors and cursor-identity
// errors exit immediately. Without this, one 503 during startup would kill a
// durable watcher that the reconnect loop was built to keep alive.
func mintCursorWithRetry(ctx context.Context, logger *slog.Logger, client *feedClient, backoff time.Duration) (string, error) {
	for {
		cursor, err := client.mintCursor(ctx)
		if err == nil {
			return cursor, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if classifyWatchError(err) != watchErrTransient {
			return "", err
		}
		if logger != nil {
			logger.Warn("feed --watch: bootstrap mint failed, retrying",
				slog.String("error", err.Error()),
				slog.String("backoff", backoff.String()))
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// printNDJSON emits the full wire envelope of one event as a single JSON
// line, for machine consumers piping the watch into another program.
func printNDJSON(out io.Writer, it feedItem) error {
	encoded, err := json.Marshal(it)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	_, err = fmt.Fprintln(out, string(encoded))
	return err
}

// wakeHookError distinguishes a failing --exec hook from transport errors so
// the reconnect loop can exit instead of hammering a broken hook.
type wakeHookError struct{ err error }

func (e wakeHookError) Error() string { return e.err.Error() }
func (e wakeHookError) Unwrap() error { return e.err }

func isWakeHookErr(err error) bool {
	var hookErr wakeHookError
	return errors.As(err, &hookErr)
}

// runWakeHook executes the --exec command for one delivered event. The full
// event envelope arrives on stdin; the cursor, kind, and subject are also
// exposed as MERISTEM_EVENT_* env vars for hooks that don't want to parse
// JSON. The hook's stdout/stderr pass through so wake targets can log.
func runWakeHook(ctx context.Context, command string, ev sseEvent) error {
	encoded, err := json.Marshal(ev.Item)
	if err != nil {
		return wakeHookError{fmt.Errorf("encode event for hook: %w", err)}
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Stdin = bytes.NewReader(encoded)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"MERISTEM_EVENT_CURSOR="+ev.ID,
		"MERISTEM_EVENT_KIND="+ev.Item.Kind,
		"MERISTEM_EVENT_SUBJECT_ID="+ev.Item.SubjectID,
	)
	if err := cmd.Run(); err != nil {
		return wakeHookError{err}
	}
	return nil
}

// loadCursorFile reads the persisted cursor. A missing file is a fresh
// watcher, not an error.
func loadCursorFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// saveCursorFile persists the cursor atomically (temp file + rename in the
// same directory) so a crash mid-write can never leave a torn cursor that a
// restarted watcher would send as Last-Event-ID.
func saveCursorFile(path, cursor string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cursor-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(cursor + "\n"); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
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

// apiRequestError is a non-200 API response with its decoded error envelope,
// so the reconnect loop can classify on the server's error CODE instead of
// string-matching status text (which conflated "invalid filter" and
// "rejected cursor" — every 400 looked recoverable).
type apiRequestError struct {
	Status  int
	Code    string
	Message string
}

func (e *apiRequestError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("api %d %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("api %d: %s", e.Status, e.Message)
}

// newAPIRequestError decodes the standard {"error":{code,message}} envelope;
// a body that isn't the envelope still yields a typed error with the raw
// text as the message.
func newAPIRequestError(status int, body []byte) *apiRequestError {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Code != "" {
		return &apiRequestError{Status: status, Code: envelope.Error.Code, Message: envelope.Error.Message}
	}
	return &apiRequestError{Status: status, Message: strings.TrimSpace(string(body))}
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
// and streaming is its own slice. If that slice lands, lift this code
// into the SDK and have the CLI call it.
//
// http is for short-lived snapshot reads (Timeout enforced).
// streamHTTP is for the long-lived SSE connection (no Timeout — only
// ctx cancellation can end it). Splitting them keeps the snapshot path
// safe from a hung server while letting the stream live indefinitely.
type feedClient struct {
	baseURL string
	token   string
	// query carries the channel-lens params (scope, listen_for, actor,
	// exclude_actor, kind, exclude_kind, work_item, work_item_tree) applied
	// identically to snapshot and stream requests, so a cursor minted by one
	// is valid for the other — same filter, same fingerprint identity.
	query      url.Values
	http       *http.Client
	streamHTTP *http.Client
}

// lensQuery clones the shared lens params so per-request additions (limit)
// cannot leak between calls.
func (c *feedClient) lensQuery() url.Values {
	q := url.Values{}
	for key, values := range c.query {
		for _, v := range values {
			q.Add(key, v)
		}
	}
	return q
}

func (c *feedClient) fetch(ctx context.Context, limit int) ([]feedItem, error) {
	u, err := url.Parse(c.baseURL + "/v1/feed")
	if err != nil {
		return nil, fmt.Errorf("feed: parse api URL: %w", err)
	}
	q := c.lensQuery()
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

// mintCursor asks the server's bootstrap=head mode for an identity-bound
// cursor at the current head, under exactly the lens (query params) this
// client streams with — same normalized filter, same fingerprint, so the
// stream accepts it as a resume point. bootstrap=head is atomic on the
// server (one MAX(seq) read, no events returned), so no event can be
// consumed-and-discarded by the mint itself: everything after the minted
// point is the stream's to deliver.
func (c *feedClient) mintCursor(ctx context.Context) (string, error) {
	u, err := url.Parse(c.baseURL + "/v1/feed")
	if err != nil {
		return "", fmt.Errorf("feed: parse api URL: %w", err)
	}
	q := c.lensQuery()
	q.Set("bootstrap", "head")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("feed: mint cursor: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("feed: mint cursor read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", newAPIRequestError(resp.StatusCode, body)
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return "", fmt.Errorf("feed: mint cursor decode: %w", err)
	}
	if page.NextCursor == "" {
		return "", fmt.Errorf("feed: mint cursor: server returned no next_cursor")
	}
	return page.NextCursor, nil
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
	if lens := c.lensQuery(); len(lens) > 0 {
		u.RawQuery = lens.Encode()
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
		return lastID, fmt.Errorf("feed: stream rejected: %w", newAPIRequestError(resp.StatusCode, body))
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
			// Advance the resume cursor only after the handler accepts the
			// event. If delivery fails (e.g. the wake hook errored), the
			// reconnect resumes at the previous cursor and the server
			// redelivers this event instead of dropping it.
			if err := onEvent(ev); err != nil {
				return lastID, err
			}
			if ev.ID != "" {
				lastID = ev.ID
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

	case "work_item.metadata_updated":
		var p struct {
			To struct {
				HumanReviewStatus          string   `json:"human_review_status"`
				SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
			} `json:"to"`
		}
		_ = json.Unmarshal(it.Payload, &p)
		return fmt.Sprintf("metadata review=%s checks=%d", p.To.HumanReviewStatus, len(p.To.SuggestedConvergenceChecks))

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
//  2. MERISTEM_TOKEN_FILE, when set, names one absolute, regular, mode-0600
//     file. An invalid explicit file fails closed rather than falling back.
//  3. The first existing token file in priorityTokens, searched in the
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
	if path := strings.TrimSpace(os.Getenv("MERISTEM_TOKEN_FILE")); path != "" {
		if !filepath.IsAbs(path) {
			return "", "", errors.New("feed: MERISTEM_TOKEN_FILE must be absolute")
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return "", "", fmt.Errorf("feed: inspect MERISTEM_TOKEN_FILE: %w", statErr)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			return "", "", errors.New("feed: MERISTEM_TOKEN_FILE must be a regular mode-0600 file")
		}
		if info.Size() < 1 || info.Size() > 4096 {
			return "", "", errors.New("feed: MERISTEM_TOKEN_FILE has an invalid size")
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return "", "", fmt.Errorf("feed: read MERISTEM_TOKEN_FILE: %w", readErr)
		}
		t := strings.TrimSpace(string(data))
		if t == "" {
			return "", "", errors.New("feed: MERISTEM_TOKEN_FILE is empty")
		}
		return t, path, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("feed: getwd: %w", err)
	}
	dir := findMeristemDir(cwd)
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
// system token used by `meristem seed v1` and `meristem worker`.
var priorityTokens = []string{
	"root.token",
	"agent-A.token",
	"seed.token",
}

// findMeristemDir walks upward from start looking for a directory named
// `.meristem`. Returns the absolute path of the .meristem directory itself,
// or "" if none is found before the filesystem root. Termination is
// guaranteed by the parent==current convergence check (true on POSIX and
// Windows for the root path).
func findMeristemDir(start string) string {
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
  meristem feed [--limit=N]
  meristem feed --watch [--mentions=NAME[,NAME...]]

Prints recent activity from the meristem event feed in a human-readable
form. Without --watch, fetches the last --limit items and exits. With
--watch, holds one long-lived connection to /v1/feed/stream (SSE) and
prints each event the server pushes, reconnecting with Last-Event-ID
on disconnect so events that landed during the gap are replayed.

Token resolution (first match wins):
  1. MERISTEM_TOKEN env var
  2. .meristem/{root.token, agent-A.token, seed.token} in the nearest
     .meristem directory found walking up from the current directory.
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
  --api=URL            meristem API base URL (default http://127.0.0.1:8080)

Channel lens flags (server-side filtering; all surfaces share one normalized
filter contract, and cursors are bound to the full lens identity —
projection, version, and predicate fingerprint):
  --projection=NAME    named feed projection (activity, owner-attention, ...)
  --scope=assigned     read the assigned/addressed lane instead of the broad feed
  --listen-for=ID      delegated lane: self or a token id (requires feed.listen_for scope)
  --actor=A,B          only events authored by any of these token ids (or
                       self) — multiple values are a union, not an AND
  --exclude-actor=A,B  drop events authored by these token ids (or self);
                       events addressed to the reader always survive
  --kind=K1,K2         only these event kinds (unknown kinds fail closed)
  --exclude-kind=K1    drop these event kinds; addressed wakes always survive
  --work-item=ID       only events anchored to this work item
  --tree=ID            only events anchored to this work item's subtree

Watch ergonomics (with --watch):
  --cursor-file=PATH   durable watch. A missing/empty file is bootstrapped
                       with an identity-bound cursor at the current head
                       BEFORE the first stream connect, then the cursor of
                       each delivered event is persisted — a crash or slow
                       connect can never silently skip an event. A restarted
                       watcher resumes exactly.
  --exec=CMD           wake hook: run CMD (sh -c) per delivered event, event
                       JSON on stdin, MERISTEM_EVENT_{CURSOR,KIND,SUBJECT_ID}
                       in env. Non-zero exit stops the watcher WITHOUT
                       advancing the cursor, so the event is redelivered.
  --ndjson             print raw event envelopes as NDJSON (machine consumers)
  --reset-cursor-on-mismatch
                       if the server rejects the persisted cursor's identity
                       (the lens changed), discard it, re-mint at the current
                       head, and continue. Without this flag the watcher
                       exits loudly and preserves the cursor file — resetting
                       abandons queued history and is an explicit decision.
                       Config/auth errors (unknown kind, bad token, missing
                       scope) always exit immediately; they are never retried.

Output is one line per event, in arrival order (oldest-first by events.seq).
Format:
  MM-DD HH:MM:SS  source  subj8  per-kind summary
`)
}
