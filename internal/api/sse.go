package api

// Server-Sent Events stream of feed-visible events.
//
// Why SSE and not long-poll-with-bigger-wait or WebSocket:
//
//   - One-way (server → client) matches our use exactly. Writes go through
//     POSTs (idempotent, attributed, recorded as events); the stream only
//     delivers. Keeping it one-way is a feature, not a limitation — it
//     forces all writes through the hardened path.
//   - Plain HTTP, no upgrade dance. Goes through any proxy that doesn't
//     buffer text/event-stream (and we set X-Accel-Buffering:no for the
//     ones that try).
//   - Built-in replay-on-reconnect via the Last-Event-ID header. Browsers
//     send it automatically; CLI clients send it explicitly. We use the
//     existing v1 cursor (opaque(seq)) as the SSE id, so replay just
//     resumes the same SELECT WHERE seq > $cursor.
//   - http.Flusher is in stdlib; no library to import.
//
// Scaling note: this handler polls events.seq directly per connection,
// at ~100ms cadence. With the seq index that's microseconds per query.
// At 100 concurrent connections that's 1k qps of cheap range scans —
// fine for our scale. If connection count grows past ~1k, the right
// move is a shared in-process broadcaster (one poller, fan-out to all
// connections). Building that for v1 is premature; the per-connection
// loop is simpler and the cost is real but bounded.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/projectiondefs"
)

const (
	ssePollInterval   = 100 * time.Millisecond
	sseHeartbeatEvery = 30 * time.Second
	sseBatchSize      = 256
	// per-write deadline. Generous because the only realistic cause of
	// blockage is a TCP-level slow consumer; we kill the connection and
	// let it reconnect with Last-Event-ID rather than buffer indefinitely.
	sseWriteTimeout = 10 * time.Second
)

func (s *Server) handleFeedStream(w http.ResponseWriter, r *http.Request) {
	if s.feed == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadFeed(w, r, actor) {
		return
	}
	assignedRecipient, ok := requestedAssignedFeedRecipient(w, r, actor)
	if !ok {
		return
	}
	excludeActors, ok := requestedActorExclusions(w, r, actor)
	if !ok {
		return
	}

	// Last-Event-ID is the SSE-standard header browsers and well-behaved
	// CLI clients send on reconnect. Fall back to ?cursor= for callers
	// who can't easily set headers (e.g. quick-and-dirty curl debug).
	cursorStr := r.Header.Get("Last-Event-ID")
	if cursorStr == "" {
		cursorStr = r.URL.Query().Get("cursor")
	}
	projectionName := r.URL.Query().Get("projection")
	var projectionNameForCursor string
	var projectionVersion int
	var projectionForRead *projectiondefs.Projection
	if projectionName != "" {
		if s.projections == nil {
			writeAPIError(w, http.StatusServiceUnavailable, "projections_unavailable", "projection service is not configured")
			return
		}
		projection, err := s.projections.Get(r.Context(), projectionName)
		if err != nil {
			if !s.allowAuthoritativeReadResponse(w) {
				return
			}
			writeProjectionError(w, err)
			return
		}
		projectionNameForCursor = projection.Name
		projectionVersion = projection.Version
		projectionForRead = &projection
	}
	contentPredicates, ok := requestedContentPredicates(w, r, actor)
	if !ok {
		return
	}
	readFilter, err := s.feedReadFilter(actor, projectionForRead, assignedRecipient, excludeActors, contentPredicates)
	if err != nil {
		writeFeedFilterError(w, err)
		return
	}

	fromSeq, err := s.feed.ResolveStreamStartForIdentity(r.Context(), cursorStr, projectionNameForCursor, projectionVersion, readFilter.FingerprintHash())
	if err != nil {
		if !s.allowAuthoritativeReadResponse(w) {
			return
		}
		if errors.Is(err, feed.ErrInvalidCursor) {
			writeAPIError(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed; reconnect without Last-Event-ID to start a fresh stream")
			return
		}
		if errors.Is(err, feed.ErrCursorProjectionMismatch) {
			writeAPIError(w, http.StatusBadRequest, "cursor_projection_mismatch", "cursor was issued for a different feed projection")
			return
		}
		if errors.Is(err, feed.ErrCursorFilterMismatch) {
			writeAPIError(w, http.StatusBadRequest, "cursor_filter_mismatch", "cursor was issued under a different filter identity; reconnect without Last-Event-ID")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "stream_start_failed", "could not resolve stream start position")
		return
	}

	// Flusher is required for SSE — we need to push frames as they're
	// written, not buffer until the response handler returns. http.ResponseWriter
	// implementations that don't support it (rare; mostly synthetic test
	// recorders) get a 500 so the bug surfaces fast.
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "stream_unsupported", "response writer does not support flushing")
		return
	}

	// Disable the server-wide write timeout for this connection. SSE
	// streams are long-lived by design; the 30s default would kill them.
	// Per-write deadlines are set inside the loop instead.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		// Some response writers don't support per-request deadlines;
		// log but continue. The server-wide WriteTimeout will still
		// fire eventually, but each successful write resets it on the
		// underlying conn so for active streams this is fine.
		s.logger.Debug("sse: clear write deadline failed", "error", err.Error())
	}
	// Projection/cursor resolution can query Postgres after the request-entry
	// check. Re-read the pin at the last preflight boundary, before committing
	// the 200 response head; once SSE headers are flushed, a later mismatch can
	// only terminate the stream without an explanatory API error.
	if !s.allowAuthoritativeReadResponse(w) {
		return
	}

	// SSE wire shape:
	//   text/event-stream, chunked; one frame per event ending in \n\n.
	//   no-cache + no-transform stops CDNs/proxies from compressing or
	//   buffering. X-Accel-Buffering:no is the nginx-specific way to
	//   say the same thing.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Initial flush so the client sees response headers and any proxy
	// in the path commits to streaming mode immediately. Without this,
	// some proxies wait for content before forwarding the response head.
	flusher.Flush()
	lastWriteAt := time.Now()

	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}

		// The reviewed-v1 pin is reread for every tail. A stream can outlive a
		// deployment, so the request-entry guard alone cannot keep an old
		// process from continuing to query and emit authoritative data.
		if s.buildStatus().Blocking() {
			return
		}
		batch, err := s.feed.TailWithReadFilter(r.Context(), fromSeq, sseBatchSize, readFilter)
		if err != nil {
			// Don't write an error frame; the client can't do anything
			// useful with mid-stream errors anyway. Just drop the
			// connection. The CLI client will reconnect with its
			// Last-Event-ID and resume from where it left off.
			if !errors.Is(err, r.Context().Err()) {
				s.logger.Warn("sse: tail query failed", "error", err.Error(), "from_seq", fromSeq)
			}
			return
		}
		items := batch.Items
		fromSeq = batch.ScannedThrough

		if len(items) > 0 {
			_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
			for i := range items {
				// Recheck after the tail and access filter, immediately before
				// each frame. If the pin changes between frames, terminate the
				// already-committed stream without emitting a stale error frame.
				if s.buildStatus().Blocking() {
					return
				}
				if !writeSSEFrame(w, &items[i], projectionNameForCursor, projectionVersion, readFilter.FingerprintHash()) {
					return
				}
			}
			flusher.Flush()
			lastWriteAt = time.Now()
			// Loop without sleeping when we just drained a batch — there
			// may be more events queued behind us. The sleep below only
			// fires after an empty Tail.
			continue
		}

		// No new events. Maybe send a heartbeat to keep middleboxes
		// from killing the idle connection, then sleep briefly.
		if time.Since(lastWriteAt) >= sseHeartbeatEvery {
			if s.buildStatus().Blocking() {
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
			lastWriteAt = time.Now()
		}

		select {
		case <-r.Context().Done():
			return
		case <-time.After(ssePollInterval):
		}
	}
}

// writeSSEFrame emits one event in SSE wire format:
//
//	id: <opaque cursor>
//	event: feed
//	data: <one-line json>
//	\n (trailing blank line ends the frame)
//
// The id field is what the browser sends back as Last-Event-ID on
// reconnect; we use the v1 opaque(seq) cursor so the server's
// ResolveStreamStart treats it identically to a ?cursor= query param.
//
// data MUST be one line — newlines inside json.Marshal output would split
// the frame. Marshal flattens to compact form by default, so this is
// already true; the assertion is documentary.
//
// Returns false if the write failed (caller should give up the loop).
func writeSSEFrame(w http.ResponseWriter, item *feed.Item, projectionName string, projectionVersion int, fingerprint string) bool {
	payload, err := json.Marshal(item)
	if err != nil {
		// Marshalling can only realistically fail on a payload with an
		// unsupported type, which would be a bug in the projection that
		// produced the event. Log and skip; advancing past the event
		// rather than blocking the whole stream.
		return true
	}
	cursor := feed.EncodeCursor(item.Seq)
	if fingerprint != "" || projectionName != "" {
		cursor = feed.EncodeCursorForIdentity(item.Seq, projectionName, projectionVersion, fingerprint)
	}
	if _, err := fmt.Fprintf(w, "id: %s\nevent: feed\ndata: %s\n\n", cursor, payload); err != nil {
		return false
	}
	return true
}
