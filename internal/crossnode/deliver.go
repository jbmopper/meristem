package crossnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// CommandPath is the route every cross-node command posts to on the receiving
// node, whatever the delivery mechanism.
const CommandPath = "/v1/crossnode/commands"

// Delivery headers. Only relay candidates set Relayed; only queue candidates
// set QueueFor. A receiver reads them to decide forward-once vs durable-queue
// vs local-execute (see internal/api handleCrossnodeCommand).
const (
	// HeaderIdempotencyKey carries the caller-supplied idempotency key so a
	// retried command collapses at the home node's idempotency middleware.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderRelayed marks a relay hop. §2b: a node never forwards an already
	// relayed request, so loops are impossible structurally.
	HeaderRelayed = "X-Meristem-Relayed"
	// HeaderQueueFor names the target node a durable-queue POST should park
	// the command for.
	HeaderQueueFor = "X-Meristem-Queue-For"
)

// wireCommand is the JSON body posted to CommandPath. Idempotency and routing
// travel in headers, not the body, so the body is exactly the home-node call
// to replay.
type wireCommand struct {
	CommandPath string          `json:"command_path"`
	CommandBody json.RawMessage `json:"command_body"`
}

// Deliver walks candidates in order, posting req to each until one gives a
// definitive answer, and returns the outcome plus an updated cooldown map.
//
// Per candidate:
//   - a transport failure or a 5xx cools the route down (RouteKey recorded at
//     now) and advances to the next candidate;
//   - a 4xx is definitive: the walk stops and the response is surfaced
//     (Delivered false, StatusCode/Body set) — a home node that rejects the
//     command will reject the retry too, so trying another route is pointless;
//   - a 2xx is success: the walk stops with Delivered true.
//
// If every candidate fails at the transport/5xx level the walk exhausts and
// Deliver returns the accumulated cooldowns with ErrAllRoutesFailed.
//
// cooldowns is treated as read-only input; the returned Outcome.Cooldowns is a
// fresh map (input entries copied) so the function stays pure-ish — cooldowns
// in, cooldowns out. now stamps any cooldown this walk records.
func Deliver(ctx context.Context, client *http.Client, candidates []Candidate, req Command, cooldowns map[string]time.Time, now time.Time) (Outcome, error) {
	if client == nil {
		client = http.DefaultClient
	}
	out := Outcome{Cooldowns: copyCooldowns(cooldowns)}

	body, err := json.Marshal(wireCommand{CommandPath: req.Path, CommandBody: normalizeBody(req.Body)})
	if err != nil {
		return out, fmt.Errorf("crossnode: marshal command: %w", err)
	}

	for _, c := range candidates {
		status, respBody, reqErr := post(ctx, client, c, req, body)
		attempt := Attempt{Candidate: c, StatusCode: status, Err: reqErr}

		switch {
		case reqErr != nil || status >= http.StatusInternalServerError:
			// Transport failure or 5xx: cool this route down and advance.
			out.Cooldowns[c.RouteKey] = now
			attempt.CooledDown = true
			out.Attempts = append(out.Attempts, attempt)
			continue
		case status >= http.StatusBadRequest:
			// Definitive 4xx: stop and surface, do not try another route.
			out.Attempts = append(out.Attempts, attempt)
			out.Terminal = c
			out.StatusCode = status
			out.Body = respBody
			return out, nil
		default:
			// 2xx success.
			out.Attempts = append(out.Attempts, attempt)
			out.Delivered = true
			out.Terminal = c
			out.StatusCode = status
			out.Body = respBody
			return out, nil
		}
	}

	return out, ErrAllRoutesFailed
}

func post(ctx context.Context, client *http.Client, c Candidate, req Command, body []byte) (int, []byte, error) {
	url := strings.TrimRight(c.URL, "/") + CommandPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(HeaderIdempotencyKey, req.IdempotencyKey)
	switch c.Kind {
	case KindRelay:
		httpReq.Header.Set(HeaderRelayed, "true")
	case KindQueue:
		httpReq.Header.Set(HeaderQueueFor, req.TargetNodeID)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		// A read failure mid-response is a transport failure for routing
		// purposes: cool the route and advance.
		return 0, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// maxResponseBody bounds how much of a peer response Deliver buffers. Cross
// node command responses are small structured JSON; this only guards against a
// misbehaving peer, not a real payload limit.
const maxResponseBody = 1 << 20

func normalizeBody(b json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(b)) == 0 {
		return json.RawMessage("{}")
	}
	return b
}

func copyCooldowns(in map[string]time.Time) map[string]time.Time {
	out := make(map[string]time.Time, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
