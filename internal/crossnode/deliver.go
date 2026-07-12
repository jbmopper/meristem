package crossnode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// CommandPath is the durable queue ingress. Direct delivery calls the
// command's canonical REST path on its home node instead of wrapping it.
const CommandPath = "/v1/crossnode/commands"

// Delivery headers. Only relay candidates set Relayed; only queue candidates
// set QueueFor. A receiver reads them to decide forward-once vs durable-queue
// vs local-execute (see internal/api handleCrossnodeCommand).
const (
	// HeaderIdempotencyKey carries the caller-supplied idempotency key so a
	// retried command collapses at the home node's idempotency middleware.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderTargetNode binds a direct or queued request to the registry entry
	// selected by the sender. A canonical REST receiver rejects a non-local
	// value before appending an event.
	HeaderTargetNode = "X-Meristem-Target-Node"
	// HeaderOriginNode records the sending node as structural provenance. It
	// never substitutes for the receiver-resolved actor token.
	HeaderOriginNode = "X-Meristem-Origin-Node"
	// HeaderOriginActorToken and HeaderOriginActorSource preserve remote
	// provenance without replacing target-local request attribution.
	HeaderOriginActorToken  = "X-Meristem-Origin-Actor-Token-ID"
	HeaderOriginActorSource = "X-Meristem-Origin-Actor-Source"
	// HeaderQueueCommand identifies the authenticated queue-host envelope being
	// replayed; it is absent on direct delivery.
	HeaderQueueCommand = "X-Meristem-Queue-Command-ID"
	// HeaderCausingWorkItem identifies the origin-homed work item whose delivery
	// patience owns the command.
	HeaderCausingWorkItem = "X-Meristem-Causing-Work-Item-ID"
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
	CommandPath       string          `json:"command_path"`
	CommandBody       json.RawMessage `json:"command_body"`
	CausingWorkItemID *uuid.UUID      `json:"causing_work_item_id,omitempty"`
}

const (
	defaultHTTPTimeout    = 10 * time.Second
	defaultDirectAttempts = 3
	defaultDirectPatience = 60 * time.Second
)

// DeliveryPolicy bounds direct delivery before durable queue fallback. Zero
// values select the Stage 1 defaults. DirectBackoff is injectable so tests and
// deployments with shorter budgets can preserve the same deterministic retry
// count without sleeping on the default schedule.
type DeliveryPolicy struct {
	DirectAttempts int
	AttemptTimeout time.Duration
	DirectPatience time.Duration
	DirectBackoff  func(failedAttempt int) time.Duration
}

func defaultDeliveryPolicy() DeliveryPolicy {
	return DeliveryPolicy{
		DirectAttempts: defaultDirectAttempts,
		AttemptTimeout: defaultHTTPTimeout,
		DirectPatience: defaultDirectPatience,
		DirectBackoff: func(failedAttempt int) time.Duration {
			// Two retry gaps: 1s, then 2s. The attempt and 60-second wall-clock
			// caps remain the authoritative patience bounds.
			return time.Second << (failedAttempt - 1)
		},
	}
}

// Deliver walks candidates in order, posting req to each until one gives a
// definitive answer, and returns the outcome plus an updated cooldown map.
// credentials is mandatory and resolves a bearer for the node terminating each
// attempt; credential material is attached only to the outbound request.
//
// Per candidate:
//   - a transport failure or 502/503/504 cools the route down (RouteKey
//     recorded at now), consumes the finite direct retry budget, then advances;
//   - every other non-2xx is definitive: the walk stops and surfaces it
//     (Delivered false, StatusCode/Body set) — a home node that rejects the
//     command will reject the retry too, so trying another route is pointless;
//   - a 2xx is success: the walk stops with Delivered true.
//
// If every candidate fails at the retryable transport/peer level the walk exhausts and
// Deliver returns the accumulated cooldowns with ErrAllRoutesFailed.
//
// cooldowns is treated as read-only input; the returned Outcome.Cooldowns is a
// fresh map (input entries copied) so the function stays pure-ish — cooldowns
// in, cooldowns out. now stamps any cooldown this walk records.
func Deliver(ctx context.Context, client *http.Client, credentials BearerResolver, candidates []Candidate, req Command, cooldowns map[string]time.Time, now time.Time) (Outcome, error) {
	return DeliverWithPolicy(ctx, client, credentials, candidates, req, cooldowns, now, DeliveryPolicy{})
}

// DeliverWithPolicy is Deliver with an explicit finite direct retry budget.
// A direct route is retried only for transport failures and 502/503/504. Once
// its budget is exhausted, the walk advances to the approved durable queue
// candidates. All other HTTP responses are terminal and never bypassed through
// a queue.
func DeliverWithPolicy(ctx context.Context, client *http.Client, credentials BearerResolver, candidates []Candidate, req Command, cooldowns map[string]time.Time, now time.Time, policy DeliveryPolicy) (Outcome, error) {
	if !domain.ValidNodeID(req.OriginNodeID) {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, ErrInvalidOriginNodeID
	}
	if !domain.ValidNodeID(req.TargetNodeID) {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, ErrInvalidTargetNodeID
	}
	if err := ValidateCommandPath(req.Path); err != nil {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, err
	}
	if credentials == nil {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, ErrMissingCredential
	}
	if client == nil {
		client = http.DefaultClient
	}
	policy = normalizeDeliveryPolicy(policy)
	out := Outcome{Cooldowns: copyCooldowns(cooldowns)}
	var configurationErr error

	for _, c := range candidates {
		attempts := 1
		attemptParent := ctx
		cancelBudget := func() {}
		if c.Kind == KindDirect {
			attempts = policy.DirectAttempts
			attemptParent, cancelBudget = context.WithTimeout(ctx, policy.DirectPatience)
		}

		for attemptNumber := 1; attemptNumber <= attempts; attemptNumber++ {
			attemptCtx, cancel := context.WithTimeout(attemptParent, policy.AttemptTimeout)
			status, respBody, reqErr := post(attemptCtx, client, credentials, c, req)
			cancel()
			attempt := Attempt{Candidate: c, StatusCode: status, Err: reqErr}

			switch {
			case errors.Is(reqErr, ErrMissingCredential), errors.Is(reqErr, ErrUnsupportedRoute), errors.Is(reqErr, ErrInvalidOrigin):
				out.Attempts = append(out.Attempts, attempt)
				configurationErr = errors.Join(configurationErr, reqErr)
				attemptNumber = attempts
			case reqErr != nil || retryablePeerStatus(status):
				out.Cooldowns[c.RouteKey] = now
				attempt.CooledDown = true
				out.Attempts = append(out.Attempts, attempt)
				if c.Kind == KindDirect && attemptNumber < attempts {
					if !waitForDirectRetry(ctx, attemptParent, policy.DirectBackoff(attemptNumber)) {
						attemptNumber = attempts
					}
				}
			case status < http.StatusOK || status >= http.StatusMultipleChoices:
				out.Attempts = append(out.Attempts, attempt)
				out.Terminal = c
				out.StatusCode = status
				out.Body = respBody
				cancelBudget()
				return out, nil
			default:
				out.Attempts = append(out.Attempts, attempt)
				out.Delivered = true
				out.Terminal = c
				out.StatusCode = status
				out.Body = respBody
				cancelBudget()
				return out, nil
			}
		}
		cancelBudget()
	}

	if configurationErr != nil {
		return out, errors.Join(ErrAllRoutesFailed, configurationErr)
	}
	return out, ErrAllRoutesFailed
}

func normalizeDeliveryPolicy(policy DeliveryPolicy) DeliveryPolicy {
	defaults := defaultDeliveryPolicy()
	if policy.DirectAttempts <= 0 {
		policy.DirectAttempts = defaults.DirectAttempts
	}
	if policy.AttemptTimeout <= 0 {
		policy.AttemptTimeout = defaults.AttemptTimeout
	}
	if policy.DirectPatience <= 0 {
		policy.DirectPatience = defaults.DirectPatience
	}
	if policy.DirectBackoff == nil {
		policy.DirectBackoff = defaults.DirectBackoff
	}
	return policy
}

func retryablePeerStatus(status int) bool {
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func waitForDirectRetry(ctx, budget context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil && budget.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-budget.Done():
		return false
	}
}

func post(ctx context.Context, client *http.Client, credentials BearerResolver, c Candidate, req Command) (int, []byte, error) {
	if err := ValidateOrigin(c.URL); err != nil {
		return 0, nil, err
	}
	if c.NodeID == "" {
		return 0, nil, fmt.Errorf("%w: route has no terminating node id", ErrMissingCredential)
	}
	bearer, err := credentials(ctx, c.NodeID)
	if err != nil {
		return 0, nil, fmt.Errorf("%w for node %s: %v", ErrMissingCredential, c.NodeID, err)
	}
	if strings.TrimSpace(bearer) == "" {
		return 0, nil, fmt.Errorf("%w for node %s", ErrMissingCredential, c.NodeID)
	}

	var endpoint string
	var body []byte
	switch c.Kind {
	case KindDirect:
		endpoint = strings.TrimRight(c.URL, "/") + req.Path
		body = normalizeBody(req.Body)
	case KindQueue:
		endpoint = strings.TrimRight(c.URL, "/") + CommandPath
		body, err = json.Marshal(wireCommand{
			CommandPath:       req.Path,
			CommandBody:       normalizeBody(req.Body),
			CausingWorkItemID: req.CausingWorkItemID,
		})
		if err != nil {
			return 0, nil, fmt.Errorf("crossnode: marshal command: %w", err)
		}
	case KindRelay:
		return 0, nil, ErrUnsupportedRoute
	default:
		return 0, nil, fmt.Errorf("%w: unknown candidate kind %q", ErrUnsupportedRoute, c.Kind)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(HeaderIdempotencyKey, req.IdempotencyKey)
	httpReq.Header.Set("Authorization", "Bearer "+bearer)
	// Bind the HTTP request to the node terminating this attempt. For a queue
	// fallback that is the queue host; HeaderQueueFor separately names the
	// logical home target.
	httpReq.Header.Set(HeaderTargetNode, c.NodeID)
	httpReq.Header.Set(HeaderOriginNode, req.OriginNodeID)
	if req.OriginActorTokenID != nil {
		httpReq.Header.Set(HeaderOriginActorToken, req.OriginActorTokenID.String())
	}
	if req.OriginActorSource.Valid() {
		httpReq.Header.Set(HeaderOriginActorSource, string(req.OriginActorSource))
	}
	if req.CausingWorkItemID != nil {
		httpReq.Header.Set(HeaderCausingWorkItem, req.CausingWorkItemID.String())
	}
	switch c.Kind {
	case KindQueue:
		httpReq.Header.Set(HeaderQueueFor, req.TargetNodeID)
	}

	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	// Call the transport directly so redirects cannot move an authenticated
	// cross-node command to a different origin. The per-attempt context above
	// supplies the finite timeout normally provided by http.Client.Do.
	resp, err := transport.RoundTrip(httpReq)
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
