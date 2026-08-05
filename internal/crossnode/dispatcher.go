package crossnode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/peerhttp"
)

var (
	// ErrRegistryUnavailable marks a failed read of the local, event-backed
	// nodes projection. Dispatch never substitutes ambient topology for it.
	ErrRegistryUnavailable = errors.New("crossnode: registry snapshot unavailable")
	// ErrInvalidQualifiedRef marks a malformed or local-only work-item
	// reference passed to the remote read-through service.
	ErrInvalidQualifiedRef = errors.New("crossnode: invalid remote qualified reference")
	// ErrNoDirectReadRoute reports that a qualified home is known but does not
	// advertise an approved direct_url. Reads are never parked in command_queue.
	ErrNoDirectReadRoute = errors.New("crossnode: no direct read route")
)

// RegistrySnapshot loads the caller's locally accepted, event-backed node
// registry. A Dispatcher invokes it exactly once per operation so selection
// cannot mix revisions.
type RegistrySnapshot interface {
	Load(context.Context) ([]domain.Node, error)
}

// ProjectionRegistry loads the current nodes projection from one local
// Postgres event log. It is the production RegistrySnapshot implementation.
type ProjectionRegistry struct {
	pool *pgxpool.Pool
}

func NewProjectionRegistry(pool *pgxpool.Pool) *ProjectionRegistry {
	return &ProjectionRegistry{pool: pool}
}

func (r *ProjectionRegistry) Load(ctx context.Context) ([]domain.Node, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("%w: nodes projection is not configured", ErrRegistryUnavailable)
	}
	snapshot, err := nodes.List(ctx, r.pool)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	return snapshot, nil
}

// Dispatcher is the production call seam between a domain operation that has
// resolved an object's immutable home and the cross-node HTTP transport. It
// loads one local registry projection snapshot, applies Select, and walks the
// resulting candidates through the bounded Deliver path.
//
// The HTTP client and per-node bearer resolver are injected deliberately.
// Deployments must compose this with the safe, address-class-pinning transport;
// credential lookup remains outside registry data and Candidate values.
type Dispatcher struct {
	registry    RegistrySnapshot
	client      *http.Client
	credentials BearerResolver
	policy      DeliveryPolicy
	now         func() time.Time
}

// NewDispatcher wires the production Postgres projection loader.
func NewDispatcher(pool *pgxpool.Pool, client *http.Client, credentials BearerResolver, policy DeliveryPolicy) *Dispatcher {
	return NewDispatcherWithRegistry(NewProjectionRegistry(pool), client, credentials, policy)
}

// NewDispatcherWithRegistry permits deterministic unit tests and alternate
// event-log adapters without changing routing or delivery semantics.
func NewDispatcherWithRegistry(registry RegistrySnapshot, client *http.Client, credentials BearerResolver, policy DeliveryPolicy) *Dispatcher {
	if client == nil {
		client = peerhttp.NewClient(peerhttp.Options{})
	}
	return &Dispatcher{
		registry:    registry,
		client:      client,
		credentials: credentials,
		policy:      policy,
		now:         time.Now,
	}
}

// DispatchMutation executes one allowlisted canonical REST mutation at the
// command's home node, falling back to approved durable queue hosts only under
// Deliver's retryable reachability classification. The caller owns cooldown
// persistence; the returned Outcome contains the next map.
func (d *Dispatcher) DispatchMutation(ctx context.Context, command Command, cooldowns map[string]time.Time) (Outcome, error) {
	if d == nil || d.registry == nil {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, fmt.Errorf("%w: loader is not configured", ErrRegistryUnavailable)
	}
	now := d.now().UTC()
	snapshot, err := d.registry.Load(ctx)
	if err != nil {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	candidates, err := Select(snapshot, command.TargetNodeID, cooldowns, now)
	if err != nil {
		return Outcome{Cooldowns: copyCooldowns(cooldowns)}, err
	}
	return DeliverWithPolicy(ctx, d.client, d.credentials, candidates, command, cooldowns, now, d.policy)
}

// ReadOutcome is an authenticated direct read-through response. The body is
// bounded by maxResponseBody and remains the canonical REST response body.
type ReadOutcome struct {
	TargetNodeID string
	StatusCode   int
	Body         []byte
	Attempts     []Attempt
}

// ReadWorkItem reads a remotely-homed work item. The ref may be either
// qualified spelling — the canonical `mrs://<node_id>/work-items/<uuid>` or the
// compact `<node_id>:<uuid>` — since both normalize to the same home and id. A
// bare UUID is local and is rejected here rather than guessed at: a caller that
// reached this method with an unqualified ref has lost track of where the
// object lives, and dispatching it to an arbitrary node would be worse than
// failing.
//
// It loads one registry snapshot and considers only the home's direct route;
// queue candidates are mutation-only and are never used to fabricate remote
// read availability.
func (d *Dispatcher) ReadWorkItem(ctx context.Context, originNodeID, qualifiedRef string) (ReadOutcome, error) {
	if d == nil || d.registry == nil {
		return ReadOutcome{}, fmt.Errorf("%w: loader is not configured", ErrRegistryUnavailable)
	}
	if !domain.ValidNodeID(originNodeID) {
		return ReadOutcome{}, ErrInvalidOriginNodeID
	}
	targetNodeID, id, ok := domain.ParseQualifiedRef(qualifiedRef)
	if !ok || targetNodeID == "" {
		return ReadOutcome{}, ErrInvalidQualifiedRef
	}
	now := d.now().UTC()
	snapshot, err := d.registry.Load(ctx)
	if err != nil {
		return ReadOutcome{}, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	candidates, err := Select(snapshot, targetNodeID, nil, now)
	if err != nil {
		return ReadOutcome{}, err
	}
	var direct *Candidate
	for i := range candidates {
		if candidates[i].Kind == KindDirect {
			direct = &candidates[i]
			break
		}
	}
	if direct == nil {
		return ReadOutcome{}, ErrNoDirectReadRoute
	}
	return d.readDirect(ctx, originNodeID, targetNodeID, id.String(), *direct)
}

func (d *Dispatcher) readDirect(ctx context.Context, originNodeID, targetNodeID, workItemID string, candidate Candidate) (ReadOutcome, error) {
	if err := ValidateOrigin(candidate.URL); err != nil {
		return ReadOutcome{}, err
	}
	if candidate.NodeID != targetNodeID {
		return ReadOutcome{}, ErrNoDirectReadRoute
	}
	if d.credentials == nil {
		return ReadOutcome{}, ErrMissingCredential
	}
	// A remote read only ever terminates at the home node — there is no queue
	// hop on this path — so the terminating peer and the ultimate target are
	// the same node here, and the resolver is told so explicitly rather than
	// left to infer it.
	endpoint := strings.TrimRight(candidate.URL, "/") + "/v1/work-items/" + workItemID
	peerPath := "/v1/work-items/" + workItemID
	bearer, err := d.credentials(ctx, CredentialRequest{
		TerminatingPeer: targetNodeID,
		UltimateTarget:  targetNodeID,
		OriginNodeID:    originNodeID,
		Route:           KindDirect,
		Purpose:         PurposeRemoteRead,
		Method:          http.MethodGet,
		Path:            peerPath,
	})
	if err != nil || strings.TrimSpace(bearer) == "" {
		return ReadOutcome{}, fmt.Errorf("%w for node %s", ErrMissingCredential, targetNodeID)
	}

	policy := normalizeDeliveryPolicy(d.policy)
	budget, cancelBudget := context.WithTimeout(ctx, policy.DirectPatience)
	defer cancelBudget()
	out := ReadOutcome{TargetNodeID: targetNodeID}
	for attemptNumber := 1; attemptNumber <= policy.DirectAttempts; attemptNumber++ {
		attemptCtx, cancel := context.WithTimeout(budget, policy.AttemptTimeout)
		req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodGet, endpoint, nil)
		if reqErr == nil {
			req.Header.Set("Authorization", "Bearer "+bearer)
			req.Header.Set(HeaderTargetNode, targetNodeID)
			req.Header.Set(HeaderOriginNode, originNodeID)
		}
		status, body, roundTripErr := d.roundTrip(req, reqErr)
		cancel()
		attempt := Attempt{Candidate: candidate, StatusCode: status, Err: roundTripErr}
		if roundTripErr != nil || retryablePeerStatus(status) {
			attempt.CooledDown = true
			out.Attempts = append(out.Attempts, attempt)
			if attemptNumber < policy.DirectAttempts && waitForDirectRetry(ctx, budget, policy.DirectBackoff(attemptNumber)) {
				continue
			}
			return out, ErrAllRoutesFailed
		}
		out.Attempts = append(out.Attempts, attempt)
		out.StatusCode = status
		out.Body = body
		return out, nil
	}
	return out, ErrAllRoutesFailed
}

func (d *Dispatcher) roundTrip(req *http.Request, reqErr error) (int, []byte, error) {
	if reqErr != nil {
		return 0, nil, reqErr
	}
	transport := d.client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}
