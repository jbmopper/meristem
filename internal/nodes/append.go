package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

// RegisterParams are the operator-supplied inputs to a node.registered event.
//
// BaseURL and DirectURL are nil when the operator omits the flag, so the wire
// payload omits them (they carry the `omitempty` tag) and the event's
// deterministic id stays stable between a registration that never mentioned a
// URL and one that could not. Supplying an empty string is a different thing
// and the caller is expected to reject it before building.
type RegisterParams struct {
	NodeID    string
	BaseURL   *string
	DirectURL *string
	QueueVia  []string
	Status    string
}

// BuildRegisteredPayload validates params and returns the field-minimal v2
// node.registered payload that registeredProjector folds. It is pure: the same
// params always marshal to the same wire form, so the events writer's
// deterministic id collapses an identical re-registration onto one row while a
// genuinely changed field mints a new event (mirrors seed.go's identity model).
func BuildRegisteredPayload(p RegisterParams) (any, error) {
	if err := validateNodeID(p.NodeID); err != nil {
		return nil, err
	}
	if p.Status == "" {
		return nil, fmt.Errorf("status is required")
	}
	if err := validateQueueVia(p.QueueVia); err != nil {
		return nil, err
	}
	baseURL, err := canonicalOrigin("base_url", p.BaseURL)
	if err != nil {
		return nil, err
	}
	directURL, err := canonicalOrigin("direct_url", p.DirectURL)
	if err != nil {
		return nil, err
	}
	// Emit both spellings at payload_version 2. A pre-rename binary decoding a
	// v2 event reads relay_via only; dropping it would make that decoder fold an
	// empty route on replay or rebuild — silent route loss, not a loud failure.
	// The version does not bump because the two keys are required to be equal,
	// so the payload is still exactly a v2 payload to any reader.
	return registeredPayload{
		PayloadVersion: routePayloadVersion,
		NodeID:         p.NodeID,
		BaseURL:        baseURL,
		DirectURL:      directURL,
		QueueVia:       p.QueueVia,
		LegacyRelayVia: p.QueueVia,
		Status:         p.Status,
	}, nil
}

// RouteParams are the inputs to a node.route_updated event: a full replacement
// of the reachability route state (direct_url, queue_via, status). base_url is
// deliberately absent — registration owns the ingress URL and a route update
// never rewrites it.
type RouteParams struct {
	NodeID    string
	DirectURL *string
	QueueVia  []string
	Status    string
}

// BuildRouteUpdatedPayload validates params and returns the field-minimal v2
// node.route_updated payload. Pure, on the same terms as BuildRegisteredPayload.
func BuildRouteUpdatedPayload(p RouteParams) (any, error) {
	if err := validateNodeID(p.NodeID); err != nil {
		return nil, err
	}
	if p.Status == "" {
		return nil, fmt.Errorf("status is required")
	}
	if err := validateQueueVia(p.QueueVia); err != nil {
		return nil, err
	}
	directURL, err := canonicalOrigin("direct_url", p.DirectURL)
	if err != nil {
		return nil, err
	}
	// Both spellings, equal values — see BuildRegisteredPayload.
	return routeUpdatedPayload{
		PayloadVersion: routePayloadVersion,
		NodeID:         p.NodeID,
		DirectURL:      directURL,
		QueueVia:       p.QueueVia,
		LegacyRelayVia: p.QueueVia,
		Status:         p.Status,
	}, nil
}

func validateOrigin(field string, value *string) error {
	_, err := canonicalOrigin(field, value)
	return err
}

func canonicalOrigin(field string, value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	canonical, err := domain.CanonicalNodeOrigin(*value)
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", field, *value, err)
	}
	return &canonical, nil
}

// validateQueueVia reports the first queue-host hop that is not a DNS-safe
// node_id. Shared by the build path (fail fast in the CLI) and
// normalizeQueueVia (fail closed in the projector) so both reject the same
// inputs.
func validateQueueVia(queueVia []string) error {
	for i, hop := range queueVia {
		if !domain.ValidNodeID(hop) {
			return fmt.Errorf("queue_via[%d] %q is not a DNS-safe node_id", i, hop)
		}
	}
	return nil
}

// List returns the current-state node registry projection, ordered by node_id
// for a stable operator view. Read-only: it folds no events, it reads the rows
// the projectors already produced — the operator's view for verifying the
// stage-0 exit.
func List(ctx context.Context, pool *pgxpool.Pool) ([]domain.Node, error) {
	rows, err := pool.Query(ctx, `
		-- Reads select relay_via, not queue_via, for the whole expand window:
		-- a drifted pre-0037 binary updates only relay_via, and reading the new
		-- column would silently serve its stale value. Reads flip to queue_via
		-- in the contract release, once no old writer can still be running.
		SELECT node_id, base_url, direct_url, relay_via, status, created_at, updated_at, registry_revision
		FROM nodes
		ORDER BY node_id
	`)
	if err != nil {
		return nil, fmt.Errorf("nodes: list: %w", err)
	}
	defer rows.Close()

	var out []domain.Node
	for rows.Next() {
		var (
			n      domain.Node
			relay  []byte
			status string
		)
		if err := rows.Scan(&n.NodeID, &n.BaseURL, &n.DirectURL, &relay, &status, &n.CreatedAt, &n.UpdatedAt, &n.RegistryRevision); err != nil {
			return nil, fmt.Errorf("nodes: scan row: %w", err)
		}
		n.Status = domain.NodeStatus(status)
		if err := json.Unmarshal(relay, &n.QueueVia); err != nil {
			return nil, fmt.Errorf("nodes: decode queue_via: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
