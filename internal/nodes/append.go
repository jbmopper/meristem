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
	RelayVia  []string
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
	if err := validateRelayVia(p.RelayVia); err != nil {
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
	return registeredPayload{
		PayloadVersion: routePayloadVersion,
		NodeID:         p.NodeID,
		BaseURL:        baseURL,
		DirectURL:      directURL,
		RelayVia:       p.RelayVia,
		Status:         p.Status,
	}, nil
}

// RouteParams are the inputs to a node.route_updated event: a full replacement
// of the reachability route state (direct_url, relay_via, status). base_url is
// deliberately absent — registration owns the ingress URL and a route update
// never rewrites it.
type RouteParams struct {
	NodeID    string
	DirectURL *string
	RelayVia  []string
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
	if err := validateRelayVia(p.RelayVia); err != nil {
		return nil, err
	}
	directURL, err := canonicalOrigin("direct_url", p.DirectURL)
	if err != nil {
		return nil, err
	}
	return routeUpdatedPayload{
		PayloadVersion: routePayloadVersion,
		NodeID:         p.NodeID,
		DirectURL:      directURL,
		RelayVia:       p.RelayVia,
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

// validateRelayVia reports the first relay hop that is not a DNS-safe node_id.
// Shared by the build path (fail fast in the CLI) and normalizeRelayVia (fail
// closed in the projector) so both reject the same inputs.
func validateRelayVia(relay []string) error {
	for i, hop := range relay {
		if !domain.ValidNodeID(hop) {
			return fmt.Errorf("relay_via[%d] %q is not a DNS-safe node_id", i, hop)
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
		if err := json.Unmarshal(relay, &n.RelayVia); err != nil {
			return nil, fmt.Errorf("nodes: decode relay_via: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
