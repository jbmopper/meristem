package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the node-registry projection writers to registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(registeredProjector{})
	registry.Register(routeUpdatedProjector{})
	registry.Register(snapshotObservedProjector{})
}

type registeredProjector struct{}

func (registeredProjector) Kind() string { return domain.EventNodeRegistered }

// Apply folds a node.registered event into the `nodes` table as an insert.
// Re-registration (a later node.registered for the same node) or a replay
// upserts the same row: created_at is preserved from the first registration,
// updated_at advances to this event. This keeps rebuilds deterministic — the
// events fold in seq order, so created_at always resolves to the earliest
// registration's occurred_at.
func (registeredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("node.registered: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyRegisteredV1(ctx, tx, event)
	case 2:
		return applyRegisteredV2(ctx, tx, event)
	default:
		return fmt.Errorf("node.registered: unknown payload_version %d", v)
	}
}

func applyRegisteredV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	return applyRegistered(ctx, tx, event, false)
}

func applyRegisteredV2(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	return applyRegistered(ctx, tx, event, true)
}

func applyRegistered(ctx context.Context, tx pgx.Tx, event domain.Event, strictOrigins bool) error {
	var p registeredPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("node.registered: decode payload: %w", err)
	}
	if err := validateNodeID(p.NodeID); err != nil {
		return fmt.Errorf("node.registered: %w", err)
	}
	if p.Status == "" {
		return fmt.Errorf("node.registered: status is required")
	}
	baseURL, directURL := p.BaseURL, p.DirectURL
	if strictOrigins {
		var err error
		baseURL, err = canonicalOrigin("base_url", p.BaseURL)
		if err != nil {
			return fmt.Errorf("node.registered: %w", err)
		}
		directURL, err = canonicalOrigin("direct_url", p.DirectURL)
		if err != nil {
			return fmt.Errorf("node.registered: %w", err)
		}
	}
	relay, err := normalizeQueueVia(p.QueueVia)
	if err != nil {
		return fmt.Errorf("node.registered: %w", err)
	}
	// relay_via is written alongside queue_via for the expand window (migration
	// 0037): a stale binary still reads the legacy column. Both are folded from
	// the same payload value, so the projection stays a pure fold either way.
	_, err = tx.Exec(ctx, `
		INSERT INTO nodes (node_id, base_url, direct_url, queue_via, relay_via, status, created_at, updated_at, registry_revision)
		VALUES ($1, $2, $3, $4::jsonb, $4::jsonb, $5, $6, $6, $7)
		ON CONFLICT (node_id) DO UPDATE SET
			base_url = EXCLUDED.base_url,
			direct_url = EXCLUDED.direct_url,
			queue_via = EXCLUDED.queue_via,
			relay_via = EXCLUDED.queue_via,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at,
			registry_revision = EXCLUDED.registry_revision
	`, p.NodeID, baseURL, directURL, relay, p.Status, event.OccurredAt, event.Seq)
	if err != nil {
		return fmt.Errorf("node.registered: upsert projection: %w", err)
	}
	return nil
}

type routeUpdatedProjector struct{}

func (routeUpdatedProjector) Kind() string { return domain.EventNodeRouteUpdated }

// Apply folds a node.route_updated event into the `nodes` table as an update
// of the reachability columns (direct_url, queue_via, status) for an already
// registered node. base_url and created_at are untouched. An update targeting
// an unregistered node matches zero rows and is a no-op — on a clean rebuild
// the node.registered event always precedes its route updates in seq order.
func (routeUpdatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("node.route_updated: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyRouteUpdatedV1(ctx, tx, event)
	case 2:
		return applyRouteUpdatedV2(ctx, tx, event)
	default:
		return fmt.Errorf("node.route_updated: unknown payload_version %d", v)
	}
}

func applyRouteUpdatedV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	return applyRouteUpdated(ctx, tx, event, false)
}

func applyRouteUpdatedV2(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	return applyRouteUpdated(ctx, tx, event, true)
}

func applyRouteUpdated(ctx context.Context, tx pgx.Tx, event domain.Event, strictOrigins bool) error {
	var p routeUpdatedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("node.route_updated: decode payload: %w", err)
	}
	if err := validateNodeID(p.NodeID); err != nil {
		return fmt.Errorf("node.route_updated: %w", err)
	}
	if p.Status == "" {
		return fmt.Errorf("node.route_updated: status is required")
	}
	directURL := p.DirectURL
	if strictOrigins {
		var err error
		directURL, err = canonicalOrigin("direct_url", p.DirectURL)
		if err != nil {
			return fmt.Errorf("node.route_updated: %w", err)
		}
	}
	relay, err := normalizeQueueVia(p.QueueVia)
	if err != nil {
		return fmt.Errorf("node.route_updated: %w", err)
	}
	// relay_via mirrors queue_via for the expand window; see the registered
	// projector above and migration 0037.
	_, err = tx.Exec(ctx, `
		UPDATE nodes SET
			direct_url = $2,
			queue_via = $3::jsonb,
			relay_via = $3::jsonb,
			status = $4,
			updated_at = $5,
			registry_revision = $6
		WHERE node_id = $1
	`, p.NodeID, directURL, relay, p.Status, event.OccurredAt, event.Seq)
	if err != nil {
		return fmt.Errorf("node.route_updated: update projection: %w", err)
	}
	return nil
}

func validateNodeID(nodeID string) error {
	if !domain.ValidNodeID(nodeID) {
		return fmt.Errorf("node_id %q is not a DNS-safe id (lowercase alnum + internal hyphen, 1-%d chars)", nodeID, domain.MaxNodeIDLen)
	}
	return nil
}

// normalizeQueueVia validates every queue-host hop is a DNS-safe node_id and
// returns the JSONB-encoded array. A nil/empty input encodes as `[]` so the
// column's default is honoured on every write path.
func normalizeQueueVia(queueVia []string) ([]byte, error) {
	if queueVia == nil {
		queueVia = []string{}
	}
	if err := validateQueueVia(queueVia); err != nil {
		return nil, err
	}
	b, err := json.Marshal(queueVia)
	if err != nil {
		return nil, fmt.Errorf("marshal queue_via: %w", err)
	}
	return b, nil
}
