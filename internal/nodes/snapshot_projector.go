package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

type snapshotObservedProjector struct{}

func (snapshotObservedProjector) Kind() string { return domain.EventRegistrySnapshotObserved }

func (snapshotObservedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectRegistrySnapshot {
		return fmt.Errorf("registry_snapshot.observed: expected subject_kind %q, got %q", domain.SubjectRegistrySnapshot, event.SubjectKind)
	}
	var snapshot RegistrySnapshot
	if err := decode(event.Payload, &snapshot); err != nil {
		return fmt.Errorf("registry_snapshot.observed: decode: %w", err)
	}
	normalized, err := NormalizeSnapshot(snapshot, snapshot.SourceNodeID)
	if err != nil {
		return fmt.Errorf("registry_snapshot.observed: %w", err)
	}
	if SnapshotSubjectID(normalized.SourceNodeID) != event.SubjectID {
		return fmt.Errorf("registry_snapshot.observed: subject/source mismatch")
	}
	digest, err := snapshotDigest(normalized)
	if err != nil {
		return fmt.Errorf("registry_snapshot.observed: digest: %w", err)
	}
	var currentSource string
	var currentRevision int64
	var currentDigest []byte
	err = tx.QueryRow(ctx, `
		SELECT source_node_id, source_revision, snapshot_digest
		FROM registry_snapshot_state WHERE singleton
		FOR UPDATE
	`).Scan(&currentSource, &currentRevision, &currentDigest)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("registry_snapshot.observed: read state: %w", err)
	}
	if err == nil {
		if currentSource != normalized.SourceNodeID {
			return fmt.Errorf("registry_snapshot.observed: source changed")
		}
		if normalized.SourceRevision < currentRevision {
			return fmt.Errorf("registry_snapshot.observed: source revision regressed")
		}
		if normalized.SourceRevision == currentRevision {
			if bytes.Equal(digest, currentDigest) {
				return nil
			}
			return fmt.Errorf("registry_snapshot.observed: source revision content conflict")
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM nodes`); err != nil {
		return fmt.Errorf("registry_snapshot.observed: clear nodes: %w", err)
	}
	for _, entry := range normalized.Nodes {
		relay, err := json.Marshal(entry.QueueVia)
		if err != nil {
			return fmt.Errorf("registry_snapshot.observed: queue_via: %w", err)
		}
		// relay_via mirrors queue_via for the expand window (migration 0041).
		if _, err := tx.Exec(ctx, `
			INSERT INTO nodes (
				node_id, base_url, direct_url, queue_via, relay_via, status,
				created_at, updated_at, registry_revision
			) VALUES ($1, $2, $3, $4::jsonb, $4::jsonb, $5, $6, $6, $7)
		`, entry.NodeID, entry.BaseURL, entry.DirectURL, relay, string(entry.Status), event.OccurredAt, entry.RegistryRevision); err != nil {
			return fmt.Errorf("registry_snapshot.observed: insert node %s: %w", entry.NodeID, err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM registry_snapshot_state`); err != nil {
		return fmt.Errorf("registry_snapshot.observed: clear state: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO registry_snapshot_state (singleton, source_node_id, source_revision, snapshot_digest, observed_at)
		VALUES (TRUE, $1, $2, $3, $4)
	`, normalized.SourceNodeID, normalized.SourceRevision, digest, event.OccurredAt); err != nil {
		return fmt.Errorf("registry_snapshot.observed: state: %w", err)
	}
	return nil
}
