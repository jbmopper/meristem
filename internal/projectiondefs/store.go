package projectiondefs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
)

const projectionSelectSQL = `
	SELECT name, version, projection_type, rootstock, filter, description,
	       event_id, defined_at, defined_by, source
	FROM projections`

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Service) list(ctx context.Context) ([]Projection, error) {
	rows, err := s.pool.Query(ctx, projectionSelectSQL+` ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("projectiondefs: list projections: %w", err)
	}
	defer rows.Close()
	var out []Projection
	for rows.Next() {
		item, err := scanProjection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("projectiondefs: list projections: %w", err)
	}
	return out, nil
}

func getProjectionForUpdate(ctx context.Context, tx pgx.Tx, name string) (Projection, bool, error) {
	row := tx.QueryRow(ctx, projectionSelectSQL+` WHERE name = $1 FOR UPDATE`, name)
	item, err := scanProjection(row)
	if errors.Is(err, ErrUnknownProjection) {
		return Projection{}, false, nil
	}
	return item, err == nil, err
}

func definitionEventExists(ctx context.Context, tx pgx.Tx, subjectID uuid.UUID, payload map[string]any) (bool, error) {
	id, err := events.DeterministicID(events.Spec{
		SubjectKind: domain.SubjectProjection,
		SubjectID:   subjectID,
		Kind:        domain.EventProjectionDefined,
		Source:      domain.SourceSystem,
		Payload:     payload,
	})
	if err != nil {
		return false, fmt.Errorf("projectiondefs: derive definition event id: %w", err)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("projectiondefs: check definition event: %w", err)
	}
	return exists, nil
}

func scanProjection(row rowScanner) (Projection, error) {
	var (
		item      Projection
		filter    []byte
		definedBy uuid.NullUUID
		source    string
	)
	if err := row.Scan(
		&item.Name, &item.Version, &item.Type, &item.Rootstock, &filter,
		&item.Description, &item.EventID, &item.DefinedAt, &definedBy, &source,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Projection{}, ErrUnknownProjection
		}
		return Projection{}, fmt.Errorf("projectiondefs: scan projection: %w", err)
	}
	if err := json.Unmarshal(filter, &item.Filter); err != nil {
		return Projection{}, fmt.Errorf("projectiondefs: decode filter: %w", err)
	}
	normalized, err := feed.NormalizeProjectionFilter(item.Filter)
	if err != nil {
		return Projection{}, fmt.Errorf("projectiondefs: invalid stored filter: %w", err)
	}
	item.Filter = normalized
	if definedBy.Valid {
		item.DefinedBy = &definedBy.UUID
	}
	item.Source = source
	return item, nil
}
