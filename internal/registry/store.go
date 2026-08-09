package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

const tropismSelectSQL = `
	SELECT name, version, reducer_identity, reducer_version, params, description,
	       event_id, defined_at, defined_by, source
	FROM tropisms`

const cultivarSelectSQL = `
	SELECT name, version, rootstock, tropism_name, tropism_version,
	       profile, xylem, phloem, description, event_id, defined_at, defined_by, source
	FROM cultivars`

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Service) listTropisms(ctx context.Context) ([]Tropism, error) {
	rows, err := s.pool.Query(ctx, tropismSelectSQL+` ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list tropisms: %w", err)
	}
	defer rows.Close()
	var out []Tropism
	for rows.Next() {
		item, err := scanTropism(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list tropisms: %w", err)
	}
	return out, nil
}

func (s *Service) listCultivars(ctx context.Context) ([]Cultivar, error) {
	rows, err := s.pool.Query(ctx, cultivarSelectSQL+` ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list cultivars: %w", err)
	}
	defer rows.Close()
	var out []Cultivar
	for rows.Next() {
		item, err := scanCultivar(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list cultivars: %w", err)
	}
	return out, nil
}

func getTropismForUpdate(ctx context.Context, tx pgx.Tx, name string) (Tropism, bool, error) {
	row := tx.QueryRow(ctx, tropismSelectSQL+` WHERE name = $1 FOR UPDATE`, name)
	item, err := scanTropism(row)
	if errors.Is(err, ErrUnknownTropism) {
		return Tropism{}, false, nil
	}
	return item, err == nil, err
}

func getCultivarForUpdate(ctx context.Context, tx pgx.Tx, name string) (Cultivar, bool, error) {
	row := tx.QueryRow(ctx, cultivarSelectSQL+` WHERE name = $1 FOR UPDATE`, name)
	item, err := scanCultivar(row)
	if errors.Is(err, ErrUnknownCultivar) {
		return Cultivar{}, false, nil
	}
	return item, err == nil, err
}

func tropismVersionExists(ctx context.Context, tx pgx.Tx, ref TropismRef) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events
			WHERE kind = 'tropism.defined'
			  AND subject_kind = 'tropism'
			  AND payload->>'name' = $1
			  AND (payload->>'version')::integer = $2
		)
	`, ref.Name, ref.Version).Scan(&exists); err != nil {
		return false, fmt.Errorf("registry: check tropism version: %w", err)
	}
	return exists, nil
}

func definitionEventExists(ctx context.Context, tx pgx.Tx, subjectKind string, subjectID uuid.UUID, kind string, payload map[string]any) (bool, error) {
	id, err := events.DeterministicID(events.Spec{
		SubjectKind: subjectKind,
		SubjectID:   subjectID,
		Kind:        kind,
		Source:      domain.SourceSystem,
		Payload:     payload,
	})
	if err != nil {
		return false, fmt.Errorf("registry: derive definition event id: %w", err)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("registry: check definition event: %w", err)
	}
	return exists, nil
}

func scanTropism(row rowScanner) (Tropism, error) {
	var (
		item      Tropism
		params    []byte
		definedBy uuid.NullUUID
		source    string
	)
	if err := row.Scan(
		&item.Name, &item.Version, &item.Reducer.Identity, &item.Reducer.Version,
		&params, &item.Description, &item.EventID, &item.DefinedAt, &definedBy, &source,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Tropism{}, ErrUnknownTropism
		}
		return Tropism{}, fmt.Errorf("registry: scan tropism: %w", err)
	}
	item.Params = append(json.RawMessage(nil), params...)
	if definedBy.Valid {
		item.DefinedBy = &definedBy.UUID
	}
	item.Source = source
	return item, nil
}

func scanCultivar(row rowScanner) (Cultivar, error) {
	var (
		item      Cultivar
		profile   []byte
		xylem     []byte
		definedBy uuid.NullUUID
		source    string
	)
	if err := row.Scan(
		&item.Name, &item.Version, &item.Rootstock, &item.Tropism.Name, &item.Tropism.Version,
		&profile, &xylem, &item.Phloem, &item.Description, &item.EventID, &item.DefinedAt, &definedBy, &source,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Cultivar{}, ErrUnknownCultivar
		}
		return Cultivar{}, fmt.Errorf("registry: scan cultivar: %w", err)
	}
	if err := json.Unmarshal(profile, &item.Profile); err != nil {
		return Cultivar{}, fmt.Errorf("registry: decode cultivar profile: %w", err)
	}
	if strings.TrimSpace(item.Profile.DispatchCapability) == "" {
		item.Profile.DispatchCapability = legacyDispatchCapability(item.Name, item.Version, item.Rootstock)
	}
	if err := json.Unmarshal(xylem, &item.Xylem); err != nil {
		return Cultivar{}, fmt.Errorf("registry: decode cultivar xylem: %w", err)
	}
	if definedBy.Valid {
		item.DefinedBy = &definedBy.UUID
	}
	item.Source = source
	return item, nil
}

func scanCultivarDefinitionEvent(row rowScanner) (Cultivar, error) {
	var (
		eventID   uuid.UUID
		payload   []byte
		item      Cultivar
		definedBy uuid.NullUUID
		source    string
	)
	if err := row.Scan(&eventID, &payload, &item.DefinedAt, &definedBy, &source); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Cultivar{}, ErrUnknownCultivar
		}
		return Cultivar{}, fmt.Errorf("registry: scan cultivar definition event: %w", err)
	}
	var in DefineCultivarInput
	if err := json.Unmarshal(payload, &in); err != nil {
		return Cultivar{}, fmt.Errorf("registry: decode cultivar definition event: %w", err)
	}
	normalized, _, err := normalizeCultivarInput(in)
	if err != nil {
		return Cultivar{}, fmt.Errorf("registry: invalid cultivar definition event: %w", err)
	}
	item.Name = normalized.Name
	item.Version = normalized.Version
	item.Rootstock = normalized.Rootstock
	item.Tropism = normalized.Tropism
	item.Profile = normalized.Profile
	item.Xylem = normalized.Xylem
	item.Phloem = normalized.Phloem
	item.Description = normalized.Description
	item.EventID = eventID
	if definedBy.Valid {
		item.DefinedBy = &definedBy.UUID
	}
	item.Source = source
	return item, nil
}

func sameTropism(current Tropism, in DefineTropismInput) bool {
	if current.Name != in.Name ||
		current.Version != in.Version ||
		current.Reducer != in.Reducer ||
		current.Description != in.Description {
		return false
	}
	return jsonEqual(current.Params, in.Params)
}

func sameCultivar(current Cultivar, in DefineCultivarInput) bool {
	return current.Name == in.Name &&
		current.Version == in.Version &&
		current.Rootstock == in.Rootstock &&
		current.Tropism == in.Tropism &&
		current.Profile.Briefing == in.Profile.Briefing &&
		stringSlicesEqual(current.Profile.ScopesTemplate, in.Profile.ScopesTemplate) &&
		current.Profile.DispatchCapability == in.Profile.DispatchCapability &&
		xylemEqual(current.Xylem, in.Xylem) &&
		current.Phloem == in.Phloem &&
		current.Description == in.Description
}

func xylemEqual(a, b Xylem) bool {
	return a.MaxAttempts == b.MaxAttempts &&
		a.MaxWallSeconds == b.MaxWallSeconds &&
		a.MaxDepth == b.MaxDepth &&
		a.MaxChildrenPerItem == b.MaxChildrenPerItem &&
		a.MaxConcurrentRunningPerToken == b.MaxConcurrentRunningPerToken &&
		stringIntMapsEqual(a.MaxEventsPerItemPerHourByClass, b.MaxEventsPerItemPerHourByClass)
}

func jsonEqual(a, b json.RawMessage) bool {
	na, errA := normalizeJSONObject(a, "a")
	nb, errB := normalizeJSONObject(b, "b")
	return errA == nil && errB == nil && bytes.Equal(na, nb)
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringIntMapsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if b[k] != va {
			return false
		}
	}
	return true
}
