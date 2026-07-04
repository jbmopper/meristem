package projectiondefs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
)

var (
	ErrInvalidName        = errors.New("projectiondefs: invalid name")
	ErrInvalidVersion     = errors.New("projectiondefs: invalid version")
	ErrInvalidPayload     = errors.New("projectiondefs: invalid payload")
	ErrUnknownProjection  = errors.New("projectiondefs: unknown_projection")
	ErrUnknownKind        = errors.New("projectiondefs: unknown_kind")
	ErrUnknownKindClass   = errors.New("projectiondefs: unknown_kind_class")
	ErrNotProjectable     = errors.New("projectiondefs: not_projectable")
	ErrVersionConflict    = errors.New("projectiondefs: version_conflict")
	ErrRootstockImmutable = errors.New("projectiondefs: rootstock_immutable")
)

const ProjectionTypeFeed = "feed"

var subjectNamespace = uuid.MustParse("fcb38b89-3d2b-5a82-9d26-90b790d8ac4d")

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

func SubjectID(name string) uuid.UUID {
	return uuid.NewSHA1(subjectNamespace, []byte("projection|"+name))
}

func (s *Service) List(ctx context.Context) (Snapshot, error) {
	if s.pool == nil {
		return Snapshot{}, errors.New("projectiondefs: database is not configured")
	}
	items, err := s.list(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Projections: items}, nil
}

func (s *Service) Get(ctx context.Context, name string) (Projection, error) {
	if s.pool == nil {
		return Projection{}, errors.New("projectiondefs: database is not configured")
	}
	name, err := normalizeName(name)
	if err != nil {
		return Projection{}, err
	}
	row := s.pool.QueryRow(ctx, projectionSelectSQL+` WHERE name = $1`, name)
	item, err := scanProjection(row)
	if errors.Is(err, ErrUnknownProjection) {
		return Projection{}, fmt.Errorf("%w: no projection named %s; consult projections.list", ErrUnknownProjection, name)
	}
	return item, err
}

func (s *Service) Define(ctx context.Context, actor domain.Token, in DefineInput) (Projection, bool, error) {
	if s.pool == nil || s.writer == nil {
		return Projection{}, false, errors.New("projectiondefs: service is not configured")
	}
	normalized, payload, err := normalizeInput(in)
	if err != nil {
		return Projection{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Projection{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, found, err := getProjectionForUpdate(ctx, tx, normalized.Name)
	if err != nil {
		return Projection{}, false, err
	}
	if found {
		switch {
		case sameProjection(current, normalized):
			// Idempotent replay of an already-projected definition.
		default:
			exists, err := definitionEventExists(ctx, tx, SubjectID(normalized.Name), payload)
			if err != nil {
				return Projection{}, false, err
			}
			if exists {
				break
			}
			if current.Rootstock {
				return Projection{}, false, fmt.Errorf("%w: rootstock projection %s cannot be redefined", ErrRootstockImmutable, normalized.Name)
			}
			if normalized.Version != current.Version+1 {
				return Projection{}, false, fmt.Errorf("%w: %s is at version %d; got %d", ErrVersionConflict, normalized.Name, current.Version, normalized.Version)
			}
		}
	} else if normalized.Version != 1 {
		return Projection{}, false, fmt.Errorf("%w: %s must start at version 1; got %d", ErrVersionConflict, normalized.Name, normalized.Version)
	}

	_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectProjection,
		SubjectID:    SubjectID(normalized.Name),
		Kind:         domain.EventProjectionDefined,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return Projection{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Projection{}, false, err
	}
	out, err := s.Get(ctx, normalized.Name)
	return out, fresh, err
}

func normalizeInput(in DefineInput) (DefineInput, map[string]any, error) {
	name, err := normalizeName(in.Name)
	if err != nil {
		return DefineInput{}, nil, err
	}
	if reservedReason, reserved := reservedNames[name]; reserved {
		return DefineInput{}, nil, fmt.Errorf("%w: %s is reserved: %s", ErrInvalidName, name, reservedReason)
	}
	if in.Version < 1 {
		return DefineInput{}, nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidVersion)
	}
	projectionType := strings.TrimSpace(in.Type)
	if projectionType == "" {
		projectionType = ProjectionTypeFeed
	}
	if projectionType != ProjectionTypeFeed {
		return DefineInput{}, nil, fmt.Errorf("%w: type must be %q", ErrInvalidPayload, ProjectionTypeFeed)
	}
	filter, err := feed.NormalizeProjectionFilter(in.Filter)
	if err != nil {
		return DefineInput{}, nil, mapFeedFilterError(err)
	}
	in.Name = name
	in.Type = projectionType
	in.Filter = filter
	in.Description = strings.TrimSpace(in.Description)
	payload := map[string]any{
		"name":        in.Name,
		"version":     in.Version,
		"type":        in.Type,
		"rootstock":   in.Rootstock,
		"filter":      in.Filter,
		"description": in.Description,
	}
	return in, payload, nil
}

var reservedNames = map[string]string{
	"work-item-brief": "brief projections require the R3 dispatch substrate",
}

func normalizeName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrInvalidName)
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' && i > 0 && i < len(name)-1:
		default:
			return "", fmt.Errorf("%w: %q must match [a-z0-9][a-z0-9-]*", ErrInvalidName, name)
		}
	}
	if strings.Contains(name, "--") {
		return "", fmt.Errorf("%w: %q must not contain repeated hyphens", ErrInvalidName, name)
	}
	return name, nil
}

func mapFeedFilterError(err error) error {
	switch {
	case errors.Is(err, feed.ErrUnknownKind):
		return fmt.Errorf("%w: %v", ErrUnknownKind, err)
	case errors.Is(err, feed.ErrUnknownKindClass):
		return fmt.Errorf("%w: %v", ErrUnknownKindClass, err)
	case errors.Is(err, feed.ErrKindNotAllowed), errors.Is(err, feed.ErrClassNotAllowed):
		return fmt.Errorf("%w: %v", ErrNotProjectable, err)
	case errors.Is(err, feed.ErrEmptyFilter):
		return fmt.Errorf("%w: filter must include at least one kind or kind_class", ErrInvalidPayload)
	default:
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func sameProjection(current Projection, in DefineInput) bool {
	return current.Name == in.Name &&
		current.Version == in.Version &&
		current.Type == in.Type &&
		current.Rootstock == in.Rootstock &&
		current.Description == in.Description &&
		filterEqual(current.Filter, in.Filter)
}

func filterEqual(a, b feed.ProjectionFilter) bool {
	aj, errA := json.Marshal(a)
	bj, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(aj, bj)
}
