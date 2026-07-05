package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var (
	ErrInvalidName        = errors.New("registry: invalid name")
	ErrInvalidVersion     = errors.New("registry: invalid version")
	ErrUnknownReducer     = errors.New("registry: unknown_reducer")
	ErrUnknownTropism     = errors.New("registry: unknown_tropism")
	ErrUnknownCultivar    = errors.New("registry: unknown_cultivar")
	ErrVersionConflict    = errors.New("registry: version_conflict")
	ErrRootstockImmutable = errors.New("registry: rootstock_immutable")
	ErrInvalidPayload     = errors.New("registry: invalid payload")
)

var subjectNamespace = uuid.MustParse("e6f914d2-8659-55c7-86d3-28172a6e38c7")

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

func TropismSubjectID(name string) uuid.UUID {
	return uuid.NewSHA1(subjectNamespace, []byte("tropism|"+name))
}

func CultivarSubjectID(name string) uuid.UUID {
	return uuid.NewSHA1(subjectNamespace, []byte("cultivar|"+name))
}

func (s *Service) List(ctx context.Context) (Snapshot, error) {
	if s.pool == nil {
		return Snapshot{}, errors.New("registry: database is not configured")
	}
	tropisms, err := s.listTropisms(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	cultivars, err := s.listCultivars(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Tropisms: tropisms, Cultivars: cultivars}, nil
}

func (s *Service) GetTropism(ctx context.Context, name string) (Tropism, error) {
	if s.pool == nil {
		return Tropism{}, errors.New("registry: database is not configured")
	}
	name, err := normalizeName(name)
	if err != nil {
		return Tropism{}, err
	}
	row := s.pool.QueryRow(ctx, tropismSelectSQL+` WHERE name = $1`, name)
	item, err := scanTropism(row)
	if errors.Is(err, ErrUnknownTropism) {
		return Tropism{}, fmt.Errorf("%w: no tropism named %s; consult registry.list", ErrUnknownTropism, name)
	}
	return item, err
}

func (s *Service) GetCultivar(ctx context.Context, name string) (Cultivar, error) {
	if s.pool == nil {
		return Cultivar{}, errors.New("registry: database is not configured")
	}
	name, err := normalizeName(name)
	if err != nil {
		return Cultivar{}, err
	}
	row := s.pool.QueryRow(ctx, cultivarSelectSQL+` WHERE name = $1`, name)
	item, err := scanCultivar(row)
	if errors.Is(err, ErrUnknownCultivar) {
		return Cultivar{}, fmt.Errorf("%w: no cultivar named %s; consult registry.list", ErrUnknownCultivar, name)
	}
	return item, err
}

func (s *Service) GetCultivarRef(ctx context.Context, ref string) (Cultivar, error) {
	if s.pool == nil {
		return Cultivar{}, errors.New("registry: database is not configured")
	}
	name, version, err := ParseCultivarRef(ref)
	if err != nil {
		return Cultivar{}, err
	}
	if version == 0 {
		return s.GetCultivar(ctx, name)
	}
	row := s.pool.QueryRow(ctx, `
		SELECT id, payload, occurred_at, actor_token_id, source
		FROM events
		WHERE subject_kind = $1
		  AND kind = $2
		  AND payload->>'name' = $3
		  AND (payload->>'version')::integer = $4
		ORDER BY occurred_at DESC, seq DESC
		LIMIT 1
	`, domain.SubjectCultivar, domain.EventCultivarDefined, name, version)
	item, err := scanCultivarDefinitionEvent(row)
	if errors.Is(err, ErrUnknownCultivar) {
		return Cultivar{}, fmt.Errorf("%w: no cultivar named %s@%d; consult registry.list", ErrUnknownCultivar, name, version)
	}
	return item, err
}

func ParseCultivarRef(ref string) (string, int, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", 0, fmt.Errorf("%w: cultivar is required", ErrInvalidPayload)
	}
	name := ref
	version := 0
	if before, after, found := strings.Cut(ref, "@"); found {
		name = strings.TrimSpace(before)
		rawVersion := strings.TrimSpace(after)
		parsed, err := strconv.Atoi(rawVersion)
		if err != nil || parsed < 1 {
			return "", 0, fmt.Errorf("%w: cultivar version must be >= 1", ErrInvalidVersion)
		}
		version = parsed
	}
	normalized, err := normalizeName(name)
	if err != nil {
		return "", 0, err
	}
	return normalized, version, nil
}

func (s *Service) DefineTropism(ctx context.Context, actor domain.Token, in DefineTropismInput) (Tropism, bool, error) {
	if s.pool == nil || s.writer == nil {
		return Tropism{}, false, errors.New("registry: service is not configured")
	}
	normalized, payload, err := normalizeTropismInput(in)
	if err != nil {
		return Tropism{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Tropism{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, found, err := getTropismForUpdate(ctx, tx, normalized.Name)
	if err != nil {
		return Tropism{}, false, err
	}
	if found {
		if sameTropism(current, normalized) {
			// Idempotent replay of a definition that already projected.
		} else if normalized.Version != current.Version+1 {
			exists, err := definitionEventExists(ctx, tx, domain.SubjectTropism, TropismSubjectID(normalized.Name), domain.EventTropismDefined, payload)
			if err != nil {
				return Tropism{}, false, err
			}
			if !exists {
				return Tropism{}, false, fmt.Errorf("%w: %s is at version %d; got %d", ErrVersionConflict, normalized.Name, current.Version, normalized.Version)
			}
		}
	} else if normalized.Version != 1 {
		return Tropism{}, false, fmt.Errorf("%w: %s must start at version 1; got %d", ErrVersionConflict, normalized.Name, normalized.Version)
	}

	_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectTropism,
		SubjectID:    TropismSubjectID(normalized.Name),
		Kind:         domain.EventTropismDefined,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return Tropism{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Tropism{}, false, err
	}
	out, err := s.GetTropism(ctx, normalized.Name)
	return out, fresh, err
}

func (s *Service) DefineCultivar(ctx context.Context, actor domain.Token, in DefineCultivarInput) (Cultivar, bool, error) {
	if s.pool == nil || s.writer == nil {
		return Cultivar{}, false, errors.New("registry: service is not configured")
	}
	normalized, payload, err := normalizeCultivarInput(in)
	if err != nil {
		return Cultivar{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Cultivar{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	exists, err := tropismVersionExists(ctx, tx, normalized.Tropism)
	if err != nil {
		return Cultivar{}, false, err
	}
	if !exists {
		return Cultivar{}, false, fmt.Errorf("%w: no tropism named %s@%d; consult registry.list", ErrUnknownTropism, normalized.Tropism.Name, normalized.Tropism.Version)
	}

	current, found, err := getCultivarForUpdate(ctx, tx, normalized.Name)
	if err != nil {
		return Cultivar{}, false, err
	}
	if found {
		switch {
		case sameCultivar(current, normalized):
			// Idempotent replay of a definition that already projected.
		default:
			exists, err := definitionEventExists(ctx, tx, domain.SubjectCultivar, CultivarSubjectID(normalized.Name), domain.EventCultivarDefined, payload)
			if err != nil {
				return Cultivar{}, false, err
			}
			if exists {
				break
			}
			if current.Rootstock {
				return Cultivar{}, false, fmt.Errorf("%w: rootstock cultivar %s cannot be redefined", ErrRootstockImmutable, normalized.Name)
			}
			if normalized.Version != current.Version+1 {
				return Cultivar{}, false, fmt.Errorf("%w: %s is at version %d; got %d", ErrVersionConflict, normalized.Name, current.Version, normalized.Version)
			}
		}
	} else if normalized.Version != 1 {
		return Cultivar{}, false, fmt.Errorf("%w: %s must start at version 1; got %d", ErrVersionConflict, normalized.Name, normalized.Version)
	}

	_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectCultivar,
		SubjectID:    CultivarSubjectID(normalized.Name),
		Kind:         domain.EventCultivarDefined,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return Cultivar{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Cultivar{}, false, err
	}
	out, err := s.GetCultivar(ctx, normalized.Name)
	return out, fresh, err
}

func normalizeTropismInput(in DefineTropismInput) (DefineTropismInput, map[string]any, error) {
	name, err := normalizeName(in.Name)
	if err != nil {
		return DefineTropismInput{}, nil, err
	}
	if in.Version < 1 {
		return DefineTropismInput{}, nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidVersion)
	}
	if strings.TrimSpace(in.Reducer.Identity) == "" || in.Reducer.Version < 1 {
		return DefineTropismInput{}, nil, fmt.Errorf("%w: reducer identity and version are required", ErrInvalidPayload)
	}
	in.Reducer.Identity = strings.TrimSpace(in.Reducer.Identity)
	if !convergence.KnownReducer(in.Reducer.Identity, in.Reducer.Version) {
		return DefineTropismInput{}, nil, fmt.Errorf("%w: no reducer %s@%d", ErrUnknownReducer, in.Reducer.Identity, in.Reducer.Version)
	}
	params, err := normalizeJSONObject(in.Params, "params")
	if err != nil {
		return DefineTropismInput{}, nil, err
	}
	in.Name = name
	in.Params = params
	in.Description = strings.TrimSpace(in.Description)
	payload := map[string]any{
		"name":        in.Name,
		"version":     in.Version,
		"reducer":     map[string]any{"identity": in.Reducer.Identity, "version": in.Reducer.Version},
		"params":      json.RawMessage(params),
		"description": in.Description,
	}
	return in, payload, nil
}

func normalizeCultivarInput(in DefineCultivarInput) (DefineCultivarInput, map[string]any, error) {
	name, err := normalizeName(in.Name)
	if err != nil {
		return DefineCultivarInput{}, nil, err
	}
	if in.Version < 1 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidVersion)
	}
	tropismName, err := normalizeName(in.Tropism.Name)
	if err != nil {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: tropism.name: %v", ErrInvalidPayload, err)
	}
	if in.Tropism.Version < 1 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: tropism.version must be >= 1", ErrInvalidPayload)
	}
	in.Profile.Briefing = strings.TrimSpace(in.Profile.Briefing)
	if in.Profile.Briefing == "" {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: profile.briefing is required", ErrInvalidPayload)
	}
	cleanScopes := make([]string, 0, len(in.Profile.ScopesTemplate))
	seenScope := map[string]bool{}
	for i, scope := range in.Profile.ScopesTemplate {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			return DefineCultivarInput{}, nil, fmt.Errorf("%w: profile.scopes_template[%d] is blank", ErrInvalidPayload, i)
		}
		if !seenScope[scope] {
			seenScope[scope] = true
			cleanScopes = append(cleanScopes, scope)
		}
	}
	if len(cleanScopes) == 0 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: profile.scopes_template is required", ErrInvalidPayload)
	}
	if in.Xylem.MaxAttempts < 1 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: xylem.max_attempts must be >= 1", ErrInvalidPayload)
	}
	if in.Xylem.MaxWallSeconds < 1 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: xylem.max_wall_seconds must be >= 1", ErrInvalidPayload)
	}
	if in.Xylem.MaxDepth < 0 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: xylem.max_depth must be >= 0", ErrInvalidPayload)
	}
	if in.Xylem.MaxChildrenPerItem < 0 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: xylem.max_children_per_item must be >= 0", ErrInvalidPayload)
	}
	if in.Xylem.MaxConcurrentRunningPerToken < 0 {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: xylem.max_concurrent_running_items_per_token must be >= 0", ErrInvalidPayload)
	}
	in.Phloem = strings.TrimSpace(in.Phloem)
	if in.Phloem == "" {
		return DefineCultivarInput{}, nil, fmt.Errorf("%w: phloem is required", ErrInvalidPayload)
	}
	in.Name = name
	in.Tropism.Name = tropismName
	in.Profile.ScopesTemplate = cleanScopes
	in.Description = strings.TrimSpace(in.Description)
	payload := map[string]any{
		"name":        in.Name,
		"version":     in.Version,
		"rootstock":   in.Rootstock,
		"tropism":     map[string]any{"name": in.Tropism.Name, "version": in.Tropism.Version},
		"profile":     in.Profile,
		"xylem":       in.Xylem,
		"phloem":      in.Phloem,
		"description": in.Description,
	}
	return in, payload, nil
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

func normalizeJSONObject(raw json.RawMessage, field string) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`), nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %s must be valid JSON: %v", ErrInvalidPayload, field, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, fmt.Errorf("%w: %s must be a JSON object", ErrInvalidPayload, field)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: normalize %s: %v", ErrInvalidPayload, field, err)
	}
	return out, nil
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}
