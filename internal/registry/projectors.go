package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(tropismDefinedProjector{})
	registry.Register(cultivarDefinedProjector{})
}

type tropismDefinedProjector struct{}

func (tropismDefinedProjector) Kind() string { return domain.EventTropismDefined }

func (tropismDefinedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectTropism {
		return fmt.Errorf("tropism.defined: expected subject_kind %q, got %q", domain.SubjectTropism, event.SubjectKind)
	}
	payload, err := decodeTropismPayload(event.Payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO tropisms (
			name, version, reducer_identity, reducer_version, params, description,
			event_id, defined_at, defined_by, source
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10)
		ON CONFLICT (name) DO UPDATE SET
			version = EXCLUDED.version,
			reducer_identity = EXCLUDED.reducer_identity,
			reducer_version = EXCLUDED.reducer_version,
			params = EXCLUDED.params,
			description = EXCLUDED.description,
			event_id = EXCLUDED.event_id,
			defined_at = EXCLUDED.defined_at,
			defined_by = EXCLUDED.defined_by,
			source = EXCLUDED.source
	`, payload.Name, payload.Version, payload.Reducer.Identity, payload.Reducer.Version,
		[]byte(payload.Params), payload.Description, event.ID, event.OccurredAt, event.ActorTokenID, string(event.Source))
	if err != nil {
		return fmt.Errorf("tropism.defined: upsert projection: %w", err)
	}
	return nil
}

type cultivarDefinedProjector struct{}

func (cultivarDefinedProjector) Kind() string { return domain.EventCultivarDefined }

func (cultivarDefinedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectCultivar {
		return fmt.Errorf("cultivar.defined: expected subject_kind %q, got %q", domain.SubjectCultivar, event.SubjectKind)
	}
	payload, dispatchCapabilityDeclared, err := decodeCultivarPayload(event.Payload)
	if err != nil {
		return err
	}
	profileValue := any(payload.Profile)
	if !dispatchCapabilityDeclared {
		// Preserve the stored shape of historical profile payloads. The read
		// model derives their compatibility capability in memory, so a rebuild
		// does not manufacture a field that the authoritative event never had.
		profileValue = struct {
			Briefing       string   `json:"briefing"`
			ScopesTemplate []string `json:"scopes_template"`
		}{payload.Profile.Briefing, payload.Profile.ScopesTemplate}
	}
	profile, err := json.Marshal(profileValue)
	if err != nil {
		return fmt.Errorf("cultivar.defined: marshal profile: %w", err)
	}
	xylem, err := json.Marshal(payload.Xylem)
	if err != nil {
		return fmt.Errorf("cultivar.defined: marshal xylem: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO cultivars (
			name, version, rootstock, tropism_name, tropism_version,
			profile, xylem, phloem, description, event_id, defined_at, defined_by, source
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (name) DO UPDATE SET
			version = EXCLUDED.version,
			rootstock = EXCLUDED.rootstock,
			tropism_name = EXCLUDED.tropism_name,
			tropism_version = EXCLUDED.tropism_version,
			profile = EXCLUDED.profile,
			xylem = EXCLUDED.xylem,
			phloem = EXCLUDED.phloem,
			description = EXCLUDED.description,
			event_id = EXCLUDED.event_id,
			defined_at = EXCLUDED.defined_at,
			defined_by = EXCLUDED.defined_by,
			source = EXCLUDED.source
	`, payload.Name, payload.Version, payload.Rootstock, payload.Tropism.Name, payload.Tropism.Version,
		profile, xylem, payload.Phloem, payload.Description, event.ID, event.OccurredAt, event.ActorTokenID, string(event.Source))
	if err != nil {
		return fmt.Errorf("cultivar.defined: upsert projection: %w", err)
	}
	return nil
}

func decodeTropismPayload(raw any) (DefineTropismInput, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return DefineTropismInput{}, fmt.Errorf("tropism.defined: marshal payload: %w", err)
	}
	var p DefineTropismInput
	if err := json.Unmarshal(b, &p); err != nil {
		return DefineTropismInput{}, fmt.Errorf("tropism.defined: unmarshal payload: %w", err)
	}
	p, _, err = normalizeTropismInput(p)
	if err != nil {
		return DefineTropismInput{}, fmt.Errorf("tropism.defined: %w", err)
	}
	return p, nil
}

func decodeCultivarPayload(raw any) (DefineCultivarInput, bool, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return DefineCultivarInput{}, false, fmt.Errorf("cultivar.defined: marshal payload: %w", err)
	}
	var p DefineCultivarInput
	if err := json.Unmarshal(b, &p); err != nil {
		return DefineCultivarInput{}, false, fmt.Errorf("cultivar.defined: unmarshal payload: %w", err)
	}
	declared := strings.TrimSpace(p.Profile.DispatchCapability) != ""
	p, _, err = normalizeCultivarInput(p)
	if err != nil {
		return DefineCultivarInput{}, false, fmt.Errorf("cultivar.defined: %w", err)
	}
	return p, declared, nil
}
