package policyprofile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the policy-profile projection writer to the
// application registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(switchedProjector{})
}

type switchedProjector struct{}

func (switchedProjector) Kind() string { return domain.EventPolicyProfileSwitched }

func (switchedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectPolicyProfile {
		return fmt.Errorf("policy_profile.switched: expected subject_kind %q, got %q", domain.SubjectPolicyProfile, event.SubjectKind)
	}
	payload, err := decodeSwitchedPayload(event.Payload)
	if err != nil {
		return err
	}
	if payload.To == "" || payload.Fingerprint == "" {
		return fmt.Errorf("policy_profile.switched: to and fingerprint are required")
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO active_policy_profile (singleton, name, fingerprint, switched_at, switched_by)
		VALUES (TRUE, $1, $2, $3, $4)
		ON CONFLICT (singleton) DO UPDATE
		SET name = EXCLUDED.name,
		    fingerprint = EXCLUDED.fingerprint,
		    switched_at = EXCLUDED.switched_at,
		    switched_by = EXCLUDED.switched_by
	`, payload.To, payload.Fingerprint, event.OccurredAt, event.ActorTokenID)
	return err
}

func decodeSwitchedPayload(raw any) (switchedPayload, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return switchedPayload{}, fmt.Errorf("policy_profile.switched: marshal payload: %w", err)
	}
	var p switchedPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return switchedPayload{}, fmt.Errorf("policy_profile.switched: unmarshal payload: %w", err)
	}
	return p, nil
}
