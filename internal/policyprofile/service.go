// Package policyprofile owns the active safety-policy profile: an
// event-sourced singleton (policy_profile.switched -> active_policy_profile)
// that says which named envelope from internal/safety governs patience right
// now. Making the profile a fact in the system — fingerprinted, attributed,
// reported by /readyz — is R4's whole point: "we are mellow during bring-up"
// stops being lore.
package policyprofile

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/safety"
)

// SubjectID is the well-known id of the singleton policy_profile aggregate.
// Every switch event shares it, so the full switch history is one subject's
// event stream.
var SubjectID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("meristem|policy_profile|active"))

var (
	ErrHumanRequired = errors.New("policyprofile: profile switch requires a human token")
	// ErrRootForbidden enforces spec principle 7: the root token only mints
	// and revokes tokens. Operating posture is switched with an ordinary
	// non-root human token.
	ErrRootForbidden = errors.New("policyprofile: root token cannot switch profiles; use a non-root human token")
)

// Service reads and switches the active profile.
type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

// Active describes the profile currently in force.
type Active struct {
	Name        string
	Fingerprint string
	Policy      safety.Policy
}

// Active resolves the profile in force. Absent projection row (never
// switched, or pre-0014 database) resolves to steady: without operator
// action the system behaves exactly as it did before profiles existed.
func (s *Service) Active(ctx context.Context) (Active, error) {
	name := safety.ProfileSteady
	var stored string
	err := s.pool.QueryRow(ctx, `SELECT name FROM active_policy_profile WHERE singleton`).Scan(&stored)
	switch {
	case err == nil:
		name = stored
	case errors.Is(err, pgx.ErrNoRows):
		// fall through to steady
	case isUndefinedTable(err):
		// Pre-0014 databases have no projection table yet. Read paths fail
		// closed to steady; switching still requires the migration because
		// the projector must have somewhere to write.
	default:
		return Active{}, fmt.Errorf("policyprofile: read active profile: %w", err)
	}
	policy, perr := safety.ProfileByName(name)
	if perr != nil {
		// A stored name this binary does not know (newer writer, older
		// reader). Fail closed to steady rather than erroring the caller:
		// readiness and the worker must keep functioning across upgrades.
		policy, _ = safety.ProfileByName(safety.ProfileSteady)
		name = safety.ProfileSteady
	}
	fp, err := policy.Fingerprint()
	if err != nil {
		return Active{}, err
	}
	return Active{Name: name, Fingerprint: fp, Policy: policy}, nil
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// SwitchInput describes one operator switch request.
type SwitchInput struct {
	To    string
	Actor domain.Token
}

// Switch appends policy_profile.switched after validating the target profile
// and the actor. Only non-root human-source tokens may switch: the profile is
// the owner's declared posture, agents must never quietly re-mellow the
// system they are being measured by, and the root token stays confined to
// token mint/revoke per spec principle 7. Switching to the already-active
// profile is a no-op that appends nothing.
func (s *Service) Switch(ctx context.Context, in SwitchInput) (Active, bool, error) {
	if in.Actor.Source != "" && in.Actor.Source != domain.SourceHuman {
		return Active{}, false, ErrHumanRequired
	}
	if in.Actor.IsRoot {
		return Active{}, false, ErrRootForbidden
	}
	target, err := safety.ProfileByName(in.To)
	if err != nil {
		return Active{}, false, err
	}
	if verr := target.Validate(); verr != nil {
		return Active{}, false, verr
	}
	fp, err := target.Fingerprint()
	if err != nil {
		return Active{}, false, err
	}

	current, err := s.Active(ctx)
	if err != nil {
		return Active{}, false, err
	}
	if current.Name == in.To {
		return current, false, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Active{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	disc, _ := idempotency.EventDiscriminator(ctx)
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectPolicyProfile,
		SubjectID:     SubjectID,
		Kind:          domain.EventPolicyProfileSwitched,
		Source:        domain.SourceHuman,
		ActorTokenID:  &in.Actor.ID,
		Discriminator: disc,
		Payload: map[string]any{
			"from":        current.Name,
			"to":          in.To,
			"fingerprint": fp,
		},
	}); err != nil {
		return Active{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Active{}, false, err
	}
	return Active{Name: in.To, Fingerprint: fp, Policy: target}, true, nil
}

// switchedPayload is the projector-facing shape of the event payload.
type switchedPayload struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Fingerprint string `json:"fingerprint"`
}
