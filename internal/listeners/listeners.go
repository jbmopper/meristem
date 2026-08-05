package listeners

// Durable listener registrations (docs/listener-control-plane.md, slice 2).
// A registration is the stable routing address other actors target; the
// bearer credential rotates UNDER it via listener.credential_bound without
// changing the address. It is an attributed client endpoint, not a persona,
// and it grants no authority: authorization stays with the bound token's own
// scopes at claim/write time.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var (
	ErrInvalidRequest = errors.New("listeners: invalid request")
	ErrNotFound       = errors.New("listeners: listener not found")
	ErrNameTaken      = errors.New("listeners: listener name already registered")
	ErrRetired        = errors.New("listeners: listener is retired")
	ErrStalePolicy    = errors.New("listeners: observed policy revision is stale")
	ErrNotAuthorized  = errors.New("listeners: actor is not authorized for this operation")
)

// Registration is the projected durable listener state.
type Registration struct {
	ID                       uuid.UUID
	Name                     string
	PrincipalTokenID         uuid.UUID
	Provider                 string
	Capabilities             []string
	MaxConcurrentAssignments int
	Policy                   *Policy
	PolicyFingerprint        string
	PolicyEventID            *uuid.UUID
	StateEventID             uuid.UUID
	RetiredAt                *time.Time
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// eventDiscriminator distinguishes distinct logical actions whose event
// payloads legitimately repeat — policy A -> B -> A cycles and credential
// rebinding cycles (LCP2-R2-B1). Under the idempotency contract it is the
// caller's (token, scope, key) identity: stable across retries of ONE action,
// distinct across actions. Direct service calls outside that contract fall
// back to the current state/policy predecessor event id, which advances with
// every accepted mutation, so a repeated payload after intervening change is
// a new event rather than a silent collapse onto the old one.
func listenerEventDiscriminator(ctx context.Context, label string, predecessor uuid.UUID) string {
	if disc, ok := idempotency.EventDiscriminator(ctx); ok {
		return disc
	}
	return label + ":" + predecessor.String()
}

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

// listenerAdminActor pins the separation of duties as a DOMAIN invariant
// (LCP2-B3): registration, credential binding, retirement, and wide policy
// replacement are authorized by the single access.CanAdminListeners reducer —
// an explicitly scoped, non-root human credential. Enforcing it here, not in
// the transports, means an internal caller (reconciler, CLI, future adapter)
// cannot bypass the scope by calling the service directly.
func listenerAdminActor(actor domain.Token) error {
	if actor.ID == uuid.Nil {
		return fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if actor.IsRoot {
		return fmt.Errorf("%w: root token is mint/revoke-only", ErrNotAuthorized)
	}
	if !access.CanAdminListeners(actor) {
		return fmt.Errorf("%w: listener administration requires a non-root human credential holding %s", ErrNotAuthorized, access.ScopeListenersAdmin)
	}
	return nil
}

type RegisterInput struct {
	Name             string
	PrincipalTokenID uuid.UUID
	Provider         string
	Capabilities     []string
	Actor            domain.Token
}

func (s *Service) Register(ctx context.Context, in RegisterInput) (Registration, error) {
	if err := listenerAdminActor(in.Actor); err != nil {
		return Registration{}, err
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Registration{}, fmt.Errorf("%w: name is required", ErrInvalidRequest)
	}
	if in.PrincipalTokenID == uuid.Nil {
		return Registration{}, fmt.Errorf("%w: principal_token_id is required", ErrInvalidRequest)
	}
	capabilities, err := normalizeCapabilities(in.Capabilities)
	if err != nil {
		return Registration{}, err
	}
	if len(capabilities) == 0 {
		return Registration{}, fmt.Errorf("%w: at least one capability is required", ErrInvalidRequest)
	}
	id := newSubjectID(ctx)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Registration{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.requireLiveToken(ctx, tx, in.PrincipalTokenID); err != nil {
		return Registration{}, err
	}
	var nameTaken bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM listener_registrations WHERE name=$1)`, name).Scan(&nameTaken); err != nil {
		return Registration{}, fmt.Errorf("listeners: check name: %w", err)
	}
	if nameTaken {
		return Registration{}, fmt.Errorf("%w: %s", ErrNameTaken, name)
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectListener,
		SubjectID:    id,
		Kind:         domain.EventListenerRegistered,
		Source:       domain.SourceHuman,
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"payload_version":            1,
			"name":                       name,
			"principal_token_id":         in.PrincipalTokenID,
			"provider":                   strings.TrimSpace(in.Provider),
			"capabilities":               capabilities,
			"max_concurrent_assignments": 1,
		},
	}); err != nil {
		return Registration{}, err
	}
	reg, err := scanRegistration(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, err
	}
	return reg, nil
}

// BindCredential rotates the principal credential under the stable address.
// It never creates a new assignment generation and never touches policy:
// authorization for in-flight work resolves against the CURRENT binding at
// event time (design LDR-M2 resolution).
func (s *Service) BindCredential(ctx context.Context, id uuid.UUID, principalTokenID uuid.UUID, actor domain.Token) (Registration, error) {
	if err := listenerAdminActor(actor); err != nil {
		return Registration{}, err
	}
	if principalTokenID == uuid.Nil {
		return Registration{}, fmt.Errorf("%w: principal_token_id is required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Registration{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	reg, err := scanRegistrationForUpdate(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if reg.RetiredAt != nil {
		return Registration{}, fmt.Errorf("%w: %s", ErrRetired, id)
	}
	if reg.PrincipalTokenID == principalTokenID {
		// Rebinding the already-bound principal is an idempotent no-op:
		// nothing rotates, so no event is minted.
		return reg, tx.Commit(ctx)
	}
	if err := s.requireLiveToken(ctx, tx, principalTokenID); err != nil {
		return Registration{}, err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectListener,
		SubjectID:     id,
		Kind:          domain.EventListenerCredentialBound,
		Source:        domain.SourceHuman,
		ActorTokenID:  &actor.ID,
		Discriminator: listenerEventDiscriminator(ctx, "listener_binding", reg.StateEventID),
		Payload: map[string]any{
			"payload_version":    1,
			"principal_token_id": principalTokenID,
			"previous_token_id":  reg.PrincipalTokenID,
		},
	}); err != nil {
		return Registration{}, err
	}
	out, err := scanRegistration(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, err
	}
	return out, nil
}

// SetPolicy replaces the base policy. ObservedPolicyEventID is the caller's
// read of the current policy revision: a mismatch is a pure conflict that
// appends nothing. The listener's own principal may replace an EXISTING
// policy only when the replacement Narrows the prior one; the initial policy
// and every wider move require access.CanAdminListeners — decided here from
// the actor token alone, never from a caller-supplied surface flag.
type SetPolicyInput struct {
	Policy                Policy
	ObservedPolicyEventID *uuid.UUID
	Actor                 domain.Token
}

func (s *Service) SetPolicy(ctx context.Context, id uuid.UUID, in SetPolicyInput) (Registration, error) {
	if in.Actor.ID == uuid.Nil {
		return Registration{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if in.Actor.IsRoot {
		return Registration{}, fmt.Errorf("%w: root token is mint/revoke-only", ErrNotAuthorized)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Registration{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	reg, err := scanRegistrationForUpdate(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if reg.RetiredAt != nil {
		return Registration{}, fmt.Errorf("%w: %s", ErrRetired, id)
	}
	switch {
	case in.ObservedPolicyEventID == nil && reg.PolicyEventID != nil,
		in.ObservedPolicyEventID != nil && reg.PolicyEventID == nil,
		in.ObservedPolicyEventID != nil && reg.PolicyEventID != nil && *in.ObservedPolicyEventID != *reg.PolicyEventID:
		return Registration{}, fmt.Errorf("%w: observed=%v current=%v", ErrStalePolicy, in.ObservedPolicyEventID, reg.PolicyEventID)
	}
	normalized, fingerprint, err := NormalizePolicy(in.Policy, id, reg.Capabilities)
	if err != nil {
		return Registration{}, err
	}
	switch {
	case access.CanAdminListeners(in.Actor):
		// Listener administration: full replacement allowed, wider included.
	case in.Actor.ID == reg.PrincipalTokenID:
		// The listener's own principal may only NARROW an existing policy —
		// the reducer above never admits a non-human, so an agent can never
		// widen its own lens whatever route it took. The INITIAL policy is
		// the baseline being narrowed, so it requires administration too: a
		// principal cannot invent its own starting lens (LCP2-B2).
		if reg.Policy == nil {
			return Registration{}, fmt.Errorf("%w: the initial base policy requires listener administration", ErrNotAuthorized)
		}
		if !Narrows(*reg.Policy, normalized) {
			return Registration{}, fmt.Errorf("%w: a listener may narrow its own policy but not widen it", ErrNotAuthorized)
		}
	default:
		return Registration{}, fmt.Errorf("%w: policy replacement requires listener administration or the listener's own principal", ErrNotAuthorized)
	}
	payload := map[string]any{
		"payload_version":            PolicyVersion,
		"listener_id":                id,
		"projection":                 normalized.Projection,
		"predicates":                 normalized.Predicates,
		"capabilities":               normalized.Capabilities,
		"max_concurrent_assignments": normalized.MaxConcurrentAssignments,
		"focus":                      normalized.Focus,
		"predicate_fingerprint":      fingerprint,
	}
	// The policy predecessor advances with every accepted revision (the
	// initial revision descends from the registration state event), so an
	// A -> B -> A cycle mints three distinct events and the projector runs
	// for each; the idempotency identity still collapses genuine retries.
	predecessor := reg.StateEventID
	if reg.PolicyEventID != nil {
		predecessor = *reg.PolicyEventID
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectListener,
		SubjectID:     id,
		Kind:          domain.EventListenerPolicySet,
		Source:        sourceForActor(in.Actor),
		ActorTokenID:  &in.Actor.ID,
		Discriminator: listenerEventDiscriminator(ctx, "listener_policy", predecessor),
		Payload:       payload,
	}); err != nil {
		return Registration{}, err
	}
	out, err := scanRegistration(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, err
	}
	return out, nil
}

func (s *Service) Retire(ctx context.Context, id uuid.UUID, reason string, actor domain.Token) (Registration, error) {
	if err := listenerAdminActor(actor); err != nil {
		return Registration{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Registration{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	reg, err := scanRegistrationForUpdate(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if reg.RetiredAt != nil {
		return reg, tx.Commit(ctx)
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectListener,
		SubjectID:    id,
		Kind:         domain.EventListenerRetired,
		Source:       domain.SourceHuman,
		ActorTokenID: &actor.ID,
		Payload:      map[string]any{"payload_version": 1, "reason": strings.TrimSpace(reason)},
	}); err != nil {
		return Registration{}, err
	}
	out, err := scanRegistration(ctx, tx, id)
	if err != nil {
		return Registration{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, err
	}
	return out, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Registration, error) {
	return scanRegistration(ctx, s.pool, id)
}

func (s *Service) GetByName(ctx context.Context, name string) (Registration, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT id FROM listener_registrations WHERE name=$1`, strings.TrimSpace(name)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Registration{}, fmt.Errorf("%w: name %s", ErrNotFound, name)
	}
	if err != nil {
		return Registration{}, fmt.Errorf("listeners: resolve name: %w", err)
	}
	return scanRegistration(ctx, s.pool, id)
}

func (s *Service) List(ctx context.Context, includeRetired bool) ([]Registration, error) {
	query := `SELECT id FROM listener_registrations`
	if !includeRetired {
		query += ` WHERE retired_at IS NULL`
	}
	query += ` ORDER BY created_at, id`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("listeners: list: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Registration, 0, len(ids))
	for _, id := range ids {
		reg, err := scanRegistration(ctx, s.pool, id)
		if err != nil {
			return nil, err
		}
		out = append(out, reg)
	}
	return out, nil
}

func (s *Service) requireLiveToken(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	var revoked pgtype.Timestamptz
	err := tx.QueryRow(ctx, `SELECT revoked_at FROM tokens WHERE id=$1`, id).Scan(&revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: principal token %s does not exist", ErrInvalidRequest, id)
	}
	if err != nil {
		return fmt.Errorf("listeners: check principal token: %w", err)
	}
	if revoked.Valid {
		return fmt.Errorf("%w: principal token %s is revoked", ErrInvalidRequest, id)
	}
	return nil
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanRegistrationForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Registration, error) {
	return scanRegistrationQuery(ctx, tx, id, true)
}

func scanRegistration(ctx context.Context, q queryer, id uuid.UUID) (Registration, error) {
	return scanRegistrationQuery(ctx, q, id, false)
}

func scanRegistrationQuery(ctx context.Context, q queryer, id uuid.UUID, forUpdate bool) (Registration, error) {
	query := `
		SELECT id, name, principal_token_id, provider, capabilities,
		       max_concurrent_assignments, policy, policy_fingerprint,
		       policy_event_id, state_event_id, retired_at, created_at, updated_at
		FROM listener_registrations
		WHERE id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var (
		reg          Registration
		capabilities []byte
		policyRaw    []byte
		fingerprint  pgtype.Text
		policyEvent  pgtype.UUID
		retiredAt    pgtype.Timestamptz
	)
	err := q.QueryRow(ctx, query, id).Scan(
		&reg.ID, &reg.Name, &reg.PrincipalTokenID, &reg.Provider, &capabilities,
		&reg.MaxConcurrentAssignments, &policyRaw, &fingerprint,
		&policyEvent, &reg.StateEventID, &retiredAt, &reg.CreatedAt, &reg.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Registration{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Registration{}, fmt.Errorf("listeners: scan registration: %w", err)
	}
	if err := json.Unmarshal(capabilities, &reg.Capabilities); err != nil {
		return Registration{}, fmt.Errorf("listeners: decode capabilities: %w", err)
	}
	if len(policyRaw) > 0 {
		var policy Policy
		if err := json.Unmarshal(policyRaw, &policy); err != nil {
			return Registration{}, fmt.Errorf("listeners: decode policy: %w", err)
		}
		reg.Policy = &policy
	}
	if fingerprint.Valid {
		reg.PolicyFingerprint = fingerprint.String
	}
	if policyEvent.Valid {
		id := uuid.UUID(policyEvent.Bytes)
		reg.PolicyEventID = &id
	}
	if retiredAt.Valid {
		t := retiredAt.Time
		reg.RetiredAt = &t
	}
	return reg, nil
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func newSubjectID(ctx context.Context) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, "listener"); ok {
		return id
	}
	return uuid.New()
}
