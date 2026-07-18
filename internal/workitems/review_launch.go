package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
)

// Atomic reviewer provisioning (ee916614 slice 3a, accepted lifecycle design
// revision 4): one transaction verifies the caller's capability and exact
// queue lease, mints a single-use exact-child reviewer credential, appends
// its mode=spawn binding, and reserves durable launch capacity — all against
// one database clock, so token expiry and binding ExpiresAt are equal by
// construction and an issued-but-unbound credential is structurally
// impossible. The review_launch reservation/handle/outcome rows are caused by
// events and projected in the same transaction; only job_queue lease fields
// remain the narrow operational direct-update exception.

var (
	// ErrProvisionLeaseFenced refuses provisioning when the named queue lease
	// is not exactly the caller's: wrong owner, stale generation, expired
	// deadline, or a different attempt. state='leased' alone is never trusted.
	ErrProvisionLeaseFenced = errors.New("workitems: review provisioning lease fence failed")
	// ErrProvisionRoundInvalid refuses provisioning while the child does not
	// declare a current review round with exactly one valid artifact commit.
	ErrProvisionRoundInvalid = errors.New("workitems: review provisioning requires a current round with one exact artifact")
	// ErrReviewLaunchCapacity reports that every launch slot is durably
	// reserved. The caller parks the job dormant WITHOUT consuming an attempt
	// (jobqueue.ReturnReviewDormant); no credential was minted.
	ErrReviewLaunchCapacity = errors.New("workitems: review launch capacity exhausted")
	// ErrReviewLaunchNotFound reports a handle/resolve call naming a launch
	// attempt that was never reserved.
	ErrReviewLaunchNotFound = errors.New("workitems: review launch not found")
	// ErrReviewLaunchState refuses a handle/resolve call against a launch row
	// whose durable state cannot legally take that step.
	ErrReviewLaunchState = errors.New("workitems: review launch is not in a legal state for this operation")
	// ErrSpawnAssigneeIsImplementer structurally refuses self-review: the
	// token that authored the current round's implementation.ready_for_review
	// marker can never be bound as its reviewer, under any attachment
	// strategy.
	ErrSpawnAssigneeIsImplementer = errors.New("workitems: spawn assignee authored the current review round and cannot review it")
	// ErrSpawnAssigneeAlreadyUsed enforces single-use reviewer identities: a
	// token that appears in any prior work_item.assigned event binds at most
	// once, ever, so two live process trees can never share a bearer.
	ErrSpawnAssigneeAlreadyUsed = errors.New("workitems: spawn assignee was already bound once and reviewer identities are single-use")
)

// ReviewLaunchOutcome is the closed vocabulary for resolving one launch.
type ReviewLaunchOutcome string

const (
	// ReviewLaunchSucceeded is the only outcome that lets the queue job
	// complete: the reviewer process was created for the exact binding and
	// its handle is durable.
	ReviewLaunchSucceeded ReviewLaunchOutcome = "succeeded"
	// ReviewLaunchFailed frees capacity immediately: the launch is confirmed
	// dead (supervisor-owned kill/Wait, adoption-confirmed absence) or never
	// ran. The binding generation is released and the credential revoked in
	// the same transaction.
	ReviewLaunchFailed ReviewLaunchOutcome = "failed"
	// ReviewLaunchAbandoned is the handle-less supervisor-loss path: only a
	// phase-1 bootstrap can exist, so authority is revoked and the binding
	// released at once, but the reservation keeps counting against capacity
	// until its durable deadline bounds any pathological remnant.
	ReviewLaunchAbandoned ReviewLaunchOutcome = "abandoned"
	// ReviewLaunchExited is the terminal confirmed-death/normal-exit record
	// for a launched reviewer: the supervisor's Wait returned, or an adopter
	// confirmed pid+start-time absence. Only this outcome frees the capacity
	// a succeeded (running) launch holds.
	ReviewLaunchExited ReviewLaunchOutcome = "exited"
)

const (
	reviewLaunchStateReserved  = "reserved"
	reviewLaunchStateHandled   = "handled"
	reviewLaunchStateSucceeded = "succeeded"
	reviewLaunchStateFailed    = "failed"
	reviewLaunchStateAbandoned = "abandoned"

	reviewLaunchStateExited = "exited"

	// reviewLaunchPayloadVersionV1 is the exact-parent (30b96d8) event
	// shape: reservations without issuer/lease fencing fields and resolved
	// outcomes without exited. Projectors keep replaying it with its
	// original semantics; fencing fails closed on the missing fields, so a
	// v1 reservation can never take a handle or a success.
	reviewLaunchPayloadVersionV1 = 1
	// reviewLaunchPayloadVersion is the current emission version.
	reviewLaunchPayloadVersion = 2
)

// ReviewerCredentialService is the narrow slice of internal/auth that
// provisioning needs inside its transaction.
type ReviewerCredentialService interface {
	MintReviewerCredential(ctx context.Context, tx pgx.Tx, in auth.MintReviewerCredentialInput) (auth.CreateTokenResult, error)
	RevokeInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, actor domain.Token, reason string) error
}

type ProvisionSpawnedReviewInput struct {
	JobID           uuid.UUID
	WorkItemID      uuid.UUID
	Attempt         int
	LeaseGeneration int64
	// MaxConcurrent bounds live launches (reserved/handled/abandoned rows
	// whose deadline has not passed). Required positive.
	MaxConcurrent int
}

type ProvisionSpawnedReviewResult struct {
	Assignment      domain.WorkItemAssignment
	ReviewerTokenID uuid.UUID
	// Secret is returned exactly once, by the transaction that minted it. A
	// replay of a committed (child, round, attempt) returns SecretAvailable
	// false and the caller must resolve the launch failed
	// (stage=delivery_lost) before any replacement attempt.
	Secret          string
	SecretAvailable bool
	RoundSeq        int64
	RoundCommit     string
	Deadline        time.Time
}

type reviewLaunchReservedPayload struct {
	PayloadVersion    int       `json:"payload_version"`
	JobID             uuid.UUID `json:"job_id"`
	RoundSeq          int64     `json:"round_seq"`
	Attempt           int       `json:"attempt"`
	AssignmentEventID uuid.UUID `json:"assignment_event_id"`
	ReviewerTokenID   uuid.UUID `json:"reviewer_token_id"`
	IssuerTokenID     uuid.UUID `json:"issuer_token_id"`
	LeaseOwner        uuid.UUID `json:"lease_owner"`
	LeaseGeneration   int64     `json:"lease_generation"`
	Deadline          time.Time `json:"deadline"`
}

type reviewLaunchHandlePayload struct {
	PayloadVersion    int       `json:"payload_version"`
	RoundSeq          int64     `json:"round_seq"`
	Attempt           int       `json:"attempt"`
	AssignmentEventID uuid.UUID `json:"assignment_event_id"`
	Pid               int64     `json:"pid"`
	Pgid              int64     `json:"pgid"`
	StartToken        string    `json:"start_token"`
}

type reviewLaunchResolvedPayload struct {
	PayloadVersion int                 `json:"payload_version"`
	RoundSeq       int64               `json:"round_seq"`
	Attempt        int                 `json:"attempt"`
	Outcome        ReviewLaunchOutcome `json:"outcome"`
	Stage          string              `json:"stage,omitempty"`
}

type reviewLaunchTerminationDuePayload struct {
	PayloadVersion int   `json:"payload_version"`
	RoundSeq       int64 `json:"round_seq"`
	Attempt        int   `json:"attempt"`
}

// ProvisionSpawnedReview is the single admission transaction of the accepted
// lifecycle design. It is idempotent on (work item, round, attempt): a retry
// of a committed provision returns the committed identifiers without the
// secret and never yields a second credential or assignment generation.
func (s *Service) ProvisionSpawnedReview(ctx context.Context, creds ReviewerCredentialService, in ProvisionSpawnedReviewInput, actor domain.Token) (ProvisionSpawnedReviewResult, error) {
	if creds == nil {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: reviewer credential service is required", ErrInvalidRequest)
	}
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceSystem {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: review provisioning requires a dedicated non-root system actor", ErrInvalidRequest)
	}
	if !actorHasScope(actor, auth.ScopeReviewerCredentialsIssue) {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: actor %s lacks the %s capability", ErrInvalidRequest, actor.ID, auth.ScopeReviewerCredentialsIssue)
	}
	if in.JobID == uuid.Nil || in.WorkItemID == uuid.Nil {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: job id and work item id are required", ErrInvalidRequest)
	}
	if in.Attempt <= 0 {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: attempt must be positive", ErrInvalidRequest)
	}
	if in.MaxConcurrent <= 0 {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: max concurrent launches must be positive", ErrInvalidRequest)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock order mirrors StartReviewDispatch: job row, then work item, then
	// the assignment placeholder.
	if err := verifyReviewLease(ctx, tx, in, actor); err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	item, err := scanWorkItemForUpdate(ctx, tx, in.WorkItemID)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	if err := claimableWorkItem(item); err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	if item.State != domain.WorkItemRunning {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: work item is %s, want running (admit before provisioning)", ErrInvalidRequest, item.State)
	}

	roundSeq, roundCommit, err := currentReviewRound(ctx, tx, in.WorkItemID)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}

	// Replay of a committed provision: return identifiers, never the secret.
	if existing, found, err := scanReviewLaunchForUpdate(ctx, tx, in.WorkItemID, roundSeq, in.Attempt); err != nil {
		return ProvisionSpawnedReviewResult{}, err
	} else if found {
		if existing.JobID != in.JobID {
			return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: launch attempt %d for round %d belongs to job %s", ErrProvisionLeaseFenced, in.Attempt, roundSeq, existing.JobID)
		}
		state, err := scanAssignmentState(ctx, tx, in.WorkItemID, false)
		if err != nil {
			return ProvisionSpawnedReviewResult{}, err
		}
		result := ProvisionSpawnedReviewResult{
			ReviewerTokenID: existing.ReviewerTokenID,
			SecretAvailable: false,
			RoundSeq:        roundSeq,
			RoundCommit:     roundCommit,
			Deadline:        existing.Deadline,
		}
		if state.Assignment != nil && state.Assignment.AssignmentEventID == existing.AssignmentEventID {
			result.Assignment = *state.Assignment
		}
		if err := tx.Commit(ctx); err != nil {
			return ProvisionSpawnedReviewResult{}, err
		}
		return result, nil
	}

	// Durable capacity: the singleton capacity row serializes same-instant
	// checks portably (no advisory locks — the storage contract must survive
	// SQLite-per-node); the projected rows are the truth and survive queue
	// re-lease and supervisor death. A running reviewer (succeeded, not yet
	// exited) still holds its slot: launch success is process creation, not
	// process exit. clock_timestamp is read after the blocking lock so a lock
	// wait cannot smuggle in a stale deadline comparison.
	var capacitySingleton bool
	if err := tx.QueryRow(ctx, `SELECT singleton FROM review_launch_capacity FOR UPDATE`).Scan(&capacitySingleton); err != nil {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("workitems: lock launch capacity row: %w", err)
	}
	var live int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM review_launch
		WHERE state IN ('reserved', 'handled', 'succeeded')
		   OR (state = 'abandoned' AND deadline > clock_timestamp())
	`).Scan(&live); err != nil {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("workitems: count live review launches: %w", err)
	}
	if live >= in.MaxConcurrent {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: %d live of %d", ErrReviewLaunchCapacity, live, in.MaxConcurrent)
	}

	// Incumbent handling before the clock observation, exactly as
	// AssignSpawned: a live different holder aborts (rolling back everything),
	// an expired incumbent is released in this transaction.
	state, err := scanAssignmentStateForUpdate(ctx, tx, in.WorkItemID)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	observedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	if state.Assignment != nil && state.Assignment.ExpiresAt.After(observedAt) {
		return ProvisionSpawnedReviewResult{}, &ClaimHeldError{
			HolderTokenID:     state.Assignment.HolderTokenID,
			AssignmentEventID: state.Assignment.AssignmentEventID,
			ExpiresAt:         state.Assignment.ExpiresAt,
		}
	}
	if state.Assignment != nil {
		if _, err := s.appendAssignmentReleaseInTx(ctx, tx, *state.Assignment, domain.AssignmentReleaseExpired, "", observedAt, actor); err != nil {
			return ProvisionSpawnedReviewResult{}, err
		}
		state, err = scanAssignmentState(ctx, tx, in.WorkItemID, false)
		if err != nil {
			return ProvisionSpawnedReviewResult{}, err
		}
	}

	// One clock observation feeds the credential expiry, the binding lease,
	// and the reservation deadline: equal by construction.
	lease, leaseSource, err := s.resolveClaimLease(ctx, tx, in.WorkItemID)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	claimedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	deadline := claimedAt.Add(lease)

	template, err := reviewerScopesTemplate(ctx, tx, in.WorkItemID)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	minted, err := creds.MintReviewerCredential(ctx, tx, auth.MintReviewerCredentialInput{
		Name:           fmt.Sprintf("reviewer:%s:r%da%d", in.WorkItemID, roundSeq, in.Attempt),
		ChildID:        in.WorkItemID,
		TemplateScopes: template,
		ExpiresAt:      deadline,
		Actor:          actor,
	})
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}

	assignment, err := s.appendSpawnAssignmentLocked(ctx, tx, item, state, minted.Token.ID, actor, lease, leaseSource, claimedAt)
	if err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}

	reservedSpec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     in.WorkItemID,
		Kind:          domain.EventReviewLaunchReserved,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actor.ID,
		Discriminator: fmt.Sprintf("review_launch:%s:%d:%d", in.WorkItemID, roundSeq, in.Attempt),
		Payload: reviewLaunchReservedPayload{
			PayloadVersion:    reviewLaunchPayloadVersion,
			JobID:             in.JobID,
			RoundSeq:          roundSeq,
			Attempt:           in.Attempt,
			AssignmentEventID: assignment.AssignmentEventID,
			ReviewerTokenID:   minted.Token.ID,
			IssuerTokenID:     actor.ID,
			LeaseOwner:        actor.ID,
			LeaseGeneration:   in.LeaseGeneration,
			Deadline:          deadline,
		},
	}
	if _, fresh, err := s.writer.Append(ctx, tx, reservedSpec); err != nil {
		return ProvisionSpawnedReviewResult{}, err
	} else if !fresh {
		return ProvisionSpawnedReviewResult{}, fmt.Errorf("%w: review_launch_reserved unexpectedly deduped", ErrUnexpectedEventDedupe)
	}

	if err := tx.Commit(ctx); err != nil {
		return ProvisionSpawnedReviewResult{}, err
	}
	return ProvisionSpawnedReviewResult{
		Assignment:      assignment,
		ReviewerTokenID: minted.Token.ID,
		Secret:          minted.Secret,
		SecretAvailable: true,
		RoundSeq:        roundSeq,
		RoundCommit:     roundCommit,
		Deadline:        deadline,
	}, nil
}

// verifyReviewLease locks the queue row and fences on the concrete lease:
// owner, generation, unexpired deadline, and exact attempt. state='leased'
// alone is never sufficient.
func verifyReviewLease(ctx context.Context, tx pgx.Tx, in ProvisionSpawnedReviewInput, actor domain.Token) error {
	var (
		kind            string
		workItemID      uuid.UUID
		jobState        string
		attempts        int
		leaseOwner      *uuid.UUID
		leaseGeneration int64
		leaseLive       bool
	)
	err := tx.QueryRow(ctx, `
		SELECT kind, work_item_id, state, attempts, lease_owner, lease_generation,
		       (lease_until IS NOT NULL AND lease_until > clock_timestamp())
		FROM job_queue
		WHERE id = $1
		FOR UPDATE
	`, in.JobID).Scan(&kind, &workItemID, &jobState, &attempts, &leaseOwner, &leaseGeneration, &leaseLive)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: job %s not found", ErrProvisionLeaseFenced, in.JobID)
		}
		return fmt.Errorf("workitems: lock review job for provisioning: %w", err)
	}
	switch {
	case kind != reviewDispatchJobKind:
		return fmt.Errorf("%w: job %s kind %s", ErrProvisionLeaseFenced, in.JobID, kind)
	case workItemID != in.WorkItemID:
		return fmt.Errorf("%w: job %s belongs to work item %s", ErrProvisionLeaseFenced, in.JobID, workItemID)
	case jobState != "leased":
		return fmt.Errorf("%w: job %s state %s, want leased", ErrProvisionLeaseFenced, in.JobID, jobState)
	case leaseOwner == nil || *leaseOwner != actor.ID:
		return fmt.Errorf("%w: job %s lease owner mismatch", ErrProvisionLeaseFenced, in.JobID)
	case leaseGeneration != in.LeaseGeneration:
		return fmt.Errorf("%w: job %s lease generation %d, caller has %d", ErrProvisionLeaseFenced, in.JobID, leaseGeneration, in.LeaseGeneration)
	case !leaseLive:
		return fmt.Errorf("%w: job %s lease expired", ErrProvisionLeaseFenced, in.JobID)
	case attempts != in.Attempt:
		return fmt.Errorf("%w: job %s attempt %d, caller has %d", ErrProvisionLeaseFenced, in.JobID, attempts, in.Attempt)
	}
	return nil
}

// currentReviewRound requires the child to declare a current
// implementation.ready_for_review round with exactly one valid commit; a
// reviewer is only ever provisioned against an exact artifact.
func currentReviewRound(ctx context.Context, tx pgx.Tx, id uuid.UUID) (int64, string, error) {
	var roundSeq int64
	var rawInner []byte
	err := tx.QueryRow(ctx, `
		SELECT seq, payload->'inner'
		FROM events
		WHERE subject_kind = 'work_item'
		  AND subject_id = $1
		  AND kind = $2
		  AND payload->>'inner_kind' = 'implementation.ready_for_review'
		ORDER BY seq DESC
		LIMIT 1
	`, id, domain.EventWorkItemEventAppended).Scan(&roundSeq, &rawInner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", fmt.Errorf("%w: no implementation.ready_for_review round", ErrProvisionRoundInvalid)
		}
		return 0, "", fmt.Errorf("workitems: read current review round: %w", err)
	}
	commit, err := reviewRoundCommit(rawInner)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %v", ErrProvisionRoundInvalid, err)
	}
	return roundSeq, commit, nil
}

// reviewerScopesTemplate loads the child's raw cultivar ScopesTemplate; the
// {root} resolution and exact-child validation live inside
// auth.MintReviewerCredential so no caller can widen a credential's tree
// authority (round-1 P2 finding).
func reviewerScopesTemplate(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) ([]string, error) {
	meta, err := workItemLaunchMetadata(ctx, tx, workItemID)
	if err != nil {
		return nil, err
	}
	cultivarRef := strings.TrimSpace(meta.Cultivar)
	if cultivarRef == "" {
		return nil, fmt.Errorf("%w: work item %s declares no cultivar", ErrProvisionRoundInvalid, workItemID)
	}
	profile, resolvedRef, err := cultivarProfileForRefInTx(ctx, tx, cultivarRef)
	if err != nil {
		return nil, err
	}
	if len(profile.ScopesTemplate) == 0 {
		return nil, fmt.Errorf("%w: cultivar %s has no scopes_template", ErrProvisionRoundInvalid, resolvedRef)
	}
	return profile.ScopesTemplate, nil
}

// cultivarProfileForRefInTx resolves the immutable profile payload through
// the caller's transaction, mirroring cultivarXylemForRefInTx.
func cultivarProfileForRefInTx(ctx context.Context, tx pgx.Tx, cultivarRef string) (registry.Profile, string, error) {
	name, version, err := registry.ParseCultivarRef(cultivarRef)
	if err != nil {
		return registry.Profile{}, "", err
	}
	var rawProfile []byte
	resolvedVersion := version
	if version == 0 {
		err = tx.QueryRow(ctx, `SELECT version, profile FROM cultivars WHERE name = $1`, name).Scan(&resolvedVersion, &rawProfile)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT payload->'profile'
			FROM events
			WHERE subject_kind = $1
			  AND kind = $2
			  AND payload->>'name' = $3
			  AND (payload->>'version')::integer = $4
			ORDER BY seq DESC
			LIMIT 1
		`, domain.SubjectCultivar, domain.EventCultivarDefined, name, version).Scan(&rawProfile)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return registry.Profile{}, "", fmt.Errorf("%w: no cultivar named %s", registry.ErrUnknownCultivar, cultivarRef)
		}
		return registry.Profile{}, "", fmt.Errorf("workitems: resolve cultivar %s profile: %w", cultivarRef, err)
	}
	var profile registry.Profile
	if err := json.Unmarshal(rawProfile, &profile); err != nil {
		return registry.Profile{}, "", fmt.Errorf("workitems: decode cultivar %s profile: %w", cultivarRef, err)
	}
	return profile, fmt.Sprintf("%s@%d", name, resolvedVersion), nil
}

type reviewLaunchRow struct {
	WorkItemID        uuid.UUID
	RoundSeq          int64
	Attempt           int
	JobID             uuid.UUID
	AssignmentEventID uuid.UUID
	ReviewerTokenID   uuid.UUID
	// Fencing identity is nullable: v1 (exact-parent) reservations predate
	// it, and every fenced operation fails closed on nil.
	IssuerTokenID    *uuid.UUID
	LeaseOwner       *uuid.UUID
	LeaseGeneration  *int64
	State            string
	Stage            *string
	TerminationDue   bool
	HandlePid        *int64
	HandlePgid       *int64
	HandleStartToken *string
	Deadline         time.Time
}

func scanReviewLaunchForUpdate(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID, roundSeq int64, attempt int) (reviewLaunchRow, bool, error) {
	var row reviewLaunchRow
	err := tx.QueryRow(ctx, `
		SELECT work_item_id, round_seq, attempt, job_id, assignment_event_id,
		       reviewer_token_id, issuer_token_id, lease_owner, lease_generation,
		       state, stage, termination_due, handle_pid, handle_pgid,
		       handle_start_token, deadline
		FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
		FOR UPDATE
	`, workItemID, roundSeq, attempt).Scan(
		&row.WorkItemID, &row.RoundSeq, &row.Attempt, &row.JobID, &row.AssignmentEventID,
		&row.ReviewerTokenID, &row.IssuerTokenID, &row.LeaseOwner, &row.LeaseGeneration,
		&row.State, &row.Stage, &row.TerminationDue, &row.HandlePid, &row.HandlePgid,
		&row.HandleStartToken, &row.Deadline,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return reviewLaunchRow{}, false, nil
		}
		return reviewLaunchRow{}, false, fmt.Errorf("workitems: scan review launch: %w", err)
	}
	return row, true, nil
}

// requireLiveLaunchIncarnation locks the reservation's job row and verifies
// the exact leased incarnation is still live — state, owner, generation,
// attempt, and an unexpired lease on the post-lock clock — and that the
// reservation's binding is the item's current unexpired assignment. Every
// check fails closed; a v1 row fails at the nil fencing fields upstream.
func requireLiveLaunchIncarnation(ctx context.Context, tx pgx.Tx, row reviewLaunchRow) error {
	var (
		jobState        string
		attempts        int
		leaseOwner      *uuid.UUID
		leaseGeneration int64
		leaseLive       bool
	)
	err := tx.QueryRow(ctx, `
		SELECT state, attempts, lease_owner, lease_generation,
		       (lease_until IS NOT NULL AND lease_until > clock_timestamp())
		FROM job_queue
		WHERE id = $1
		FOR UPDATE
	`, row.JobID).Scan(&jobState, &attempts, &leaseOwner, &leaseGeneration, &leaseLive)
	if err != nil {
		return fmt.Errorf("workitems: lock launch job incarnation: %w", err)
	}
	if jobState != "leased" || !leaseLive ||
		leaseOwner == nil || row.LeaseOwner == nil || *leaseOwner != *row.LeaseOwner ||
		row.LeaseGeneration == nil || leaseGeneration != *row.LeaseGeneration ||
		attempts != row.Attempt {
		return fmt.Errorf("%w: reservation no longer owns the live job lease incarnation", ErrReviewLaunchState)
	}
	state, err := scanAssignmentStateForUpdate(ctx, tx, row.WorkItemID)
	if err != nil {
		return err
	}
	observed, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return err
	}
	if state.Assignment == nil || state.Assignment.AssignmentEventID != row.AssignmentEventID || !state.Assignment.ExpiresAt.After(observed) {
		return fmt.Errorf("%w: reservation binding is not the current live assignment", ErrReviewLaunchState)
	}
	return nil
}

// requireLaunchIssuer fails closed on rows without fencing identity (v1
// reservations) and on any actor other than the exact issuer.
func requireLaunchIssuer(row reviewLaunchRow, actorID uuid.UUID) error {
	if row.IssuerTokenID == nil || row.LeaseOwner == nil || row.LeaseGeneration == nil {
		return fmt.Errorf("%w: reservation predates fencing identity (v1) and cannot be operated on", ErrReviewLaunchState)
	}
	if *row.IssuerTokenID != actorID {
		return fmt.Errorf("%w: actor %s is not reservation issuer %s", ErrReviewLaunchState, actorID, *row.IssuerTokenID)
	}
	return nil
}

type ReviewLaunchHandleInput struct {
	WorkItemID        uuid.UUID
	RoundSeq          int64
	Attempt           int
	AssignmentEventID uuid.UUID
	Pid               int64
	Pgid              int64
	StartToken        string
}

// RecordReviewLaunchHandle durably stores the supervisor-verified
// pid/pgid/start-time run handle. The supervisor commits this BEFORE writing
// the bootstrap release byte, so reviewer code can never run without an
// adoptable handle (design revision 4).
func (s *Service) RecordReviewLaunchHandle(ctx context.Context, in ReviewLaunchHandleInput, actor domain.Token) error {
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceSystem {
		return fmt.Errorf("%w: launch handles require a dedicated non-root system actor", ErrInvalidRequest)
	}
	if in.Pid <= 0 || in.Pgid <= 0 || strings.TrimSpace(in.StartToken) == "" {
		return fmt.Errorf("%w: pid, pgid, and start token are required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItemForUpdate(ctx, tx, in.WorkItemID); err != nil {
		return err
	}
	row, found, err := scanReviewLaunchForUpdate(ctx, tx, in.WorkItemID, in.RoundSeq, in.Attempt)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: (%s, %d, %d)", ErrReviewLaunchNotFound, in.WorkItemID, in.RoundSeq, in.Attempt)
	}
	// The handle is a live-supervisor act: only the exact issuer that
	// provisioned this reservation may record it, and only while the
	// reservation still owns the live job lease and the current unexpired
	// assignment — after reclaim, expiry, or release, a stale supervisor
	// must not persist a handle and release its bootstrap (round-2 finding).
	if err := requireLaunchIssuer(row, actor.ID); err != nil {
		return err
	}
	if err := requireLiveLaunchIncarnation(ctx, tx, row); err != nil {
		return err
	}
	if row.State == reviewLaunchStateHandled {
		// An exact retry must match every identity field; a conflicting
		// replay claiming the same lifecycle key fails closed.
		if row.AssignmentEventID == in.AssignmentEventID &&
			row.HandlePid != nil && *row.HandlePid == in.Pid &&
			row.HandlePgid != nil && *row.HandlePgid == in.Pgid &&
			row.HandleStartToken != nil && *row.HandleStartToken == strings.TrimSpace(in.StartToken) {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("%w: conflicting handle replay for (%s, %d, %d)", ErrReviewLaunchState, in.WorkItemID, in.RoundSeq, in.Attempt)
	}
	if row.State != reviewLaunchStateReserved {
		return fmt.Errorf("%w: state %s cannot take a handle", ErrReviewLaunchState, row.State)
	}
	if row.AssignmentEventID != in.AssignmentEventID {
		return fmt.Errorf("%w: handle names assignment %s, reservation has %s", ErrReviewLaunchState, in.AssignmentEventID, row.AssignmentEventID)
	}
	// clock_timestamp after the blocking locks: a lock wait must not let a
	// stale transaction-start clock pass the deadline gate.
	var live bool
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() < $1::timestamptz`, row.Deadline).Scan(&live); err != nil {
		return fmt.Errorf("workitems: check launch deadline: %w", err)
	}
	if !live {
		return fmt.Errorf("%w: reservation deadline passed", ErrReviewLaunchState)
	}
	spec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     in.WorkItemID,
		Kind:          domain.EventReviewLaunchHandleRecorded,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actor.ID,
		Discriminator: fmt.Sprintf("review_launch_handle:%s:%d:%d", in.WorkItemID, in.RoundSeq, in.Attempt),
		Payload: reviewLaunchHandlePayload{
			PayloadVersion:    reviewLaunchPayloadVersion,
			RoundSeq:          in.RoundSeq,
			Attempt:           in.Attempt,
			AssignmentEventID: in.AssignmentEventID,
			Pid:               in.Pid,
			Pgid:              in.Pgid,
			StartToken:        strings.TrimSpace(in.StartToken),
		},
	}
	if _, _, err := s.writer.Append(ctx, tx, spec); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type ResolveReviewLaunchInput struct {
	WorkItemID uuid.UUID
	RoundSeq   int64
	Attempt    int
	Outcome    ReviewLaunchOutcome
	Stage      string
}

// ResolveReviewLaunch advances one launch attempt: succeeded marks the
// reviewer process RUNNING for the exact binding (capacity stays held);
// exited is the confirmed-death/normal-exit terminal that frees it; failed
// and abandoned release the exact bound generation (reason launch_failed)
// and revoke the credential in the same transaction. A stale generation —
// something already rebound — is left alone.
//
// Fencing (round-1 finding): succeeded and exited are live-supervisor acts
// and require the exact reservation issuer; failed and abandoned are also
// open to an explicitly authorized recovery actor — any non-root system
// token holding reviewer_credentials.issue. Succeeded additionally verifies
// the reservation still owns the job lease generation and that its binding
// is the item's current unexpired assignment.
func (s *Service) ResolveReviewLaunch(ctx context.Context, creds ReviewerCredentialService, in ResolveReviewLaunchInput, actor domain.Token) error {
	if creds == nil {
		return fmt.Errorf("%w: reviewer credential service is required", ErrInvalidRequest)
	}
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceSystem {
		return fmt.Errorf("%w: launch resolution requires a dedicated non-root system actor", ErrInvalidRequest)
	}
	switch in.Outcome {
	case ReviewLaunchSucceeded, ReviewLaunchExited, ReviewLaunchFailed, ReviewLaunchAbandoned:
	default:
		return fmt.Errorf("%w: unknown launch outcome %q", ErrInvalidRequest, in.Outcome)
	}
	if in.Outcome != ReviewLaunchSucceeded && strings.TrimSpace(in.Stage) == "" {
		return fmt.Errorf("%w: a non-succeeded outcome requires a stage", ErrInvalidRequest)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItemForUpdate(ctx, tx, in.WorkItemID); err != nil {
		return err
	}
	row, found, err := scanReviewLaunchForUpdate(ctx, tx, in.WorkItemID, in.RoundSeq, in.Attempt)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: (%s, %d, %d)", ErrReviewLaunchNotFound, in.WorkItemID, in.RoundSeq, in.Attempt)
	}
	switch in.Outcome {
	case ReviewLaunchSucceeded, ReviewLaunchExited:
		if err := requireLaunchIssuer(row, actor.ID); err != nil {
			return err
		}
	default:
		issuerMatch := row.IssuerTokenID != nil && *row.IssuerTokenID == actor.ID
		if !issuerMatch && !actorHasScope(actor, auth.ScopeReviewerCredentialsIssue) {
			return fmt.Errorf("%w: recovery resolution requires the reservation issuer or the %s capability", ErrReviewLaunchState, auth.ScopeReviewerCredentialsIssue)
		}
	}
	switch row.State {
	case reviewLaunchStateReserved, reviewLaunchStateHandled:
		// Resolvable below.
	case reviewLaunchStateSucceeded:
		// A running reviewer terminates via exited (confirmed) or failed
		// (confirmed death during recovery); nothing else. A repeated
		// succeeded is handled as an exact no-op retry below.
		if in.Outcome != ReviewLaunchExited && in.Outcome != ReviewLaunchFailed && in.Outcome != ReviewLaunchSucceeded {
			return fmt.Errorf("%w: a running launch can only exit or be confirmed failed", ErrReviewLaunchState)
		}
	case reviewLaunchStateAbandoned:
		// An abandoned launch may still be confirmed failed later; any other
		// transition is refused (a repeated abandoned no-ops below).
		if in.Outcome != ReviewLaunchFailed && in.Outcome != ReviewLaunchAbandoned {
			return fmt.Errorf("%w: abandoned launch can only be confirmed failed", ErrReviewLaunchState)
		}
	}
	if row.State == string(in.Outcome) {
		// An exact retry must match the persisted stage; a conflicting
		// replay claiming the same terminal fails closed (round-2 finding).
		persisted := ""
		if row.Stage != nil {
			persisted = *row.Stage
		}
		if persisted != strings.TrimSpace(in.Stage) {
			return fmt.Errorf("%w: outcome %s already recorded with stage %q, retry names %q", ErrReviewLaunchState, in.Outcome, persisted, in.Stage)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return nil
	}
	if row.State != reviewLaunchStateReserved && row.State != reviewLaunchStateHandled &&
		row.State != reviewLaunchStateSucceeded && row.State != reviewLaunchStateAbandoned {
		return fmt.Errorf("%w: launch already resolved %s", ErrReviewLaunchState, row.State)
	}
	if in.Outcome == ReviewLaunchSucceeded {
		// Success requires the containment handshake: no durable handle, no
		// succeeded launch (design revision 4). It also atomically completes
		// the exact live queue incarnation in this same transaction, closing
		// the success-versus-reclaim race (round-2 finding): the job cannot
		// be re-leased past a success that already committed, and a success
		// whose incarnation moved fails closed here.
		if row.State != reviewLaunchStateHandled {
			return fmt.Errorf("%w: launch cannot succeed without a recorded handle", ErrReviewLaunchState)
		}
		if err := requireLiveLaunchIncarnation(ctx, tx, row); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			UPDATE job_queue
			SET state = 'done',
			    lease_until = NULL,
			    updated_at = now()
			WHERE id = $1
			  AND state = 'leased'
			  AND lease_owner = $2
			  AND lease_generation = $3
		`, row.JobID, *row.LeaseOwner, *row.LeaseGeneration)
		if err != nil {
			return fmt.Errorf("workitems: complete job on launch success: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: job lease moved past this reservation during success", ErrReviewLaunchState)
		}
	}
	if in.Outcome == ReviewLaunchExited && row.State != reviewLaunchStateSucceeded {
		return fmt.Errorf("%w: exited is only legal from a running (succeeded) launch", ErrReviewLaunchState)
	}

	spec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     in.WorkItemID,
		Kind:          domain.EventReviewLaunchResolved,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actor.ID,
		Discriminator: fmt.Sprintf("review_launch_resolved:%s:%d:%d:%s", in.WorkItemID, in.RoundSeq, in.Attempt, in.Outcome),
		Payload: reviewLaunchResolvedPayload{
			PayloadVersion: reviewLaunchPayloadVersion,
			RoundSeq:       in.RoundSeq,
			Attempt:        in.Attempt,
			Outcome:        in.Outcome,
			Stage:          strings.TrimSpace(in.Stage),
		},
	}
	if _, _, err := s.writer.Append(ctx, tx, spec); err != nil {
		return err
	}

	switch in.Outcome {
	case ReviewLaunchFailed, ReviewLaunchAbandoned:
		// The launch never produced (or no longer has) a live reviewer:
		// release the exact bound generation and retire the credential.
		state, err := scanAssignmentStateForUpdate(ctx, tx, in.WorkItemID)
		if err != nil {
			return err
		}
		if state.Assignment != nil && state.Assignment.AssignmentEventID == row.AssignmentEventID {
			releasedAt, err := readAssignmentClock(ctx, tx)
			if err != nil {
				return err
			}
			if _, err := s.appendAssignmentReleaseInTx(ctx, tx, *state.Assignment, domain.AssignmentReleaseLaunchFailed, "", releasedAt, actor); err != nil {
				return err
			}
		}
		if err := creds.RevokeInTx(ctx, tx, row.ReviewerTokenID, actor, "review_launch_"+string(in.Outcome)); err != nil {
			return err
		}
	case ReviewLaunchExited:
		// Normal confirmed exit: the process is gone, so its single-use
		// credential retires; the binding is not disturbed — verdict and
		// checklist machinery own its remaining lifecycle.
		if err := creds.RevokeInTx(ctx, tx, row.ReviewerTokenID, actor, "review_launch_exited"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MarkReviewLaunchTerminationDue is the server-side deadline demand: a
// handled or succeeded (running) launch past its deadline is flagged for
// termination, but its capacity, credential linkage, and state are NOT
// freed — only confirmed death (exited, or an explicitly attested failed)
// terminally resolves a launch that may still have a live process tree
// (round-1 finding: the deadline pass must never violate confirmed-death).
func (s *Service) MarkReviewLaunchTerminationDue(ctx context.Context, workItemID uuid.UUID, roundSeq int64, attempt int, actor domain.Token) error {
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceSystem {
		return fmt.Errorf("%w: termination marking requires a dedicated non-root system actor", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItemForUpdate(ctx, tx, workItemID); err != nil {
		return err
	}
	row, found, err := scanReviewLaunchForUpdate(ctx, tx, workItemID, roundSeq, attempt)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: (%s, %d, %d)", ErrReviewLaunchNotFound, workItemID, roundSeq, attempt)
	}
	// The kill demand is irreversible for the supervised process: only the
	// reservation issuer or an authorized recovery actor may make it, and
	// only once the durable deadline has actually passed on the post-lock
	// clock (round-2 finding).
	issuerMatch := row.IssuerTokenID != nil && *row.IssuerTokenID == actor.ID
	if !issuerMatch && !actorHasScope(actor, auth.ScopeReviewerCredentialsIssue) {
		return fmt.Errorf("%w: termination marking requires the reservation issuer or the %s capability", ErrReviewLaunchState, auth.ScopeReviewerCredentialsIssue)
	}
	var due bool
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() >= $1::timestamptz`, row.Deadline).Scan(&due); err != nil {
		return fmt.Errorf("workitems: check termination deadline: %w", err)
	}
	if !due {
		return fmt.Errorf("%w: deadline %s has not passed", ErrReviewLaunchState, row.Deadline.UTC().Format(time.RFC3339))
	}
	if row.TerminationDue {
		return tx.Commit(ctx)
	}
	if row.State != reviewLaunchStateHandled && row.State != reviewLaunchStateSucceeded {
		return fmt.Errorf("%w: termination_due applies to handled or running launches, not %s", ErrReviewLaunchState, row.State)
	}
	spec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     workItemID,
		Kind:          domain.EventReviewLaunchTerminationDue,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actor.ID,
		Discriminator: fmt.Sprintf("review_launch_termination_due:%s:%d:%d", workItemID, roundSeq, attempt),
		Payload: reviewLaunchTerminationDuePayload{
			PayloadVersion: reviewLaunchPayloadVersion,
			RoundSeq:       roundSeq,
			Attempt:        attempt,
		},
	}
	if _, _, err := s.writer.Append(ctx, tx, spec); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReconcileReviewLaunches is the durable-state recovery pass. Reserved rows
// past deadline resolve failed: phase gating guarantees no reviewer code
// ever ran, and the wrapper watchdog plus enforced token expiry bound the
// inert bootstrap. Handled and running rows past deadline are only MARKED
// termination-due — a supervisor or adopter must kill and confirm before
// anything is freed. Succeeded launches whose queue job is still leased on
// the reservation's own lease generation get the completion the crashed
// worker never issued. Everything derives from projected rows.
func (s *Service) ReconcileReviewLaunches(ctx context.Context, creds ReviewerCredentialService, actor domain.Token) (int, error) {
	if creds == nil {
		return 0, fmt.Errorf("%w: reviewer credential service is required", ErrInvalidRequest)
	}
	type key struct {
		workItemID uuid.UUID
		roundSeq   int64
		attempt    int
		state      string
	}
	rows, err := s.pool.Query(ctx, `
		SELECT work_item_id, round_seq, attempt, state
		FROM review_launch
		WHERE state IN ('reserved', 'handled', 'succeeded')
		  AND deadline <= clock_timestamp()
		  AND NOT termination_due
		ORDER BY deadline ASC
	`)
	if err != nil {
		return 0, fmt.Errorf("workitems: list expired review launches: %w", err)
	}
	var expired []key
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.workItemID, &k.roundSeq, &k.attempt, &k.state); err != nil {
			rows.Close()
			return 0, err
		}
		expired = append(expired, k)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	resolved := 0
	for _, k := range expired {
		if k.state == reviewLaunchStateReserved {
			err = s.ResolveReviewLaunch(ctx, creds, ResolveReviewLaunchInput{
				WorkItemID: k.workItemID,
				RoundSeq:   k.roundSeq,
				Attempt:    k.attempt,
				Outcome:    ReviewLaunchFailed,
				Stage:      "deadline_expired",
			}, actor)
		} else {
			err = s.MarkReviewLaunchTerminationDue(ctx, k.workItemID, k.roundSeq, k.attempt, actor)
		}
		if err != nil {
			return resolved, err
		}
		resolved++
	}

	// Completion repair, fenced to the reservation's own lease incarnation: a
	// renewed lease means a newer attempt owns the job and a stale success
	// must never complete it.
	if _, err := s.pool.Exec(ctx, `
		UPDATE job_queue jq
		SET state = 'done',
		    lease_until = NULL,
		    updated_at = now()
		FROM review_launch rl
		WHERE rl.job_id = jq.id
		  AND rl.state = 'succeeded'
		  AND jq.state = 'leased'
		  AND jq.lease_owner = rl.lease_owner
		  AND jq.lease_generation = rl.lease_generation
	`); err != nil {
		return resolved, fmt.Errorf("workitems: repair succeeded launch jobs: %w", err)
	}
	return resolved, nil
}

func actorHasScope(actor domain.Token, want string) bool {
	for _, scope := range actor.Scopes {
		if scope == want {
			return true
		}
	}
	return false
}
