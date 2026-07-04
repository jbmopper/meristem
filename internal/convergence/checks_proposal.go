package convergence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

const (
	ScribeCultivar   = "convergence-scribe@1"
	ScribeChildCheck = "query:parent_checks_defined"

	checksProposalMaxAttempts = 3

	checkClassMachine = "machine"
	checkClassHuman   = "human"

	checkPrefixCommand = "cmd:"
	checkPrefixEvent   = "event:"
	checkPrefixQuery   = "query:"
	checkPrefixHuman   = "human-ack:"
)

var ErrChecksProposalNotFound = errors.New("convergence: work item not found")

type ChecksProposalService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewChecksProposalService(pool *pgxpool.Pool, writer *events.Writer) *ChecksProposalService {
	return &ChecksProposalService{pool: pool, writer: writer}
}

type CheckClassification struct {
	Check string `json:"check"`
	Class string `json:"class"`
}

type ProposeChecksInput struct {
	ProposalOf uuid.UUID             `json:"proposal_of"`
	Checks     []string              `json:"checks"`
	Classified []CheckClassification `json:"classified"`
	Rationale  string                `json:"rationale"`
	Cultivar   string                `json:"cultivar"`
}

type ChecksProposalResult struct {
	ParentID        uuid.UUID `json:"parent_id"`
	ScribeChildID   uuid.UUID `json:"scribe_child_id"`
	ProposalEventID uuid.UUID `json:"proposal_event_id"`
	ProposalFresh   bool      `json:"proposal_fresh"`
	VerdictEventID  uuid.UUID `json:"verdict_event_id,omitempty"`
	VerdictFresh    bool      `json:"verdict_fresh"`
	Verdict         Verdict   `json:"verdict"`
	Applied         bool      `json:"applied"`
	Stale           bool      `json:"stale"`
	BudgetExhausted bool      `json:"budget_exhausted"`
}

type checksProposalValidation struct {
	Accept bool
	Reason string
	Checks []string
}

type proposalPayload struct {
	ProposalOf uuid.UUID             `json:"proposal_of"`
	Checks     []string              `json:"checks"`
	Classified []CheckClassification `json:"classified"`
	Rationale  string                `json:"rationale"`
	Cultivar   string                `json:"cultivar"`
}

type workItemSnapshot struct {
	ID                         uuid.UUID
	State                      domain.WorkItemState
	SuggestedConvergenceChecks []string
	HumanReviewStatus          domain.HumanReviewStatus
}

type proposalVerdictRecord struct {
	Attempt      int
	InputsDigest string
	Verdict      Verdict
}

func ScribeChildID(parentID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(parentID.String()+"|convergence-scribe|1"))
}

func KnownQueryCheck(name string) bool {
	return name == "parent_checks_defined"
}

func (s *ChecksProposalService) ProposeChecks(ctx context.Context, parentID uuid.UUID, in ProposeChecksInput, actor domain.Token) (ChecksProposalResult, error) {
	if s == nil || s.pool == nil || s.writer == nil {
		return ChecksProposalResult{}, errors.New("convergence: checks proposal service is not configured")
	}
	if parentID == uuid.Nil {
		return ChecksProposalResult{}, fmt.Errorf("%w: parent id is required", ErrChecksProposalNotFound)
	}
	cultivar := strings.TrimSpace(in.Cultivar)
	if cultivar == "" {
		cultivar = ScribeCultivar
	}
	payload := proposalPayload{
		ProposalOf: in.ProposalOf,
		Checks:     append([]string(nil), in.Checks...),
		Classified: append([]CheckClassification(nil), in.Classified...),
		Rationale:  strings.TrimSpace(in.Rationale),
		Cultivar:   cultivar,
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ChecksProposalResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	parent, err := scanProposalWorkItem(ctx, tx, parentID)
	if err != nil {
		return ChecksProposalResult{}, err
	}
	expectedChildID := ScribeChildID(parentID)
	childValid, child, err := scanScribeChild(ctx, tx, parentID, expectedChildID, in.ProposalOf)
	if err != nil {
		return ChecksProposalResult{}, err
	}

	source := sourceForActor(actor)
	effectSource := domain.SourceSystem
	actorID := &actor.ID
	proposalEventID, proposalFresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     parentID,
		Kind:          domain.EventConvergenceChecksProposed,
		Source:        source,
		ActorTokenID:  actorID,
		Discriminator: eventDiscriminator(ctx),
		Payload:       payload,
	})
	if err != nil {
		return ChecksProposalResult{}, err
	}

	validation := ValidateChecksProposal(in, len(parent.SuggestedConvergenceChecks) > 0, childValid)
	signals, err := checksProposalSignals(payload, validation)
	if err != nil {
		return ChecksProposalResult{}, err
	}
	last, err := latestChecksProposalVerdict(ctx, tx, parentID)
	if err != nil {
		return ChecksProposalResult{}, err
	}
	attempt := 1
	if last != nil {
		attempt = last.Attempt + 1
	}
	reduction, err := Run(ChecksProposal{}, signals, attempt)
	if err != nil {
		return ChecksProposalResult{}, err
	}

	result := ChecksProposalResult{
		ParentID:        parentID,
		ScribeChildID:   in.ProposalOf,
		ProposalEventID: proposalEventID,
		ProposalFresh:   proposalFresh,
		Verdict:         reduction.Verdict,
	}
	if last != nil && last.InputsDigest == reduction.InputsDigest && last.Verdict.Disposition == domain.VerdictReject {
		result.Stale = true
		result.Verdict = last.Verdict
		if err := tx.Commit(ctx); err != nil {
			return ChecksProposalResult{}, err
		}
		return result, nil
	}

	verdictEventID, verdictFresh, err := AppendVerdict(ctx, tx, s.writer, domain.SourceSystem, actorID, parentID, reduction)
	if err != nil {
		return ChecksProposalResult{}, err
	}
	result.VerdictEventID = verdictEventID
	result.VerdictFresh = verdictFresh

	switch {
	case validation.Accept:
		if err := appendMetadataUpdated(ctx, tx, s.writer, effectSource, actorID, eventDiscriminator(ctx), parent, validation.Checks, parent.HumanReviewStatus); err != nil {
			return ChecksProposalResult{}, err
		}
		if childValid {
			if err := appendTransitioned(ctx, tx, s.writer, effectSource, actorID, eventDiscriminator(ctx), child, domain.WorkItemDone, "checks proposal accepted"); err != nil {
				return ChecksProposalResult{}, err
			}
		}
		result.Applied = true
	case validation.Reason == "checks_already_defined":
		if childValid {
			if err := appendTransitioned(ctx, tx, s.writer, effectSource, actorID, eventDiscriminator(ctx), child, domain.WorkItemDone, "parent checks already defined"); err != nil {
				return ChecksProposalResult{}, err
			}
		}
	case reduction.Verdict.Disposition == domain.VerdictReject && reduction.Attempt >= checksProposalMaxAttempts && childValid:
		if err := appendMetadataUpdated(ctx, tx, s.writer, effectSource, actorID, eventDiscriminator(ctx), child, child.SuggestedConvergenceChecks, domain.HumanReviewBlocked); err != nil {
			return ChecksProposalResult{}, err
		}
		if err := appendTransitioned(ctx, tx, s.writer, effectSource, actorID, eventDiscriminator(ctx), child, domain.WorkItemBlocked, "checks proposal budget exhausted"); err != nil {
			return ChecksProposalResult{}, err
		}
		result.BudgetExhausted = true
	}

	if err := tx.Commit(ctx); err != nil {
		return ChecksProposalResult{}, err
	}
	return result, nil
}

func ValidateChecksProposal(in ProposeChecksInput, parentAlreadyDefined, scribeChildValid bool) checksProposalValidation {
	if !scribeChildValid {
		return checksProposalValidation{Reason: "invalid_scribe_child"}
	}
	if parentAlreadyDefined {
		return checksProposalValidation{Reason: "checks_already_defined"}
	}
	checks := make([]string, 0, len(in.Checks))
	seen := make(map[string]bool, len(in.Checks))
	for i, check := range in.Checks {
		trimmed := strings.TrimSpace(check)
		if trimmed == "" {
			return checksProposalValidation{Reason: fmt.Sprintf("blank_check:%d", i)}
		}
		if seen[trimmed] {
			return checksProposalValidation{Reason: "duplicate_check:" + trimmed}
		}
		seen[trimmed] = true
		checks = append(checks, trimmed)
	}
	if len(checks) == 0 {
		return checksProposalValidation{Reason: "empty_checks"}
	}

	classified := make(map[string]string, len(in.Classified))
	for _, entry := range in.Classified {
		check := strings.TrimSpace(entry.Check)
		class := strings.TrimSpace(entry.Class)
		if check == "" {
			return checksProposalValidation{Reason: "blank_classified_check"}
		}
		if classified[check] != "" {
			return checksProposalValidation{Reason: "duplicate_classification:" + check}
		}
		classified[check] = class
	}
	for check := range classified {
		if !seen[check] {
			return checksProposalValidation{Reason: "classification_for_unknown_check:" + check}
		}
	}

	for _, check := range checks {
		class := classified[check]
		if class == "" {
			return checksProposalValidation{Reason: "missing_classification:" + check}
		}
		if !hasKnownCheckPrefix(check) {
			return checksProposalValidation{Reason: "unclassified_check:" + check}
		}
		switch class {
		case checkClassMachine:
			if !hasMachineCheckPrefix(check) {
				return checksProposalValidation{Reason: "classification_mismatch:" + check}
			}
		case checkClassHuman:
			if !strings.HasPrefix(check, checkPrefixHuman) {
				return checksProposalValidation{Reason: "classification_mismatch:" + check}
			}
		default:
			return checksProposalValidation{Reason: "invalid_classification:" + check}
		}
		if strings.HasPrefix(check, checkPrefixQuery) {
			name := strings.TrimPrefix(check, checkPrefixQuery)
			if !KnownQueryCheck(name) {
				return checksProposalValidation{Reason: "unknown_query_check:" + name}
			}
		}
	}

	return checksProposalValidation{
		Accept: true,
		Reason: "checks proposal validated",
		Checks: checks,
	}
}

func hasKnownCheckPrefix(check string) bool {
	return hasMachineCheckPrefix(check) || strings.HasPrefix(check, checkPrefixHuman)
}

func hasMachineCheckPrefix(check string) bool {
	return strings.HasPrefix(check, checkPrefixCommand) ||
		strings.HasPrefix(check, checkPrefixEvent) ||
		strings.HasPrefix(check, checkPrefixQuery)
}

func checksProposalSignals(payload proposalPayload, validation checksProposalValidation) ([]Signal, error) {
	payloadDigest, err := checksProposalPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	pass := validation.Accept
	raw := validation.Reason
	if raw == "" {
		raw = "proposal validation failed"
	}
	return []Signal{
		{Kind: checksProposalValidKind, Pass: &pass, Raw: raw},
		{Kind: "checks_proposal.payload", Raw: payloadDigest},
	}, nil
}

func checksProposalPayloadDigest(payload proposalPayload) (string, error) {
	canonical, err := events.CanonicalJSON(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func scanProposalWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) (workItemSnapshot, error) {
	var out workItemSnapshot
	var state string
	var review string
	var checksJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT id, state, suggested_convergence_checks, human_review_status
		FROM work_items
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&out.ID, &state, &checksJSON, &review)
	if errors.Is(err, pgx.ErrNoRows) {
		return workItemSnapshot{}, fmt.Errorf("%w: %s", ErrChecksProposalNotFound, id)
	}
	if err != nil {
		return workItemSnapshot{}, err
	}
	out.State = domain.WorkItemState(state)
	out.HumanReviewStatus = domain.HumanReviewStatus(review)
	if len(checksJSON) > 0 {
		if err := json.Unmarshal(checksJSON, &out.SuggestedConvergenceChecks); err != nil {
			return workItemSnapshot{}, fmt.Errorf("convergence: decode suggested_convergence_checks for %s: %w", id, err)
		}
	}
	return out, nil
}

func scanScribeChild(ctx context.Context, tx pgx.Tx, parentID, expectedChildID, proposalOf uuid.UUID) (bool, workItemSnapshot, error) {
	if proposalOf == uuid.Nil || proposalOf != expectedChildID {
		return false, workItemSnapshot{ID: proposalOf}, nil
	}
	var child workItemSnapshot
	var state string
	var review string
	var checksJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT wi.id, wi.state, wi.suggested_convergence_checks, wi.human_review_status
		FROM work_items wi
		JOIN work_item_relations wir ON wir.child_id = wi.id
		WHERE wir.parent_id = $1 AND wi.id = $2
		FOR UPDATE OF wi
	`, parentID, proposalOf).Scan(&child.ID, &state, &checksJSON, &review)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, workItemSnapshot{ID: proposalOf}, nil
	}
	if err != nil {
		return false, workItemSnapshot{}, err
	}
	child.State = domain.WorkItemState(state)
	child.HumanReviewStatus = domain.HumanReviewStatus(review)
	if len(checksJSON) > 0 {
		if err := json.Unmarshal(checksJSON, &child.SuggestedConvergenceChecks); err != nil {
			return false, workItemSnapshot{}, fmt.Errorf("convergence: decode child suggested_convergence_checks for %s: %w", proposalOf, err)
		}
	}
	return true, child, nil
}

func latestChecksProposalVerdict(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) (*proposalVerdictRecord, error) {
	var out proposalVerdictRecord
	var disposition string
	var reason string
	reducer := ChecksProposal{}
	err := tx.QueryRow(ctx, `
		SELECT attempt, inputs_digest, disposition, reason
		FROM convergence_verdicts
		WHERE work_item_id = $1
			AND reducer_identity = $2
			AND reducer_version = $3
		ORDER BY attempt DESC, occurred_at DESC
		LIMIT 1
	`, workItemID, reducer.Identity(), reducer.Version()).Scan(&out.Attempt, &out.InputsDigest, &disposition, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out.Verdict = Verdict{Disposition: domain.Verdict(disposition), Reason: reason}
	return &out, nil
}

func appendMetadataUpdated(ctx context.Context, tx pgx.Tx, writer *events.Writer, source domain.Source, actorID *uuid.UUID, discriminator string, current workItemSnapshot, checks []string, humanReview domain.HumanReviewStatus) error {
	_, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     current.ID,
		Kind:          domain.EventWorkItemMetadataUpdated,
		Source:        source,
		ActorTokenID:  actorID,
		Discriminator: discriminator,
		Payload: map[string]any{
			"from": map[string]any{
				"suggested_convergence_checks": current.SuggestedConvergenceChecks,
				"human_review_status":          current.HumanReviewStatus,
			},
			"to": map[string]any{
				"suggested_convergence_checks": checks,
				"human_review_status":          humanReview,
			},
		},
	})
	return err
}

func appendTransitioned(ctx context.Context, tx pgx.Tx, writer *events.Writer, source domain.Source, actorID *uuid.UUID, discriminator string, current workItemSnapshot, to domain.WorkItemState, reason string) error {
	if current.State == to {
		return nil
	}
	if !domain.CanTransition(current.State, to) {
		return nil
	}
	_, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     current.ID,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        source,
		ActorTokenID:  actorID,
		Discriminator: discriminator,
		Payload: map[string]any{
			"from":   current.State,
			"to":     to,
			"reason": reason,
		},
	})
	return err
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func eventDiscriminator(ctx context.Context) string {
	disc, _ := idempotency.EventDiscriminator(ctx)
	return disc
}
