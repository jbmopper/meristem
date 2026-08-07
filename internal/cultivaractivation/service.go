package cultivaractivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/escalations"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/registry"
)

type Service struct {
	pool        *pgxpool.Pool
	writer      *events.Writer
	grants      *grants.IssuanceService
	registry    *registry.Service
	escalations *escalations.Service
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{
		pool:        pool,
		writer:      writer,
		grants:      grants.NewIssuanceService(pool, writer, nil, nil),
		registry:    registry.NewService(pool, writer),
		escalations: escalations.NewService(pool, writer),
	}
}

type ActivateInput struct {
	Actor      domain.Token
	WorkItemID uuid.UUID
	Cultivar   registry.DefineCultivarInput
}

type Result struct {
	ActivationID    uuid.UUID
	WorkItemID      uuid.UUID
	Disposition     grants.Disposition
	Reason          string
	Scopes          []string
	Cultivar        *registry.Cultivar
	RequestEventID  uuid.UUID
	OutcomeEventID  uuid.UUID
	EscalationID    uuid.UUID
	HumanWorkItemID uuid.UUID
}

func (s *Service) Activate(ctx context.Context, in ActivateInput) (Result, error) {
	if s.pool == nil || s.writer == nil || s.grants == nil || s.registry == nil || s.escalations == nil {
		return Result{}, errors.New("cultivar activation: service is not configured")
	}
	if in.Actor.ID == uuid.Nil {
		return Result{}, errors.New("cultivar activation: actor token is required")
	}
	if in.WorkItemID == uuid.Nil {
		return Result{}, errors.New("cultivar activation: work_item_id is required")
	}

	activationID := newActivationID(ctx)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	scopes := resolveProfileScopes(in.Cultivar.Profile.ScopesTemplate, in.WorkItemID)
	evaluation, err := s.grants.EvaluateInTx(ctx, tx, grants.EvaluationInput{
		Parent:          in.Actor,
		WorkItemID:      in.WorkItemID,
		Template:        grants.TemplateSameTreeWorker,
		RequestedScopes: scopes,
	})
	if err != nil {
		return Result{}, err
	}

	decision := evaluation.Decision
	if in.Cultivar.Rootstock {
		decision = grants.Decision{
			Disposition: grants.DispositionDeny,
			Reason:      "rootstock cultivars cannot be activated by worker self-extension",
		}
	}
	if decision.Disposition == grants.DispositionGrant {
		separated, reason, err := s.approvalSeparated(ctx, tx, in.WorkItemID, in.Actor.ID)
		if err != nil {
			return Result{}, err
		}
		if !separated {
			decision = grants.Decision{
				Disposition: grants.DispositionEscalate,
				Reason:      reason,
			}
		}
	}

	source := sourceForActor(in.Actor)
	requestEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectCultivarActivation,
		SubjectID:    activationID,
		Kind:         domain.EventCultivarActivationRequested,
		Source:       source,
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"work_item_id":              in.WorkItemID,
			"parent_token_id":           in.Actor.ID,
			"grant_reducer":             "subactor_grant.reduce",
			"grant_template":            grants.TemplateSameTreeWorker,
			"requested_scopes":          evaluation.RequestedScopes,
			"tree_relation":             evaluation.TreeRelation,
			"human_review_status":       evaluation.HumanReviewStatus,
			"delegation_depth_known":    evaluation.DelegationDepthKnown,
			"delegation_depth":          evaluation.DelegationDepth,
			"max_delegation_depth":      evaluation.MaxDelegationDepth,
			"depth_budget_source":       evaluation.DepthBudgetSource,
			"proposed_cultivar_name":    strings.TrimSpace(in.Cultivar.Name),
			"proposed_cultivar_version": in.Cultivar.Version,
			"proposed_rootstock":        in.Cultivar.Rootstock,
		},
	})
	if err != nil {
		return Result{}, err
	}

	result := Result{
		ActivationID:   activationID,
		WorkItemID:     in.WorkItemID,
		Disposition:    decision.Disposition,
		Reason:         decision.Reason,
		Scopes:         append([]string(nil), decision.Scopes...),
		RequestEventID: requestEventID,
	}

	switch decision.Disposition {
	case grants.DispositionGrant:
		item, _, err := s.registry.DefineCultivarInTx(ctx, tx, in.Actor, in.Cultivar)
		if err != nil {
			return Result{}, err
		}
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectCultivarActivation,
			SubjectID:    activationID,
			Kind:         domain.EventCultivarActivationGranted,
			Source:       source,
			ActorTokenID: &in.Actor.ID,
			Payload: map[string]any{
				"request_event_id":  requestEventID,
				"work_item_id":      in.WorkItemID,
				"reason":            decision.Reason,
				"scopes":            decision.Scopes,
				"cultivar":          fmt.Sprintf("%s@%d", item.Name, item.Version),
				"cultivar_event_id": item.EventID,
			},
		})
		if err != nil {
			return Result{}, err
		}
		result.OutcomeEventID = outcomeEventID
		result.Cultivar = &item
	case grants.DispositionDeny:
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectCultivarActivation,
			SubjectID:    activationID,
			Kind:         domain.EventCultivarActivationDenied,
			Source:       source,
			ActorTokenID: &in.Actor.ID,
			Payload: map[string]any{
				"request_event_id": requestEventID,
				"work_item_id":     in.WorkItemID,
				"reason":           decision.Reason,
			},
		})
		if err != nil {
			return Result{}, err
		}
		result.OutcomeEventID = outcomeEventID
	case grants.DispositionEscalate:
		escalation, err := s.escalations.RequestInTx(ctx, tx, escalations.RequestInput{
			WorkItemID: in.WorkItemID,
			Reason:     "cultivar activation requires review: " + decision.Reason,
			Summary:    activationSummary(evaluation.WorkItemTitle, in.Cultivar, decision.Reason),
			Actor:      in.Actor,
		})
		if err != nil {
			return Result{}, err
		}
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectCultivarActivation,
			SubjectID:    activationID,
			Kind:         domain.EventCultivarActivationEscalated,
			Source:       source,
			ActorTokenID: &in.Actor.ID,
			Payload: map[string]any{
				"request_event_id":   requestEventID,
				"work_item_id":       in.WorkItemID,
				"reason":             decision.Reason,
				"escalation_id":      escalation.EscalationID,
				"human_work_item_id": escalation.HumanWorkItemID,
			},
		})
		if err != nil {
			return Result{}, err
		}
		result.OutcomeEventID = outcomeEventID
		result.EscalationID = escalation.EscalationID
		result.HumanWorkItemID = escalation.HumanWorkItemID
	default:
		return Result{}, fmt.Errorf("cultivar activation: unsupported reducer disposition %q", decision.Disposition)
	}

	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) approvalSeparated(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID, actorID uuid.UUID) (bool, string, error) {
	var (
		approver uuid.NullUUID
		seq      int64
		source   string
	)
	err := tx.QueryRow(ctx, `
		SELECT seq, actor_token_id, source
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND (
		    (kind = $3 AND payload->'to'->>'human_review_status' = $5)
		    OR
		    (kind = $4 AND COALESCE(payload->>'human_review_status', '') = $5)
		  )
		ORDER BY seq DESC
		LIMIT 1
	`, domain.SubjectWorkItem, workItemID, domain.EventWorkItemMetadataUpdated, domain.EventWorkItemCreated, domain.HumanReviewApproved).Scan(&seq, &approver, &source)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "explicit human_review_status=approved event is required before cultivar activation", nil
		}
		return false, "", err
	}
	if approver.Valid && approver.UUID == actorID {
		return false, "cultivar proposer cannot approve its own activation work item", nil
	}
	reviewEvent := domain.Event{Seq: seq, Source: domain.Source(source)}
	if approver.Valid {
		reviewEvent.ActorTokenID = &approver.UUID
	}
	authorized, err := access.CanDecideHumanReviewEvent(ctx, tx, reviewEvent)
	if err != nil {
		return false, "", err
	}
	if !authorized {
		return false, "approved human review lacks an authorized work_items.review_decide attribution", nil
	}
	return true, "", nil
}

func resolveProfileScopes(scopes []string, root uuid.UUID) []string {
	out := make([]string, 0, len(scopes))
	rootText := root.String()
	for _, scope := range scopes {
		out = append(out, strings.ReplaceAll(scope, "{root}", rootText))
	}
	return out
}

func newActivationID(ctx context.Context) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, "cultivar_activation"); ok {
		return id
	}
	return uuid.New()
}

func activationSummary(title string, in registry.DefineCultivarInput, reason string) string {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		name = "<unnamed>"
	}
	return fmt.Sprintf("Cultivar activation %s@%d for %q requires review: %s", name, in.Version, title, reason)
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceAgent
}
