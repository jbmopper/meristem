package grants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/escalations"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var ErrWorkItemNotFound = errors.New("grants: work_item not found")

type IssuanceService struct {
	pool        *pgxpool.Pool
	writer      *events.Writer
	auth        *auth.Service
	escalations *escalations.Service
}

func NewIssuanceService(pool *pgxpool.Pool, writer *events.Writer, authSvc *auth.Service, escalationSvc *escalations.Service) *IssuanceService {
	if authSvc == nil {
		authSvc = auth.NewService(pool, writer)
	}
	if escalationSvc == nil {
		escalationSvc = escalations.NewService(pool, writer)
	}
	return &IssuanceService{
		pool:        pool,
		writer:      writer,
		auth:        authSvc,
		escalations: escalationSvc,
	}
}

type IssueInput struct {
	Parent          domain.Token
	WorkItemID      uuid.UUID
	Template        Template
	RequestedScopes []string
	Name            string
}

type IssueResult struct {
	GrantID         uuid.UUID
	WorkItemID      uuid.UUID
	Template        Template
	Disposition     Disposition
	Reason          string
	Scopes          []string
	Token           *domain.Token
	TokenSecret     string
	RequestEventID  uuid.UUID
	OutcomeEventID  uuid.UUID
	EscalationID    uuid.UUID
	HumanWorkItemID uuid.UUID
}

func (s *IssuanceService) Issue(ctx context.Context, in IssueInput) (IssueResult, error) {
	if in.Parent.ID == uuid.Nil {
		return IssueResult{}, fmt.Errorf("grants: parent token is required")
	}
	if in.WorkItemID == uuid.Nil {
		return IssueResult{}, fmt.Errorf("grants: work_item_id is required")
	}
	requestedScopes, err := normalizeRequestedScopes(in.RequestedScopes)
	if err != nil {
		return IssueResult{}, err
	}
	tokenName := strings.TrimSpace(in.Name)
	if tokenName == "" {
		tokenName = defaultSubactorName(in.Template, in.WorkItemID)
	}
	grantID := newGrantID(ctx)
	source := sourceForActor(in.Parent)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return IssueResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if existing, ok, err := existingIssueResult(ctx, tx, grantID); err != nil {
		return IssueResult{}, err
	} else if ok {
		if err := tx.Commit(ctx); err != nil {
			return IssueResult{}, err
		}
		return existing, nil
	}

	item, err := scanGrantWorkItem(ctx, tx, in.WorkItemID)
	if err != nil {
		return IssueResult{}, err
	}
	relation, err := s.treeRelation(ctx, tx, in.Parent, in.WorkItemID)
	if err != nil {
		return IssueResult{}, err
	}
	requestPayload := map[string]any{
		"parent_token_id":     in.Parent.ID,
		"work_item_id":        in.WorkItemID,
		"template":            in.Template,
		"requested_source":    domain.SourceAgent,
		"requested_scopes":    requestedScopes,
		"token_name":          tokenName,
		"tree_relation":       relation,
		"human_review_status": item.HumanReviewStatus,
	}
	requestEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectSubactorGrant,
		SubjectID:    grantID,
		Kind:         domain.EventSubactorGrantRequested,
		Source:       source,
		ActorTokenID: &in.Parent.ID,
		Payload:      requestPayload,
	})
	if err != nil {
		return IssueResult{}, err
	}

	decision := Reduce(Request{
		Parent:            in.Parent,
		Template:          in.Template,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: in.WorkItemID,
		RequestedScopes:   requestedScopes,
		TreeRelation:      relation,
		HumanReviewStatus: item.HumanReviewStatus,
	})

	result := IssueResult{
		GrantID:        grantID,
		WorkItemID:     in.WorkItemID,
		Template:       in.Template,
		Disposition:    decision.Disposition,
		Reason:         decision.Reason,
		Scopes:         append([]string(nil), decision.Scopes...),
		RequestEventID: requestEventID,
	}

	switch decision.Disposition {
	case DispositionGrant:
		tokenResult, err := s.auth.CreateDelegatedToken(ctx, tx, auth.CreateDelegatedTokenInput{
			ID:     deterministicSubactorTokenID(grantID),
			Name:   tokenName,
			Scopes: decision.Scopes,
			Source: domain.SourceAgent,
			Actor:  in.Parent,
		})
		if err != nil {
			return IssueResult{}, err
		}
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectSubactorGrant,
			SubjectID:    grantID,
			Kind:         domain.EventSubactorGrantGranted,
			Source:       source,
			ActorTokenID: &in.Parent.ID,
			Payload: map[string]any{
				"request_event_id": requestEventID,
				"work_item_id":     in.WorkItemID,
				"template":         in.Template,
				"reason":           decision.Reason,
				"token_id":         tokenResult.Token.ID,
				"token_name":       tokenResult.Token.Name,
				"scopes":           decision.Scopes,
			},
		})
		if err != nil {
			return IssueResult{}, err
		}
		result.OutcomeEventID = outcomeEventID
		result.Token = &tokenResult.Token
		result.TokenSecret = tokenResult.Secret
	case DispositionDeny:
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectSubactorGrant,
			SubjectID:    grantID,
			Kind:         domain.EventSubactorGrantDenied,
			Source:       source,
			ActorTokenID: &in.Parent.ID,
			Payload: map[string]any{
				"request_event_id": requestEventID,
				"work_item_id":     in.WorkItemID,
				"template":         in.Template,
				"reason":           decision.Reason,
			},
		})
		if err != nil {
			return IssueResult{}, err
		}
		result.OutcomeEventID = outcomeEventID
	case DispositionEscalate:
		escalation, err := s.escalations.RequestInTx(ctx, tx, escalations.RequestInput{
			WorkItemID: in.WorkItemID,
			Reason:     "subactor grant requires review: " + decision.Reason,
			Summary:    escalationSummary(item, in.Template, decision.Reason),
			Actor:      in.Parent,
		})
		if err != nil {
			return IssueResult{}, err
		}
		outcomeEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectSubactorGrant,
			SubjectID:    grantID,
			Kind:         domain.EventSubactorGrantEscalated,
			Source:       source,
			ActorTokenID: &in.Parent.ID,
			Payload: map[string]any{
				"request_event_id":   requestEventID,
				"work_item_id":       in.WorkItemID,
				"template":           in.Template,
				"reason":             decision.Reason,
				"escalation_id":      escalation.EscalationID,
				"human_work_item_id": escalation.HumanWorkItemID,
			},
		})
		if err != nil {
			return IssueResult{}, err
		}
		result.OutcomeEventID = outcomeEventID
		result.EscalationID = escalation.EscalationID
		result.HumanWorkItemID = escalation.HumanWorkItemID
	default:
		return IssueResult{}, fmt.Errorf("grants: unsupported reducer disposition %q", decision.Disposition)
	}

	if err := tx.Commit(ctx); err != nil {
		return IssueResult{}, err
	}
	return result, nil
}

type grantWorkItem struct {
	ID                uuid.UUID
	Title             string
	HumanReviewStatus domain.HumanReviewStatus
}

func scanGrantWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) (grantWorkItem, error) {
	var item grantWorkItem
	var humanReview string
	err := tx.QueryRow(ctx, `
		SELECT id, title, human_review_status
		FROM work_items
		WHERE id = $1
	`, id).Scan(&item.ID, &item.Title, &humanReview)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return grantWorkItem{}, ErrWorkItemNotFound
		}
		return grantWorkItem{}, err
	}
	item.HumanReviewStatus = domain.HumanReviewStatus(humanReview)
	if !item.HumanReviewStatus.Valid() {
		return grantWorkItem{}, fmt.Errorf("grants: invalid human_review_status %q", humanReview)
	}
	return item, nil
}

func existingIssueResult(ctx context.Context, tx pgx.Tx, grantID uuid.UUID) (IssueResult, bool, error) {
	requestEventID, err := existingGrantRequestEvent(ctx, tx, grantID)
	if err != nil {
		return IssueResult{}, false, err
	}
	var eventID uuid.UUID
	var kind string
	var payloadJSON []byte
	err = tx.QueryRow(ctx, `
		SELECT id, kind, payload
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND kind IN ($3, $4, $5)
		ORDER BY occurred_at DESC
		LIMIT 1
	`, domain.SubjectSubactorGrant, grantID, domain.EventSubactorGrantGranted, domain.EventSubactorGrantDenied, domain.EventSubactorGrantEscalated).Scan(&eventID, &kind, &payloadJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return IssueResult{}, false, nil
		}
		return IssueResult{}, false, err
	}
	var payload struct {
		WorkItemID      uuid.UUID `json:"work_item_id"`
		Template        Template  `json:"template"`
		Reason          string    `json:"reason"`
		TokenID         uuid.UUID `json:"token_id"`
		TokenName       string    `json:"token_name"`
		Scopes          []string  `json:"scopes"`
		EscalationID    uuid.UUID `json:"escalation_id"`
		HumanWorkItemID uuid.UUID `json:"human_work_item_id"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return IssueResult{}, false, fmt.Errorf("grants: decode existing outcome: %w", err)
	}
	result := IssueResult{
		GrantID:         grantID,
		WorkItemID:      payload.WorkItemID,
		Template:        payload.Template,
		Reason:          payload.Reason,
		Scopes:          payload.Scopes,
		RequestEventID:  requestEventID,
		OutcomeEventID:  eventID,
		EscalationID:    payload.EscalationID,
		HumanWorkItemID: payload.HumanWorkItemID,
	}
	switch kind {
	case domain.EventSubactorGrantGranted:
		result.Disposition = DispositionGrant
		result.Token = &domain.Token{
			ID:     payload.TokenID,
			Name:   payload.TokenName,
			Scopes: payload.Scopes,
			Source: domain.SourceAgent,
		}
	case domain.EventSubactorGrantDenied:
		result.Disposition = DispositionDeny
	case domain.EventSubactorGrantEscalated:
		result.Disposition = DispositionEscalate
	default:
		return IssueResult{}, false, fmt.Errorf("grants: unsupported existing outcome %q", kind)
	}
	return result, true, nil
}

func existingGrantRequestEvent(ctx context.Context, tx pgx.Tx, grantID uuid.UUID) (uuid.UUID, error) {
	var eventID uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT id
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND kind = $3
		ORDER BY occurred_at ASC
		LIMIT 1
	`, domain.SubjectSubactorGrant, grantID, domain.EventSubactorGrantRequested).Scan(&eventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, nil
		}
		return uuid.Nil, err
	}
	return eventID, nil
}

func (s *IssuanceService) treeRelation(ctx context.Context, tx pgx.Tx, parent domain.Token, target uuid.UUID) (TreeRelation, error) {
	roots := parentTreeRoots(parent.Scopes)
	if len(roots) == 0 {
		return TreeUnknown, nil
	}
	for _, root := range roots {
		if root == target {
			return TreeSame, nil
		}
		ok, err := s.workItemInTree(ctx, tx, root, target)
		if err != nil {
			return TreeUnknown, err
		}
		if ok {
			return TreeDescendant, nil
		}
	}
	return TreeOutside, nil
}

func (s *IssuanceService) workItemInTree(ctx context.Context, tx pgx.Tx, root, target uuid.UUID) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, `
		WITH RECURSIVE subtree(id) AS (
			SELECT $1::uuid
			UNION
			SELECT wir.child_id
			FROM work_item_relations wir
			JOIN subtree s ON wir.parent_id = s.id
		)
		SELECT EXISTS (SELECT 1 FROM subtree WHERE id = $2)
	`, root, target).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("grants: tree relation: %w", err)
	}
	return ok, nil
}

func parentTreeRoots(scopes []string) []uuid.UUID {
	var roots []uuid.UUID
	for _, scope := range scopes {
		raw := strings.TrimSpace(scope)
		if !strings.HasPrefix(raw, "work_items.tree:") {
			continue
		}
		id, err := uuid.Parse(strings.TrimPrefix(raw, "work_items.tree:"))
		if err == nil && id != uuid.Nil {
			roots = append(roots, id)
		}
	}
	return roots
}

func normalizeRequestedScopes(scopes []string) ([]string, error) {
	seen := map[string]bool{}
	for i, scope := range scopes {
		trimmed := strings.TrimSpace(scope)
		if trimmed == "" {
			return nil, fmt.Errorf("grants: requested_scopes[%d] is blank", i)
		}
		seen[trimmed] = true
	}
	out := make([]string, 0, len(seen))
	for scope := range seen {
		out = append(out, scope)
	}
	sort.Strings(out)
	return out, nil
}

func defaultSubactorName(template Template, workItemID uuid.UUID) string {
	prefix := workItemID.String()
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	if template == "" {
		template = "subactor"
	}
	return fmt.Sprintf("%s-%s", template, prefix)
}

func escalationSummary(item grantWorkItem, template Template, reason string) string {
	return fmt.Sprintf("Subactor grant %q for work_item %s (%s) requires review: %s", template, item.ID, item.Title, reason)
}

func newGrantID(ctx context.Context) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, "subactor_grant"); ok {
		return id
	}
	return uuid.New()
}

func deterministicSubactorTokenID(grantID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("meristem\x00subactor-grant-token\x00"+grantID.String()))
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceAgent
}
