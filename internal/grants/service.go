package grants

import (
	"context"
	"database/sql"
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
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
)

var (
	ErrWorkItemNotFound = errors.New("grants: work_item not found")
	ErrInvalidRequest   = errors.New("grants: invalid request")
)

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

type EvaluationInput struct {
	Parent          domain.Token
	WorkItemID      uuid.UUID
	Template        Template
	RequestedScopes []string
}

type EvaluationResult struct {
	WorkItemID           uuid.UUID
	WorkItemTitle        string
	Template             Template
	RequestedScopes      []string
	TreeRelation         TreeRelation
	HumanReviewStatus    domain.HumanReviewStatus
	DelegationDepthKnown bool
	DelegationDepth      int
	MaxDelegationDepth   int
	DepthBudgetSource    string
	Cultivar             string
	Decision             Decision
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
		return IssueResult{}, fmt.Errorf("%w: parent token is required", ErrInvalidRequest)
	}
	if in.WorkItemID == uuid.Nil {
		return IssueResult{}, fmt.Errorf("%w: work_item_id is required", ErrInvalidRequest)
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

	evaluation, err := s.EvaluateInTx(ctx, tx, EvaluationInput{
		Parent:          in.Parent,
		WorkItemID:      in.WorkItemID,
		Template:        in.Template,
		RequestedScopes: in.RequestedScopes,
	})
	if err != nil {
		return IssueResult{}, err
	}
	requestPayload := map[string]any{
		"parent_token_id":        in.Parent.ID,
		"work_item_id":           in.WorkItemID,
		"template":               in.Template,
		"requested_source":       domain.SourceAgent,
		"requested_scopes":       evaluation.RequestedScopes,
		"token_name":             tokenName,
		"tree_relation":          evaluation.TreeRelation,
		"human_review_status":    evaluation.HumanReviewStatus,
		"delegation_depth_known": evaluation.DelegationDepthKnown,
		"max_delegation_depth":   evaluation.MaxDelegationDepth,
		"depth_budget_source":    evaluation.DepthBudgetSource,
	}
	if evaluation.DelegationDepthKnown {
		requestPayload["delegation_depth"] = evaluation.DelegationDepth
	}
	if evaluation.Cultivar != "" {
		requestPayload["cultivar"] = evaluation.Cultivar
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

	result := IssueResult{
		GrantID:        grantID,
		WorkItemID:     in.WorkItemID,
		Template:       in.Template,
		Disposition:    evaluation.Decision.Disposition,
		Reason:         evaluation.Decision.Reason,
		Scopes:         append([]string(nil), evaluation.Decision.Scopes...),
		RequestEventID: requestEventID,
	}

	switch evaluation.Decision.Disposition {
	case DispositionGrant:
		tokenResult, err := s.auth.CreateDelegatedToken(ctx, tx, auth.CreateDelegatedTokenInput{
			ID:     deterministicSubactorTokenID(grantID),
			Name:   tokenName,
			Scopes: evaluation.Decision.Scopes,
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
				"reason":           evaluation.Decision.Reason,
				"token_id":         tokenResult.Token.ID,
				"token_name":       tokenResult.Token.Name,
				"scopes":           evaluation.Decision.Scopes,
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
				"reason":           evaluation.Decision.Reason,
			},
		})
		if err != nil {
			return IssueResult{}, err
		}
		result.OutcomeEventID = outcomeEventID
	case DispositionEscalate:
		escalation, err := s.escalations.RequestInTx(ctx, tx, escalations.RequestInput{
			WorkItemID: in.WorkItemID,
			Reason:     "subactor grant requires review: " + evaluation.Decision.Reason,
			Summary:    escalationSummary(grantWorkItem{ID: evaluation.WorkItemID, Title: evaluation.WorkItemTitle}, in.Template, evaluation.Decision.Reason),
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
				"reason":             evaluation.Decision.Reason,
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
		return IssueResult{}, fmt.Errorf("grants: unsupported reducer disposition %q", evaluation.Decision.Disposition)
	}

	if err := tx.Commit(ctx); err != nil {
		return IssueResult{}, err
	}
	return result, nil
}

func (s *IssuanceService) Evaluate(ctx context.Context, in EvaluationInput) (EvaluationResult, error) {
	if in.Parent.ID == uuid.Nil {
		return EvaluationResult{}, fmt.Errorf("%w: parent token is required", ErrInvalidRequest)
	}
	if in.WorkItemID == uuid.Nil {
		return EvaluationResult{}, fmt.Errorf("%w: work_item_id is required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return EvaluationResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.EvaluateInTx(ctx, tx, in)
	if err != nil {
		return EvaluationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EvaluationResult{}, err
	}
	return result, nil
}

func (s *IssuanceService) EvaluateInTx(ctx context.Context, tx pgx.Tx, in EvaluationInput) (EvaluationResult, error) {
	if in.Parent.ID == uuid.Nil {
		return EvaluationResult{}, fmt.Errorf("%w: parent token is required", ErrInvalidRequest)
	}
	if in.WorkItemID == uuid.Nil {
		return EvaluationResult{}, fmt.Errorf("%w: work_item_id is required", ErrInvalidRequest)
	}
	requestedScopes, err := normalizeRequestedScopes(in.RequestedScopes)
	if err != nil {
		return EvaluationResult{}, err
	}
	item, err := scanGrantWorkItem(ctx, tx, in.WorkItemID)
	if err != nil {
		return EvaluationResult{}, err
	}
	relation, err := s.treeRelation(ctx, tx, in.Parent, in.WorkItemID)
	if err != nil {
		return EvaluationResult{}, err
	}
	depthBudget, err := s.delegationDepthBudget(ctx, tx, in.Parent, in.WorkItemID, item.Cultivar)
	if err != nil {
		return EvaluationResult{}, err
	}
	decision := Reduce(Request{
		Parent:               in.Parent,
		Template:             in.Template,
		RequestedSource:      domain.SourceAgent,
		RequestedTreeRoot:    in.WorkItemID,
		RequestedScopes:      requestedScopes,
		TreeRelation:         relation,
		DelegationDepthKnown: depthBudget.Known,
		DelegationDepth:      depthBudget.Depth,
		MaxDelegationDepth:   depthBudget.MaxDepth,
		DepthBudgetSource:    depthBudget.Source,
		HumanReviewStatus:    item.HumanReviewStatus,
	})
	return EvaluationResult{
		WorkItemID:           item.ID,
		WorkItemTitle:        item.Title,
		Template:             in.Template,
		RequestedScopes:      requestedScopes,
		TreeRelation:         relation,
		HumanReviewStatus:    item.HumanReviewStatus,
		DelegationDepthKnown: depthBudget.Known,
		DelegationDepth:      depthBudget.Depth,
		MaxDelegationDepth:   depthBudget.MaxDepth,
		DepthBudgetSource:    depthBudget.Source,
		Cultivar:             depthBudget.Cultivar,
		Decision:             decision,
	}, nil
}

type grantWorkItem struct {
	ID                uuid.UUID
	Title             string
	HumanReviewStatus domain.HumanReviewStatus
	Cultivar          string
}

func scanGrantWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) (grantWorkItem, error) {
	var item grantWorkItem
	var humanReview string
	err := tx.QueryRow(ctx, `
		SELECT wi.id, wi.title, wi.human_review_status, COALESCE(created.payload->>'cultivar', '')
		FROM work_items wi
		LEFT JOIN events created
			ON created.subject_kind = $2
			AND created.subject_id = wi.id
			AND created.kind = $3
		WHERE wi.id = $1
	`, id, domain.SubjectWorkItem, domain.EventWorkItemCreated).Scan(&item.ID, &item.Title, &humanReview, &item.Cultivar)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return grantWorkItem{}, ErrWorkItemNotFound
		}
		return grantWorkItem{}, err
	}
	item.HumanReviewStatus = domain.HumanReviewStatus(humanReview)
	if !item.HumanReviewStatus.Valid() {
		return grantWorkItem{}, fmt.Errorf("%w: invalid human_review_status %q", ErrInvalidRequest, humanReview)
	}
	return item, nil
}

type delegationDepthBudget struct {
	Known    bool
	Depth    int
	MaxDepth int
	Source   string
	Cultivar string
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

func (s *IssuanceService) delegationDepthBudget(ctx context.Context, tx pgx.Tx, parent domain.Token, target uuid.UUID, cultivarRef string) (delegationDepthBudget, error) {
	budget := delegationDepthBudget{
		MaxDepth: safety.DefaultPolicy().MaxDelegationDepth,
		Source:   "safety_policy",
	}
	cultivarRef = strings.TrimSpace(cultivarRef)
	if cultivarRef != "" {
		item, err := registry.NewService(s.pool, nil).GetCultivarRef(ctx, cultivarRef)
		if err != nil {
			return delegationDepthBudget{}, err
		}
		budget.MaxDepth = item.Xylem.MaxDepth
		budget.Source = fmt.Sprintf("cultivar:%s@%d", item.Name, item.Version)
		budget.Cultivar = fmt.Sprintf("%s@%d", item.Name, item.Version)
	}
	depth, known, err := s.delegationDepth(ctx, tx, parent, target)
	if err != nil {
		return delegationDepthBudget{}, err
	}
	budget.Known = known
	budget.Depth = depth
	return budget, nil
}

func (s *IssuanceService) delegationDepth(ctx context.Context, tx pgx.Tx, parent domain.Token, target uuid.UUID) (int, bool, error) {
	roots := parentTreeRoots(parent.Scopes)
	if len(roots) == 0 {
		return 0, false, nil
	}
	known := false
	minDepth := 0
	for _, root := range roots {
		var depth sql.NullInt64
		err := tx.QueryRow(ctx, `
			WITH RECURSIVE subtree(id, depth) AS (
				SELECT $1::uuid, 0
				UNION ALL
				SELECT wir.child_id, subtree.depth + 1
				FROM work_item_relations wir
				JOIN subtree ON wir.parent_id = subtree.id
			)
			SELECT MIN(depth) FROM subtree WHERE id = $2
		`, root, target).Scan(&depth)
		if err != nil {
			return 0, false, fmt.Errorf("grants: delegation depth: %w", err)
		}
		if !depth.Valid {
			continue
		}
		current := int(depth.Int64)
		if !known || current < minDepth {
			known = true
			minDepth = current
		}
	}
	return minDepth, known, nil
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
			return nil, fmt.Errorf("%w: requested_scopes[%d] is blank", ErrInvalidRequest, i)
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
