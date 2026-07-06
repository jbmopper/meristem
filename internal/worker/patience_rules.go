package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
)

const (
	budgetSourceItemMetadata = "item"
	budgetSourceCultivar     = "cultivar"
	budgetSourcePolicy       = "policy_profile"
)

type launchMetadata struct {
	Cultivar              string                `json:"cultivar"`
	PatienceBudgetSeconds int                   `json:"patience_budget_seconds"`
	EscalationRule        domain.EscalationRule `json:"escalation_rule"`
}

type patienceResolution struct {
	Budget         time.Duration
	BudgetSource   string
	EscalationRule domain.EscalationRule
	Cultivar       string
}

func (w *Worker) breachCandidates(ctx context.Context) ([]Candidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT wi.id, wi.state, wi.state_entered_at, wi.human_review_status, COALESCE(created.payload, '{}'::jsonb)
		FROM work_items wi
		LEFT JOIN events created
			ON created.subject_kind = $1
			AND created.subject_id = wi.id
			AND created.kind = $2
		WHERE wi.state = ANY($3::text[])
		ORDER BY wi.state_entered_at ASC
	`, domain.SubjectWorkItem, domain.EventWorkItemCreated, nonTerminalStateStrings())
	if err != nil {
		return nil, fmt.Errorf("worker: query work_items: %w", err)
	}
	defer rows.Close()

	var out []Candidate
	for rows.Next() {
		var c Candidate
		var st string
		var review string
		var createdPayload []byte
		if err := rows.Scan(&c.ID, &st, &c.StateEnteredAt, &review, &createdPayload); err != nil {
			return nil, fmt.Errorf("worker: scan work_items row: %w", err)
		}
		c.State = domain.WorkItemState(st)
		c.HumanReviewStatus = domain.HumanReviewStatus(review)
		resolved, err := w.resolvePatienceRule(ctx, c.State, createdPayload)
		if err != nil {
			return nil, fmt.Errorf("worker: resolve patience rule for %s: %w", c.ID, err)
		}
		c.Budget = resolved.Budget
		c.BudgetSource = resolved.BudgetSource
		c.EscalationRule = resolved.EscalationRule
		c.Cultivar = resolved.Cultivar
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("worker: iterate work_items: %w", err)
	}
	return out, nil
}

func (w *Worker) resolvePatienceRule(ctx context.Context, state domain.WorkItemState, raw []byte) (patienceResolution, error) {
	meta, err := decodeLaunchMetadata(raw)
	if err != nil {
		return patienceResolution{}, err
	}
	rule := meta.EscalationRule
	if rule == "" {
		rule = domain.EscalationRuleHandToHuman
	}
	if !rule.Valid() {
		return patienceResolution{}, fmt.Errorf("invalid escalation_rule %q", rule)
	}

	cultivarRef := strings.TrimSpace(meta.Cultivar)
	if meta.PatienceBudgetSeconds < 0 {
		return patienceResolution{}, fmt.Errorf("patience_budget_seconds must be >= 0")
	}
	if meta.PatienceBudgetSeconds > 0 {
		budget := capPatienceBudgetSeconds(meta.PatienceBudgetSeconds)
		return patienceResolution{
			Budget:         budget,
			BudgetSource:   budgetSourceItemMetadata,
			EscalationRule: rule,
			Cultivar:       cultivarRef,
		}, nil
	}
	if cultivarRef != "" && state == domain.WorkItemRunning {
		item, err := registry.NewService(w.pool, nil).GetCultivarRef(ctx, cultivarRef)
		if err != nil {
			return patienceResolution{}, err
		}
		if item.Xylem.MaxWallSeconds <= 0 {
			return patienceResolution{}, fmt.Errorf("cultivar %s@%d has non-positive xylem.max_wall_seconds", item.Name, item.Version)
		}
		budget := capPatienceBudgetSeconds(item.Xylem.MaxWallSeconds)
		return patienceResolution{
			Budget:         budget,
			BudgetSource:   budgetSourceCultivar,
			EscalationRule: rule,
			Cultivar:       fmt.Sprintf("%s@%d", item.Name, item.Version),
		}, nil
	}
	return patienceResolution{
		Budget:         w.budgets.ByState[state],
		BudgetSource:   budgetSourcePolicy,
		EscalationRule: rule,
		Cultivar:       cultivarRef,
	}, nil
}

func decodeLaunchMetadata(raw []byte) (launchMetadata, error) {
	if len(raw) == 0 {
		return launchMetadata{}, nil
	}
	var meta launchMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return launchMetadata{}, fmt.Errorf("decode work_item.created launch metadata: %w", err)
	}
	meta.Cultivar = strings.TrimSpace(meta.Cultivar)
	return meta, nil
}

func capPatienceBudgetSeconds(seconds int) time.Duration {
	if int64(seconds) > int64(safety.MaxPatienceBudget/time.Second) {
		return safety.MaxPatienceBudget
	}
	return time.Duration(seconds) * time.Second
}

func nonTerminalStateStrings() []string {
	return []string{
		string(domain.WorkItemCaptured),
		string(domain.WorkItemTriaged),
		string(domain.WorkItemPlanned),
		string(domain.WorkItemAwaitingApproval),
		string(domain.WorkItemRunning),
		string(domain.WorkItemBlocked),
	}
}
