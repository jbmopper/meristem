//go:build convergence_worker_experiment

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	defaultConvergenceMaxAttempts = 3
)

// convergencePassResult tracks the effects of one ScanOnce convergence kernel.
type convergencePassResult struct {
	ConvergenceCandidatesScanned       int
	ConvergenceVerdictsRecorded        int
	ConvergenceVerdictsAlreadyRecorded int
	ConvergenceAccepts                 int
	ConvergenceRetries                 int
	ConvergenceEscalations             int
}

var defaultConvergenceBudget = convergence.Budget{
	MaxAttempts: defaultConvergenceMaxAttempts,
	Escalation:  convergence.EscalateFail,
}

type convergenceCandidate struct {
	ID                         uuid.UUID
	SuggestedConvergenceChecks []string
}

type eventAppendedSignalEnvelope struct {
	InnerKind string         `json:"inner_kind"`
	Inner     map[string]any `json:"inner"`
}

// scanConvergence runs one convergence-driven reconcile pass across running
// work_items that declared convergence checks.
func (w *Worker) scanConvergence(ctx context.Context) (convergencePassResult, error) {
	candidates, err := w.convergenceCandidates(ctx)
	if err != nil {
		return convergencePassResult{}, fmt.Errorf("scan running candidates: %w", err)
	}

	result := convergencePassResult{
		ConvergenceCandidatesScanned: len(candidates),
	}

	service := workitems.NewService(w.pool, w.writer)
	actor := domain.Token{
		Source: domain.SourceSystem,
	}
	if w.actor != nil {
		actor.ID = *w.actor
	}

	for _, c := range candidates {
		attempt, err := w.nextConvergenceAttempt(ctx, c.ID)
		if err != nil {
			return result, err
		}

		signals, err := w.convergenceSignalsForItem(ctx, c.ID)
		if err != nil {
			return result, err
		}

		reducer := convergence.AllPassChecklist{Required: c.SuggestedConvergenceChecks}
		reduction, err := convergence.Run(reducer, signals, attempt)
		if err != nil {
			return result, err
		}

		fresh, err := w.emitConvergenceVerdict(ctx, c.ID, reduction)
		if err != nil {
			return result, err
		}
		if fresh {
			result.ConvergenceVerdictsRecorded++
		} else {
			result.ConvergenceVerdictsAlreadyRecorded++
		}

		outcome, escalation := defaultConvergenceBudget.Next(reduction.Verdict, attempt)
		switch outcome {
		case convergence.OutcomeAccept:
			reason := convergenceReason("accept", attempt, reduction)
			_, err := service.Transition(ctx, c.ID, domain.WorkItemDone, reason, actor)
			if err != nil {
				if !shouldIgnoreConvergenceTransitionError(err) {
					return result, err
				}
			} else {
				result.ConvergenceAccepts++
			}
		case convergence.OutcomeRetry:
			// Keep the item in running; the next pass will gather new signals.
			result.ConvergenceRetries++

		case convergence.OutcomeEscalate:
			err := w.escalateConvergence(ctx, service, c.ID, c.SuggestedConvergenceChecks, attempt, escalation, reduction.Verdict.Reason, actor)
			if err != nil {
				if !shouldIgnoreConvergenceTransitionError(err) {
					return result, err
				}
			} else {
				result.ConvergenceEscalations++
			}

		}
	}

	return result, nil
}

// convergenceCandidates loads running work_items that are eligible for convergence.
func (w *Worker) convergenceCandidates(ctx context.Context) ([]convergenceCandidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT id, suggested_convergence_checks
		FROM work_items
		WHERE state = $1
		ORDER BY updated_at ASC
	`, domain.WorkItemRunning)
	if err != nil {
		return nil, fmt.Errorf("query candidates: %w", err)
	}
	defer rows.Close()

	var out []convergenceCandidate
	for rows.Next() {
		var c convergenceCandidate
		var checksJSON []byte
		if err := rows.Scan(&c.ID, &checksJSON); err != nil {
			return nil, fmt.Errorf("scan candidate row: %w", err)
		}
		if len(checksJSON) > 0 {
			if err := json.Unmarshal(checksJSON, &c.SuggestedConvergenceChecks); err != nil {
				return nil, fmt.Errorf("decode suggested_convergence_checks for %s: %w", c.ID, err)
			}
		}
		if len(c.SuggestedConvergenceChecks) == 0 {
			continue
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidates: %w", err)
	}
	return out, nil
}

// nextConvergenceAttempt returns 1-based attempt count for the next reduction of
// this work_item in the convergence loop.
func (w *Worker) nextConvergenceAttempt(ctx context.Context, workItemID uuid.UUID) (int, error) {
	var last int
	err := w.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX((payload->>'attempt')::int), 0)
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectConvergence, workItemID, domain.EventConvergenceVerdictRecorded).Scan(&last)
	if err != nil {
		return 0, err
	}
	return last + 1, nil
}

// convergenceSignalsForItem gathers deterministic signals the reducer can consume.
func (w *Worker) convergenceSignalsForItem(ctx context.Context, workItemID uuid.UUID) ([]convergence.Signal, error) {
	var out []convergence.Signal

	fromAppended, err := w.convergenceSignalsFromEventAppended(ctx, workItemID)
	if err != nil {
		return nil, err
	}
	out = append(out, fromAppended...)

	fromSignalRows, err := w.convergenceSignalsFromSignalsTable(ctx, workItemID)
	if err != nil {
		return nil, err
	}
	out = append(out, fromSignalRows...)
	return out, nil
}

func (w *Worker) convergenceSignalsFromEventAppended(ctx context.Context, workItemID uuid.UUID) ([]convergence.Signal, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1
			AND subject_id = $2
			AND kind = $3
		ORDER BY occurred_at ASC
	`, domain.SubjectWorkItem, workItemID, domain.EventWorkItemEventAppended)
	if err != nil {
		return nil, fmt.Errorf("query event_appended signals for %s: %w", workItemID, err)
	}
	defer rows.Close()

	var out []convergence.Signal
	for rows.Next() {
		var payload json.RawMessage
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan event_appended signal for %s: %w", workItemID, err)
		}
		var env eventAppendedSignalEnvelope
		if err := json.Unmarshal(payload, &env); err != nil {
			return nil, fmt.Errorf("decode event_appended payload for %s: %w", workItemID, err)
		}
		if env.InnerKind == "" || env.Inner == nil {
			continue
		}
		s := toConvergenceSignal(env.InnerKind, env.Inner)
		if s == nil {
			continue
		}
		out = append(out, *s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event_appended signals for %s: %w", workItemID, err)
	}
	return out, nil
}

func (w *Worker) convergenceSignalsFromSignalsTable(ctx context.Context, workItemID uuid.UUID) ([]convergence.Signal, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT signal_kind, source, work_spec
		FROM signals
		WHERE work_item_id = $1
		ORDER BY received_at ASC
	`, workItemID)
	if err != nil {
		return nil, fmt.Errorf("query signals projection for %s: %w", workItemID, err)
	}
	defer rows.Close()

	var out []convergence.Signal
	for rows.Next() {
		var kind string
		var source string
		var workSpec json.RawMessage
		if err := rows.Scan(&kind, &source, &workSpec); err != nil {
			return nil, fmt.Errorf("scan signal projection for %s: %w", workItemID, err)
		}
		var payload map[string]any
		if len(workSpec) > 0 {
			if err := json.Unmarshal(workSpec, &payload); err != nil {
				return nil, fmt.Errorf("decode signal work_spec for %s: %w", workItemID, err)
			}
		}
		s := toConvergenceSignalFromSignalRow(kind, source, payload)
		if s == nil {
			continue
		}
		out = append(out, *s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate signals for %s: %w", workItemID, err)
	}
	return out, nil
}

func toConvergenceSignal(signalKind string, payload map[string]any) *convergence.Signal {
	raw := parseSignalRaw(payload)
	pass, hasPass := parsePass(payload)
	score, hasScore := parseScore(payload)
	if !hasPass && !hasScore && raw == "" {
		return nil
	}
	s := &convergence.Signal{
		Kind:  signalKind,
		Raw:   raw,
		Pass:  pass,
		Score: score,
	}
	s.Source = parseSignalSource(payload)
	return s
}

func toConvergenceSignalFromSignalRow(signalKind, sourceName string, payload map[string]any) *convergence.Signal {
	if signalKind == "" {
		return nil
	}
	raw := parseSignalRaw(payload)
	pass, hasPass := parsePass(payload)
	score, hasScore := parseScore(payload)
	if !hasPass && !hasScore && raw == "" && sourceName == "" {
		return nil
	}
	s := &convergence.Signal{
		Kind:  signalKind,
		Raw:   raw,
		Pass:  pass,
		Score: score,
	}
	if sourceName != "" {
		s.Source = convergence.SignalSource{Model: sourceName}
	}
	return s
}

func parsePass(payload map[string]any) (*bool, bool) {
	raw, ok := payload["pass"]
	if !ok {
		return nil, false
	}
	pass, ok := raw.(bool)
	if !ok {
		return nil, false
	}
	return &pass, true
}

func parseScore(payload map[string]any) (*float64, bool) {
	raw, ok := payload["score"]
	if !ok {
		return nil, false
	}
	switch v := raw.(type) {
	case float64:
		return &v, true
	case int:
		n := float64(v)
		return &n, true
	case int32:
		n := float64(v)
		return &n, true
	case int64:
		n := float64(v)
		return &n, true
	case json.Number:
		asFloat, err := v.Float64()
		if err != nil {
			return nil, false
		}
		return &asFloat, true
	default:
		return nil, false
	}
}

func parseSignalRaw(payload map[string]any) string {
	switch v := payload["raw"].(type) {
	case string:
		return strings.TrimSpace(v)
	case nil:
		return ""
	default:
		return ""
	}
}

func parseSignalSource(payload map[string]any) convergence.SignalSource {
	var source convergence.SignalSource
	if model, ok := payload["model"].(string); ok {
		source.Model = strings.TrimSpace(model)
	}
	if promptVersion, ok := payload["prompt_version"].(string); ok {
		source.PromptVersion = strings.TrimSpace(promptVersion)
	}
	if sampleID, ok := payload["sample_id"].(string); ok {
		source.SampleID = strings.TrimSpace(sampleID)
	}
	if source.Model == "" && source.PromptVersion == "" && source.SampleID == "" {
		return convergence.SignalSource{}
	}
	return source
}

func (w *Worker) emitConvergenceVerdict(ctx context.Context, workItemID uuid.UUID, reduction convergence.Reduction) (bool, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, fresh, err := w.writer.Append(ctx, tx, eventsSpecForConvergenceVerdict(w.actor, workItemID, reduction))
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

func eventsSpecForConvergenceVerdict(actor *uuid.UUID, workItemID uuid.UUID, reduction convergence.Reduction) events.Spec {
	return events.Spec{
		SubjectKind:  domain.SubjectConvergence,
		SubjectID:    workItemID,
		Kind:         domain.EventConvergenceVerdictRecorded,
		Source:       domain.SourceSystem,
		ActorTokenID: actor,
		Payload:      reduction.EventPayload(),
	}
}

// escalateConvergence applies the convergence budget escalation rule.
func (w *Worker) escalateConvergence(ctx context.Context, service *workitems.Service, workItemID uuid.UUID, checks []string, attempt int, escalation convergence.Escalation, verdictReason string, actor domain.Token) error {
	reason := escalationReason(attempt, verdictReason)
	switch escalation {
	case convergence.EscalateFail:
		_, err := service.Transition(ctx, workItemID, domain.WorkItemFailed, reason, actor)
		return err
	case convergence.EscalateHandToHuman, convergence.EscalateRequestApproval:
		if err := w.setBlockedForConvergence(ctx, service, workItemID, checks, actor); err != nil {
			return err
		}
		_, err := service.Transition(ctx, workItemID, domain.WorkItemBlocked, reason, actor)
		return err
	default:
		return fmt.Errorf("unsupported escalation rule %q for %s", escalation, workItemID)
	}
}

func (w *Worker) setBlockedForConvergence(ctx context.Context, service *workitems.Service, workItemID uuid.UUID, checks []string, actor domain.Token) error {
	_, err := service.UpdateMetadata(ctx, workItemID, workitems.UpdateMetadataInput{
		SuggestedConvergenceChecks: checks,
		HumanReviewStatus:          domain.HumanReviewBlocked,
		Actor:                      actor,
	})
	return err
}

func escalationReason(attempt int, verdictReason string) string {
	if verdictReason == "" {
		return fmt.Sprintf("convergence escalated at attempt %d after bounded budget exhaustion", attempt)
	}
	return fmt.Sprintf("convergence escalated at attempt %d (%s)", attempt, verdictReason)
}

func convergenceReason(kind string, attempt int, reduction convergence.Reduction) string {
	if reduction.Verdict.Reason != "" {
		return fmt.Sprintf("convergence %s attempt %d: %s", kind, attempt, reduction.Verdict.Reason)
	}
	return fmt.Sprintf("convergence %s attempt %d", kind, attempt)
}

func shouldIgnoreConvergenceTransitionError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, workitems.ErrNotFound) {
		return true
	}
	return strings.Contains(err.Error(), "invalid transition from")
}
