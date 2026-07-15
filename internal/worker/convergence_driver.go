package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	defaultConvergenceMaxAttempts = 3
)

// convergencePassResult tracks the effects of one ScanOnce convergence kernel.
type convergencePassResult struct {
	ConvergenceCandidatesScanned        int
	ConvergenceVerdictsRecorded         int
	ConvergenceVerdictsAlreadyRecorded  int
	ConvergenceStaleInputsSkipped       int
	ConvergenceAccepts                  int
	ConvergenceRetries                  int
	ConvergenceEscalations              int
	ConvergenceMalformedPayloadsSkipped int
}

var defaultConvergenceBudget = convergence.Budget{
	MaxAttempts: defaultConvergenceMaxAttempts,
	Escalation:  convergence.EscalateHandToHuman,
}

type convergenceCandidate struct {
	ID                         uuid.UUID
	SuggestedConvergenceChecks []string
}

type convergenceVerdictRecord struct {
	Attempt      int
	InputsDigest string
	Verdict      convergence.Verdict
}

type convergenceDecision struct {
	AppendCurrent bool
	SkipStale     bool
	Outcome       convergence.Outcome
	Attempt       int
	Escalation    convergence.Escalation
	Verdict       convergence.Verdict
}

type eventAppendedSignalEnvelope struct {
	InnerKind string          `json:"inner_kind"`
	Inner     json.RawMessage `json:"inner"`
}

// decodeEventAppendedInner extracts the inner signal object from one
// event_appended payload without ever failing the pass. Historical payloads
// carry inner as a JSON object, as a string-encoded JSON object (legacy
// writers), or as free prose; only the first two can contribute signals.
// It returns (nil, "") for benign no-signal payloads and a non-empty reason
// when the payload is malformed and worth deterministic evidence.
func decodeEventAppendedInner(payload []byte) (inner map[string]any, innerKind string, reason string) {
	var env eventAppendedSignalEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, "", "payload is not an event_appended envelope: " + err.Error()
	}
	if len(env.Inner) == 0 || string(env.Inner) == "null" {
		return nil, env.InnerKind, ""
	}
	var obj map[string]any
	if err := json.Unmarshal(env.Inner, &obj); err == nil {
		return obj, env.InnerKind, ""
	}
	var s string
	if err := json.Unmarshal(env.Inner, &s); err == nil {
		if err := json.Unmarshal([]byte(s), &obj); err == nil {
			return obj, env.InnerKind, ""
		}
		return nil, env.InnerKind, "inner is a string that does not encode a JSON object"
	}
	return nil, env.InnerKind, "inner is not a JSON object"
}

// scanConvergence runs one convergence-driven reconcile pass across running
// work_items that declared convergence checks.
func (w *Worker) scanConvergence(ctx context.Context) (convergencePassResult, error) {
	if err := defaultConvergenceBudget.Validate(); err != nil {
		return convergencePassResult{}, err
	}
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
		reducer := convergence.AllPassChecklist{Required: c.SuggestedConvergenceChecks}
		last, err := w.latestConvergenceVerdict(ctx, c.ID, reducer)
		if err != nil {
			return result, err
		}

		signals, malformed, err := w.convergenceSignalsForItem(ctx, c.ID, c.SuggestedConvergenceChecks)
		result.ConvergenceMalformedPayloadsSkipped += malformed
		if err != nil {
			return result, err
		}

		attempt := 1
		if last != nil {
			attempt = last.Attempt + 1
		}
		reduction, err := convergence.Run(reducer, signals, attempt)
		if err != nil {
			return result, err
		}

		decision := decideConvergenceStep(last, reduction, defaultConvergenceBudget)
		if decision.SkipStale {
			result.ConvergenceStaleInputsSkipped++
			continue
		}

		if decision.AppendCurrent {
			fresh, err := w.emitConvergenceVerdict(ctx, c.ID, reduction)
			if err != nil {
				return result, err
			}
			if fresh {
				result.ConvergenceVerdictsRecorded++
			} else {
				result.ConvergenceVerdictsAlreadyRecorded++
			}
		}

		switch decision.Outcome {
		case convergence.OutcomeAccept:
			reason := convergenceReason("accept", decision.Attempt, decision.Verdict.Reason)
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
			err := w.escalateConvergence(ctx, service, c.ID, c.SuggestedConvergenceChecks, decision.Attempt, decision.Escalation, decision.Verdict.Reason, actor)
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

func decideConvergenceStep(last *convergenceVerdictRecord, current convergence.Reduction, budget convergence.Budget) convergenceDecision {
	if last != nil && last.InputsDigest == current.InputsDigest {
		outcome, escalation := budget.Next(last.Verdict, last.Attempt)
		if outcome == convergence.OutcomeRetry {
			return convergenceDecision{
				SkipStale: true,
				Outcome:   outcome,
				Attempt:   last.Attempt,
				Verdict:   last.Verdict,
			}
		}
		return convergenceDecision{
			Outcome:    outcome,
			Attempt:    last.Attempt,
			Escalation: escalation,
			Verdict:    last.Verdict,
		}
	}

	outcome, escalation := budget.Next(current.Verdict, current.Attempt)
	return convergenceDecision{
		AppendCurrent: true,
		Outcome:       outcome,
		Attempt:       current.Attempt,
		Escalation:    escalation,
		Verdict:       current.Verdict,
	}
}

// convergenceCandidates loads running work_items that are eligible for convergence.
func (w *Worker) convergenceCandidates(ctx context.Context) ([]convergenceCandidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT id, suggested_convergence_checks
		FROM work_items
		WHERE state = $1
			AND human_review_status <> $2
		ORDER BY updated_at ASC
	`, domain.WorkItemRunning, domain.HumanReviewBlocked)
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

// latestConvergenceVerdict returns the latest verdict for this work_item and
// reducer. The inputs digest includes reducer configuration, so changing the
// declared checklist produces a different current digest and a fresh attempt.
func (w *Worker) latestConvergenceVerdict(ctx context.Context, workItemID uuid.UUID, reducer convergence.Reducer) (*convergenceVerdictRecord, error) {
	var out convergenceVerdictRecord
	var disposition string
	var reason string
	err := w.pool.QueryRow(ctx, `
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
	out.Verdict = convergence.Verdict{
		Disposition: domain.Verdict(disposition),
		Reason:      reason,
	}
	return &out, nil
}

// convergenceSignalsForItem gathers deterministic signals the reducer can
// consume, plus the count of malformed event_appended payloads it skipped.
func (w *Worker) convergenceSignalsForItem(ctx context.Context, workItemID uuid.UUID, checks []string) ([]convergence.Signal, int, error) {
	var out []convergence.Signal

	fromAppended, malformed, err := w.convergenceSignalsFromEventAppended(ctx, workItemID)
	if err != nil {
		return nil, malformed, err
	}
	out = append(out, fromAppended...)

	fromSignalRows, err := w.convergenceSignalsFromSignalsTable(ctx, workItemID)
	if err != nil {
		return nil, malformed, err
	}
	out = append(out, fromSignalRows...)

	fromBuiltinQueries, err := w.convergenceSignalsFromBuiltinQueries(ctx, workItemID, checks)
	if err != nil {
		return nil, malformed, err
	}
	out = append(out, fromBuiltinQueries...)
	return out, malformed, nil
}

func (w *Worker) convergenceSignalsFromBuiltinQueries(ctx context.Context, workItemID uuid.UUID, checks []string) ([]convergence.Signal, error) {
	var out []convergence.Signal
	for _, check := range checks {
		if !strings.HasPrefix(check, "query:") {
			continue
		}
		name := strings.TrimPrefix(check, "query:")
		if !convergence.KnownQueryCheck(name) {
			continue
		}
		pass, err := w.evaluateBuiltinQueryCheck(ctx, workItemID, name)
		if err != nil {
			return nil, err
		}
		out = append(out, convergence.Signal{
			Kind: "checklist.item:" + check,
			Pass: &pass,
		})
	}
	return out, nil
}

func (w *Worker) evaluateBuiltinQueryCheck(ctx context.Context, workItemID uuid.UUID, name string) (bool, error) {
	switch name {
	case "parent_checks_defined":
		var ok bool
		err := w.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM work_item_relations wir
				JOIN work_items parent ON parent.id = wir.parent_id
				WHERE wir.child_id = $1
					AND jsonb_array_length(parent.suggested_convergence_checks) > 0
			)
		`, workItemID).Scan(&ok)
		return ok, err
	default:
		return false, nil
	}
}

func (w *Worker) convergenceSignalsFromEventAppended(ctx context.Context, workItemID uuid.UUID) ([]convergence.Signal, int, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT id, payload
		FROM events
		WHERE subject_kind = $1
			AND subject_id = $2
			AND kind = $3
		ORDER BY occurred_at ASC
	`, domain.SubjectWorkItem, workItemID, domain.EventWorkItemEventAppended)
	if err != nil {
		return nil, 0, fmt.Errorf("query event_appended signals for %s: %w", workItemID, err)
	}
	defer rows.Close()

	type appendedRow struct {
		id      uuid.UUID
		payload json.RawMessage
	}
	var scanned []appendedRow
	for rows.Next() {
		var row appendedRow
		if err := rows.Scan(&row.id, &row.payload); err != nil {
			return nil, 0, fmt.Errorf("scan event_appended signal for %s: %w", workItemID, err)
		}
		scanned = append(scanned, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate event_appended signals for %s: %w", workItemID, err)
	}

	var out []convergence.Signal
	malformed := 0
	for _, row := range scanned {
		inner, innerKind, reason := decodeEventAppendedInner(row.payload)
		if reason != "" {
			// Malformed history must not abort the pass (or suppress the
			// breach pass behind it); it is skipped with durable evidence.
			malformed++
			w.reportMalformedEventAppended(ctx, workItemID, row.id, reason)
			continue
		}
		if innerKind == "" || inner == nil {
			continue
		}
		s := toConvergenceSignal(innerKind, inner)
		if s == nil {
			continue
		}
		out = append(out, *s)
	}
	return out, malformed, nil
}

// reportMalformedEventAppended records deterministic evidence that one
// event_appended payload was skipped by the convergence fold. The idempotency
// identity is derived from the offending event id, so every subsequent pass
// collapses onto the same deterministic_error subject and event rows instead
// of re-reporting each tick. Evidence failures are logged, never propagated:
// evidence must not become a second way to abort the pass.
func (w *Worker) reportMalformedEventAppended(ctx context.Context, workItemID, eventID uuid.UUID, reason string) {
	if w.actor == nil {
		slog.WarnContext(ctx, "convergence skipped malformed event_appended payload without durable evidence: worker has no actor token",
			"work_item_id", workItemID, "event_id", eventID, "reason", reason)
		return
	}
	details, err := json.Marshal(map[string]string{
		"work_item_id": workItemID.String(),
		"event_id":     eventID.String(),
		"reason":       reason,
	})
	if err != nil {
		slog.WarnContext(ctx, "convergence malformed-payload evidence marshal failed",
			"work_item_id", workItemID, "event_id", eventID, "error", err)
		return
	}
	rctx := idempotency.WithRequest(ctx, idempotency.Request{
		TokenID: *w.actor,
		Scope:   "worker.convergence.malformed_event_appended",
		Key:     eventID.String(),
	})
	svc := errorreporting.NewService(w.pool, w.writer)
	if _, err := svc.Report(rctx, errorreporting.ReportInput{
		Component: "worker.convergence",
		Code:      "malformed_event_appended_payload",
		Message:   "event_appended payload skipped by convergence fold: " + reason,
		Severity:  domain.DeterministicErrorWarning,
		Details:   details,
		Actor:     domain.Token{ID: *w.actor, Source: domain.SourceSystem},
	}); err != nil {
		slog.WarnContext(ctx, "convergence malformed-payload evidence report failed",
			"work_item_id", workItemID, "event_id", eventID, "error", err)
	}
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

	_, fresh, err := convergence.AppendVerdict(ctx, tx, w.writer, domain.SourceSystem, w.actor, workItemID, reduction)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
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

func convergenceReason(kind string, attempt int, verdictReason string) string {
	if verdictReason != "" {
		return fmt.Sprintf("convergence %s attempt %d: %s", kind, attempt, verdictReason)
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
