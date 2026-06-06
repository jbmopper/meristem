// Package worker drives the bounded-patience invariant: every non-terminal
// work_item must either reach a terminal state or have its lengthening
// dwell time recorded as a fact in the event log. v0 left this as a manual
// concern; the worker is the v1 substrate that closes it.
//
// Scope of this slice:
//
//   - One-shot scan over work_items in non-terminal states (ScanOnce).
//   - Per-state patience budgets (Budgets), with sane defaults from
//     DefaultBudgets so callers can run without configuration.
//   - One patience.breached event per (work_item, state-epoch) breach
//     observed. The event is record-only; subsequent slices add the
//     escalation, retry, and forced-fail policies that consume it.
//
// Out of scope (next slices):
//
//   - Daemon loop. ScanOnce is the kernel; wrapping it in time.Tick + the
//     shutdown dance is mechanical and lives in the cmd-side runner.
//   - SELECT … FOR UPDATE SKIP LOCKED job queue. ScanOnce is safe under
//     concurrent execution today (the deterministic event_id collapses
//     duplicate observations); the SKIP LOCKED queue is the v1 work_item
//     "Worker with job_queue and SELECT … FOR UPDATE SKIP LOCKED" and ships
//     when there is real work to gate.
//   - Resolution events (patience.resolved when an item leaves the breached
//     state). For now resolution is implicit in the work_item.transitioned
//     event already in the log; consumers correlate on (subject_id, time).
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/safety"
)

// Budgets maps a work_item state to the longest a healthy item should sit
// in that state before the worker records a patience.breached event. Only
// non-terminal states are eligible; passing a terminal state from the
// validate() check is a programmer error and refused at construction.
//
// A state absent from the map is implicitly infinite (no breach ever
// emitted for it). This is deliberate: it lets operators opt states in
// gradually rather than forcing a budget for every state up front.
type Budgets struct {
	ByState map[domain.WorkItemState]time.Duration
}

// DefaultBudgets returns the conservative defaults for v1's first slice.
// They are deliberately generous; the spec invariant is "no item waits
// indefinitely", not "every item is impatient." Operators can tighten via
// configuration once the daemon ships.
func DefaultBudgets() Budgets {
	policy := safety.DefaultPolicy()
	out := make(map[domain.WorkItemState]time.Duration, len(policy.PatienceBudgets))
	for state, budget := range policy.PatienceBudgets {
		out[state] = budget
	}
	return Budgets{
		ByState: out,
	}
}

func (b Budgets) validate() error {
	for state, dur := range b.ByState {
		if !state.Valid() {
			return fmt.Errorf("worker: budget for unknown state %q", state)
		}
		if state.Terminal() {
			return fmt.Errorf("worker: budget for terminal state %q is meaningless", state)
		}
		if dur < 0 {
			return fmt.Errorf("worker: negative budget for state %q", state)
		}
	}
	return nil
}

// states returns the non-terminal states with a positive budget, in stable
// order (alphabetical for determinism in queries and tests). The order is
// exposed only as a SQL parameter; consumers should not rely on it.
func (b Budgets) states() []string {
	out := make([]string, 0, len(b.ByState))
	for s, dur := range b.ByState {
		if dur <= 0 {
			continue
		}
		out = append(out, string(s))
	}
	// Sort to keep the SQL parameter and any test diffs stable.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Candidate is one work_item read from the projection table for breach
// evaluation. Exported so the pure decision logic in EvaluateBreaches can
// be unit-tested without a database.
type Candidate struct {
	ID        uuid.UUID
	State     domain.WorkItemState
	UpdatedAt time.Time
}

// Breach is the per-row outcome of EvaluateBreaches: a candidate that
// exceeded its budget, paired with the budget and observed age. Holding
// these values in one struct keeps the IO layer's emit step a pure
// translation from Breach to events.Spec.
type Breach struct {
	Candidate Candidate
	Budget    time.Duration
	Age       time.Duration
}

// EvaluateBreaches is the pure breach decision: given the current time, a
// list of candidates, and the budget map, return one Breach per candidate
// whose dwell time exceeds its state's budget. Candidates whose state has
// no budget (or a non-positive one) are skipped silently.
//
// Order of returned breaches matches the input order. Stable input order +
// deterministic event_id means a re-scan with the same candidates yields
// the same emit sequence.
func EvaluateBreaches(now time.Time, candidates []Candidate, budgets Budgets) []Breach {
	out := make([]Breach, 0, len(candidates))
	for _, c := range candidates {
		budget, ok := budgets.ByState[c.State]
		if !ok || budget <= 0 {
			continue
		}
		age := now.Sub(c.UpdatedAt)
		if age <= budget {
			continue
		}
		out = append(out, Breach{Candidate: c, Budget: budget, Age: age})
	}
	return out
}

// Result is the outcome of a single ScanOnce call. The three counters are
// disjoint by construction: a row is either skipped (no breach), freshly
// emitted, or already on record.
type Result struct {
	// Scanned is the count of non-terminal work_items inspected, including
	// those that were not in breach.
	Scanned int
	// BreachesEmitted is the count of patience.breached events freshly
	// appended this pass.
	BreachesEmitted int
	// BreachesAlreadyRecorded is the count of breaches the scan observed
	// that already had a corresponding event_id in the log (the
	// deterministic-id ON CONFLICT DO NOTHING path). Equal to len(breaches)
	// minus BreachesEmitted; reported separately so an operator inspecting
	// `meristem worker --once` output can see "the scan saw N breaches; M
	// were new this run."
	BreachesAlreadyRecorded int

	// ConvergenceCandidatesScanned is the count of running work_items
	// with suggested convergence checks and therefore a chance to advance
	// through the convergence loop in this pass.
	ConvergenceCandidatesScanned int
	// ConvergenceVerdictsRecorded is the number of fresh convergence verdict
	// events appended this pass.
	ConvergenceVerdictsRecorded int
	// ConvergenceVerdictsAlreadyRecorded is the number of convergence verdicts
	// observed but already present in the event log.
	ConvergenceVerdictsAlreadyRecorded int
	// ConvergenceAccepts is how many candidates reached accept and moved to done.
	ConvergenceAccepts int
	// ConvergenceRetries is how many candidates were rejected but kept within
	// budget for another attempt.
	ConvergenceRetries int
	// ConvergenceEscalations is how many candidates exhausted budget or
	// escalated directly and were moved out of the running loop.
	ConvergenceEscalations int
}

// Worker scans the work_items projection for breaches and emits events.
//
// Worker holds no state across calls; ScanOnce is safe to call concurrently
// from multiple processes (each will independently observe the breach and
// attempt the insert; the deterministic event_id makes the second insert
// a no-op via the events writer's ON CONFLICT DO NOTHING). A future slice
// will introduce SKIP LOCKED claim semantics to make the parallel topology
// efficient; nothing in this slice precludes it.
type Worker struct {
	pool    *pgxpool.Pool
	writer  *events.Writer
	budgets Budgets
	actor   *uuid.UUID
	clock   func() time.Time
}

// New constructs a Worker. `actor` is the system-source token id to attach
// to emitted events; nil is permitted and the events will record no
// actor_token_id (the source field still attributes to "system"). A nil
// `clock` defaults to time.Now; tests pass a fixed clock to make breach
// observation deterministic.
func New(pool *pgxpool.Pool, writer *events.Writer, budgets Budgets, actor *uuid.UUID, clock func() time.Time) (*Worker, error) {
	if pool == nil {
		return nil, errors.New("worker: pool is required")
	}
	if writer == nil {
		return nil, errors.New("worker: writer is required")
	}
	if err := budgets.validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	return &Worker{pool: pool, writer: writer, budgets: budgets, actor: actor, clock: clock}, nil
}

// ScanOnce runs one breach pass: read every non-terminal work_item with a
// budget once, evaluate each against EvaluateBreaches, and emit one
// patience.breached event per observed breach. Each emit lands in its own
// transaction so a per-row failure does not abort the whole pass.
//
// ScanOnce never transitions work_items; that is a separate concern owned
// by future slices. The breach event is the record; the transition (or
// retry, or escalation) is downstream.
func (w *Worker) ScanOnce(ctx context.Context) (Result, error) {
	now := w.clock().UTC()
	states := w.budgets.states()
	if len(states) == 0 {
		return Result{}, nil
	}

	rows, err := w.pool.Query(ctx, `
		SELECT id, state, updated_at
		FROM work_items
		WHERE state = ANY($1::text[])
		ORDER BY updated_at ASC
	`, states)
	if err != nil {
		return Result{}, fmt.Errorf("worker: query work_items: %w", err)
	}
	var candidates []Candidate
	for rows.Next() {
		var c Candidate
		var st string
		if err := rows.Scan(&c.ID, &st, &c.UpdatedAt); err != nil {
			rows.Close()
			return Result{}, fmt.Errorf("worker: scan work_items row: %w", err)
		}
		c.State = domain.WorkItemState(st)
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("worker: iterate work_items: %w", err)
	}

	out := Result{Scanned: len(candidates)}
	for _, breach := range EvaluateBreaches(now, candidates, w.budgets) {
		fresh, err := w.emitBreach(ctx, breach)
		if err != nil {
			return out, fmt.Errorf("worker: emit breach for %s: %w", breach.Candidate.ID, err)
		}
		if fresh {
			out.BreachesEmitted++
		} else {
			out.BreachesAlreadyRecorded++
		}
	}

	convergenceResult, err := w.scanConvergence(ctx)
	if err != nil {
		return out, fmt.Errorf("worker: convergence pass: %w", err)
	}
	out.ConvergenceCandidatesScanned = convergenceResult.ConvergenceCandidatesScanned
	out.ConvergenceVerdictsRecorded = convergenceResult.ConvergenceVerdictsRecorded
	out.ConvergenceVerdictsAlreadyRecorded = convergenceResult.ConvergenceVerdictsAlreadyRecorded
	out.ConvergenceAccepts = convergenceResult.ConvergenceAccepts
	out.ConvergenceRetries = convergenceResult.ConvergenceRetries
	out.ConvergenceEscalations = convergenceResult.ConvergenceEscalations

	return out, nil
}

// emitBreach appends one patience.breached event in its own transaction.
// Returns fresh=true when the event was newly inserted, fresh=false when
// the deterministic id already existed (the breach was on record from a
// prior scan with the same observation).
//
// Payload contents drive the deterministic event_id, so they are chosen to
// make "a new state-epoch is a new event" true and "a re-scan of the same
// epoch is a no-op" also true:
//
//   - state: the work_item state at observation. Re-entering the same state
//     after leaving it does not by itself create a fresh event_id.
//   - budget_seconds: the budget the breach was measured against. Tightening
//     the budget mid-flight causes a re-evaluation of the same observation
//     to emit a fresh event, which is desirable: a tighter budget is a new
//     judgment about the same dwell.
//   - state_entered_at_unix: the work_item's updated_at as a unix second.
//     Including this is what makes "left state X, came back in" emit a new
//     event: the new state-entry has a different updated_at and therefore
//     a different event_id, so the new breach is recorded distinctly. Unix
//     seconds (not RFC3339) keeps the canonical representation stable
//     across timezone-rendered serializations.
//
// Observed age is *not* in the payload: it is wall-clock-coupled to the
// scan moment, and including it would make every scan emit a fresh event
// even when nothing about the work_item has changed. Consumers that need
// the precise observed age should compute (event.occurred_at - state_entered_at).
func (w *Worker) emitBreach(ctx context.Context, b Breach) (bool, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, fresh, err := w.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    b.Candidate.ID,
		Kind:         domain.EventPatienceBreached,
		Source:       domain.SourceSystem,
		ActorTokenID: w.actor,
		Payload: map[string]any{
			"state":                 string(b.Candidate.State),
			"budget_seconds":        int64(b.Budget.Seconds()),
			"state_entered_at_unix": b.Candidate.UpdatedAt.Unix(),
		},
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}
