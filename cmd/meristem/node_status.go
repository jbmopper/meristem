// `meristem node status` is the read-only diagnostics half of the operator's
// node surface (work item 0b5d514b, parent db2d2408 "Boring Network"). It
// answers, without psql: what route plan would delivery walk to each node
// right now, what is sitting in this node's durable command queue and in
// which retry state, what failed last, and when expiry eligibility hits.
//
// Like `node list`, it connects straight to Postgres via
// MERISTEM_DATABASE_URL and only reads projections. Every section is read in
// one repeatable-read READ ONLY transaction, so the report is a coherent
// snapshot and the database itself refuses any write from this path. Route
// plans are computed by the same pure crossnode.Select the sender uses, from
// the registry snapshot with an empty cooldown map: sender cooldowns are
// process-local hints inside a delivering process and are not observable —
// or worth observing — from here.
//
//	meristem node status [--target <node-id>] [--json]
//
// Retry semantics, stated exactly: a pending command retries on the target's
// next drain tick only while attempts remain (of 5) and its 24h deadline has
// not passed. At 5/5 attempts the queue host refuses further execution and
// the row waits for the expiry worker; past its deadline the row is due for
// expiry on the worker's next pass. expires_at is an eligibility deadline,
// not the transition timestamp.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/nodes"
)

// redactedOrigin replaces any registry origin the current validation rules
// reject (userinfo, path, query, fragment, bad scheme). Legacy replay can
// retain such a value in the projection; the diagnostic must neither print it
// nor present it as a usable route.
const redactedOrigin = "invalid-origin(redacted)"

// nodeStatusReport is the --json shape. Field order mirrors the text output:
// routes first (intent), then queue truth, then origin-side observations,
// then the spoke's own bookmarks.
type nodeStatusReport struct {
	GeneratedAt  time.Time                     `json:"generated_at"`
	Routes       []routePlanReport             `json:"routes"`
	Queue        []crossnode.QueueTargetStatus `json:"queue"`
	Outcomes     []crossnode.OutcomeHostStatus `json:"outcomes"`
	SpokeCursors []crossnode.SpokeCursor       `json:"spoke_cursors"`
}

// routePlanReport is one target's current route plan: the ordered candidate
// walk crossnode.Select emits from the live registry snapshot, or the reason
// there is none.
type routePlanReport struct {
	TargetNodeID string   `json:"target_node_id"`
	Status       string   `json:"status,omitempty"`
	Plan         []string `json:"plan,omitempty"`
	Error        string   `json:"error,omitempty"`
}

func statusNodes(ctx context.Context, pool *pgxpool.Pool, w io.Writer, args []string, now time.Time) error {
	fs := flag.NewFlagSet("node status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	target := fs.String("target", "", "limit routes and queue to one node id")
	asJSON := fs.Bool("json", false, "emit one JSON report instead of text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("node status: unexpected argument %q", fs.Arg(0))
	}

	// One coherent snapshot: repeatable read pins a single MVCC view for all
	// sections; READ ONLY makes the database enforce that a diagnostic never
	// writes. The rollback is unconditional — there is nothing to commit.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("node status: begin read-only snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	registry, err := nodes.ListFrom(ctx, tx)
	if err != nil {
		return fmt.Errorf("node status: read registry: %w", err)
	}
	queue, err := crossnode.QueueStatus(ctx, tx, now)
	if err != nil {
		return fmt.Errorf("node status: read queue: %w", err)
	}
	outcomes, err := crossnode.OutcomeStatus(ctx, tx)
	if err != nil {
		return fmt.Errorf("node status: read outcomes: %w", err)
	}
	cursors, err := crossnode.SpokeCursors(ctx, tx)
	if err != nil {
		return fmt.Errorf("node status: read spoke cursors: %w", err)
	}
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		return fmt.Errorf("node status: close snapshot: %w", err)
	}

	report := nodeStatusReport{
		GeneratedAt:  now.UTC(),
		Routes:       buildRoutePlans(registry, now),
		Queue:        queue,
		Outcomes:     outcomes,
		SpokeCursors: cursors,
	}
	if *target != "" {
		report.Routes = filterRoutes(report.Routes, registry, *target, now)
		report.Queue = filterQueue(report.Queue, *target)
	}

	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	renderNodeStatus(w, report)
	return nil
}

// buildRoutePlans runs the pure selection rule once per registered node with
// an empty cooldown map: the plan a fresh sender would walk right now.
func buildRoutePlans(registry []domain.Node, now time.Time) []routePlanReport {
	plans := make([]routePlanReport, 0, len(registry))
	for _, n := range registry {
		plans = append(plans, routePlanFor(registry, n.NodeID, string(n.Status), now))
	}
	return plans
}

func routePlanFor(registry []domain.Node, target, status string, now time.Time) routePlanReport {
	p := routePlanReport{TargetNodeID: target, Status: status}
	candidates, err := crossnode.Select(registry, target, nil, now)
	if err != nil {
		p.Error = err.Error()
	}
	for _, c := range candidates {
		url := c.URL
		if crossnode.ValidateOrigin(url) != nil {
			url = redactedOrigin
		}
		switch c.Kind {
		case crossnode.KindDirect:
			p.Plan = append(p.Plan, "direct "+url)
		case crossnode.KindQueue:
			p.Plan = append(p.Plan, "queue via "+c.Via+" "+url)
		default:
			p.Plan = append(p.Plan, string(c.Kind)+" "+url)
		}
	}
	return p
}

// filterRoutes keeps the requested target's plan. A target absent from the
// registry still gets a report row carrying Select's refusal — an operator
// asking about a ghost node must see "unknown target", not an empty screen.
func filterRoutes(in []routePlanReport, registry []domain.Node, target string, now time.Time) []routePlanReport {
	for _, r := range in {
		if r.TargetNodeID == target {
			return []routePlanReport{r}
		}
	}
	return []routePlanReport{routePlanFor(registry, target, "", now)}
}

// filterQueue keeps the requested target's queue summary. Queue rows can
// exist for a target absent from the registry, so this filters the read
// result rather than consulting the registry.
func filterQueue(in []crossnode.QueueTargetStatus, target string) []crossnode.QueueTargetStatus {
	var out []crossnode.QueueTargetStatus
	for _, q := range in {
		if q.TargetNodeID == target {
			out = append(out, q)
		}
	}
	return out
}

func renderNodeStatus(w io.Writer, r nodeStatusReport) {
	fmt.Fprintf(w, "node status at %s\n", r.GeneratedAt.Format(time.RFC3339))

	fmt.Fprintf(w, "\nroutes (from the registry snapshot; sender cooldowns are process-local and not shown)\n")
	if len(r.Routes) == 0 {
		fmt.Fprintln(w, "  no nodes registered")
	}
	for _, p := range r.Routes {
		plan := strings.Join(p.Plan, "; ")
		if p.Error != "" {
			plan = "no plan: " + p.Error
		}
		status := p.Status
		if status == "" {
			status = "-"
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\n", p.TargetNodeID, status, plan)
	}

	fmt.Fprintf(w, "\nqueue (this node's durable command_queue)\n")
	if len(r.Queue) == 0 {
		fmt.Fprintln(w, "  queue empty: nothing pending or terminal")
	}
	for _, q := range r.Queue {
		fmt.Fprintf(w, "  %s\tpending=%d", q.TargetNodeID, q.Pending)
		if q.Pending > 0 {
			fmt.Fprintf(w, " (retryable=%d exhausted=%d due=%d)", q.PendingRetryable, q.PendingExhausted, q.PendingDue)
			fmt.Fprintf(w, "\tattempts=%d/%d", q.MaxAttempts, crossnode.MaxCommandAttempts)
			if q.OldestQueuedAt != nil {
				fmt.Fprintf(w, "\toldest=%s", q.OldestQueuedAt.Format(time.RFC3339))
			}
			if q.LastAttemptAt != nil {
				fmt.Fprintf(w, "\tlast_attempt=%s", q.LastAttemptAt.Format(time.RFC3339))
			}
			if q.NextExpiresAt != nil {
				fmt.Fprintf(w, "\texpiry_eligible=%s", q.NextExpiresAt.Format(time.RFC3339))
			}
		}
		fmt.Fprintf(w, "\tdone=%d refused=%d failed=%d expired=%d", q.Done, q.Refused, q.Failed, q.Expired)
		if t := q.LastTerminal; t != nil {
			fmt.Fprintf(w, "\tlast_terminal=%s", terminalBrief(t))
		}
		if f := q.LastFailure; f != nil {
			fmt.Fprintf(w, "\tlast_failure=%s", terminalBrief(f))
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "\noutcome reconciliation (origin-side view of queue hosts)\n")
	if len(r.Outcomes) == 0 {
		fmt.Fprintln(w, "  no queue-host cursors: sync-outcomes has not run here")
	}
	for _, o := range r.Outcomes {
		fmt.Fprintf(w, "  host=%s origin=%s cursor_seq=%d updated=%s observed=%d",
			o.QueueHostNodeID, o.OriginNodeID, o.CursorSeq, o.CursorUpdatedAt.Format(time.RFC3339), o.Observations)
		if lo := o.LastObserved; lo != nil {
			code := "-"
			if lo.StatusCode != nil {
				code = fmt.Sprintf("%d", *lo.StatusCode)
			}
			fmt.Fprintf(w, "\tlast=%s target=%s status=%s at=%s",
				lo.Outcome, lo.TargetNodeID, code, lo.RemoteOccurredAt.Format(time.RFC3339))
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "\nspoke cursors (empty on a hub: only pull-only nodes advance these)\n")
	if len(r.SpokeCursors) == 0 {
		fmt.Fprintln(w, "  none")
	}
	for _, c := range r.SpokeCursors {
		fmt.Fprintf(w, "  %s\t%s\tupdated=%s\n", c.Key, c.Value, c.UpdatedAt.Format(time.RFC3339))
	}
}

func terminalBrief(t *crossnode.TerminalOutcome) string {
	code := "-"
	if t.StatusCode != nil {
		code = fmt.Sprintf("%d", *t.StatusCode)
	}
	reason := t.Reason
	if reason == "" {
		reason = "-"
	}
	return fmt.Sprintf("%s status=%s reason=%s at=%s path=%s",
		t.State, code, reason, t.At.Format(time.RFC3339), t.CommandPath)
}
