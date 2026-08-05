package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/storage"
)

// runStatus is the non-mutating runtime evidence probe (listener control
// plane, slice 0): one command that distinguishes configuration/restart
// issues from missing code. It reports the compiled commit and build-guard
// state, database reachability and head seq, and — for --work-item — the
// projected assignment generation. It never reads token secret material and
// appends nothing.
//
// Deliberately NOT in commandUsesCoordinationState: like healthcheck, status
// must keep working on a stale build — reporting that staleness is its job.
// Listener registration, policy revision, effective mode, cursor identity,
// and last activation outcome attach to this same report as their
// projections land in later slices.
func runStatus(ctx context.Context, _ *slog.Logger, args []string, build buildguard.StatusProvider) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workItemFlag := fs.String("work-item", "", "optional work item uuid: include its projected assignment state")
	jsonFlag := fs.Bool("json", false, "emit a single JSON object instead of text lines")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("status: %w", err)
	}

	report := map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}
	guard := build.Status()
	report["build"] = map[string]any{
		"protocol":        buildGuardProtocol,
		"compiled_commit": guard.CompiledCommit,
		"state":           string(guard.State),
		"current":         guard.Current(),
		"blocking":        guard.Blocking(),
		"warning":         guard.Warning(),
	}

	dbReport := map[string]any{"reachable": false}
	report["database"] = dbReport
	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		dbReport["error"] = err.Error()
		return emitStatus(os.Stdout, report, *jsonFlag)
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		dbReport["error"] = err.Error()
		return emitStatus(os.Stdout, report, *jsonFlag)
	}
	defer pool.Close()

	var headSeq *int64
	if err := pool.QueryRow(ctx, `SELECT max(seq) FROM events`).Scan(&headSeq); err != nil {
		dbReport["error"] = fmt.Sprintf("read head seq: %v", err)
		return emitStatus(os.Stdout, report, *jsonFlag)
	}
	dbReport["reachable"] = true
	if headSeq != nil {
		dbReport["head_seq"] = *headSeq
	}

	if *workItemFlag != "" {
		id, err := uuid.Parse(*workItemFlag)
		if err != nil {
			return fmt.Errorf("status: --work-item must be a uuid: %w", err)
		}
		assignment, err := readAssignmentStatus(ctx, pool, id)
		if err != nil {
			return err
		}
		report["assignment"] = assignment
	}

	return emitStatus(os.Stdout, report, *jsonFlag)
}

// readAssignmentStatus reads the projection row directly rather than going
// through the API so the probe stays useful when the API process is the
// thing being diagnosed.
func readAssignmentStatus(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID) (map[string]any, error) {
	var (
		holder, assignmentEvent *uuid.UUID
		mode                    *string
		claimedAt, expiresAt    *time.Time
		stateEventID            uuid.UUID
		stateEventSeq           int64
	)
	err := q.QueryRow(ctx, `
		SELECT holder_token_id, mode, assignment_event_id, claimed_at, expires_at,
		       state_event_id, state_event_seq
		FROM work_item_assignment_state
		WHERE work_item_id = $1`, id).
		Scan(&holder, &mode, &assignmentEvent, &claimedAt, &expiresAt, &stateEventID, &stateEventSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[string]any{"work_item_id": id.String(), "state": "no assignment-state row"}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("status: read assignment state for %s: %w", id, err)
	}
	out := map[string]any{
		"work_item_id":    id.String(),
		"state_event_id":  stateEventID.String(),
		"state_event_seq": stateEventSeq,
	}
	if holder == nil {
		out["state"] = "unassigned"
		return out, nil
	}
	out["state"] = "assigned"
	out["holder_token_id"] = holder.String()
	if mode != nil {
		out["mode"] = *mode
	}
	if assignmentEvent != nil {
		out["assignment_event_id"] = assignmentEvent.String()
	}
	if claimedAt != nil {
		out["claimed_at"] = claimedAt.UTC().Format(time.RFC3339Nano)
	}
	if expiresAt != nil {
		out["expires_at"] = expiresAt.UTC().Format(time.RFC3339Nano)
	}
	return out, nil
}

func emitStatus(w io.Writer, report map[string]any, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	blob, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(blob))
	return err
}
