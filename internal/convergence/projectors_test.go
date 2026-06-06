package convergence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestVerdictRecordedProjector_DerivesProjectionRow(t *testing.T) {
	workItemID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	eventID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	actorID := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	occurredAt := time.Unix(1700000000, 0).UTC()
	ev := domain.Event{
		ID:           eventID,
		SubjectKind:  domain.SubjectConvergence,
		SubjectID:    workItemID,
		Kind:         domain.EventConvergenceVerdictRecorded,
		Source:       domain.SourceSystem,
		ActorTokenID: &actorID,
		Payload: map[string]any{
			"reducer_identity": "majority_vote",
			"reducer_version":  1,
			"attempt":          2,
			"inputs_digest":    strings.Repeat("a", 64),
			"reducer_config": map[string]any{
				"signal_kind": "grader.pass",
			},
			"verdict": map[string]any{
				"disposition": "accept",
				"reason":      "2/3 graders passed (majority)",
			},
			"signals": []any{
				map[string]any{"kind": "grader.pass", "pass": true},
				map[string]any{"kind": "grader.pass", "pass": false},
				map[string]any{"kind": "grader.pass", "pass": true},
			},
		},
		OccurredAt: occurredAt,
	}
	tx := &captureTx{}

	if err := (verdictRecordedProjector{}).Apply(context.Background(), tx, ev); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if tx.execs != 1 {
		t.Fatalf("Exec calls = %d, want 1", tx.execs)
	}
	args := tx.args
	assertArg(t, args, 0, eventID)
	assertArg(t, args, 1, workItemID)
	assertArg(t, args, 2, "majority_vote")
	assertArg(t, args, 3, 1)
	assertArg(t, args, 4, 2)
	assertArg(t, args, 5, strings.Repeat("a", 64))
	assertArg(t, args, 6, "accept")
	assertArg(t, args, 7, "2/3 graders passed (majority)")
	if config, ok := args[8].([]byte); !ok || !strings.Contains(string(config), `"signal_kind":"grader.pass"`) {
		t.Fatalf("reducer_config arg = %#v, want JSON containing signal_kind", args[8])
	}
	if signals, ok := args[9].([]byte); !ok || !strings.Contains(string(signals), `"kind":"grader.pass"`) {
		t.Fatalf("signals arg = %#v, want JSON containing grader.pass", args[9])
	}
	assertArg(t, args, 10, &actorID)
	assertArg(t, args, 11, "system")
	assertArg(t, args, 12, occurredAt)
}

func TestVerdictRecordedProjector_ValidatesPayload(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{"reducer identity", func(p map[string]any) { delete(p, "reducer_identity") }, "reducer_identity is required"},
		{"reducer version", func(p map[string]any) { p["reducer_version"] = 0 }, "reducer_version must be >= 1"},
		{"attempt", func(p map[string]any) { p["attempt"] = 0 }, "attempt must be >= 1"},
		{"digest length", func(p map[string]any) { p["inputs_digest"] = "abc" }, "inputs_digest must be 64 hex characters"},
		{"digest hex", func(p map[string]any) { p["inputs_digest"] = strings.Repeat("z", 64) }, "inputs_digest must be hex"},
		{"disposition", func(p map[string]any) { p["verdict"] = map[string]any{"disposition": "maybe", "reason": "x"} }, "invalid disposition"},
		{"reason", func(p map[string]any) { p["verdict"] = map[string]any{"disposition": "accept"} }, "verdict.reason is required"},
		{"signals", func(p map[string]any) { p["signals"] = map[string]any{"kind": "grader.pass"} }, "signals must be a JSON array"},
		{"reducer config", func(p map[string]any) { p["reducer_config"] = []any{"not", "object"} }, "reducer_config must be a JSON object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := validVerdictPayload()
			tc.mutate(payload)
			ev := validVerdictEvent()
			ev.Payload = payload
			err := (verdictRecordedProjector{}).Apply(context.Background(), nil, ev)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %q error, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestVerdictRecordedProjector_RejectsWrongSubjectKind(t *testing.T) {
	ev := validVerdictEvent()
	ev.SubjectKind = domain.SubjectWorkItem
	err := (verdictRecordedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "expected subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}
}

func validVerdictEvent() domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectConvergence,
		SubjectID:   uuid.New(),
		Kind:        domain.EventConvergenceVerdictRecorded,
		Source:      domain.SourceSystem,
		Payload:     validVerdictPayload(),
		OccurredAt:  time.Unix(0, 0),
	}
}

func validVerdictPayload() map[string]any {
	return map[string]any{
		"reducer_identity": "all_pass_checklist",
		"reducer_version":  1,
		"attempt":          1,
		"inputs_digest":    strings.Repeat("0", 64),
		"verdict": map[string]any{
			"disposition": "accept",
			"reason":      "all required checks passed",
		},
		"signals": []any{},
	}
}

func assertArg[T comparable](t *testing.T, args []any, idx int, want T) {
	t.Helper()
	if len(args) <= idx {
		t.Fatalf("missing arg %d in %#v", idx, args)
	}
	got, ok := args[idx].(T)
	if !ok || got != want {
		t.Fatalf("arg %d = %#v, want %#v", idx, args[idx], want)
	}
}

type captureTx struct {
	pgx.Tx
	execs int
	args  []any
}

func (tx *captureTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	tx.execs++
	tx.args = append([]any(nil), args...)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *captureTx) Commit(context.Context) error   { return errors.New("unexpected Commit") }
func (tx *captureTx) Rollback(context.Context) error { return errors.New("unexpected Rollback") }
