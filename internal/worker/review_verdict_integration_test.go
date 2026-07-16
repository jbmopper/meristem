package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestReviewVerdictPipelineClosesOnlyPassingVerdicts(t *testing.T) {
	tests := []struct {
		name            string
		verdict         workitems.ReviewVerdict
		wantDisposition domain.Verdict
		wantState       domain.WorkItemState
		wantPass        bool
	}{
		{
			name:            "accepted",
			verdict:         workitems.ReviewVerdictAccepted,
			wantDisposition: domain.VerdictAccept,
			wantState:       domain.WorkItemDone,
			wantPass:        true,
		},
		{
			name:            "accepted with finding",
			verdict:         workitems.ReviewVerdictAcceptedWithFinding,
			wantDisposition: domain.VerdictAccept,
			wantState:       domain.WorkItemDone,
			wantPass:        true,
		},
		{
			name:            "blocking finding",
			verdict:         workitems.ReviewVerdictBlockingFinding,
			wantDisposition: domain.VerdictReject,
			wantState:       domain.WorkItemRunning,
			wantPass:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationPool(t)
			if err := storage.Migrate(ctx, pool, nil); err != nil {
				t.Fatalf("migrate: %v", err)
			}

			writer := app.NewEventWriter()
			authService := auth.NewService(pool, writer)
			root, err := authService.CreateToken(ctx, auth.CreateTokenInput{
				Name:   "root",
				IsRoot: true,
				Source: domain.SourceHuman,
			})
			if err != nil {
				t.Fatalf("create root token: %v", err)
			}
			workerToken, err := authService.CreateToken(ctx, auth.CreateTokenInput{
				Name:   "review-verdict-worker",
				Source: domain.SourceSystem,
				Actor:  &root.Token,
			})
			if err != nil {
				t.Fatalf("create worker token: %v", err)
			}
			reviewerToken, err := authService.CreateToken(ctx, auth.CreateTokenInput{
				Name:   "review-verdict-agent",
				Source: domain.SourceAgent,
				Actor:  &root.Token,
			})
			if err != nil {
				t.Fatalf("create reviewer token: %v", err)
			}

			seedReviewerCultivar(t, ctx, pool, writer, workerToken.Token)
			service := workitems.NewService(pool, writer)
			parent, err := service.Create(ctx, workitems.CreateInput{
				Title:                      "implementation ready for " + tt.name,
				State:                      domain.WorkItemDone,
				SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
				HumanReviewStatus:          domain.HumanReviewWavedThrough,
				Actor:                      workerToken.Token,
			})
			if err != nil {
				t.Fatalf("create implementation parent: %v", err)
			}
			if err := service.AppendEvent(ctx, parent.ID, "coordination.implementation_ready", map[string]any{
				"commit": "review-verdict-test",
			}, workerToken.Token); err != nil {
				t.Fatalf("append implementation marker: %v", err)
			}

			w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &workerToken.Token.ID, nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			first, err := w.ScanOnce(ctx)
			if err != nil {
				t.Fatalf("ScanOnce dispatch: %v", err)
			}
			if first.ReviewDispatchJobsStarted != 1 {
				t.Fatalf("review dispatch jobs started = %d, want 1", first.ReviewDispatchJobsStarted)
			}
			childID := singleChildForParent(t, ctx, pool, parent.ID)
			child, err := service.Get(ctx, childID)
			if err != nil {
				t.Fatalf("get running review child: %v", err)
			}
			if child.State != domain.WorkItemRunning {
				t.Fatalf("review child state before verdict = %s, want running", child.State)
			}

			if err := service.AppendEvent(ctx, childID, workitems.ReviewVerdictInnerKind, map[string]any{
				"payload_version": 1,
				"verdict":         tt.verdict,
				"summary":         "bounded integration-test evidence",
			}, reviewerToken.Token); err != nil {
				t.Fatalf("append typed review verdict: %v", err)
			}
			var (
				verdictEventID uuid.UUID
				actorTokenID   uuid.UUID
				source         string
			)
			if err := pool.QueryRow(ctx, `
				SELECT id, actor_token_id, source
				FROM events
				WHERE subject_kind = $1
				  AND subject_id = $2
				  AND kind = $3
				  AND payload->>'inner_kind' = $4
				ORDER BY seq DESC
				LIMIT 1
			`, domain.SubjectWorkItem, childID, domain.EventWorkItemEventAppended, workitems.ReviewVerdictInnerKind).Scan(
				&verdictEventID, &actorTokenID, &source,
			); err != nil {
				t.Fatalf("read authoritative review verdict event: %v", err)
			}
			if actorTokenID != reviewerToken.Token.ID || source != string(domain.SourceAgent) {
				t.Fatalf("review verdict attribution = actor %s source %s, want actor %s source %s", actorTokenID, source, reviewerToken.Token.ID, domain.SourceAgent)
			}

			second, err := w.ScanOnce(ctx)
			if err != nil {
				t.Fatalf("ScanOnce verdict reconciliation: %v", err)
			}
			child, err = service.Get(ctx, childID)
			if err != nil {
				t.Fatalf("get reconciled review child: %v", err)
			}
			if child.State != tt.wantState {
				t.Fatalf("review child state after %s = %s, want %s", tt.verdict, child.State, tt.wantState)
			}
			if tt.wantDisposition == domain.VerdictAccept && second.ConvergenceAccepts != 1 {
				t.Fatalf("convergence accepts = %d, want 1", second.ConvergenceAccepts)
			}
			if tt.wantDisposition == domain.VerdictReject && second.ConvergenceRetries != 1 {
				t.Fatalf("convergence retries = %d, want 1", second.ConvergenceRetries)
			}

			var (
				disposition string
				rawSignals  []byte
			)
			if err := pool.QueryRow(ctx, `
				SELECT disposition, signals
				FROM convergence_verdicts
				WHERE work_item_id = $1
				ORDER BY attempt DESC, occurred_at DESC
				LIMIT 1
			`, childID).Scan(&disposition, &rawSignals); err != nil {
				t.Fatalf("read convergence verdict: %v", err)
			}
			if disposition != string(tt.wantDisposition) {
				t.Fatalf("convergence disposition = %s, want %s", disposition, tt.wantDisposition)
			}
			var signals []struct {
				Kind   string `json:"kind"`
				Pass   *bool  `json:"pass"`
				Source struct {
					EventID string `json:"event_id"`
				} `json:"source"`
			}
			if err := json.Unmarshal(rawSignals, &signals); err != nil {
				t.Fatalf("decode convergence signals: %v", err)
			}
			if len(signals) != 1 || signals[0].Kind != workitems.ReviewVerdictCheckKind || signals[0].Pass == nil || *signals[0].Pass != tt.wantPass {
				t.Fatalf("derived review signal = %+v, want one %s pass=%t", signals, workitems.ReviewVerdictCheckKind, tt.wantPass)
			}
			if signals[0].Source.EventID != verdictEventID.String() {
				t.Fatalf("derived signal event_id = %q, want %s", signals[0].Source.EventID, verdictEventID)
			}
		})
	}
}
