package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/worker"
	"github.com/jbmopper/meristem/internal/workitems"
)

const testScribeCultivar = "convergence-scribe@1"

func TestScribeProposalFlowIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "scribe-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	systemResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "scribe-worker",
		Source: domain.SourceSystem,
		Actor:  &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedAPIScribeCultivar(t, ctx, pool, writer, systemResult.Token)
	server := New(pool, nil)

	created := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items", rootResult.Secret, "scribe-parent", []byte(`{"title":"checkless parent"}`))
	assertRESTStatus(t, created, http.StatusCreated)
	var createdResp struct {
		WorkItem struct {
			ID uuid.UUID `json:"id"`
		} `json:"work_item"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdResp); err != nil {
		t.Fatalf("decode create: %v body=%s", err, created.Body.String())
	}
	parentID := createdResp.WorkItem.ID

	w, err := worker.New(pool, writer, worker.Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemResult.Token.ID, nil)
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}
	first, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	if first.ScribeChildrenSpawned != 1 {
		t.Fatalf("scribe children spawned = %d, want 1", first.ScribeChildrenSpawned)
	}
	childID := convergence.ScribeChildID(parentID)

	blocked := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parentID.String()+"/transition", rootResult.Secret, "scribe-parent-blocked", []byte(`{"to":"planned","reason":"too soon"}`))
	assertRESTStatus(t, blocked, http.StatusConflict)
	assertErrorCode(t, blocked, "convergence_checks_required")

	body := mustJSON(t, map[string]any{
		"proposal_of": childID.String(),
		"checks":      []string{"cmd:go test ./...", "human-ack:operator approval"},
		"classified": []map[string]string{
			{"check": "cmd:go test ./...", "class": "machine"},
			{"check": "human-ack:operator approval", "class": "human"},
		},
		"rationale": "tests plus owner acknowledgement cover this parent",
		"cultivar":  testScribeCultivar,
	})
	proposed := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parentID.String()+"/convergence-proposal", rootResult.Secret, "scribe-valid-proposal", body)
	assertRESTStatus(t, proposed, http.StatusCreated)
	var proposedResp struct {
		Applied bool `json:"applied"`
		Verdict struct {
			Disposition domain.Verdict `json:"disposition"`
			Reason      string         `json:"reason"`
		} `json:"verdict"`
	}
	if err := json.Unmarshal(proposed.Body.Bytes(), &proposedResp); err != nil {
		t.Fatalf("decode proposal: %v body=%s", err, proposed.Body.String())
	}
	if !proposedResp.Applied || proposedResp.Verdict.Disposition != domain.VerdictAccept {
		t.Fatalf("proposal response = %+v body=%s", proposedResp, proposed.Body.String())
	}

	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Get(ctx, parentID)
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if len(parent.SuggestedConvergenceChecks) != 2 {
		t.Fatalf("parent checks = %v, want 2", parent.SuggestedConvergenceChecks)
	}
	child, err := workSvc.Get(ctx, childID)
	if err != nil {
		t.Fatalf("get child: %v", err)
	}
	if child.State != domain.WorkItemDone {
		t.Fatalf("child state = %s, want done", child.State)
	}

	advanced := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parentID.String()+"/transition", rootResult.Secret, "scribe-parent-advance", []byte(`{"to":"planned","reason":"checks defined"}`))
	assertRESTStatus(t, advanced, http.StatusOK)
}

func TestScribeProposalInvalidInputsRecordRejectVerdicts(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "scribe-invalid-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	systemResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "scribe-invalid-worker",
		Source: domain.SourceSystem,
		Actor:  &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	server := New(pool, nil)
	workSvc := workitems.NewService(pool, writer)

	cases := []struct {
		name       string
		checks     []string
		classified []map[string]string
		wantReason string
	}{
		{name: "empty", checks: nil, classified: nil, wantReason: "empty_checks"},
		{name: "blank", checks: []string{"cmd:go test", " "}, classified: []map[string]string{{"check": "cmd:go test", "class": "machine"}}, wantReason: "blank_check:1"},
		{name: "unprefixed", checks: []string{"read the code"}, classified: []map[string]string{{"check": "read the code", "class": "machine"}}, wantReason: "unclassified_check:read the code"},
		{name: "unknown-query", checks: []string{"query:not_registered"}, classified: []map[string]string{{"check": "query:not_registered", "class": "machine"}}, wantReason: "unknown_query_check:not_registered"},
		{name: "duplicate", checks: []string{"cmd:go test", "cmd:go test"}, classified: []map[string]string{{"check": "cmd:go test", "class": "machine"}}, wantReason: "duplicate_check:cmd:go test"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parentID, childID := createParentAndScribeChild(t, ctx, workSvc, systemResult.Token, tc.name)
			body := mustJSON(t, map[string]any{
				"proposal_of": childID.String(),
				"checks":      tc.checks,
				"classified":  tc.classified,
				"rationale":   "invalid case",
				"cultivar":    testScribeCultivar,
			})
			rec := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parentID.String()+"/convergence-proposal", rootResult.Secret, "scribe-invalid-"+tc.name, body)
			assertRESTStatus(t, rec, http.StatusCreated)
			var resp struct {
				Applied bool `json:"applied"`
				Verdict struct {
					Disposition domain.Verdict `json:"disposition"`
					Reason      string         `json:"reason"`
				} `json:"verdict"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode proposal: %v body=%s", err, rec.Body.String())
			}
			if resp.Applied {
				t.Fatalf("invalid proposal applied: %s", rec.Body.String())
			}
			if resp.Verdict.Disposition != domain.VerdictReject || resp.Verdict.Reason != tc.wantReason {
				t.Fatalf("verdict = %+v, want reject/%s body=%s", resp.Verdict, tc.wantReason, rec.Body.String())
			}
			parent, err := workSvc.Get(ctx, parentID)
			if err != nil {
				t.Fatalf("get parent: %v", err)
			}
			if len(parent.SuggestedConvergenceChecks) != 0 {
				t.Fatalf("invalid proposal mutated parent checks: %v", parent.SuggestedConvergenceChecks)
			}
		})
	}
}

func createParentAndScribeChild(t *testing.T, ctx context.Context, service *workitems.Service, actor domain.Token, name string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	parent, err := service.Create(ctx, workitems.CreateInput{
		Title:             "invalid proposal parent " + name,
		State:             domain.WorkItemCaptured,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             actor,
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	childID := convergence.ScribeChildID(parent.ID)
	if _, _, err := service.SpawnChildWithID(ctx, parent.ID, childID, workitems.CreateInput{
		Title:                      "Define convergence for: invalid proposal parent " + name,
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{convergence.ScribeChildCheck},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   testScribeCultivar,
		Actor:                      actor,
	}); err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	return parent.ID, childID
}

func seedAPIScribeCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "checklist-all",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      json.RawMessage(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "all checklist items pass",
	})
	if err != nil {
		t.Fatalf("define scribe tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "convergence-scribe",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/convergence-scribe.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: 1800, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "scribe rootstock",
	})
	if err != nil {
		t.Fatalf("define scribe cultivar: %v", err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return out
}
