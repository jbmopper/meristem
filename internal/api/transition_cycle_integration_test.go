package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestTransitionCycleWithRepeatedPayloadIsNotSwallowed encodes the live
// repro from the 2026-07-03 self-review: a work item cycling
// running → blocked → running → blocked with the *same* reason used to
// collapse the second blocked transition into the first one's event id
// (payload-only identity), so the API answered 200 while the projection
// silently stayed running. The idempotency-derived event discriminator
// must keep the two blocked transitions distinct.
func TestTransitionCycleWithRepeatedPayloadIsNotSwallowed(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "transition-cycle",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)

	created := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items", tokenResult.Secret, "cycle-create",
		[]byte(`{"title":"transition cycle regression"}`))
	if created.Code != http.StatusCreated {
		t.Fatalf("create work item: want 201, got %d body=%s", created.Code, created.Body.String())
	}
	var createdResp struct {
		WorkItem struct {
			ID string `json:"id"`
		} `json:"work_item"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdResp); err != nil {
		t.Fatalf("decode create response: %v body=%s", err, created.Body.String())
	}
	id := createdResp.WorkItem.ID
	transitionPath := "/v1/work-items/" + id + "/transition"

	steps := []struct {
		key  string
		body string
		want domain.WorkItemState
	}{
		{"cycle-1", `{"to":"running","reason":"start"}`, domain.WorkItemRunning},
		{"cycle-2", `{"to":"blocked","reason":"same-reason"}`, domain.WorkItemBlocked},
		{"cycle-3", `{"to":"running","reason":"resume"}`, domain.WorkItemRunning},
		// The critical step: identical payload to cycle-2 on the same
		// subject, but a distinct action. Pre-fix this returned 200
		// with the item still running.
		{"cycle-4", `{"to":"blocked","reason":"same-reason"}`, domain.WorkItemBlocked},
	}
	for _, step := range steps {
		rec := doREST(t, server.Handler(), http.MethodPost, transitionPath, tokenResult.Secret, step.key, []byte(step.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("transition %s: want 200, got %d body=%s", step.key, rec.Code, rec.Body.String())
		}
		var resp struct {
			WorkItem struct {
				State domain.WorkItemState `json:"state"`
			} `json:"work_item"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode transition %s: %v body=%s", step.key, err, rec.Body.String())
		}
		if resp.WorkItem.State != step.want {
			t.Fatalf("transition %s: want state %s, got %s", step.key, step.want, resp.WorkItem.State)
		}
	}

	var transitions int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE subject_id = $1 AND kind = $2
	`, id, domain.EventWorkItemTransitioned).Scan(&transitions); err != nil {
		t.Fatalf("count transition events: %v", err)
	}
	if transitions != len(steps) {
		t.Fatalf("want %d transition events, got %d (repeated payload collapsed)", len(steps), transitions)
	}

	// A retry of the same action (same idempotency key and body) must still
	// collapse: replayed response, no fifth event.
	retry := doREST(t, server.Handler(), http.MethodPost, transitionPath, tokenResult.Secret, "cycle-4", []byte(steps[3].body))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry: want 200, got %d body=%s", retry.Code, retry.Body.String())
	}
	if retry.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("retry should be served from the idempotency cache, headers=%v", retry.Header())
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE subject_id = $1 AND kind = $2
	`, id, domain.EventWorkItemTransitioned).Scan(&transitions); err != nil {
		t.Fatalf("recount transition events: %v", err)
	}
	if transitions != len(steps) {
		t.Fatalf("retry appended a new event: want %d, got %d", len(steps), transitions)
	}
}
