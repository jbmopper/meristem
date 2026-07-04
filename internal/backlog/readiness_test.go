package backlog

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestSummarizeGroupsVisibleBacklog(t *testing.T) {
	asOf := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	items := []domain.WorkItem{
		workItem("R1: Self-defining convergence via scribe subtask", domain.WorkItemPlanned, asOf.Add(-2*time.Hour)),
		workItem("Backlog readiness projection for v1 substrate visibility", domain.WorkItemRunning, asOf.Add(-time.Hour)),
		workItem("Per-agent git worktrees for the meristem repo", domain.WorkItemBlocked, asOf.Add(-time.Hour)),
		workItem("Repeat create-item parent", domain.WorkItemCaptured, asOf.Add(-time.Hour)),
		workItem("Older unrelated idea", domain.WorkItemTriaged, asOf.Add(-31*24*time.Hour)),
		workItem("Fresh unrelated idea", domain.WorkItemTriaged, asOf.Add(-time.Hour)),
		workItem("Done item", domain.WorkItemDone, asOf.Add(-time.Hour)),
	}

	summary := Summarize(items, Options{Limit: 200, AsOf: asOf})

	if summary.Contract != Contract {
		t.Fatalf("contract = %q, want %q", summary.Contract, Contract)
	}
	if summary.Totals.Visible != len(items) {
		t.Fatalf("visible total = %d, want %d", summary.Totals.Visible, len(items))
	}
	assertTitles(t, "v1 substrate", summary.Groups.V1Substrate,
		"Backlog readiness projection for v1 substrate visibility",
		"R1: Self-defining convergence via scribe subtask",
	)
	assertTitles(t, "blockers", summary.Groups.Blockers, "Per-agent git worktrees for the meristem repo")
	assertTitles(t, "running", summary.Groups.Running, "Backlog readiness projection for v1 substrate visibility")
	assertTitles(t, "ready next", summary.Groups.ReadyNext,
		"R1: Self-defining convergence via scribe subtask",
		"Fresh unrelated idea",
	)
	assertTitles(t, "stale noise", summary.Groups.StaleNoise,
		"Older unrelated idea",
		"Repeat create-item parent",
	)
	if len(summary.SpecSeedDrift) != 8 {
		t.Fatalf("spec drift entries = %v, want R2-R9 missing", summary.SpecSeedDrift)
	}
}

func workItem(title string, state domain.WorkItemState, stateEnteredAt time.Time) domain.WorkItem {
	return domain.WorkItem{
		ID:                         uuid.NewSHA1(uuid.NameSpaceOID, []byte(title)),
		Title:                      title,
		State:                      state,
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		SuggestedConvergenceChecks: []string{"check"},
		StateEnteredAt:             stateEnteredAt,
		UpdatedAt:                  stateEnteredAt,
	}
}

func assertTitles(t *testing.T, name string, got []Item, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s titles = %v, want %v", name, titles(got), want)
	}
	for i, item := range got {
		if item.Title != want[i] {
			t.Fatalf("%s title[%d] = %q, want %q (all: %v)", name, i, item.Title, want[i], titles(got))
		}
	}
}

func titles(items []Item) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Title)
	}
	return out
}
