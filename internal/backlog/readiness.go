// Package backlog derives operator-facing backlog summaries from projections.
package backlog

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

const Contract = "backlog.readiness.v1"

var refreshItemRE = regexp.MustCompile(`^R([1-9])(?::| remainder:)`)

type Options struct {
	Limit int
	AsOf  time.Time
}

type Summary struct {
	Contract            string                       `json:"contract"`
	Source              string                       `json:"source"`
	Limit               int                          `json:"limit"`
	AsOf                time.Time                    `json:"as_of"`
	Totals              Totals                       `json:"totals"`
	StateCounts         map[domain.WorkItemState]int `json:"state_counts"`
	Groups              Groups                       `json:"groups"`
	SpecSeedDrift       []string                     `json:"spec_seed_drift"`
	ClassificationRules []string                     `json:"classification_rules"`
}

type Totals struct {
	Visible     int `json:"visible"`
	Terminal    int `json:"terminal"`
	NonTerminal int `json:"non_terminal"`
}

type Groups struct {
	V1Substrate []Item `json:"v1_substrate"`
	ReadyNext   []Item `json:"ready_next"`
	Blockers    []Item `json:"blockers"`
	Running     []Item `json:"running"`
	StaleNoise  []Item `json:"stale_noise"`
}

type Item struct {
	ID                         uuid.UUID                `json:"id"`
	Title                      string                   `json:"title"`
	State                      domain.WorkItemState     `json:"state"`
	StateReason                *string                  `json:"state_reason,omitempty"`
	HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks,omitempty"`
	StateEnteredAt             time.Time                `json:"state_entered_at"`
	UpdatedAt                  time.Time                `json:"updated_at"`
	Tags                       []string                 `json:"tags,omitempty"`
}

func Summarize(items []domain.WorkItem, opts Options) Summary {
	asOf := opts.AsOf
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	stateCounts := make(map[domain.WorkItemState]int)
	summary := Summary{
		Contract:    Contract,
		Source:      "events -> work_items projection; filtered by existing access reducer before classification",
		Limit:       opts.Limit,
		AsOf:        asOf,
		StateCounts: stateCounts,
		Groups: Groups{
			V1Substrate: []Item{},
			ReadyNext:   []Item{},
			Blockers:    []Item{},
			Running:     []Item{},
			StaleNoise:  []Item{},
		},
		SpecSeedDrift: []string{},
		ClassificationRules: []string{
			"v1_substrate: refresh R1-R9 items, R3 remainder, refresh parent, token/MCP/spec/backlog substrate titles",
			"ready_next: visible captured/triaged/planned items that are not stale/noise",
			"blockers: visible blocked or awaiting_approval items",
			"running: visible running items",
			"stale_noise: terminal failed/canceled items, demo/test titles, or stale non-terminal epochs outside the declared thresholds",
		},
	}

	seenRefresh := make(map[string]bool)
	for _, item := range items {
		summary.Totals.Visible++
		stateCounts[item.State]++
		if item.State.Terminal() {
			summary.Totals.Terminal++
		} else {
			summary.Totals.NonTerminal++
		}

		tags := classifyTags(item, asOf)
		out := toItem(item, tags)
		if n, ok := refreshNumber(item.Title); ok {
			seenRefresh[n] = true
		}
		if hasTag(tags, "v1_substrate") {
			summary.Groups.V1Substrate = append(summary.Groups.V1Substrate, out)
		}
		if hasTag(tags, "blocked") {
			summary.Groups.Blockers = append(summary.Groups.Blockers, out)
		}
		if hasTag(tags, "running") {
			summary.Groups.Running = append(summary.Groups.Running, out)
		}
		if hasTag(tags, "ready_next") {
			summary.Groups.ReadyNext = append(summary.Groups.ReadyNext, out)
		}
		if hasTag(tags, "stale_noise") {
			summary.Groups.StaleNoise = append(summary.Groups.StaleNoise, out)
		}
	}

	// Only a *partial* refresh backlog is drift: if the DB carries at least
	// one R1-R9 item, every missing sibling is suspicious. A fresh seed with
	// zero refresh items is simply not a refresh-tracking DB — the R1-R9
	// refresh is a completed one-time initiative (parent c6ba707b), so a new
	// bring-up raises no drift rather than nine false positives.
	seenAnyRefresh := false
	for i := 1; i <= 9; i++ {
		if seenRefresh[fmt.Sprintf("R%d", i)] {
			seenAnyRefresh = true
			break
		}
	}
	if seenAnyRefresh {
		for i := 1; i <= 9; i++ {
			key := fmt.Sprintf("R%d", i)
			if !seenRefresh[key] {
				summary.SpecSeedDrift = append(summary.SpecSeedDrift, "missing_refresh_item:"+key)
			}
		}
	}
	sortItems(summary.Groups.V1Substrate)
	sortItems(summary.Groups.ReadyNext)
	sortItems(summary.Groups.Blockers)
	sortItems(summary.Groups.Running)
	sortItems(summary.Groups.StaleNoise)
	sort.Strings(summary.SpecSeedDrift)
	return summary
}

func classifyTags(item domain.WorkItem, asOf time.Time) []string {
	var tags []string
	if isV1Substrate(item.Title) {
		tags = append(tags, "v1_substrate")
	}
	if item.State == domain.WorkItemBlocked || item.State == domain.WorkItemAwaitingApproval {
		tags = append(tags, "blocked")
	}
	if item.State == domain.WorkItemRunning {
		tags = append(tags, "running")
	}
	if isStaleNoise(item, asOf) {
		tags = append(tags, "stale_noise")
	}
	if isReadyNext(item.State) && !hasTag(tags, "stale_noise") {
		tags = append(tags, "ready_next")
	}
	return tags
}

func isReadyNext(state domain.WorkItemState) bool {
	switch state {
	case domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemPlanned:
		return true
	default:
		return false
	}
}

func isV1Substrate(title string) bool {
	if strings.HasPrefix(title, "Refresh: disciplined spin-up") {
		return true
	}
	if _, ok := refreshNumber(title); ok {
		return true
	}
	for _, prefix := range []string{
		"Token model:",
		"MCP/spec parity:",
		"Backlog readiness projection",
		"Self-building gate",
		"First slice:",
		"Roadmap extraction:",
	} {
		if strings.HasPrefix(title, prefix) {
			return true
		}
	}
	return false
}

func refreshNumber(title string) (string, bool) {
	matches := refreshItemRE.FindStringSubmatch(title)
	if len(matches) != 2 {
		return "", false
	}
	return "R" + matches[1], true
}

func isStaleNoise(item domain.WorkItem, asOf time.Time) bool {
	if item.State == domain.WorkItemFailed || item.State == domain.WorkItemCanceled {
		return true
	}
	title := strings.ToLower(item.Title)
	for _, marker := range []string{
		"repeat create-item",
		"create an item",
		"decision engine decision",
		"demo",
		"scratch",
	} {
		if strings.Contains(title, marker) {
			return true
		}
	}
	age := asOf.Sub(item.StateEnteredAt)
	switch item.State {
	case domain.WorkItemRunning:
		return age > 24*time.Hour
	case domain.WorkItemBlocked, domain.WorkItemAwaitingApproval:
		return age > 7*24*time.Hour
	case domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemPlanned:
		return age > 30*24*time.Hour && !isV1Substrate(item.Title)
	default:
		return false
	}
}

func toItem(item domain.WorkItem, tags []string) Item {
	return Item{
		ID:                         item.ID,
		Title:                      item.Title,
		State:                      item.State,
		StateReason:                item.StateReason,
		HumanReviewStatus:          item.HumanReviewStatus,
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		StateEnteredAt:             item.StateEnteredAt,
		UpdatedAt:                  item.UpdatedAt,
		Tags:                       tags,
	}
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func sortItems(items []Item) {
	sort.SliceStable(items, func(i, j int) bool {
		pi, pj := statePriority(items[i].State), statePriority(items[j].State)
		if pi != pj {
			return pi < pj
		}
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return items[i].ID.String() < items[j].ID.String()
	})
}

func statePriority(state domain.WorkItemState) int {
	switch state {
	case domain.WorkItemBlocked, domain.WorkItemAwaitingApproval:
		return 0
	case domain.WorkItemRunning:
		return 1
	case domain.WorkItemPlanned:
		return 2
	case domain.WorkItemTriaged:
		return 3
	case domain.WorkItemCaptured:
		return 4
	case domain.WorkItemFailed, domain.WorkItemCanceled:
		return 5
	case domain.WorkItemDone:
		return 6
	default:
		return 9
	}
}
