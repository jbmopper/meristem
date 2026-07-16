// Package dogma keeps AGENTS.md and the code honest with each other. The
// conformance test maps Techniques bullets to checks; this file projects
// role-scoped briefings for the rootstock cultivars. A briefing is the
// distillation a leaf worker actually holds in context: its tools, its
// tropism, its budgets, its refusal semantics — never the full catechism.
// Initiation sized to the pledge (refresh-requirements R9).
package dogma

import (
	"fmt"
	"os"
	"strings"
)

// AgentsPath is the dogma source of truth the briefings project from.
const AgentsPath = "AGENTS.md"

// MaxBriefingLines is the R9 ceiling: a briefing a small model can hold.
const MaxBriefingLines = 40

// sourceSections are the AGENTS.md headings each briefing distills. The
// guard test fails if any disappears from AGENTS.md, so a section rename
// forces briefing regeneration instead of silent drift.
var sourceSections = []string{
	"## Principles",
	"## Techniques (load-bearing, but not philosophy)",
	"## Coordination with other agents",
	"## Things not to do",
}

// rootstockBriefing mirrors the registry seed fixture values a worker needs
// in-context. cmd/meristem/seed_registry.go owns the durable fixture; the
// guard test cross-checks the briefing paths against those declarations.
type rootstockBriefing struct {
	Cultivar string
	Role     string
	Tropism  string
	Budget   string
	Task     []string
}

var rootstockBriefings = []rootstockBriefing{
	{
		Cultivar: "convergence-scribe@1",
		Role:     "You define convergence checks for a work item that has none.",
		Tropism:  "checklist-all@1 (all declared checks must pass)",
		Budget:   "3 attempts, 30 minutes wall, depth 1; exhaustion escalates to the owner",
		Task: []string{
			"Read the parent item named in your child item's body.",
			"Propose checks via convergence.propose_checks with an idempotency_key.",
			"Every check needs an explicit class prefix: cmd:, event:, query:, or human-ack:.",
			"Unprefixed prose is refused (unclassified_check). No duplicates. At least one entry.",
			"If checks already exist, stop: the reducer records checks_already_defined and you are done.",
		},
	},
	{
		Cultivar: "human-attention@1",
		Role:     "You carry a question to the owner and wait for their answer.",
		Tropism:  "human-ack@1 (verdict follows an explicit owner decision event)",
		Budget:   "1 attempt, 7 days wall, depth 0; you spawn nothing",
		Task: []string{
			"Present the escalation reason and origin state from the item body to the owner.",
			"Record the owner's decision as an event on the item; never decide for them.",
			"Your item is done when human_response_recorded is satisfied — not before.",
		},
	},
	{
		Cultivar: "checklist-worker@1",
		Role:     "You execute a work item's declared convergence checks.",
		Tropism:  "checklist-all@1 (all declared checks must pass)",
		Budget:   "3 attempts, 60 minutes wall, depth 1; exhaustion escalates to the owner",
		Task: []string{
			"Run each cmd:/query: check exactly as written; report event: checks by appending them.",
			"Append checks_passed/checks_failed evidence events; the reducer issues the verdict, not you.",
			"Never transition the item to done yourself on judgment — the verdict machinery does that.",
		},
	},
	{
		Cultivar: "reviewer@1",
		Role:     "You independently review an implementation another actor landed.",
		Tropism:  "checklist-all@1 (all declared checks must pass)",
		Budget:   "2 attempts, 60 minutes wall, depth 1; exhaustion escalates to the owner",
		Task: []string{
			"Run the full suite at the exact commit under review; refuse stale or dirty trees.",
			"Review against the parent item's checks and cited spec, not against taste.",
			"Never review your own work; if the implementation attribution matches your token, stand down.",
			"File severity-labeled finding children for defects with cmd:/event:/query:/human-ack: checks.",
			"Append one typed review.verdict_recorded verdict (accepted, accepted_with_finding, or blocking_finding); the worker derives its checklist signal.",
		},
	},
}

// sharedRules are the imperative floor every briefing carries, distilled
// from the source sections above.
var sharedRules = []string{
	"Read the feed by cursor, never by wall-clock timestamp.",
	"Every mutation needs an idempotency_key; reuse the same key only for retries of the same action.",
	"Structured refusals (insufficient_scope, unknown_cultivar, *_budget*) are answers, not obstacles: report them, never work around them.",
	"Coordinate only through work_items, events, and the feed. docs/coord/ is outage fallback only; replay it when the substrate returns.",
	"Append evidence as events with full honesty; your token attributes everything you do.",
	"If your budget or scope does not cover the next step, escalate — do not improvise authority.",
}

// GenerateBriefing renders the deterministic briefing for a rootstock
// cultivar. The output is what lands in docs/briefings/<name>.md; the guard
// test keeps the committed artifact byte-identical to this projection and
// under MaxBriefingLines.
func GenerateBriefing(cultivar string) (string, error) {
	for _, b := range rootstockBriefings {
		if strings.HasPrefix(b.Cultivar, cultivar+"@") || b.Cultivar == cultivar {
			return renderBriefing(b), nil
		}
	}
	return "", fmt.Errorf("dogma: no rootstock briefing for cultivar %q", cultivar)
}

func renderBriefing(b rootstockBriefing) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Briefing: %s\n\n", b.Cultivar)
	fmt.Fprintf(&sb, "%s\n\n", b.Role)
	fmt.Fprintf(&sb, "- Tropism: %s\n", b.Tropism)
	fmt.Fprintf(&sb, "- Budget: %s\n\n", b.Budget)
	sb.WriteString("## Your task\n\n")
	for _, t := range b.Task {
		fmt.Fprintf(&sb, "1. %s\n", t)
	}
	sb.WriteString("\n## Rules (non-negotiable)\n\n")
	for _, r := range sharedRules {
		fmt.Fprintf(&sb, "- %s\n", r)
	}
	sb.WriteString("\nProjection of AGENTS.md (Principles; Techniques; Coordination with other\nagents; Things not to do). Regenerate via internal/dogma; do not hand-edit.\n")
	return sb.String()
}

// VerifySourceSections confirms every heading the briefings cite still
// exists in AGENTS.md, read relative to the repository root.
func VerifySourceSections(repoRoot string) error {
	raw, err := os.ReadFile(repoRoot + "/" + AgentsPath)
	if err != nil {
		return fmt.Errorf("dogma: read %s: %w", AgentsPath, err)
	}
	content := string(raw)
	for _, heading := range sourceSections {
		if !strings.Contains(content, heading+"\n") {
			return fmt.Errorf("dogma: AGENTS.md section %q cited by briefings no longer exists; regenerate briefings against the new structure", heading)
		}
	}
	return nil
}
