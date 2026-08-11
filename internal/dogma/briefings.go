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

const (
	checklistEvidenceExampleCheck = "cmd:go test ./..."
	checklistEvidenceExampleArgs  = `{"id":"<assigned-work-item-id>","kind":"checklist.item:cmd:go test ./...","payload":{"pass":true,"raw":"passed on local attempt 2/3; bounded audit-safe evidence"},"idempotency_key":"<stable-sha256-final-key-for-item-and-check>"}`
	checklistBlockerExampleArgs   = `{"id":"<assigned-work-item-id>","kind":"checklist.blocked:cmd:go test ./...","payload":{"raw":"cannot run: required tool is outside the assigned scope"},"idempotency_key":"<stable-sha256-blocker-key-for-item-and-check>"}`
)

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
			"Treat every declared check as CHECK verbatim, including its cmd:/query:/event:/human-ack: prefix; never rename or normalize it.",
			"Begin only after a reviewed, assignment-fenced start path has admitted this exact assignment generation to running. If the item is not running, do not evaluate or append checklist.item evidence; append at most one checklist.blocked:<exact CHECK> audit event naming the observed state, then return control to the supervisor. That blocker does not transition or dispose the item.",
			"Before each CHECK, inspect the available assigned feed/history for an existing checklist.item:<exact CHECK> event and stop for that CHECK when one exists. On every restart, reuse the same lowercase SHA-256 idempotency key derived from the literal bytes checklist-final\\0<work-item-id>\\0<CHECK>, so an ambiguous repeat append collapses even when history is incomplete.",
			"For each runnable cmd:/query: CHECK, evaluate it up to 3 bounded local attempts.",
			"For event: CHECK inspect authoritative event evidence; accept human-ack: only with the declared human signal. Treat unavailable evidence as cannot-run, never invent a result.",
			"Do not append checklist.item evidence for intermediate attempts. After local retries or evidence inspection ends, append exactly one final result for that CHECK with kind checklist.item:<exact CHECK> and object payload containing boolean pass and bounded string raw that summarizes the attempts.",
			fmt.Sprintf("Final-result example for CHECK %s: %s", checklistEvidenceExampleCheck, checklistEvidenceExampleArgs),
			"If CHECK cannot be evaluated because authority, tools, inputs, or external evidence are unavailable, do not set pass=false: append one checklist.blocked:<exact CHECK> event with bounded raw and a stable lowercase SHA-256 key derived from checklist-blocked\\0<work-item-id>\\0<CHECK>, then stop. The blocker is audit evidence only; when the item is running, running-state wall patience owns escalation.",
			fmt.Sprintf("Cannot-run example for CHECK %s: %s", checklistEvidenceExampleCheck, checklistBlockerExampleArgs),
			"A runnable CHECK that still fails after its local attempts emits one final pass=false with the bounded attempt summary; do not append another checklist.item result for that CHECK. Under checklist-all@1 this is irrevocable for the item and must hand to the owner rather than pretending a later true can heal it.",
			"kind and payload are separate work_items.append_event arguments; never nest kind inside payload.",
			"Never emit checks_passed/checks_failed or a prose-only verdict; neither satisfies the reducer.",
			"Never transition the item to done or failed from free-form judgment — deterministic verdict machinery owns lifecycle disposal.",
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
