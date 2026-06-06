// Package convergence holds meristem's convergence engine: the deterministic
// reducer core plus the event-log projector for persisted reducer verdicts.
// The reducer core is the load-bearing realization of AGENTS.md principle #12
// ("Convergence has a deterministic reduction").
//
// The system spec (docs/spec.md → "Convergence Patterns") draws a hard line:
// the probabilistic subsystem may *propose and judge* — sample at high
// temperature, fan out across models, draft a plan, grade another model's
// patch — but a deterministic *reduction* is what disposes. The reducer path
// is pure:
//
//   - It takes a set of Signals (which may themselves be probabilistic: a
//     model's grade, a confidence score, a parsed JSON verdict, a test result).
//   - A Reducer — a hand-coded, versioned, replayable pure function — folds
//     those signals into one of three Verdicts: accept, reject, escalate.
//   - Run() captures the reduction as a Reduction value: reducer identity and
//     version, a digest of the exact signals reduced over, the raw signals,
//     and the verdict. That value is what a caller persists as a
//     convergence.verdict_recorded event so the decision is reconstructable
//     from the log alone (spec rule #2: "the verdict is an event").
//
// What the reducer core deliberately does NOT do, because durable truth lives
// in events + projections (AGENTS.md), and because the engine must stay unit-
// testable without a database:
//
//   - It does not append events, touch Postgres, or read a work_item.
//   - It does not decide retries. Whether a reject becomes a retry or a
//     terminal failure is the Budget's call (see budget.go), applied by the
//     worker reconciler, not the reducer.
//   - It does not call models. Producing signals (running graders, tests,
//     samples) is the probabilistic subsystem's job; this package only
//     reduces the signals it is handed.
//
// The seam: the worker reconciler gathers signals, calls Run with the reducer
// the work_item declares, and translates the returned Reduction into a
// convergence.verdict_recorded event. The projector in this package derives
// convergence_verdicts from that event. See docs/convergence-engine.md for the
// handoff describing the remaining wiring.
package convergence

import (
	"errors"
	"fmt"

	"github.com/jbmopper/meristem/internal/domain"
)

// SignalSource attributes a probabilistic signal back to what produced it, so
// "we accepted because reducer Y had three of four graders pass at prompt Z"
// is reconstructable. All fields are optional; a purely deterministic signal
// (a test suite, a schema check) may leave them blank.
type SignalSource struct {
	// Model is the model identifier that produced the signal, when one did
	// (e.g. "claude-sonnet-4", "gpt-5"). Blank for deterministic signals.
	Model string `json:"model,omitempty"`
	// PromptVersion identifies the grader/proposer prompt revision, so a
	// reducer's inputs digest changes when the prompt changes.
	PromptVersion string `json:"prompt_version,omitempty"`
	// SampleID disambiguates multiple samples from the same model+prompt
	// (resample pattern), keeping each signal distinct in the digest.
	SampleID string `json:"sample_id,omitempty"`
}

// Signal is one piece of evidence a reducer folds over. It is intentionally a
// small, closed shape: a Kind label, optional boolean and numeric readings,
// and the raw evidence text kept for audit. Reducers read Pass/Score/Kind;
// Raw and Source exist so the event can record *why* without the reducer
// having to parse free-form text in a non-replayable way.
//
// A Signal must be safe for durable audit storage. Do not put secrets or raw
// private message content in Raw; put a digest or a bounded excerpt.
type Signal struct {
	// Kind labels what the signal measures: "grader.pass", "test.result",
	// "schema.match", "checklist.item:has_tests". Reducers select on it.
	Kind string `json:"kind"`
	// Source attributes a probabilistic signal to its producer.
	Source SignalSource `json:"source,omitempty"`
	// Pass is the boolean reading, when the signal is boolean (a grader's
	// verdict, a passing test). nil when the signal carries only a score.
	Pass *bool `json:"pass,omitempty"`
	// Score is the numeric reading, when the signal is scalar (a confidence,
	// a graded rating). nil when the signal is boolean-only.
	Score *float64 `json:"score,omitempty"`
	// Raw is the evidence kept for audit: the grader's one-line rationale, a
	// test summary, a digest of the candidate. Audit-safe content only.
	Raw string `json:"raw,omitempty"`
}

// Reducer is a hand-coded, versioned, pure function over signals. Given the
// same signals, Reduce always returns the same verdict — that is the whole
// contract. Identity and Version are recorded with every Reduction so a
// stricter future reducer (a new Version, or a different Identity) can be
// distinguished when re-folding the log.
//
// Reduce returns an error only for *misuse* (a signal the reducer cannot
// interpret, a misconfigured reducer). "Not enough signal to decide" is not
// an error — it is a VerdictEscalate. Errors must be deterministic too: the
// same bad input yields the same error.
type Reducer interface {
	Identity() string
	Version() int
	Reduce(signals []Signal) (Verdict, error)
}

// Verdict pairs the disposition with the reason the reducer reached it. The
// disposition is domain.Verdict (the durable wire vocabulary); Reason is a
// short, stable, human-readable explanation that is also recorded.
//
// Reason must be a pure function of the signals (no clock, no randomness) so
// it is replayable like the disposition.
type Verdict struct {
	Disposition domain.Verdict `json:"disposition"`
	Reason      string         `json:"reason"`
}

// Reduction is the complete, replayable record of one reduction. It is the
// payload a caller turns into a convergence.verdict_recorded event. Everything
// needed to re-derive the verdict — and to re-judge it under a stricter
// reducer later — is here.
type Reduction struct {
	// ReducerIdentity and ReducerVersion pin which reducer produced this and
	// at what revision. A new Version on the same Identity is a deliberate
	// "we changed our mind about how to reduce" and is visible in the log.
	ReducerIdentity string `json:"reducer_identity"`
	ReducerVersion  int    `json:"reducer_version"`
	// Attempt is the 1-based attempt counter within a convergence loop. It is
	// not used by the reducer; it is recorded so the patience budget's
	// accounting is visible in the log.
	Attempt int `json:"attempt"`
	// InputsDigest is the SHA-256 (hex) of the canonical encoding of Signals.
	// Two reductions over the same signals share a digest; it is what makes
	// "this verdict was over exactly these inputs" checkable without diffing
	// the raw signal blobs.
	InputsDigest string `json:"inputs_digest"`
	// Verdict is the disposition + reason the reducer produced.
	Verdict Verdict `json:"verdict"`
	// Signals is the exact set reduced over, kept for audit and re-folding.
	Signals []Signal `json:"signals"`
}

// ErrUnknownReducer is returned by Registry.Get for an unregistered identity.
var ErrUnknownReducer = errors.New("convergence: unknown reducer")

// Run executes reducer r over signals and packages the result as a Reduction
// with the inputs digest computed. attempt is the 1-based attempt counter the
// caller is tracking; it is recorded but does not affect the verdict.
//
// Run is pure: same reducer + same signals + same attempt → byte-identical
// Reduction (the digest is content-addressed, the verdict is the reducer's
// pure output). A reducer error is propagated unchanged.
func Run(r Reducer, signals []Signal, attempt int) (Reduction, error) {
	if r == nil {
		return Reduction{}, errors.New("convergence: nil reducer")
	}
	if attempt < 1 {
		return Reduction{}, fmt.Errorf("convergence: attempt must be >= 1, got %d", attempt)
	}
	digest, err := digestSignals(signals)
	if err != nil {
		return Reduction{}, err
	}
	v, err := r.Reduce(signals)
	if err != nil {
		return Reduction{}, fmt.Errorf("convergence: reducer %q v%d: %w", r.Identity(), r.Version(), err)
	}
	if !v.Disposition.Valid() {
		return Reduction{}, fmt.Errorf("convergence: reducer %q returned invalid disposition %q", r.Identity(), v.Disposition)
	}
	// Normalize the signal slice so an empty input and a nil input produce
	// the same recorded shape (and the same digest already agrees).
	recorded := signals
	if recorded == nil {
		recorded = []Signal{}
	}
	return Reduction{
		ReducerIdentity: r.Identity(),
		ReducerVersion:  r.Version(),
		Attempt:         attempt,
		InputsDigest:    digest,
		Verdict:         v,
		Signals:         recorded,
	}, nil
}

// EventPayload renders the Reduction as the canonical payload map for a
// convergence.verdict_recorded event. Provided here (pure) so the persistence
// slice does not re-invent the wire shape and risk drift from the digest.
func (red Reduction) EventPayload() map[string]any {
	signals := make([]any, 0, len(red.Signals))
	for _, s := range red.Signals {
		entry := map[string]any{"kind": s.Kind}
		if s.Pass != nil {
			entry["pass"] = *s.Pass
		}
		if s.Score != nil {
			entry["score"] = *s.Score
		}
		if s.Raw != "" {
			entry["raw"] = s.Raw
		}
		src := map[string]any{}
		if s.Source.Model != "" {
			src["model"] = s.Source.Model
		}
		if s.Source.PromptVersion != "" {
			src["prompt_version"] = s.Source.PromptVersion
		}
		if s.Source.SampleID != "" {
			src["sample_id"] = s.Source.SampleID
		}
		if len(src) > 0 {
			entry["source"] = src
		}
		signals = append(signals, entry)
	}
	return map[string]any{
		"reducer_identity": red.ReducerIdentity,
		"reducer_version":  red.ReducerVersion,
		"attempt":          red.Attempt,
		"inputs_digest":    red.InputsDigest,
		"verdict": map[string]any{
			"disposition": string(red.Verdict.Disposition),
			"reason":      red.Verdict.Reason,
		},
		"signals": signals,
	}
}

// Registry maps reducer identity → reducer, so a replay path can look up the
// reducer named in a recorded event. Registration is not concurrent-safe;
// build the registry once at startup and treat it as read-only thereafter.
type Registry struct {
	byIdentity map[string]Reducer
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byIdentity: make(map[string]Reducer)}
}

// Register adds r under its identity. A duplicate identity is refused: two
// reducers answering to the same name would make replay ambiguous.
func (reg *Registry) Register(r Reducer) error {
	if r == nil {
		return errors.New("convergence: cannot register nil reducer")
	}
	id := r.Identity()
	if id == "" {
		return errors.New("convergence: reducer identity is empty")
	}
	if _, exists := reg.byIdentity[id]; exists {
		return fmt.Errorf("convergence: reducer %q already registered", id)
	}
	reg.byIdentity[id] = r
	return nil
}

// Get returns the reducer registered under identity, or ErrUnknownReducer.
func (reg *Registry) Get(identity string) (Reducer, error) {
	r, ok := reg.byIdentity[identity]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownReducer, identity)
	}
	return r, nil
}
