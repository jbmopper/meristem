package convergence

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

// This file holds the canonical reducers — "the smallest thing that does the
// job" (spec). Each is a pure function over signals, versioned, and named so
// its verdicts are replayable. They map onto the recurring convergence shapes
// in docs/spec.md; they are not privileged, just the common cases pre-built so
// every new pattern does not start from scratch.

// MajorityVote accepts when a strict majority of boolean signals (of the
// configured kind) pass, rejects when a strict majority fail, and escalates on
// a tie or when there are no qualifying signals. This is the deterministic
// reduction for the multi-model and generate-and-validate shapes with several
// graders: "three of four graders passed" is an accept; two of four is a tie.
//
// Only signals whose Kind == SignalKind and whose Pass is non-nil count.
// Score-only signals are ignored (use Threshold for those).
type MajorityVote struct {
	// SignalKind selects which signals vote. A grader fan-out would set this
	// to e.g. "grader.pass"; leaving it blank votes over every boolean signal.
	SignalKind string
}

func (MajorityVote) Identity() string { return "majority_vote" }
func (MajorityVote) Version() int     { return 1 }

func (m MajorityVote) ReducerConfig() map[string]any {
	if m.SignalKind == "" {
		return nil
	}
	return map[string]any{"signal_kind": m.SignalKind}
}

func (m MajorityVote) Reduce(signals []Signal) (Verdict, error) {
	var pass, fail int
	for _, s := range signals {
		if m.SignalKind != "" && s.Kind != m.SignalKind {
			continue
		}
		if s.Pass == nil {
			continue
		}
		if *s.Pass {
			pass++
		} else {
			fail++
		}
	}
	total := pass + fail
	if total == 0 {
		return Verdict{
			Disposition: domain.VerdictEscalate,
			Reason:      "no qualifying boolean signals to vote over",
		}, nil
	}
	switch {
	case pass*2 > total:
		return Verdict{
			Disposition: domain.VerdictAccept,
			Reason:      fmt.Sprintf("%d/%d graders passed (majority)", pass, total),
		}, nil
	case fail*2 > total:
		return Verdict{
			Disposition: domain.VerdictReject,
			Reason:      fmt.Sprintf("%d/%d graders failed (majority)", fail, total),
		}, nil
	default:
		return Verdict{
			Disposition: domain.VerdictEscalate,
			Reason:      fmt.Sprintf("tie: %d pass, %d fail", pass, fail),
		}, nil
	}
}

// Unanimous accepts only when every qualifying boolean signal passes (and
// there is at least one), rejects when any fails, and escalates when there are
// no qualifying signals. The strict end of the generate-and-validate shape
// ("require unanimity").
type Unanimous struct {
	SignalKind string
}

func (Unanimous) Identity() string { return "unanimous" }
func (Unanimous) Version() int     { return 1 }

func (u Unanimous) ReducerConfig() map[string]any {
	if u.SignalKind == "" {
		return nil
	}
	return map[string]any{"signal_kind": u.SignalKind}
}

func (u Unanimous) Reduce(signals []Signal) (Verdict, error) {
	var seen, failed int
	for _, s := range signals {
		if u.SignalKind != "" && s.Kind != u.SignalKind {
			continue
		}
		if s.Pass == nil {
			continue
		}
		seen++
		if !*s.Pass {
			failed++
		}
	}
	if seen == 0 {
		return Verdict{
			Disposition: domain.VerdictEscalate,
			Reason:      "no qualifying boolean signals; cannot require unanimity",
		}, nil
	}
	if failed > 0 {
		return Verdict{
			Disposition: domain.VerdictReject,
			Reason:      fmt.Sprintf("%d/%d graders failed; unanimity required", failed, seen),
		}, nil
	}
	return Verdict{
		Disposition: domain.VerdictAccept,
		Reason:      fmt.Sprintf("all %d graders passed", seen),
	}, nil
}

// Threshold accepts when the mean Score across qualifying scalar signals is at
// or above Accept, rejects when it is below, and escalates when there are no
// qualifying signals. The reduction for confidence-scored or rated outputs.
//
// Mean (not min/max) is the deliberate default: it is the least surprising
// aggregate and is stable under signal reordering. A reducer needing min or
// max is a distinct Identity, not a flag on this one.
type Threshold struct {
	// SignalKind selects which scalar signals are averaged. Blank averages
	// every signal that carries a Score.
	SignalKind string
	// Accept is the inclusive lower bound on the mean score for an accept.
	Accept float64
}

func (Threshold) Identity() string { return "threshold_mean" }
func (Threshold) Version() int     { return 1 }

func (t Threshold) ReducerConfig() map[string]any {
	config := map[string]any{"accept": t.Accept}
	if t.SignalKind != "" {
		config["signal_kind"] = t.SignalKind
	}
	return config
}

func (t Threshold) Reduce(signals []Signal) (Verdict, error) {
	var sum float64
	var n int
	for _, s := range signals {
		if t.SignalKind != "" && s.Kind != t.SignalKind {
			continue
		}
		if s.Score == nil {
			continue
		}
		sum += *s.Score
		n++
	}
	if n == 0 {
		return Verdict{
			Disposition: domain.VerdictEscalate,
			Reason:      "no qualifying scalar signals to threshold",
		}, nil
	}
	mean := sum / float64(n)
	if mean >= t.Accept {
		return Verdict{
			Disposition: domain.VerdictAccept,
			Reason:      fmt.Sprintf("mean score %.4f >= threshold %.4f (n=%d)", mean, t.Accept, n),
		}, nil
	}
	return Verdict{
		Disposition: domain.VerdictReject,
		Reason:      fmt.Sprintf("mean score %.4f < threshold %.4f (n=%d)", mean, t.Accept, n),
	}, nil
}

// AllPassChecklist accepts only when every required check has a passing
// signal. This consumes a work_item's suggested_convergence_checks (spec: the
// checklist is advisory "until a reducer consumes it" — this is that reducer).
//
// A check is identified by a signal of Kind "checklist.item:<name>". A check
// is satisfied iff such a signal exists with Pass == true. A required check
// with no signal, or a failing signal, rejects (the reason names what is
// missing or failing). An empty Required set escalates rather than vacuously
// accepting: "nothing was required" is not the same as "everything passed".
type AllPassChecklist struct {
	// Required is the set of check names that must all pass. Conventionally
	// sourced from work_items.suggested_convergence_checks.
	Required []string
}

func (AllPassChecklist) Identity() string { return "all_pass_checklist" }
func (AllPassChecklist) Version() int     { return 1 }

func (c AllPassChecklist) ReducerConfig() map[string]any {
	if len(c.Required) == 0 {
		return nil
	}
	required := append([]string(nil), c.Required...)
	return map[string]any{"required": required}
}

const checklistItemPrefix = "checklist.item:"

func (c AllPassChecklist) Reduce(signals []Signal) (Verdict, error) {
	if len(c.Required) == 0 {
		return Verdict{
			Disposition: domain.VerdictEscalate,
			Reason:      "no required checks declared; cannot claim convergence",
		}, nil
	}
	// Collapse signals by check name. A duplicate check signal is resolved
	// deterministically: a failure anywhere for a check makes that check fail
	// (strict, audit-friendly).
	passed := make(map[string]bool, len(c.Required))
	failed := make(map[string]bool)
	for _, s := range signals {
		if !strings.HasPrefix(s.Kind, checklistItemPrefix) {
			continue
		}
		if s.Pass == nil {
			continue
		}
		name := strings.TrimPrefix(s.Kind, checklistItemPrefix)
		if *s.Pass {
			if _, alreadyFailed := failed[name]; !alreadyFailed {
				passed[name] = true
			}
		} else {
			failed[name] = true
			delete(passed, name)
		}
	}

	var missing, failing []string
	for _, name := range c.Required {
		switch {
		case failed[name]:
			failing = append(failing, name)
		case !passed[name]:
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(failing)

	if len(missing) == 0 && len(failing) == 0 {
		return Verdict{
			Disposition: domain.VerdictAccept,
			Reason:      fmt.Sprintf("all %d required checks passed", len(c.Required)),
		}, nil
	}
	var parts []string
	if len(failing) > 0 {
		parts = append(parts, "failing: "+strings.Join(failing, ", "))
	}
	if len(missing) > 0 {
		parts = append(parts, "missing: "+strings.Join(missing, ", "))
	}
	return Verdict{
		Disposition: domain.VerdictReject,
		Reason:      strings.Join(parts, "; "),
	}, nil
}

// DefaultRegistry returns a registry pre-loaded with the parameter-free
// canonical reducers, so a worker has a working set without bespoke wiring.
//
// The parameterized reducers (Threshold.Accept, AllPassChecklist.Required) are
// deliberately NOT registered here: their behavior depends on configuration
// that must travel with the work_item, so a caller constructs the configured
// instance and either runs it directly via Run or registers it under a
// distinct identity that encodes its parameters. Registering a zero-value
// Threshold would make replay silently use the wrong threshold.
func DefaultRegistry() *Registry {
	reg := NewRegistry()
	// These cannot error: identities are non-empty and distinct.
	_ = reg.Register(MajorityVote{})
	_ = reg.Register(Unanimous{})
	return reg
}
