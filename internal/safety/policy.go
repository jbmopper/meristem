// Package safety defines the deterministic resource controls that must be
// present before meristem is allowed to run.
package safety

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

// Policy is intentionally code-owned in this slice: startup safety must not
// depend on mutable process memory, environment-variable luck, or a partially
// initialized database. Later slices may project operator-authored policy from
// the event log, but API/worker startup should still fail closed if the
// projected policy is absent or invalid.
type Policy struct {
	MaxRequestBodyBytes int64                                  `json:"max_request_body_bytes"`
	MaxFeedWait         time.Duration                          `json:"max_feed_wait"`
	PatienceBudgets     map[domain.WorkItemState]time.Duration `json:"patience_budgets"`
	MaxDelegationDepth  int                                    `json:"max_delegation_depth"`
}

const (
	// Max request body is deliberately small while v0/v1 only accept text,
	// JSON work specs, and coordination notes. Large artifacts belong behind
	// the future object-storage interface, not in Postgres request bodies.
	defaultMaxRequestBodyBytes int64 = 1 << 20 // 1 MiB
	defaultMaxFeedWait               = 60 * time.Second
	defaultMaxDelegationDepth        = 5

	// MaxPatienceBudget is the ceiling on any patience budget in
	// any profile. Bounded patience is the invariant (spec principle 3);
	// "relaxed" may stretch a budget, never sever it. A budget above this
	// cap is treated as an attempt to encode "wait forever" and fails
	// validation.
	MaxPatienceBudget = 30 * 24 * time.Hour
)

// Profile names. The profile set is code-owned like the policy itself:
// startup safety must not depend on mutable state. Which profile is *active*
// is runtime state, event-sourced through policy_profile.switched and
// projected into active_policy_profile (see internal/policyprofile).
const (
	// ProfileSteady is the spec-normal envelope and the default when no
	// switch event has ever been appended: absent operator action, the
	// system behaves exactly as it did before profiles existed.
	ProfileSteady = "steady"
	// ProfileBringUp relaxes patience for a system whose backlog and
	// reconcilers are still being stood up: budgets are generous but every
	// one remains finite and under MaxPatienceBudget.
	ProfileBringUp = "bring-up"
)

// DefaultPolicy returns the resource-safety policy required for startup.
func DefaultPolicy() Policy {
	return Policy{
		MaxRequestBodyBytes: defaultMaxRequestBodyBytes,
		MaxFeedWait:         defaultMaxFeedWait,
		MaxDelegationDepth:  defaultMaxDelegationDepth,
		PatienceBudgets: map[domain.WorkItemState]time.Duration{
			domain.WorkItemCaptured:         24 * time.Hour,
			domain.WorkItemTriaged:          72 * time.Hour,
			domain.WorkItemPlanned:          72 * time.Hour,
			domain.WorkItemAwaitingApproval: 48 * time.Hour,
			domain.WorkItemRunning:          24 * time.Hour,
			domain.WorkItemBlocked:          24 * time.Hour,
		},
	}
}

// Profiles returns every named policy profile. Every profile shares the
// request-body and feed-wait limits; profiles vary patience only. (R7 adds
// xylem budgets as a later dimension.)
func Profiles() map[string]Policy {
	return map[string]Policy{
		ProfileSteady: DefaultPolicy(),
		ProfileBringUp: {
			MaxRequestBodyBytes: defaultMaxRequestBodyBytes,
			MaxFeedWait:         defaultMaxFeedWait,
			MaxDelegationDepth:  defaultMaxDelegationDepth,
			PatienceBudgets: map[domain.WorkItemState]time.Duration{
				domain.WorkItemCaptured:         7 * 24 * time.Hour,
				domain.WorkItemTriaged:          14 * 24 * time.Hour,
				domain.WorkItemPlanned:          14 * 24 * time.Hour,
				domain.WorkItemAwaitingApproval: 7 * 24 * time.Hour,
				domain.WorkItemRunning:          3 * 24 * time.Hour,
				domain.WorkItemBlocked:          7 * 24 * time.Hour,
			},
		},
	}
}

// ProfileByName resolves a named profile. Unknown names return a structured
// error listing the known set, matching the registry-style refusal shape.
func ProfileByName(name string) (Policy, error) {
	profiles := Profiles()
	p, ok := profiles[name]
	if !ok {
		names := make([]string, 0, len(profiles))
		for n := range profiles {
			names = append(names, n)
		}
		sort.Strings(names)
		return Policy{}, fmt.Errorf("safety: unknown policy profile %q; known profiles: %s", name, strings.Join(names, ", "))
	}
	return p, nil
}

// Validate is the startup gate. An invalid policy is a hard failure because
// running without bounded inputs or bounded patience is exactly the unsafe
// restart mode this package exists to prevent.
func (p Policy) Validate() error {
	if p.MaxRequestBodyBytes <= 0 {
		return fmt.Errorf("safety: max_request_body_bytes must be positive")
	}
	if p.MaxFeedWait <= 0 {
		return fmt.Errorf("safety: max_feed_wait must be positive")
	}
	if p.MaxDelegationDepth < 0 {
		return fmt.Errorf("safety: max_delegation_depth must be >= 0")
	}
	for _, state := range nonTerminalStates() {
		dur, ok := p.PatienceBudgets[state]
		if !ok {
			return fmt.Errorf("safety: missing patience budget for state %q", state)
		}
		if dur <= 0 {
			return fmt.Errorf("safety: patience budget for state %q must be positive", state)
		}
	}
	for state, dur := range p.PatienceBudgets {
		if !state.Valid() {
			return fmt.Errorf("safety: patience budget for unknown state %q", state)
		}
		if state.Terminal() {
			return fmt.Errorf("safety: patience budget for terminal state %q is meaningless", state)
		}
		if dur <= 0 {
			return fmt.Errorf("safety: patience budget for state %q must be positive", state)
		}
		if dur > MaxPatienceBudget {
			return fmt.Errorf("safety: patience budget for state %q exceeds the %s finite cap; bounded patience admits no effectively-infinite budget", state, MaxPatienceBudget)
		}
	}
	return nil
}

// MustValidateStartup returns the default policy after validating every named
// profile. Runtime entry points call this before opening long-lived services;
// a single invalid profile fails startup even if it is not the active one,
// because an operator must be able to switch profiles without discovering at
// switch time that the target never validated.
func MustValidateStartup() (Policy, error) {
	for name, p := range Profiles() {
		if err := p.Validate(); err != nil {
			return Policy{}, fmt.Errorf("profile %q: %w", name, err)
		}
	}
	return DefaultPolicy(), nil
}

// Fingerprint is a stable identifier for the effective policy. It is useful in
// logs, health output, and restart checklists because it makes "what controls
// were active?" cheap to answer without dumping the whole policy each time.
func (p Policy) Fingerprint() (string, error) {
	canonical := struct {
		MaxRequestBodyBytes int64            `json:"max_request_body_bytes"`
		MaxFeedWaitSeconds  int64            `json:"max_feed_wait_seconds"`
		MaxDelegationDepth  int              `json:"max_delegation_depth"`
		PatienceSeconds     map[string]int64 `json:"patience_seconds"`
	}{
		MaxRequestBodyBytes: p.MaxRequestBodyBytes,
		MaxFeedWaitSeconds:  int64(p.MaxFeedWait.Seconds()),
		MaxDelegationDepth:  p.MaxDelegationDepth,
		PatienceSeconds:     make(map[string]int64, len(p.PatienceBudgets)),
	}
	for state, dur := range p.PatienceBudgets {
		canonical.PatienceSeconds[string(state)] = int64(dur.Seconds())
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:8]), nil
}

func nonTerminalStates() []domain.WorkItemState {
	return []domain.WorkItemState{
		domain.WorkItemCaptured,
		domain.WorkItemTriaged,
		domain.WorkItemPlanned,
		domain.WorkItemAwaitingApproval,
		domain.WorkItemRunning,
		domain.WorkItemBlocked,
	}
}
