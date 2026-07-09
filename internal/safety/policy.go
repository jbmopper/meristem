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
	"github.com/jbmopper/meristem/internal/feed"
)

// Policy is intentionally code-owned in this slice: startup safety must not
// depend on mutable process memory, environment-variable luck, or a partially
// initialized database. Later slices may project operator-authored policy from
// the event log, but API/worker startup should still fail closed if the
// projected policy is absent or invalid.
type Policy struct {
	MaxRequestBodyBytes            int64                                  `json:"max_request_body_bytes"`
	MaxFeedWait                    time.Duration                          `json:"max_feed_wait"`
	PoolMaxConns                   int32                                  `json:"pool_max_conns"`
	PoolMinConns                   int32                                  `json:"pool_min_conns"`
	WorkerTickInterval             time.Duration                          `json:"worker_tick_interval"`
	PatienceBudgets                map[domain.WorkItemState]time.Duration `json:"patience_budgets"`
	MaxDelegationDepth             int                                    `json:"max_delegation_depth"`
	MaxChildrenPerItem             int                                    `json:"max_children_per_item"`
	MaxConcurrentRunningPerToken   int                                    `json:"max_concurrent_running_items_per_token"`
	MaxEventsPerItemPerHourByClass map[string]int                         `json:"max_events_per_item_per_hour_by_class"`
	MaxSignalItemsPerTokenPerHour  int                                    `json:"max_signal_items_per_token_per_hour"`
}

const (
	// Max request body is deliberately small while v0/v1 only accept text,
	// JSON work specs, and coordination notes. Large artifacts belong behind
	// the future object-storage interface, not in Postgres request bodies.
	defaultMaxRequestBodyBytes              int64 = 1 << 20 // 1 MiB
	defaultMaxFeedWait                            = 60 * time.Second
	defaultPoolMaxConns                           = 10
	defaultPoolMinConns                           = 1
	defaultWorkerTickInterval                     = 30 * time.Second
	defaultMaxDelegationDepth                     = 5
	defaultMaxChildrenPerItem                     = 32
	defaultMaxConcurrentRunningPerToken           = 8
	defaultMaxLifecycleEventsPerItemPerHour       = 120
	defaultMaxDecisionEventsPerItemPerHour        = 120
	defaultMaxProgressEventsPerItemPerHour        = 240

	// Signal admission budget: the maximum number of NEW work_items one
	// source token may create through /v1/signals per rolling hour. A signal
	// that dedupe-links onto an existing live work_item is not metered — only
	// item creation is. Over-budget admission records the signal for audit,
	// refuses the work_item creation, and escalates to the owner rather than
	// silently dropping. These are the owner-suggested starting values, split
	// out as named constants so the human-ack can retune them without hunting
	// through the profile literals. steady is the spec-normal envelope;
	// bring-up is tighter while backlog and reconcilers are still standing up.
	defaultMaxSignalItemsPerTokenPerHour = 30
	bringUpMaxSignalItemsPerTokenPerHour = 10

	// MaxPatienceBudget is the ceiling on any patience budget in
	// any profile. Bounded patience is the invariant (spec principle 3);
	// "relaxed" may stretch a budget, never sever it. A budget above this
	// cap is treated as an attempt to encode "wait forever" and fails
	// validation.
	MaxPatienceBudget = 30 * 24 * time.Hour

	// MaxPoolMaxConns is the hard ceiling on profile-declared pool sizes.
	MaxPoolMaxConns = 64
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
		MaxRequestBodyBytes:            defaultMaxRequestBodyBytes,
		MaxFeedWait:                    defaultMaxFeedWait,
		PoolMaxConns:                   defaultPoolMaxConns,
		PoolMinConns:                   defaultPoolMinConns,
		WorkerTickInterval:             defaultWorkerTickInterval,
		MaxDelegationDepth:             defaultMaxDelegationDepth,
		MaxChildrenPerItem:             defaultMaxChildrenPerItem,
		MaxConcurrentRunningPerToken:   defaultMaxConcurrentRunningPerToken,
		MaxEventsPerItemPerHourByClass: defaultMaxEventsPerItemPerHourByClass(),
		MaxSignalItemsPerTokenPerHour:  defaultMaxSignalItemsPerTokenPerHour,
		PatienceBudgets: map[domain.WorkItemState]time.Duration{
			domain.WorkItemCaptured:         24 * time.Hour,
			domain.WorkItemTriaged:          72 * time.Hour,
			domain.WorkItemPlanned:          72 * time.Hour,
			domain.WorkItemAwaitingApproval: 48 * time.Hour,
			domain.WorkItemRunning:          24 * time.Hour,
			// Blocked items are the owner's review court: they sit until a
			// human acts, so a tight timer only re-surfaces them as noise. A
			// 7d budget keeps bounded patience (spec principle 3) while giving
			// the owner a working week before a blocked stay re-escalates. One
			// escalation per blocked epoch is already guaranteed by the
			// deterministic escalation id and the human_review_status=blocked
			// skip (see internal/worker: TestScanOnceEscalationChildrenDoNotBreed).
			domain.WorkItemBlocked: 7 * 24 * time.Hour,
		},
	}
}

// Profiles returns every named policy profile. Every profile shares the
// request-body and feed-wait limits; profiles vary operational posture such
// as patience, pool fan-out, and worker cadence.
func Profiles() map[string]Policy {
	return map[string]Policy{
		ProfileSteady: DefaultPolicy(),
		ProfileBringUp: {
			MaxRequestBodyBytes:            defaultMaxRequestBodyBytes,
			MaxFeedWait:                    defaultMaxFeedWait,
			PoolMaxConns:                   4,
			PoolMinConns:                   1,
			WorkerTickInterval:             60 * time.Second,
			MaxDelegationDepth:             defaultMaxDelegationDepth,
			MaxChildrenPerItem:             defaultMaxChildrenPerItem,
			MaxConcurrentRunningPerToken:   defaultMaxConcurrentRunningPerToken,
			MaxEventsPerItemPerHourByClass: defaultMaxEventsPerItemPerHourByClass(),
			MaxSignalItemsPerTokenPerHour:  bringUpMaxSignalItemsPerTokenPerHour,
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
	if p.PoolMaxConns <= 0 {
		return fmt.Errorf("safety: pool_max_conns must be positive")
	}
	if p.PoolMaxConns > MaxPoolMaxConns {
		return fmt.Errorf("safety: pool_max_conns must be <= %d", MaxPoolMaxConns)
	}
	if p.PoolMinConns <= 0 {
		return fmt.Errorf("safety: pool_min_conns must be positive")
	}
	if p.PoolMinConns > p.PoolMaxConns {
		return fmt.Errorf("safety: pool_min_conns must be <= pool_max_conns")
	}
	if p.WorkerTickInterval <= 0 {
		return fmt.Errorf("safety: worker_tick_interval must be positive")
	}
	if p.MaxDelegationDepth < 0 {
		return fmt.Errorf("safety: max_delegation_depth must be >= 0")
	}
	if p.MaxChildrenPerItem <= 0 {
		return fmt.Errorf("safety: max_children_per_item must be positive")
	}
	if p.MaxConcurrentRunningPerToken <= 0 {
		return fmt.Errorf("safety: max_concurrent_running_items_per_token must be positive")
	}
	if err := validateEventRateBudgetMap(p.MaxEventsPerItemPerHourByClass); err != nil {
		return err
	}
	if p.MaxSignalItemsPerTokenPerHour <= 0 {
		return fmt.Errorf("safety: max_signal_items_per_token_per_hour must be positive")
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
		MaxRequestBodyBytes            int64            `json:"max_request_body_bytes"`
		MaxFeedWaitSeconds             int64            `json:"max_feed_wait_seconds"`
		PoolMaxConns                   int32            `json:"pool_max_conns"`
		PoolMinConns                   int32            `json:"pool_min_conns"`
		WorkerTickSeconds              int64            `json:"worker_tick_seconds"`
		MaxDelegationDepth             int              `json:"max_delegation_depth"`
		MaxChildrenPerItem             int              `json:"max_children_per_item"`
		MaxConcurrentRunningPerToken   int              `json:"max_concurrent_running_items_per_token"`
		MaxEventsPerItemPerHourByClass map[string]int   `json:"max_events_per_item_per_hour_by_class"`
		MaxSignalItemsPerTokenPerHour  int              `json:"max_signal_items_per_token_per_hour"`
		PatienceSeconds                map[string]int64 `json:"patience_seconds"`
	}{
		MaxRequestBodyBytes:            p.MaxRequestBodyBytes,
		MaxFeedWaitSeconds:             int64(p.MaxFeedWait.Seconds()),
		PoolMaxConns:                   p.PoolMaxConns,
		PoolMinConns:                   p.PoolMinConns,
		WorkerTickSeconds:              int64(p.WorkerTickInterval.Seconds()),
		MaxDelegationDepth:             p.MaxDelegationDepth,
		MaxChildrenPerItem:             p.MaxChildrenPerItem,
		MaxConcurrentRunningPerToken:   p.MaxConcurrentRunningPerToken,
		MaxEventsPerItemPerHourByClass: copyStringIntMap(p.MaxEventsPerItemPerHourByClass),
		MaxSignalItemsPerTokenPerHour:  p.MaxSignalItemsPerTokenPerHour,
		PatienceSeconds:                make(map[string]int64, len(p.PatienceBudgets)),
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

func defaultMaxEventsPerItemPerHourByClass() map[string]int {
	return map[string]int{
		feed.KindClassLifecycle: defaultMaxLifecycleEventsPerItemPerHour,
		feed.KindClassDecision:  defaultMaxDecisionEventsPerItemPerHour,
		feed.KindClassProgress:  defaultMaxProgressEventsPerItemPerHour,
	}
}

func validateEventRateBudgetMap(budgets map[string]int) error {
	if budgets == nil {
		return fmt.Errorf("safety: max_events_per_item_per_hour_by_class is required")
	}
	for _, class := range []string{feed.KindClassLifecycle, feed.KindClassDecision, feed.KindClassProgress} {
		if budgets[class] <= 0 {
			return fmt.Errorf("safety: max_events_per_item_per_hour_by_class[%s] must be positive", class)
		}
	}
	for class, max := range budgets {
		if !feed.ProjectableKindClass(class) {
			return fmt.Errorf("safety: max_events_per_item_per_hour_by_class[%s] is not a projectable kind class", class)
		}
		if max <= 0 {
			return fmt.Errorf("safety: max_events_per_item_per_hour_by_class[%s] must be positive", class)
		}
	}
	return nil
}

func copyStringIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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
