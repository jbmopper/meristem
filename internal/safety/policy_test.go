package safety

import (
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestDefaultPolicyValidates(t *testing.T) {
	p, err := MustValidateStartup()
	if err != nil {
		t.Fatalf("default startup policy must validate: %v", err)
	}
	if p.MaxRequestBodyBytes <= 0 {
		t.Fatal("default policy must bound request bodies")
	}
	if p.MaxFeedWait <= 0 {
		t.Fatal("default policy must bound feed waits")
	}
	if p.PoolMaxConns <= 0 || p.PoolMinConns <= 0 {
		t.Fatal("default policy must bound pool sizes")
	}
	if p.WorkerTickInterval <= 0 {
		t.Fatal("default policy must bound worker tick interval")
	}
	if p.MaxDelegationDepth <= 0 {
		t.Fatal("default policy must bound delegation depth")
	}
	if p.MaxChildrenPerItem <= 0 {
		t.Fatal("default policy must bound children per item")
	}
	if p.MaxConcurrentRunningPerToken <= 0 {
		t.Fatal("default policy must bound concurrent running items per token")
	}
	if len(p.MaxEventsPerItemPerHourByClass) == 0 {
		t.Fatal("default policy must bound per-item event rates by class")
	}
}

func TestValidateRequiresEveryNonTerminalBudget(t *testing.T) {
	p := DefaultPolicy()
	delete(p.PatienceBudgets, domain.WorkItemBlocked)

	err := p.Validate()
	if err == nil {
		t.Fatal("expected missing budget to fail validation")
	}
	if !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("expected error to name missing state; got %v", err)
	}
}

func TestValidateRejectsTerminalBudget(t *testing.T) {
	p := DefaultPolicy()
	p.PatienceBudgets[domain.WorkItemDone] = time.Hour

	err := p.Validate()
	if err == nil {
		t.Fatal("expected terminal budget to fail validation")
	}
	if !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("expected terminal-state error; got %v", err)
	}
}

func TestValidateRejectsZeroPatience(t *testing.T) {
	p := DefaultPolicy()
	p.PatienceBudgets[domain.WorkItemCaptured] = 0

	if err := p.Validate(); err == nil {
		t.Fatal("expected zero patience to fail validation")
	}
}

func TestValidateRejectsNegativePatience(t *testing.T) {
	p := DefaultPolicy()
	p.PatienceBudgets[domain.WorkItemRunning] = -time.Hour

	if err := p.Validate(); err == nil {
		t.Fatal("expected negative patience to fail validation")
	}
}

func TestValidateRejectsNegativeDelegationDepth(t *testing.T) {
	p := DefaultPolicy()
	p.MaxDelegationDepth = -1

	if err := p.Validate(); err == nil {
		t.Fatal("expected negative delegation depth to fail validation")
	}
}

func TestValidateRejectsInvalidPoolSizing(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Policy)
		want string
	}{
		{name: "non-positive max", edit: func(p *Policy) { p.PoolMaxConns = 0 }, want: "pool_max_conns"},
		{name: "non-positive min", edit: func(p *Policy) { p.PoolMinConns = 0 }, want: "pool_min_conns"},
		{name: "min exceeds max", edit: func(p *Policy) { p.PoolMinConns = p.PoolMaxConns + 1 }, want: "pool_min_conns"},
		{name: "max exceeds ceiling", edit: func(p *Policy) { p.PoolMaxConns = MaxPoolMaxConns + 1 }, want: "pool_max_conns"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := DefaultPolicy()
			tc.edit(&p)
			err := p.Validate()
			if err == nil {
				t.Fatal("expected invalid pool sizing to fail validation")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error to contain %q, got %v", tc.want, err)
			}
		})
	}
}

func TestValidateRejectsNonPositiveWorkerTickInterval(t *testing.T) {
	p := DefaultPolicy()
	p.WorkerTickInterval = 0
	if err := p.Validate(); err == nil {
		t.Fatal("expected non-positive worker tick interval to fail validation")
	}
}

func TestValidateRejectsNonPositiveChildrenPerItem(t *testing.T) {
	for _, value := range []int{0, -1} {
		p := DefaultPolicy()
		p.MaxChildrenPerItem = value

		if err := p.Validate(); err == nil {
			t.Fatalf("expected max_children_per_item=%d to fail validation", value)
		}
	}
}

func TestValidateRejectsNonPositiveConcurrentRunningPerToken(t *testing.T) {
	for _, value := range []int{0, -1} {
		p := DefaultPolicy()
		p.MaxConcurrentRunningPerToken = value

		if err := p.Validate(); err == nil {
			t.Fatalf("expected max_concurrent_running_items_per_token=%d to fail validation", value)
		}
	}
}

func TestValidateRejectsNonPositiveSignalItemsPerTokenPerHour(t *testing.T) {
	for _, value := range []int{0, -1} {
		p := DefaultPolicy()
		p.MaxSignalItemsPerTokenPerHour = value

		if err := p.Validate(); err == nil {
			t.Fatalf("expected max_signal_items_per_token_per_hour=%d to fail validation", value)
		}
	}
}

func TestDefaultAndBringUpSignalBudgetsDiffer(t *testing.T) {
	steady := DefaultPolicy().MaxSignalItemsPerTokenPerHour
	bringUp := Profiles()[ProfileBringUp].MaxSignalItemsPerTokenPerHour
	if steady != defaultMaxSignalItemsPerTokenPerHour {
		t.Fatalf("steady signal budget = %d, want %d", steady, defaultMaxSignalItemsPerTokenPerHour)
	}
	if bringUp != bringUpMaxSignalItemsPerTokenPerHour {
		t.Fatalf("bring-up signal budget = %d, want %d", bringUp, bringUpMaxSignalItemsPerTokenPerHour)
	}
	if bringUp >= steady {
		t.Fatalf("bring-up budget %d should be tighter than steady %d", bringUp, steady)
	}
}

func TestValidateRejectsInvalidEventRateBudgets(t *testing.T) {
	cases := []struct {
		name string
		edit func(map[string]int)
		want string
	}{
		{
			name: "missing class",
			edit: func(m map[string]int) {
				delete(m, "progress")
			},
			want: "progress",
		},
		{
			name: "non-positive class",
			edit: func(m map[string]int) {
				m["decision"] = 0
			},
			want: "decision",
		},
		{
			name: "admin class",
			edit: func(m map[string]int) {
				m["admin"] = 1
			},
			want: "admin",
		},
		{
			name: "unknown class",
			edit: func(m map[string]int) {
				m["mystery"] = 1
			},
			want: "mystery",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := DefaultPolicy()
			tc.edit(p.MaxEventsPerItemPerHourByClass)

			err := p.Validate()
			if err == nil {
				t.Fatal("expected invalid event rate budget to fail validation")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error to contain %q, got %v", tc.want, err)
			}
		})
	}
}

func TestFingerprintIsStable(t *testing.T) {
	p := DefaultPolicy()
	a, err := p.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	b, err := p.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint again: %v", err)
	}
	if a == "" || a != b {
		t.Fatalf("fingerprint not stable: %q vs %q", a, b)
	}
}
