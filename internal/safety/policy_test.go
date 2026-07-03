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
