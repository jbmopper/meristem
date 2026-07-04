package safety

import (
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestEveryProfileValidates(t *testing.T) {
	for name, p := range Profiles() {
		if err := p.Validate(); err != nil {
			t.Errorf("profile %q does not validate: %v", name, err)
		}
	}
}

func TestProfileFingerprintsAreDistinct(t *testing.T) {
	seen := make(map[string]string)
	for name, p := range Profiles() {
		fp, err := p.Fingerprint()
		if err != nil {
			t.Fatalf("fingerprint %q: %v", name, err)
		}
		if other, dup := seen[fp]; dup {
			t.Errorf("profiles %q and %q share fingerprint %s", name, other, fp)
		}
		seen[fp] = name
	}
}

func TestProfileByNameUnknownIsStructured(t *testing.T) {
	_, err := ProfileByName("mellow")
	if err == nil {
		t.Fatal("expected error for unknown profile")
	}
	msg := err.Error()
	if !strings.Contains(msg, "mellow") || !strings.Contains(msg, ProfileSteady) || !strings.Contains(msg, ProfileBringUp) {
		t.Errorf("unknown-profile error should name the request and the known set, got: %s", msg)
	}
}

func TestValidateRejectsEffectivelyInfinitePatience(t *testing.T) {
	p := DefaultPolicy()
	p.PatienceBudgets[domain.WorkItemCaptured] = 365 * 24 * time.Hour
	if err := p.Validate(); err == nil {
		t.Fatal("expected a patience budget beyond the finite cap to fail validation")
	}
}

func TestMustValidateStartupCoversAllProfiles(t *testing.T) {
	if _, err := MustValidateStartup(); err != nil {
		t.Fatalf("startup validation failed: %v", err)
	}
}
