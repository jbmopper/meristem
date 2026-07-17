package buildguard

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	commitA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	commitB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestGuardCurrentAndDynamicMismatch(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "meristem.v1-pin")
	writePin(t, pin, commitA+"\n")
	g := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin})

	status := g.Status()
	if !status.Current() || status.Blocking() || status.Version() != commitA {
		t.Fatalf("current status = %+v", status)
	}

	writePin(t, pin, commitB+"\n")
	status = g.Status()
	if status.State != StateMismatch || !status.Blocking() || status.ExpectedCommit != commitB {
		t.Fatalf("mismatch status = %+v", status)
	}
}

func TestGuardReleasePinLifecycleAndRestartAfterDeletion(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "meristem.v1-pin")
	g := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin})

	if status := g.Status(); status.State != StateMissing || !status.Blocking() {
		t.Fatalf("initial status = %+v", status)
	}
	writePin(t, pin, commitA+"\n")
	if status := g.Status(); status.State != StateCurrent {
		t.Fatalf("status after pin appears = %+v", status)
	}
	if err := os.Remove(pin); err != nil {
		t.Fatal(err)
	}
	if status := g.Status(); status.State != StateMissing || !status.Blocking() {
		t.Fatalf("status after pin removal = %+v", status)
	}
	restarted := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin})
	if status := restarted.Status(); status.State != StateMissing || !status.Blocking() {
		t.Fatalf("restarted release without pin = %+v", status)
	}
}

func TestGuardUnstampedDevelopmentWithoutPinIsUnmanaged(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "missing")
	status := NewWithConfig(Config{CompiledCommit: "dev", PinFile: pin}).Status()
	if status.State != StateUnmanaged || status.Blocking() {
		t.Fatalf("unstamped development status = %+v", status)
	}
}

func TestGuardExplicitMissingPinBlocks(t *testing.T) {
	g := NewWithConfig(Config{
		CompiledCommit: commitA,
		PinFile:        filepath.Join(t.TempDir(), "missing"),
		PinExplicit:    true,
	})
	if status := g.Status(); status.State != StateMissing || !status.Blocking() {
		t.Fatalf("status = %+v", status)
	}
}

func TestGuardRejectsMalformedPins(t *testing.T) {
	for _, value := range []string{
		"short\n",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
		commitA + " \n",
		commitA + "\n\n",
		commitA + "\r\n",
	} {
		t.Run(value, func(t *testing.T) {
			pin := filepath.Join(t.TempDir(), "pin")
			writePin(t, pin, value)
			status := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin}).Status()
			if status.State != StateMalformed || !status.Blocking() || status.ExpectedCommit != "" {
				t.Fatalf("status = %+v", status)
			}
		})
	}
}

func TestGuardBoundsOversizedPinRead(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "pin")
	writePin(t, pin, strings.Repeat("a", 1<<20))
	status := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin}).Status()
	if status.State != StateMalformed || !status.Blocking() {
		t.Fatalf("oversized pin status = %+v", status)
	}
}

func TestGuardDirtyAndInvalidCompiledMetadata(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "pin")
	writePin(t, pin, commitA+"\n")

	dirty := NewWithConfig(Config{CompiledCommit: commitA, CompiledModified: true, PinFile: pin}).Status()
	if dirty.State != StateMismatch || dirty.CompiledMetadata != CompiledDirty || !dirty.Blocking() || dirty.Version() != "unknown" {
		t.Fatalf("dirty status = %+v", dirty)
	}

	invalid := NewWithConfig(Config{CompiledCommit: "dev", PinFile: pin}).Status()
	if invalid.State != StateMismatch || invalid.CompiledMetadata != CompiledInvalid || invalid.CompiledCommit != "" || !invalid.Blocking() {
		t.Fatalf("invalid status = %+v", invalid)
	}

	unmanaged := NewWithConfig(Config{CompiledCommit: "dev", PinFile: filepath.Join(t.TempDir(), "absent")}).Status()
	if unmanaged.State != StateUnmanaged || unmanaged.CompiledMetadata != CompiledInvalid || unmanaged.Blocking() {
		t.Fatalf("unmanaged invalid status = %+v", unmanaged)
	}
}

func TestStatusDoesNotExposePinPathOrMalformedContents(t *testing.T) {
	pin := filepath.Join(t.TempDir(), "secret-path")
	writePin(t, pin, "not-a-commit-or-safe-response\n")
	status := NewWithConfig(Config{CompiledCommit: commitA, PinFile: pin}).Status()
	for _, value := range []string{status.Reason, status.CompiledCommit, status.ExpectedCommit} {
		if value == pin || value == "not-a-commit-or-safe-response" {
			t.Fatalf("status exposed pin material: %+v", status)
		}
	}
}

func TestDisabledIsNonBlocking(t *testing.T) {
	status := Disabled(commitA).Status()
	if status.State != StateUnmanaged || status.Blocking() || status.Version() != commitA {
		t.Fatalf("status = %+v", status)
	}
}

func TestRequireNonBlockingUsesOnlySafeValues(t *testing.T) {
	provider := ProviderFunc(func() Status {
		return Status{
			State:            StateMalformed,
			CompiledCommit:   "/private/compiled-secret",
			ExpectedCommit:   "/private/pin-secret",
			CompiledMetadata: CompiledInvalid,
			Reason:           "/private/config/meristem.v1-pin",
		}
	})
	err := RequireNonBlocking(provider)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("error = %v", err)
	}
	if got := err.Error(); got != "build consistency check blocked: state=malformed reason=reviewed v1 pin is malformed compiled=unknown expected=unknown" {
		t.Fatalf("error = %q", got)
	}
	if err := RequireNonBlocking(Disabled()); err != nil {
		t.Fatalf("disabled provider: %v", err)
	}
}

func writePin(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
