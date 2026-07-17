// Package buildguard prevents a Meristem process compiled from one revision
// from presenting itself as the owner-reviewed v1 binary from another.
//
// The compiled fingerprint and the v1 pin deliberately have independent
// provenance. The former is stamped by the Go linker; the latter is a sibling
// file published by the release script. Guard.Status rereads the pin on every
// call so an already-running process notices when v1 advances.
package buildguard

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

const (
	// PinFileEnv overrides the default sibling pin location.
	PinFileEnv = "MERISTEM_V1_PIN_FILE"
)

// linkedCommit is set by scripts/rebuild-meristem-bin.sh with:
//
//	-X github.com/jbmopper/meristem/internal/buildguard.linkedCommit=<full SHA>
//
// Do not fall back to a shortened version string: only a full commit can be
// compared to the separately published reviewed-v1 pin.
var linkedCommit = "dev"

// State is the relationship between this process and the reviewed-v1 pin.
type State string

const (
	StateCurrent   State = "current"
	StateUnmanaged State = "unmanaged"
	StateMismatch  State = "mismatch"
	StateMissing   State = "missing"
	StateMalformed State = "malformed"
)

// CompiledMetadata describes the integrity of the fingerprint embedded in the
// running binary. Dirty and invalid metadata are reported separately from pin
// state so an unmanaged development build remains non-blocking but loud.
type CompiledMetadata string

const (
	CompiledValid   CompiledMetadata = "valid"
	CompiledDirty   CompiledMetadata = "dirty"
	CompiledInvalid CompiledMetadata = "invalid"
)

// Status is safe to expose through readiness and MCP initialize responses. It
// intentionally contains no filesystem path and never includes malformed pin
// contents.
type Status struct {
	State            State            `json:"state"`
	CompiledCommit   string           `json:"compiled_commit,omitempty"`
	ExpectedCommit   string           `json:"expected_commit,omitempty"`
	CompiledMetadata CompiledMetadata `json:"compiled_metadata"`
	Reason           string           `json:"reason"`
}

// Blocking reports whether authoritative reads and writes must be refused.
// An unmanaged process is intentionally non-blocking for local development;
// every managed-pin failure blocks.
func (s Status) Blocking() bool {
	return s.State != StateCurrent && s.State != StateUnmanaged
}

// Current reports whether the compiled fingerprint exactly matches a readable,
// well-formed reviewed-v1 pin and the build metadata is clean.
func (s Status) Current() bool { return s.State == StateCurrent }

// Managed reports whether a pin has been explicitly configured or observed.
func (s Status) Managed() bool { return s.State != StateUnmanaged }

// Warning returns the stable, secret-free explanation callers should surface.
// It is empty only for a current build.
func (s Status) Warning() string {
	if s.Current() {
		return ""
	}
	return s.Reason
}

// Version returns the full compiled commit when it is valid, and "unknown"
// otherwise. This is suitable for MCP serverInfo.version.
func (s Status) Version() string {
	if s.CompiledMetadata == CompiledValid && isFullCommit(s.CompiledCommit) {
		return s.CompiledCommit
	}
	return "unknown"
}

// StatusProvider dynamically reports build consistency.
type StatusProvider interface {
	Status() Status
}

// ProviderFunc adapts a function to StatusProvider.
type ProviderFunc func() Status

func (f ProviderFunc) Status() Status { return f() }

// ErrBlocked marks a managed build-consistency failure.
var ErrBlocked = errors.New("build consistency check blocked")

// RequireNonBlocking returns nil for current and explicitly unmanaged builds.
// Blocking errors contain only enumerated state/reason text and validated
// commit fingerprints; they never include a configured pin path or malformed
// pin contents.
func RequireNonBlocking(provider StatusProvider) error {
	if provider == nil {
		return nil
	}
	status := provider.Status()
	if !status.Blocking() {
		return nil
	}
	compiled := status.CompiledCommit
	if !isFullCommit(compiled) {
		compiled = "unknown"
	}
	expected := status.ExpectedCommit
	if !isFullCommit(expected) {
		expected = "unknown"
	}
	return fmt.Errorf("%w: state=%s reason=%s compiled=%s expected=%s",
		ErrBlocked, status.State, safeBlockingReason(status), compiled, expected)
}

// Disabled returns a non-blocking provider for tests and explicitly unmanaged
// embedding contexts. If supplied, version must be a full commit to be exposed.
func Disabled(version ...string) StatusProvider {
	commit := ""
	metadata := CompiledInvalid
	if len(version) > 0 && isFullCommit(version[0]) {
		commit = version[0]
		metadata = CompiledValid
	}
	return ProviderFunc(func() Status {
		return Status{
			State:            StateUnmanaged,
			CompiledCommit:   commit,
			CompiledMetadata: metadata,
			Reason:           "build consistency guard is disabled",
		}
	})
}

// Config exists to make the pure guard behavior testable. Production callers
// should use New.
type Config struct {
	CompiledCommit   string
	CompiledModified bool
	CompiledInvalid  bool
	PinFile          string
	PinExplicit      bool
}

// Guard compares one process fingerprint against a dynamically read pin.
type Guard struct {
	compiledCommit   string
	compiledModified bool
	compiledInvalid  bool
	pinFile          string
	pinExplicit      bool
	seenPin          atomic.Bool
}

// New constructs the process guard. MERISTEM_V1_PIN_FILE is an explicit,
// required pin; otherwise the default is os.Executable()+".v1-pin".
func New() *Guard {
	commit, modified, invalid := runtimeFingerprint()
	pinFile, explicit := os.LookupEnv(PinFileEnv)
	if !explicit {
		if executable, err := os.Executable(); err == nil {
			pinFile = executable + ".v1-pin"
		}
	}
	return NewWithConfig(Config{
		CompiledCommit:   commit,
		CompiledModified: modified,
		CompiledInvalid:  invalid,
		PinFile:          pinFile,
		PinExplicit:      explicit,
	})
}

// NewWithConfig constructs a guard with injected build metadata and pin
// location. It performs no cached pin read; it only remembers whether a pin
// existed at construction so later deletion fails closed.
func NewWithConfig(config Config) *Guard {
	g := &Guard{
		compiledCommit:   config.CompiledCommit,
		compiledModified: config.CompiledModified,
		compiledInvalid:  config.CompiledInvalid,
		pinFile:          config.PinFile,
		pinExplicit:      config.PinExplicit,
	}
	if config.PinFile != "" {
		if _, err := os.Stat(config.PinFile); err == nil || !errors.Is(err, os.ErrNotExist) {
			g.seenPin.Store(true)
		}
	}
	return g
}

// Status rereads the pin every time. The only retained state is whether the
// default pin has ever existed, which makes deleting a once-managed pin block.
func (g *Guard) Status() Status {
	compiledCommit := ""
	metadata := CompiledValid
	if g.compiledInvalid || !isFullCommit(g.compiledCommit) {
		metadata = CompiledInvalid
	} else {
		compiledCommit = g.compiledCommit
		if g.compiledModified {
			metadata = CompiledDirty
		}
	}

	if g.pinFile == "" {
		if g.pinExplicit || isFullCommit(g.compiledCommit) {
			return managedFailure(StateMissing, compiledCommit, "configured v1 pin is missing", metadata)
		}
		return unmanagedStatus(compiledCommit, metadata)
	}

	raw, err := readPin(g.pinFile)
	if err != nil {
		if g.pinExplicit || g.seenPin.Load() || isFullCommit(g.compiledCommit) || !errors.Is(err, os.ErrNotExist) {
			return managedFailure(StateMissing, compiledCommit, "reviewed v1 pin is missing or unreadable", metadata)
		}
		return unmanagedStatus(compiledCommit, metadata)
	}
	g.seenPin.Store(true)

	expected, ok := parsePin(raw)
	if !ok {
		return managedFailure(StateMalformed, compiledCommit, "reviewed v1 pin is malformed", metadata)
	}
	if metadata == CompiledInvalid {
		return Status{
			State:            StateMismatch,
			ExpectedCommit:   expected,
			CompiledMetadata: metadata,
			Reason:           "compiled commit metadata is invalid",
		}
	}
	if metadata == CompiledDirty {
		return Status{
			State:            StateMismatch,
			CompiledCommit:   compiledCommit,
			ExpectedCommit:   expected,
			CompiledMetadata: metadata,
			Reason:           "compiled build is dirty",
		}
	}
	if compiledCommit != expected {
		return Status{
			State:            StateMismatch,
			CompiledCommit:   compiledCommit,
			ExpectedCommit:   expected,
			CompiledMetadata: metadata,
			Reason:           "compiled commit does not match the reviewed v1 pin",
		}
	}
	return Status{
		State:            StateCurrent,
		CompiledCommit:   compiledCommit,
		ExpectedCommit:   expected,
		CompiledMetadata: metadata,
		Reason:           "compiled commit matches the reviewed v1 pin",
	}
}

// readPin bounds the only runtime file read. Valid pins are at most 41 bytes
// (40 lowercase hex characters plus an optional LF); the 42nd byte is enough
// to classify the file as malformed without allocating based on its size.
func readPin(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, 42))
}

func managedFailure(state State, compiledCommit, reason string, metadata CompiledMetadata) Status {
	return Status{
		State:            state,
		CompiledCommit:   compiledCommit,
		CompiledMetadata: metadata,
		Reason:           reason,
	}
}

func unmanagedStatus(compiledCommit string, metadata CompiledMetadata) Status {
	reason := "no reviewed v1 pin is configured or present"
	switch metadata {
	case CompiledDirty:
		reason += "; compiled build is dirty"
	case CompiledInvalid:
		reason += "; compiled commit metadata is invalid"
	}
	return Status{
		State:            StateUnmanaged,
		CompiledCommit:   compiledCommit,
		CompiledMetadata: metadata,
		Reason:           reason,
	}
}

func parsePin(raw []byte) (string, bool) {
	value := string(raw)
	if strings.HasSuffix(value, "\n") {
		value = strings.TrimSuffix(value, "\n")
	}
	if !isFullCommit(value) {
		return "", false
	}
	return value, true
}

func isFullCommit(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func runtimeFingerprint() (commit string, modified, invalid bool) {
	commit = linkedCommit
	if !isFullCommit(commit) {
		invalid = true
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return commit, false, invalid
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if setting.Value != "" && (!isFullCommit(setting.Value) || (isFullCommit(commit) && setting.Value != commit)) {
				invalid = true
			}
		case "vcs.modified":
			switch setting.Value {
			case "true":
				modified = true
			case "false", "":
			default:
				invalid = true
			}
		}
	}
	return commit, modified, invalid
}

func safeBlockingReason(status Status) string {
	switch status.State {
	case StateMissing:
		return "reviewed v1 pin is missing or unreadable"
	case StateMalformed:
		return "reviewed v1 pin is malformed"
	case StateMismatch:
		switch status.CompiledMetadata {
		case CompiledDirty:
			return "compiled build is dirty"
		case CompiledInvalid:
			return "compiled commit metadata is invalid"
		default:
			return "compiled commit does not match the reviewed v1 pin"
		}
	default:
		return "managed build is not current"
	}
}
