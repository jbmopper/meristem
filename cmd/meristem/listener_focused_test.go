package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/buildguard"
)

func TestActivationAdapterEnvironmentIsVendorNeutralAndDeniesSecrets(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "019f6309-db25-75c2-b87d-41d3050581db")
	t.Setenv("MERISTEM_LISTENER_CODEX_HOME", "/tmp/ambient-codex-home")
	t.Setenv("MERISTEM_LISTENER_CODEX_SQLITE_HOME", "/tmp/ambient-codex-state")
	t.Setenv("CODEX_MERISTEM_TOKEN_FILE", "/tmp/ambient-must-not-pass.token")
	t.Setenv("MERISTEM_TOKEN", "raw-bearer-must-not-pass")
	t.Setenv("MERISTEM_DATABASE_URL", "postgres://must-not-pass")
	t.Setenv("MERISTEM_MCP_EXPECT_ACTOR_ID", uuid.NewString())
	t.Setenv("MERISTEM_MCP_LISTENER_ACTIVATION_ID", uuid.NewString())

	got := make(map[string]string)
	for _, entry := range activationAdapterEnvironment() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed adapter environment entry %q", entry)
		}
		got[key] = value
	}

	for _, key := range []string{
		"MERISTEM_TOKEN", "MERISTEM_DATABASE_URL", "CODEX_MERISTEM_TOKEN_FILE",
		"CODEX_THREAD_ID", "MERISTEM_LISTENER_CODEX_HOME",
		"MERISTEM_LISTENER_CODEX_SQLITE_HOME", "MERISTEM_MCP_EXPECT_ACTOR_ID",
		"MERISTEM_MCP_LISTENER_ACTIVATION_ID",
	} {
		if _, ok := got[key]; ok {
			t.Fatalf("sensitive %s unexpectedly passed to activation adapter", key)
		}
	}
}

func TestActivationSecurityProfileIsMandatoryAndFailClosed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	currentBuild := buildguard.ProviderFunc(func() buildguard.Status {
		return buildguard.Status{
			State: buildguard.StateCurrent, CompiledCommit: strings.Repeat("a", 40),
			CompiledMetadata: buildguard.CompiledValid,
		}
	})
	adapter := filepath.Join(t.TempDir(), "adapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	for name, args := range map[string][]string{
		"profile_without_adapter": {
			"--name", "test", "--activation-security-profile", activationSecurityProfileMeristemGitV1,
		},
		"missing_profile": {
			"--name", "test", "--activation-adapter", adapter,
			"--activation-binding-generation", "binding", "--activation-consumer-generation", "consumer",
		},
		"unknown_profile": {
			"--name", "test", "--activation-adapter", adapter,
			"--activation-security-profile", "unknown-v1",
			"--activation-binding-generation", "binding", "--activation-consumer-generation", "consumer",
		},
		"whitespace_wrapped_profile": {
			"--name", "test", "--activation-adapter", adapter,
			"--activation-security-profile", " " + activationSecurityProfileMeristemGitV1,
			"--activation-binding-generation", "binding", "--activation-consumer-generation", "consumer",
		},
		"known_profile_missing_fields": {
			"--name", "test", "--activation-adapter", adapter,
			"--activation-security-profile", activationSecurityProfileMeristemGitV1,
			"--activation-binding-generation", "binding", "--activation-consumer-generation", "consumer",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := runListener(context.Background(), logger, args, currentBuild); err == nil {
				t.Fatal("invalid activation profile configuration unexpectedly accepted")
			}
		})
	}
}

func TestTaskPrincipalMustDifferFromRegistrationPrincipal(t *testing.T) {
	principalID := uuid.New()
	sup := &listenerSupervisor{activationAdapter: "/unused", activationTaskPrincipalID: principalID}
	if err := sup.requireTaskPrincipalSeparationFromRegistration(listenerView{PrincipalTokenID: principalID}); err == nil {
		t.Fatal("same listener/task principal unexpectedly accepted")
	}
	if err := sup.requireTaskPrincipalSeparationFromRegistration(listenerView{PrincipalTokenID: uuid.New()}); err != nil {
		t.Fatalf("separate task principal rejected: %v", err)
	}
}

func TestVerifyActivationBundleRejectsWorkingTreeDrift(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is unavailable")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scripts := filepath.Join(root, "scripts")
	if err := os.Mkdir(scripts, 0o700); err != nil {
		t.Fatal(err)
	}
	adapter := filepath.Join(scripts, "adapter")
	dependency := filepath.Join(scripts, "dependency")
	cleanTarget := filepath.Join(scripts, "tool")
	largeHelper := filepath.Join(scripts, "large-helper")
	dataModule := filepath.Join(scripts, "module.py")
	for path, contents := range map[string]string{
		adapter:     "#!/bin/sh\nexit 0\n",
		dependency:  "#!/bin/sh\nexit 0\n",
		cleanTarget: "#!/bin/sh\nexit 0\n",
		largeHelper: "#!/bin/sh\n#" + strings.Repeat("x", 2<<20) + "\nexit 0\n",
		dataModule:  "VALUE = 1\n",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dataModule, 0o600); err != nil {
		t.Fatal(err)
	}
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(gitPath, append([]string{"-C", root}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Listener Test", "GIT_AUTHOR_EMAIL=listener@example.invalid",
			"GIT_COMMITTER_NAME=Listener Test", "GIT_COMMITTER_EMAIL=listener@example.invalid",
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("init", "-q")
	runGit("add", "scripts")
	runGit("-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture")
	commit := runGit("rev-parse", "HEAD")

	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency", "scripts/module.py"}, []string{largeHelper}, commit); err != nil {
		t.Fatalf("reviewed activation bundle rejected: %v", err)
	}
	outsideRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outsideExecutable := filepath.Join(outsideRoot, "outside-adapter-helper")
	if err := os.WriteFile(outsideExecutable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, []string{"--helper", outsideExecutable}, commit); err == nil ||
		!strings.Contains(err.Error(), "executable activation argument") {
		t.Fatalf("outside executable activation argument error = %v", err)
	}
	taskPrincipalID := uuid.New()
	listenerPrincipalID := uuid.New()
	registrationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"listener": listenerView{ID: uuid.New(), Name: "test", PrincipalTokenID: listenerPrincipalID}})
	}))
	t.Cleanup(registrationServer.Close)
	var statusReads atomic.Int32
	changingBuild := buildguard.ProviderFunc(func() buildguard.Status {
		if statusReads.Add(1) == 1 {
			return buildguard.Status{
				State: buildguard.StateCurrent, CompiledCommit: commit,
				CompiledMetadata: buildguard.CompiledValid,
			}
		}
		return buildguard.Status{
			State: buildguard.StateMismatch, CompiledCommit: commit,
			ExpectedCommit: strings.Repeat("b", 40), CompiledMetadata: buildguard.CompiledValid,
		}
	})
	changing := &listenerSupervisor{
		api: registrationServer.URL, name: "test", token: "listener-secret", http: registrationServer.Client(), build: changingBuild,
		activationAdapter: adapter, activationCheckoutRoot: root,
		activationBundlePaths:     []string{"scripts/dependency"},
		activationTaskPrincipalID: taskPrincipalID,
		activationSecurityProfile: activationSecurityProfileMeristemGitV1,
	}
	if err := changing.requireActivationPreflight(context.Background()); err == nil || !strings.Contains(err.Error(), "changed during preflight") {
		t.Fatalf("mid-preflight build change error = %v", err)
	}
	for _, name := range []string{"outside=helper", "outside-helper "} {
		path := filepath.Join(outsideRoot, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, []string{path}, commit); err == nil ||
			!strings.Contains(err.Error(), "executable activation argument") {
			t.Fatalf("outside executable %q error = %v", name, err)
		}
	}
	symlinkArgument := filepath.Join(scripts, "outside-link")
	if err := os.Symlink(outsideExecutable, symlinkArgument); err != nil {
		t.Fatal(err)
	}
	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, []string{symlinkArgument}, commit); err == nil ||
		!strings.Contains(err.Error(), "symlink-free") {
		t.Fatalf("symlink executable argument error = %v", err)
	}
	externalParent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	externalChild := filepath.Join(externalParent, "child")
	if err := os.Mkdir(externalChild, 0o700); err != nil {
		t.Fatal(err)
	}
	externalViaDotDot := filepath.Join(externalParent, "tool")
	if err := os.WriteFile(externalViaDotDot, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	intermediateLink := filepath.Join(scripts, "intermediate-link")
	if err := os.Symlink(externalChild, intermediateLink); err != nil {
		t.Fatal(err)
	}
	rawDotDotPath := intermediateLink + string(filepath.Separator) + ".." + string(filepath.Separator) + "tool"
	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, []string{rawDotDotPath}, commit); err == nil ||
		!strings.Contains(err.Error(), "exact, symlink-free") {
		t.Fatalf("symlink plus dot-dot executable argument error = %v", err)
	}
	adapterLink := filepath.Join(scripts, "adapter-link")
	if err := os.Symlink(adapter, adapterLink); err != nil {
		t.Fatal(err)
	}
	if err := verifyActivationBundle(root, adapterLink, []string{"scripts/dependency"}, nil, commit); err == nil ||
		!strings.Contains(err.Error(), "activation adapter") {
		t.Fatalf("symlink adapter error = %v", err)
	}
	if err := os.WriteFile(dependency, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, nil, commit); err == nil ||
		!strings.Contains(err.Error(), "differs from the reviewed commit") {
		t.Fatalf("dirty activation bundle error = %v", err)
	}
	if err := os.WriteFile(dependency, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dependency, []byte("#!/bin/sh\nexit 2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runGit("add", "scripts/dependency")
	runGit("-c", "core.hooksPath=/dev/null", "commit", "-qm", "replacement fixture")
	replacement := runGit("rev-parse", "HEAD")
	runGit("replace", commit, replacement)
	runGit("update-ref", "HEAD", commit)
	if err := verifyActivationBundle(root, adapter, []string{"scripts/dependency"}, nil, commit); err == nil ||
		!strings.Contains(err.Error(), "differs from the reviewed commit") {
		t.Fatalf("Git replacement object error = %v", err)
	}
}

func TestFocusedTreatsActivationConflictAfterHandbackAsRelease(t *testing.T) {
	workItemID := uuid.New()
	assignmentEventID := uuid.New()
	listenerID := uuid.New()
	holderID := uuid.New()
	activationID := uuid.New()
	activationStateEventID := uuid.New()

	var assignmentReads atomic.Int32
	var ensureCalls atomic.Int32
	var beginCalls atomic.Int32
	var statusCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/listeners/by-name/codex-review":
			_ = json.NewEncoder(w).Encode(map[string]any{"listener": listenerView{ID: listenerID, Name: "codex-review", PrincipalTokenID: holderID}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/work-items/"+workItemID.String()+"/assignment":
			if assignmentReads.Add(1) > 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"assignment": map[string]any{
					"assignment_event_id": assignmentEventID,
					"holder_token_id":     holderID,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/listeners/"+listenerID.String()+"/activations/ensure":
			ensureCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"activation": map[string]any{
					"id":                  activationID,
					"state":               "requested",
					"state_event_id":      activationStateEventID,
					"assignment_event_id": assignmentEventID,
					"work_item_id":        workItemID,
					"binding_generation":  "binding-v1",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/listener-activations/"+activationID.String()+"/begin":
			beginCalls.Add(1)
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    "listener_activation_conflict",
					"message": "listeneractivation: no matching active assignment",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/work-items/"+workItemID.String()+"/events":
			statusCalls.Add(1)
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected listener request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	cursorDir := t.TempDir()
	focusCursor := filepath.Join(cursorDir, "focus-"+assignmentEventID.String()+".cursor")
	if err := os.WriteFile(focusCursor, []byte("durable-cursor"), 0o600); err != nil {
		t.Fatalf("write focus cursor: %v", err)
	}
	sup := &listenerSupervisor{
		api:                          server.URL,
		token:                        "listener-token",
		name:                         "codex-review",
		cursorDir:                    cursorDir,
		backoff:                      time.Millisecond,
		logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
		http:                         server.Client(),
		activationAdapter:            "/unused",
		activationTaskPrincipalID:    uuid.New(),
		activationPreflight:          func() error { return nil },
		activationBindingGeneration:  "binding-v1",
		activationConsumerGeneration: "consumer-v1",
	}
	held := heldAssignment{
		WorkItemID:        workItemID,
		AssignmentEventID: assignmentEventID,
		ListenerID:        &listenerID,
	}

	if err := sup.focused(context.Background(), listenerView{ID: listenerID, PrincipalTokenID: holderID}, held); err != nil {
		t.Fatalf("focused returned handback race as fatal: %v", err)
	}
	if got := assignmentReads.Load(); got != 2 {
		t.Fatalf("assignment projection reads = %d, want 2", got)
	}
	if got := ensureCalls.Load(); got != 1 {
		t.Fatalf("activation ensure calls = %d, want 1", got)
	}
	if got := beginCalls.Load(); got != 1 {
		t.Fatalf("activation begin calls = %d, want 1", got)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("release status calls = %d, want 1", got)
	}
	if _, err := os.Stat(focusCursor); !os.IsNotExist(err) {
		t.Fatalf("focus cursor still exists after observed release: %v", err)
	}
}
