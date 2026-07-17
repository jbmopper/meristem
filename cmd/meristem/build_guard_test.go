package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/buildguard"
)

const (
	testBuildCommitA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testBuildCommitB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestCheckCommandBuildRefusesStatefulCommandButLeavesDiagnostics(t *testing.T) {
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		return buildguard.Status{
			State:            buildguard.StateMismatch,
			CompiledCommit:   testBuildCommitA,
			ExpectedCommit:   testBuildCommitB,
			CompiledMetadata: buildguard.CompiledValid,
			Reason:           "compiled commit does not match the reviewed v1 pin",
		}
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := checkCommandBuild("worker", provider, logger); err == nil {
		t.Fatal("worker build check succeeded, want refusal")
	}
	for _, command := range []string{"api", "mcp"} {
		if err := checkCommandBuild(command, provider, logger); err != nil {
			t.Fatalf("%s build check = %v, want diagnostic-only process allowed to start", command, err)
		}
	}
	for _, command := range []string{"version", "healthcheck", "help", "safety", "git", "export-context"} {
		if err := checkCommandBuild(command, provider, logger); err != nil {
			t.Fatalf("%s build check = %v, want diagnostic command allowed", command, err)
		}
	}
}

func TestCheckCommandBuildAllowsUnmanagedDevelopment(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	if err := checkCommandBuild("worker", buildguard.Disabled(), logger); err != nil {
		t.Fatalf("unmanaged development build refused: %v", err)
	}
	if err := checkCommandBuild("api", buildguard.Disabled(), logger); err != nil {
		t.Fatalf("unmanaged API development build refused: %v", err)
	}
	if !bytes.Contains(logs.Bytes(), []byte("unmanaged build")) {
		t.Fatalf("unmanaged runtime did not log its status: %s", logs.String())
	}
}

func TestWorkerScanRechecksBuildBeforeDatabaseOrQueueAccess(t *testing.T) {
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		return buildguard.Status{
			State:            buildguard.StateMismatch,
			CompiledCommit:   testBuildCommitA,
			ExpectedCommit:   testBuildCommitB,
			CompiledMetadata: buildguard.CompiledValid,
			Reason:           "compiled commit does not match the reviewed v1 pin",
		}
	})
	runtime := &workerRuntime{build: provider}

	if _, err := runtime.ScanOnce(context.Background()); !errors.Is(err, buildguard.ErrBlocked) {
		t.Fatalf("ScanOnce error = %v, want buildguard.ErrBlocked before nil database access", err)
	}
}

func TestRunVersionCommitReportsGuardFingerprint(t *testing.T) {
	var output bytes.Buffer
	if err := runVersion(&output, []string{"--commit"}, buildguard.Disabled(testBuildCommitA)); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != testBuildCommitA+"\n" {
		t.Fatalf("version --commit = %q, want full guard fingerprint", got)
	}
}

func TestRunBuildGuardStatusReportsVersionedCapability(t *testing.T) {
	var output bytes.Buffer
	if err := runBuildGuardStatus(&output, nil, buildguard.Disabled(testBuildCommitA)); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != buildGuardProtocol+" "+testBuildCommitA+"\n" {
		t.Fatalf("build-guard-status = %q, want versioned capability and full fingerprint", got)
	}
	if err := runBuildGuardStatus(io.Discard, []string{"unexpected"}, buildguard.Disabled(testBuildCommitA)); err == nil {
		t.Fatal("build-guard-status accepted trailing arguments")
	}
}
