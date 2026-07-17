package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/buildguard"
)

func TestCheckedIntervalLoopStopsBeforeSecondTickWhenBuildChanges(t *testing.T) {
	checks := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		checks++
		if checks == 1 {
			return buildguard.Status{
				State:            buildguard.StateCurrent,
				CompiledCommit:   testBuildCommitA,
				ExpectedCommit:   testBuildCommitA,
				CompiledMetadata: buildguard.CompiledValid,
				Reason:           "compiled commit matches the reviewed v1 pin",
			}
		}
		return buildguard.Status{
			State:            buildguard.StateMismatch,
			CompiledCommit:   testBuildCommitA,
			ExpectedCommit:   testBuildCommitB,
			CompiledMetadata: buildguard.CompiledValid,
			Reason:           "compiled commit does not match the reviewed v1 pin",
		}
	})

	ticks := 0
	err := runCheckedIntervalLoop(
		context.Background(),
		time.Nanosecond,
		func() error { return buildguard.RequireNonBlocking(provider) },
		func() error {
			ticks++
			return nil
		},
		nil,
	)
	if !errors.Is(err, buildguard.ErrBlocked) {
		t.Fatalf("loop error = %v, want buildguard.ErrBlocked", err)
	}
	if checks != 2 {
		t.Fatalf("build checks = %d, want 2", checks)
	}
	if ticks != 1 {
		t.Fatalf("tick side effects = %d, want 1 before the pin changed", ticks)
	}
}

func TestCheckedIntervalLoopDoesNotRetryBlockedTick(t *testing.T) {
	ticks := 0
	reported := 0
	err := runCheckedIntervalLoop(
		context.Background(),
		time.Hour,
		nil,
		func() error {
			ticks++
			return buildguard.ErrBlocked
		},
		func(error) { reported++ },
	)
	if !errors.Is(err, buildguard.ErrBlocked) {
		t.Fatalf("loop error = %v, want buildguard.ErrBlocked", err)
	}
	if ticks != 1 {
		t.Fatalf("ticks = %d, want 1", ticks)
	}
	if reported != 0 {
		t.Fatalf("ordinary retry reports = %d, want 0 for a blocking error", reported)
	}
}
