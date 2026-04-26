package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/jbmopper/meristem/internal/safety"
)

func runSafety(_ context.Context, logger *slog.Logger, args []string) error {
	if len(args) != 1 || args[0] != "check" {
		safetyUsage(os.Stderr)
		return fmt.Errorf("safety: unknown command (want \"check\")")
	}

	policy, policyID, err := validateStartupSafety(logger)
	if err != nil {
		return err
	}

	out := struct {
		Status              string           `json:"status"`
		SafetyPolicy        string           `json:"safety_policy"`
		MaxRequestBodyBytes int64            `json:"max_request_body_bytes"`
		MaxFeedWaitSeconds  int64            `json:"max_feed_wait_seconds"`
		PatienceSeconds     map[string]int64 `json:"patience_seconds"`
	}{
		Status:              "ok",
		SafetyPolicy:        policyID,
		MaxRequestBodyBytes: policy.MaxRequestBodyBytes,
		MaxFeedWaitSeconds:  int64(policy.MaxFeedWait.Seconds()),
		PatienceSeconds:     make(map[string]int64, len(policy.PatienceBudgets)),
	}
	for state, budget := range policy.PatienceBudgets {
		out.PatienceSeconds[string(state)] = int64(budget.Seconds())
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func validateStartupSafety(logger *slog.Logger) (safety.Policy, string, error) {
	policy, err := safety.MustValidateStartup()
	if err != nil {
		return safety.Policy{}, "", err
	}
	policyID, _ := policy.Fingerprint()
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("resource safety controls validated", slog.String("safety_policy", policyID))
	return policy, policyID, nil
}

func safetyUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem safety check

Validates the deterministic resource-safety controls required before the
system is allowed to run. This command does not touch Postgres; it is safe to
run before restart, before migrations, and in CI.
`)
}
