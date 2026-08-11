package main

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/jbmopper/meristem/internal/buildguard"
)

const buildGuardProtocol = "meristem-build-guard-v1"

func runVersion(w io.Writer, args []string, build buildguard.StatusProvider) error {
	if len(args) == 0 {
		_, err := fmt.Fprintln(w, version)
		return err
	}
	if len(args) == 1 && args[0] == "--commit" {
		_, err := fmt.Fprintln(w, build.Status().Version())
		return err
	}
	return fmt.Errorf("version: expected no arguments or --commit")
}

// runBuildGuardStatus is the unambiguous launcher capability probe. Historical
// Meristem binaries accepted and ignored trailing `version` arguments, so
// `version --commit` alone cannot prove that a binary contains the dynamic
// build guard. Pre-guard binaries reject this dedicated command entirely.
func runBuildGuardStatus(w io.Writer, args []string, build buildguard.StatusProvider) error {
	if len(args) != 0 {
		return fmt.Errorf("build-guard-status: expected no arguments")
	}
	_, err := fmt.Fprintf(w, "%s %s\n", buildGuardProtocol, build.Status().Version())
	return err
}

// checkCommandBuild keeps diagnostic/file-only commands available while a
// stale runtime is being repaired, but refuses every command that can read or
// mutate shared coordination state when a managed reviewed-v1 pin diverges.
func checkCommandBuild(command string, build buildguard.StatusProvider, logger *slog.Logger) error {
	if !commandUsesCoordinationState(command) {
		return nil
	}
	status := build.Status()
	// API readiness and MCP initialize are the diagnostic surfaces that tell
	// clients why a process is stale. Let those two processes start; their
	// route/tool boundaries and guarded event writer still refuse every
	// authoritative operation dynamically.
	if command == "api" || command == "mcp" {
		if status.Blocking() && logger != nil {
			logger.Error("starting diagnostic-only stale process",
				slog.String("command", command),
				slog.String("build_state", string(status.State)),
				slog.String("reason", status.Warning()),
			)
		} else if !status.Current() && logger != nil {
			logger.Warn("starting diagnostic runtime with an unmanaged build",
				slog.String("command", command),
				slog.String("build_state", string(status.State)),
				slog.String("compiled_commit", status.CompiledCommit),
				slog.String("compiled_metadata", string(status.CompiledMetadata)),
				slog.String("reason", status.Warning()),
			)
		}
		return nil
	}
	if err := buildguard.RequireNonBlocking(build); err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	if !status.Current() && logger != nil {
		logger.Warn("running with an unmanaged build",
			slog.String("command", command),
			slog.String("build_state", string(status.State)),
			slog.String("compiled_commit", status.CompiledCommit),
			slog.String("compiled_metadata", string(status.CompiledMetadata)),
			slog.String("reason", status.Warning()),
		)
	}
	return nil
}

func commandUsesCoordinationState(command string) bool {
	switch command {
	case "api", "migrate", "tokens", "mcp", "seed", "node", "rebuild", "export", "worker", "spoke", "feed", "listener":
		return true
	default:
		return false
	}
}
