package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
)

func runProvider(_ context.Context, _ *slog.Logger, args []string) error {
	providerUsage(os.Stderr)
	if len(args) == 0 {
		return fmt.Errorf("provider command removed in this branch")
	}
	return fmt.Errorf("provider command removed in this branch; first arg=%q", args[0])
}

func providerUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider is no longer available in this branch.

Worker handoff is MCP-native:
  meristem mcp

Use docs/mcp-worker-bootstrap.md for the approved bootstrap text.
`)
}
