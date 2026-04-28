// `meristem git` forwards to the real git(1) on PATH so all version-control
// operations are invoked consistently through the meristem binary.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
)

func runGit(ctx context.Context, _ *slog.Logger, args []string) error {
	if len(args) < 1 {
		return errors.New(`git: missing arguments (example: "meristem git status")`)
	}
	bin, err := exec.LookPath("git")
	if err != nil {
		return fmt.Errorf("git: %w", err)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		os.Exit(exitErr.ExitCode())
	}
	return err
}
