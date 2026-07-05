package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestRunSafetyCheckPrintsPolicy(t *testing.T) {
	var stdout bytes.Buffer
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = oldStdout })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := runSafety(context.Background(), logger, []string{"check"}); err != nil {
		t.Fatalf("runSafety: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	if _, err := io.Copy(&stdout, r); err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"status": "ok"`,
		`"safety_policy":`,
		`"max_request_body_bytes":`,
		`"max_children_per_item":`,
		`"max_delegation_depth":`,
		`"patience_seconds":`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("safety check output missing %q:\n%s", want, out)
		}
	}
}
