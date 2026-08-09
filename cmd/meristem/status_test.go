package main

// Regression coverage for the slice-0 status probe (review note on cc57fe6):
// output schema, invalid --work-item refusal, and the unreachable-database
// report. The probe must never error out just because the database is down —
// reporting that state is its job.

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/buildguard"
)

func TestStatusEmitSchema(t *testing.T) {
	var buf bytes.Buffer
	report := map[string]any{
		"generated_at": "2026-01-01T00:00:00Z",
		"build":        map[string]any{"protocol": buildGuardProtocol},
		"database":     map[string]any{"reachable": false},
	}
	if err := emitStatus(&buf, report, true); err != nil {
		t.Fatalf("emit json: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("emitted status is not valid JSON: %v (%s)", err, buf.String())
	}
	for _, key := range []string{"generated_at", "build", "database"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("status report missing %q: %s", key, buf.String())
		}
	}
}

func TestStatusRejectsInvalidWorkItem(t *testing.T) {
	t.Setenv("MERISTEM_DATABASE_URL", "postgres://meristem:meristem@127.0.0.1:1/meristem?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runStatus(ctx, nil, []string{"--work-item", "not-a-uuid"}, buildguard.Disabled())
	if err == nil {
		t.Fatal("invalid --work-item accepted")
	}
}

func TestStatusReportsUnreachableDatabaseWithoutFailing(t *testing.T) {
	t.Setenv("MERISTEM_DATABASE_URL", "postgres://meristem:meristem@127.0.0.1:1/meristem?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The probe reports the failure in the payload and exits zero; stdout
	// capture is not asserted here because runStatus writes to os.Stdout —
	// the contract under test is the non-error exit with a dead database.
	if err := runStatus(ctx, nil, []string{"--json"}, buildguard.Disabled()); err != nil {
		t.Fatalf("status errored on unreachable database: %v", err)
	}
}
