package errorreporting

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestReportValidatesBeforeDatabase(t *testing.T) {
	svc := NewService(nil, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceSystem}
	cases := []struct {
		name string
		in   ReportInput
		want error
	}{
		{
			name: "component",
			in:   ReportInput{Code: "projection_failed", Message: "projection failed", Actor: actor},
			want: ErrComponentRequired,
		},
		{
			name: "code",
			in:   ReportInput{Component: "projections", Message: "projection failed", Actor: actor},
			want: ErrCodeRequired,
		},
		{
			name: "message",
			in:   ReportInput{Component: "projections", Code: "projection_failed", Actor: actor},
			want: ErrMessageRequired,
		},
		{
			name: "severity",
			in: ReportInput{
				Component: "projections",
				Code:      "projection_failed",
				Message:   "projection failed",
				Severity:  domain.DeterministicErrorSeverity("loud"),
				Actor:     actor,
			},
			want: ErrInvalidSeverity,
		},
		{
			name: "details",
			in: ReportInput{
				Component: "projections",
				Code:      "projection_failed",
				Message:   "projection failed",
				Details:   json.RawMessage(`["not","an","object"]`),
				Actor:     actor,
			},
			want: ErrInvalidDetails,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Report(context.Background(), tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestNormalizeDetailsDefaultsAndRejectsTrailingTokens(t *testing.T) {
	got, err := normalizeDetails(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{}` {
		t.Fatalf("empty details should default to {}, got %s", got)
	}

	_, err = normalizeDetails(json.RawMessage(`{} {}`))
	if !errors.Is(err, ErrInvalidDetails) {
		t.Fatalf("expected ErrInvalidDetails for trailing token, got %v", err)
	}
}

func TestSourceForActorDefaultsToSystem(t *testing.T) {
	if got := sourceForActor(domain.Token{}); got != domain.SourceSystem {
		t.Fatalf("expected SourceSystem fallback, got %q", got)
	}
	for _, src := range []domain.Source{domain.SourceHuman, domain.SourceAgent, domain.SourceSystem} {
		if got := sourceForActor(domain.Token{Source: src}); got != src {
			t.Errorf("expected %q, got %q", src, got)
		}
	}
}
