package errorreporting

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func TestPolicyForToken_RootCanReadEverything(t *testing.T) {
	policy := PolicyForToken(domain.Token{IsRoot: true})
	if !policy.CanRead || !policy.CanReadDetails || !policy.CanReadRestrictedDetails || !policy.CanReadMasked {
		t.Fatalf("root policy should allow all log visibility, got %+v", policy)
	}
}

func TestPolicyForToken_ScopesReduceToVisibility(t *testing.T) {
	cases := []struct {
		name   string
		scopes []string
		want   AccessPolicy
	}{
		{
			name:   "read only",
			scopes: []string{ScopeLogsRead},
			want:   AccessPolicy{CanRead: true},
		},
		{
			name:   "details implies read",
			scopes: []string{ScopeLogsReadDetails},
			want:   AccessPolicy{CanRead: true, CanReadDetails: true},
		},
		{
			name:   "restricted implies details and read",
			scopes: []string{ScopeLogsReadRestricted},
			want:   AccessPolicy{CanRead: true, CanReadDetails: true, CanReadRestrictedDetails: true},
		},
		{
			name:   "read all",
			scopes: []string{ScopeLogsReadAll},
			want:   AccessPolicy{CanRead: true, CanReadDetails: true, CanReadRestrictedDetails: true, CanReadMasked: true},
		},
		{
			name:   "no scopes",
			scopes: nil,
			want:   AccessPolicy{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PolicyForToken(domain.Token{Scopes: tc.scopes})
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAccessPolicy_FilterDetailsByVisibility(t *testing.T) {
	raw := []byte(`{
		"safe_id": "evt_123",
		"table": "events",
		"private_note": "do not show",
		"mystery": "fail closed",
		"_visibility": {
			"safe_id": "public",
			"private_note": "restricted",
			"mystery": "surprising-new-label"
		}
	}`)
	cases := []struct {
		name   string
		policy AccessPolicy
		want   map[string]string
	}{
		{
			name:   "read only sees public fields",
			policy: AccessPolicy{CanRead: true},
			want:   map[string]string{"safe_id": "evt_123"},
		},
		{
			name:   "details sees public and internal fields",
			policy: AccessPolicy{CanRead: true, CanReadDetails: true},
			want:   map[string]string{"safe_id": "evt_123", "table": "events"},
		},
		{
			name: "restricted sees all detail labels",
			policy: AccessPolicy{
				CanRead:                  true,
				CanReadDetails:           true,
				CanReadRestrictedDetails: true,
			},
			want: map[string]string{
				"safe_id":      "evt_123",
				"table":        "events",
				"private_note": "do not show",
				"mystery":      "fail closed",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotRaw := tc.policy.FilterDetails(raw)
			got := map[string]string{}
			if err := json.Unmarshal(gotRaw, &got); err != nil {
				t.Fatalf("filtered details are not a JSON object: %v: %s", err, string(gotRaw))
			}
			if _, ok := got[detailsVisibilityKey]; ok {
				t.Fatalf("filtered details leaked %s metadata: %s", detailsVisibilityKey, string(gotRaw))
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for key, want := range tc.want {
				if got[key] != want {
					t.Fatalf("got[%s]=%q, want %q (full=%v)", key, got[key], want, got)
				}
			}
		})
	}
}

func TestAccessPolicy_FilterDeterministicError(t *testing.T) {
	id := uuid.New()
	now := time.Unix(123, 0).UTC()
	item := domain.DeterministicError{
		ID:         id,
		Component:  "projections",
		Code:       "projection_failed",
		Message:    "projection failed",
		Severity:   domain.DeterministicErrorError,
		Details:    []byte(`{"event_kind":"work_item.created","raw_payload":"secret","_visibility":{"event_kind":"public","raw_payload":"private"}}`),
		ReportedAt: now,
		UpdatedAt:  now,
	}
	filtered := AccessPolicy{CanRead: true}.Filter(item)
	if filtered.ID != id || filtered.Component != item.Component || filtered.Code != item.Code {
		t.Fatalf("filter changed summary fields: %+v", filtered)
	}
	var details map[string]string
	if err := json.Unmarshal(filtered.Details, &details); err != nil {
		t.Fatal(err)
	}
	if details["event_kind"] != "work_item.created" || details["raw_payload"] != "" {
		t.Fatalf("unexpected filtered details: %v", details)
	}
}
