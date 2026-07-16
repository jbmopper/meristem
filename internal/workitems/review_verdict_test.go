package workitems

import (
	"errors"
	"testing"
)

func TestParseReviewVerdict(t *testing.T) {
	tests := []struct {
		name     string
		payload  any
		want     ReviewVerdict
		wantPass bool
		wantErr  bool
	}{
		{name: "accepted", payload: map[string]any{"verdict": "accepted"}, want: ReviewVerdictAccepted, wantPass: true},
		{name: "accepted with finding", payload: map[string]any{"verdict": "accepted_with_finding", "findings": []any{"minor"}}, want: ReviewVerdictAcceptedWithFinding, wantPass: true},
		{name: "blocking finding", payload: map[string]any{"payload_version": 1, "verdict": "blocking_finding", "pass": true}, want: ReviewVerdictBlockingFinding},
		{name: "missing verdict", payload: map[string]any{}, wantErr: true},
		{name: "unknown verdict", payload: map[string]any{"verdict": "approved-ish"}, wantErr: true},
		{name: "explicit zero payload version", payload: map[string]any{"payload_version": 0, "verdict": "accepted"}, wantErr: true},
		{name: "null payload version", payload: map[string]any{"payload_version": nil, "verdict": "accepted"}, wantErr: true},
		{name: "future payload version", payload: map[string]any{"payload_version": 2, "verdict": "accepted"}, wantErr: true},
		{name: "non-object payload", payload: "accepted", wantErr: true},
		{name: "null payload", payload: nil, wantErr: true},
		{name: "array payload", payload: []any{"accepted"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseReviewVerdict(tc.payload)
			if tc.wantErr {
				if err == nil || !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("ParseReviewVerdict error = %v, want ErrInvalidRequest", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseReviewVerdict: %v", err)
			}
			if got != tc.want || got.ChecklistPass() != tc.wantPass {
				t.Fatalf("verdict/pass = %q/%t, want %q/%t", got, got.ChecklistPass(), tc.want, tc.wantPass)
			}
		})
	}
}
