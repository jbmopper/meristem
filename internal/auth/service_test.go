package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestNormalizeTokenSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      CreateTokenInput
		want    domain.Source
		wantErr string
	}{
		{
			name: "defaults empty source to human",
			in:   CreateTokenInput{},
			want: domain.SourceHuman,
		},
		{
			name:    "rejects invalid source",
			in:      CreateTokenInput{Source: domain.Source("bogus")},
			wantErr: "invalid token source",
		},
		{
			name: "allows human root",
			in: CreateTokenInput{
				IsRoot: true,
				Source: domain.SourceHuman,
			},
			want: domain.SourceHuman,
		},
		{
			name: "defaults root to human",
			in: CreateTokenInput{
				IsRoot: true,
			},
			want: domain.SourceHuman,
		},
		{
			name: "rejects non human root",
			in: CreateTokenInput{
				IsRoot: true,
				Source: domain.SourceSystem,
			},
			wantErr: "root tokens must use source",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := normalizeTokenSource(tc.in)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("normalizeTokenSource(%+v) returned nil error, want %q", tc.in, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("normalizeTokenSource(%+v) error = %q, want substring %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeTokenSource(%+v): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeTokenSource(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCreateDelegatedTokenRejectsLocalMCPProfile(t *testing.T) {
	svc := &Service{}
	_, err := svc.CreateDelegatedToken(context.Background(), nil, CreateDelegatedTokenInput{
		ID:     uuid.New(),
		Name:   "delegated-local-profile",
		Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead},
		Source: domain.SourceAgent,
		Actor:  domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
	})
	if err == nil || !strings.Contains(err.Error(), "delegated token cannot carry a local MCP profile marker") {
		t.Fatalf("CreateDelegatedToken error = %v, want local profile issuance refusal", err)
	}
}
