package main

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

type stubTokenAuthenticator struct {
	tok       domain.Token
	err       error
	gotSecret string
}

func (s *stubTokenAuthenticator) Authenticate(_ context.Context, secret string) (domain.Token, error) {
	s.gotSecret = secret
	if s.err != nil {
		return domain.Token{}, s.err
	}
	return s.tok, nil
}

func TestResolveSystemTokenAcceptsDedicatedSystemToken(t *testing.T) {
	t.Setenv("MERISTEM_TOKEN", "wln_seed")
	stub := &stubTokenAuthenticator{
		tok: domain.Token{
			ID:     uuid.New(),
			Source: domain.SourceSystem,
		},
	}

	got, err := resolveSystemToken(context.Background(), stub)
	if err != nil {
		t.Fatalf("resolveSystemToken: %v", err)
	}
	if stub.gotSecret != "wln_seed" {
		t.Fatalf("Authenticate secret = %q, want %q", stub.gotSecret, "wln_seed")
	}
	if got.ID != stub.tok.ID || got.Source != domain.SourceSystem || got.IsRoot {
		t.Fatalf("resolveSystemToken returned %+v, want %+v", got, stub.tok)
	}
}

func TestResolveSystemTokenRejectsRootSystemToken(t *testing.T) {
	t.Setenv("MERISTEM_TOKEN", "wln_seed")
	stub := &stubTokenAuthenticator{
		tok: domain.Token{
			ID:     uuid.New(),
			Source: domain.SourceSystem,
			IsRoot: true,
		},
	}

	_, err := resolveSystemToken(context.Background(), stub)
	if err == nil {
		t.Fatal("resolveSystemToken should reject root-backed system tokens")
	}
	if got := err.Error(); got != "seed v1: MERISTEM_TOKEN must be a dedicated system token, not root" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestResolveWorkerSystemTokenRejectsRootSystemToken(t *testing.T) {
	t.Setenv("MERISTEM_TOKEN", "wln_worker")
	stub := &stubTokenAuthenticator{
		tok: domain.Token{
			ID:     uuid.New(),
			Source: domain.SourceSystem,
			IsRoot: true,
		},
	}

	_, err := resolveWorkerSystemToken(context.Background(), stub)
	if err == nil {
		t.Fatal("resolveWorkerSystemToken should reject root-backed system tokens")
	}
	if got := err.Error(); got != "worker: MERISTEM_TOKEN must be a dedicated system token, not root" {
		t.Fatalf("unexpected error: %q", got)
	}
}
