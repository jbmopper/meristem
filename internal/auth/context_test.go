package auth

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestTokenContext_Roundtrip(t *testing.T) {
	tok := domain.Token{ID: uuid.New(), Name: "iphone", Source: domain.SourceHuman}
	ctx := WithToken(context.Background(), tok)
	got, ok := TokenFromContext(ctx)
	if !ok {
		t.Fatal("expected token to be present in context")
	}
	if got.ID != tok.ID || got.Name != tok.Name || got.Source != tok.Source {
		t.Errorf("roundtrip lost data: got %+v, want %+v", got, tok)
	}
}

func TestTokenContext_AbsentReturnsFalse(t *testing.T) {
	if _, ok := TokenFromContext(context.Background()); ok {
		t.Error("expected ok=false when no token has been attached")
	}
}

func TestTokenContext_WrongTypeReturnsFalse(t *testing.T) {
	// A foreign value at our key (impossible in practice — the key type
	// is unexported — but worth pinning so a future refactor that leaks
	// the key doesn't quietly start treating arbitrary values as tokens).
	ctx := context.WithValue(context.Background(), contextKey{}, "not a token")
	if _, ok := TokenFromContext(ctx); ok {
		t.Error("expected ok=false when the value at our key is not a domain.Token")
	}
}

func TestSourceForToken_DefaultsToHuman(t *testing.T) {
	if got := sourceForToken(nil); got != domain.SourceHuman {
		t.Errorf("nil token: got %q, want human", got)
	}
	tok := &domain.Token{Source: ""}
	if got := sourceForToken(tok); got != domain.SourceHuman {
		t.Errorf("empty source: got %q, want human", got)
	}
	tok = &domain.Token{Source: "bogus"}
	if got := sourceForToken(tok); got != domain.SourceHuman {
		t.Errorf("invalid source should default to human, got %q", got)
	}
	tok = &domain.Token{Source: domain.SourceAgent}
	if got := sourceForToken(tok); got != domain.SourceAgent {
		t.Errorf("agent source should round-trip, got %q", got)
	}
}
