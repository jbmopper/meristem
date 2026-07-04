package policyprofile

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/safety"
)

// These tests cover the paths that refuse before touching the database; the
// switch/readiness round-trip lives in internal/api's integration suite.

func TestSwitchRequiresHumanSource(t *testing.T) {
	s := NewService(nil, nil)
	_, _, err := s.Switch(context.Background(), SwitchInput{
		To:    safety.ProfileBringUp,
		Actor: domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
	})
	if !errors.Is(err, ErrHumanRequired) {
		t.Fatalf("expected ErrHumanRequired for agent token, got %v", err)
	}
	_, _, err = s.Switch(context.Background(), SwitchInput{
		To:    safety.ProfileBringUp,
		Actor: domain.Token{ID: uuid.New(), Source: domain.SourceSystem},
	})
	if !errors.Is(err, ErrHumanRequired) {
		t.Fatalf("expected ErrHumanRequired for system token, got %v", err)
	}
}

func TestSwitchRefusesRootToken(t *testing.T) {
	s := NewService(nil, nil)
	_, _, err := s.Switch(context.Background(), SwitchInput{
		To:    safety.ProfileBringUp,
		Actor: domain.Token{ID: uuid.New(), Source: domain.SourceHuman, IsRoot: true},
	})
	if !errors.Is(err, ErrRootForbidden) {
		t.Fatalf("expected ErrRootForbidden for root token, got %v", err)
	}
}

func TestSwitchRefusesUnknownProfileBeforeDB(t *testing.T) {
	s := NewService(nil, nil)
	_, _, err := s.Switch(context.Background(), SwitchInput{
		To:    "mellow",
		Actor: domain.Token{ID: uuid.New(), Source: domain.SourceHuman},
	})
	if err == nil {
		t.Fatal("expected error for unknown profile")
	}
	if !strings.Contains(err.Error(), "unknown policy profile") {
		t.Errorf("expected structured unknown-profile error, got: %v", err)
	}
}

func TestSubjectIDIsStable(t *testing.T) {
	again := uuid.NewSHA1(uuid.NameSpaceURL, []byte("meristem|policy_profile|active"))
	if SubjectID != again {
		t.Fatalf("SubjectID must be deterministic: %s vs %s", SubjectID, again)
	}
}

func TestIsUndefinedTable(t *testing.T) {
	err := &pgconn.PgError{Code: "42P01"}
	if !isUndefinedTable(err) {
		t.Fatal("expected undefined-table pg error to be recognized")
	}
	if isUndefinedTable(errors.New("other")) {
		t.Fatal("non-Postgres error should not be recognized as undefined table")
	}
}
