package projectiondefs

import (
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
)

func TestNormalizeInputRejectsReservedBriefProjection(t *testing.T) {
	_, _, err := normalizeInput(DefineInput{
		Name:    "work-item-brief",
		Version: 1,
		Type:    ProjectionTypeFeed,
		Filter:  feed.ProjectionFilter{Kinds: []string{domain.EventWorkItemCreated}},
	})
	if !errors.Is(err, ErrInvalidName) {
		t.Fatalf("expected invalid name for reserved work-item-brief, got %v", err)
	}
}

func TestNormalizeInputMapsUnknownAndAdminFilterErrors(t *testing.T) {
	tests := []struct {
		name string
		in   DefineInput
		want error
	}{
		{
			name: "unknown_kind",
			in: DefineInput{
				Name:    "bad-kind",
				Version: 1,
				Type:    ProjectionTypeFeed,
				Filter:  feed.ProjectionFilter{Kinds: []string{"made.up"}},
			},
			want: ErrUnknownKind,
		},
		{
			name: "admin_kind",
			in: DefineInput{
				Name:    "admin-kind",
				Version: 1,
				Type:    ProjectionTypeFeed,
				Filter:  feed.ProjectionFilter{Kinds: []string{domain.EventTokenCreated}},
			},
			want: ErrNotProjectable,
		},
		{
			name: "admin_class",
			in: DefineInput{
				Name:    "admin-class",
				Version: 1,
				Type:    ProjectionTypeFeed,
				Filter:  feed.ProjectionFilter{KindClasses: []string{feed.KindClassAdmin}},
			},
			want: ErrNotProjectable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := normalizeInput(tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestNormalizeInputDefaultsTypeAndCanonicalizesFilter(t *testing.T) {
	in, payload, err := normalizeInput(DefineInput{
		Name:    "activity",
		Version: 1,
		Filter: feed.ProjectionFilter{
			Kinds:       []string{domain.EventSignalReceived, domain.EventSignalReceived},
			KindClasses: []string{feed.KindClassProgress, feed.KindClassProgress},
		},
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if in.Type != ProjectionTypeFeed {
		t.Fatalf("type = %q, want feed", in.Type)
	}
	if len(in.Filter.Kinds) != 1 || in.Filter.Kinds[0] != domain.EventSignalReceived {
		t.Fatalf("kinds were not canonicalized: %+v", in.Filter.Kinds)
	}
	if len(in.Filter.KindClasses) != 1 || in.Filter.KindClasses[0] != feed.KindClassProgress {
		t.Fatalf("kind_classes were not canonicalized: %+v", in.Filter.KindClasses)
	}
	if payload["type"] != ProjectionTypeFeed {
		t.Fatalf("payload type = %v", payload["type"])
	}
}
