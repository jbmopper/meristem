package workitems

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/safety"
)

func TestClaimableWorkItemGates(t *testing.T) {
	for _, tc := range []struct {
		name string
		item domain.WorkItem
		want error
	}{
		{"planned", domain.WorkItem{State: domain.WorkItemPlanned, HumanReviewStatus: domain.HumanReviewWavedThrough}, nil},
		{"running", domain.WorkItem{State: domain.WorkItemRunning, HumanReviewStatus: domain.HumanReviewApproved}, nil},
		{"awaiting approval", domain.WorkItem{State: domain.WorkItemAwaitingApproval, HumanReviewStatus: domain.HumanReviewWavedThrough}, ErrClaimUnavailable},
		{"blocked lifecycle", domain.WorkItem{State: domain.WorkItemBlocked, HumanReviewStatus: domain.HumanReviewWavedThrough}, ErrClaimUnavailable},
		{"human review blocked", domain.WorkItem{State: domain.WorkItemPlanned, HumanReviewStatus: domain.HumanReviewBlocked}, ErrClaimUnavailable},
		{"done", domain.WorkItem{State: domain.WorkItemDone, HumanReviewStatus: domain.HumanReviewWavedThrough}, ErrClaimUnavailable},
		{"failed", domain.WorkItem{State: domain.WorkItemFailed, HumanReviewStatus: domain.HumanReviewWavedThrough}, ErrClaimUnavailable},
		{"canceled", domain.WorkItem{State: domain.WorkItemCanceled, HumanReviewStatus: domain.HumanReviewWavedThrough}, ErrClaimUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := claimableWorkItem(tc.item)
			if tc.want == nil && err != nil {
				t.Fatalf("claimableWorkItem: %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("claimableWorkItem error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBoundedClaimLeaseSecondsCapsBeforeDurationConversion(t *testing.T) {
	maxSeconds := int(safety.MaxPatienceBudget / 1e9)
	if got := boundedClaimLeaseSeconds(maxSeconds + 1); got != safety.MaxPatienceBudget {
		t.Fatalf("boundedClaimLeaseSeconds over cap = %s, want %s", got, safety.MaxPatienceBudget)
	}
	if got := boundedClaimLeaseSeconds(37); got.Seconds() != 37 {
		t.Fatalf("boundedClaimLeaseSeconds(37) = %s", got)
	}
}

func TestClaimHeldErrorExposesOnlySafeConflictFields(t *testing.T) {
	err := &ClaimHeldError{HolderTokenID: uuid.New(), AssignmentEventID: uuid.New()}
	if !errors.Is(err, ErrClaimHeld) {
		t.Fatalf("ClaimHeldError does not unwrap to ErrClaimHeld")
	}
}

func TestAssignmentDueIncludesExactDatabaseTimeBoundary(t *testing.T) {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		expiresAt time.Time
		want      bool
	}{
		{name: "before", expiresAt: now.Add(-time.Nanosecond), want: true},
		{name: "equal", expiresAt: now, want: true},
		{name: "after", expiresAt: now.Add(time.Nanosecond), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := assignmentDue(tc.expiresAt, now); got != tc.want {
				t.Fatalf("assignmentDue(%s, %s) = %v, want %v", tc.expiresAt, now, got, tc.want)
			}
		})
	}
}

func TestAssignedProjectorRejectsLeaseOverflowBeforeDurationConversion(t *testing.T) {
	actor := uuid.New()
	err := (assignedProjector{}).Apply(context.Background(), nil, domain.Event{
		ID: uuid.New(), Seq: 1, OccurredAt: time.Now(),
		SubjectKind: domain.SubjectWorkItem, SubjectID: uuid.New(),
		ActorTokenID: &actor,
		Payload: map[string]any{
			"assignee_token_id": actor,
			"mode":              domain.WorkItemAssignmentClaim,
			"lease_seconds":     int64(math.MaxInt64),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "lease exceeds bounded-patience cap") {
		t.Fatalf("overflow lease error = %v", err)
	}
}

func TestAssignedProjectorRejectsIncoherentLeaseClockFacts(t *testing.T) {
	actor := uuid.New()
	occurredAt := time.Date(2026, time.July, 17, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		claimedAt time.Time
		expiresAt time.Time
		want      string
	}{
		{
			name: "claim before event", claimedAt: occurredAt.Add(-time.Second),
			expiresAt: occurredAt.Add(59 * time.Second), want: "claimed_at cannot precede",
		},
		{
			name: "duration mismatch", claimedAt: occurredAt,
			expiresAt: occurredAt.Add(59 * time.Second), want: "plus lease_seconds",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (assignedProjector{}).Apply(context.Background(), nil, domain.Event{
				ID: uuid.New(), Seq: 1, OccurredAt: occurredAt,
				SubjectKind: domain.SubjectWorkItem, SubjectID: uuid.New(),
				ActorTokenID: &actor,
				Payload: map[string]any{
					"assignee_token_id": actor, "mode": domain.WorkItemAssignmentClaim,
					"lease_seconds": 60, "claimed_at": tc.claimedAt, "expires_at": tc.expiresAt,
				},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("clock fact error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAssignmentBudgetClassIsLifecycleWhileFeedRemainsAdmin(t *testing.T) {
	for _, kind := range []string{domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased} {
		if got, _, ok := feed.StaticKindClass(kind); !ok || got != feed.KindClassAdmin {
			t.Fatalf("feed class for %s = %q, %v; want fail-closed admin", kind, got, ok)
		}
		if got, ok := workItemEventBudgetClass(kind, nil); !ok || got != feed.KindClassLifecycle {
			t.Fatalf("budget class for %s = %q, %v; want lifecycle", kind, got, ok)
		}
	}
}

func TestAssignmentReleasedV1RejectsStandaloneDone(t *testing.T) {
	_, err := decodeAssignmentReleasedPayload(map[string]any{
		"payload_version":     1,
		"assignment_event_id": uuid.New(),
		"assignee_token_id":   uuid.New(),
		"mode":                domain.WorkItemAssignmentClaim,
		"reason":              domain.AssignmentReleaseDone,
		"terminal_state":      domain.WorkItemDone,
	})
	if err == nil || !strings.Contains(err.Error(), "done is derived from work_item.transitioned") {
		t.Fatalf("standalone done release error = %v", err)
	}
}
