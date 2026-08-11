package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

type dispatchIdentityTestRow struct {
	scan func(...any) error
}

func (r dispatchIdentityTestRow) Scan(dest ...any) error { return r.scan(dest...) }

type dispatchIdentityTestQuerier struct {
	rows []pgx.Row
}

func (q *dispatchIdentityTestQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(q.rows) == 0 {
		return dispatchIdentityTestRow{scan: func(...any) error { return errors.New("unexpected QueryRow") }}
	}
	row := q.rows[0]
	q.rows = q.rows[1:]
	return row
}

func (*dispatchIdentityTestQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}

func TestResolveDispatchIdentityExplicitStateEntry(t *testing.T) {
	workItemID := uuid.New()
	demandID := uuid.New()
	stateEntryID := uuid.New()
	enteredAt := time.Date(2026, 8, 11, 12, 0, 0, 123, time.UTC)
	payload := json.RawMessage(`{"work_item_id":"` + workItemID.String() + `","state":"planned","state_event_id":"` + stateEntryID.String() + `","state_entered_at_unix":` + "1786449600" + `}`)
	q := &dispatchIdentityTestQuerier{rows: []pgx.Row{
		dispatchIdentityTestRow{scan: func(dest ...any) error {
			*(dest[0].(*int64)) = 20
			*(dest[1].(*uuid.UUID)) = workItemID
			*(dest[2].(*json.RawMessage)) = payload
			return nil
		}},
		dispatchIdentityTestRow{scan: func(dest ...any) error {
			*(dest[0].(*uuid.UUID)) = stateEntryID
			*(dest[1].(*int64)) = 10
			*(dest[2].(*string)) = domain.EventWorkItemTransitioned
			*(dest[3].(*string)) = string(domain.WorkItemPlanned)
			*(dest[4].(*time.Time)) = enteredAt
			*(dest[5].(*json.RawMessage)) = json.RawMessage(`{"to":"planned"}`)
			return nil
		}},
	}}

	got, err := ResolveDispatchIdentity(context.Background(), q, demandID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != demandID || got.WorkItemID != workItemID || got.StateEntryID != stateEntryID ||
		got.State != domain.WorkItemPlanned || !got.StateEnteredAt.Equal(enteredAt) || !got.Explicit {
		t.Fatalf("identity=%+v", got)
	}
	if len(q.rows) != 0 {
		t.Fatalf("unused query rows=%d", len(q.rows))
	}
}

func TestResolveDispatchIdentityRejectsMismatchedExplicitEntry(t *testing.T) {
	workItemID := uuid.New()
	demandID := uuid.New()
	stateEntryID := uuid.New()
	enteredAt := time.Unix(1_786_449_600, 0).UTC()
	payload := json.RawMessage(`{"work_item_id":"` + workItemID.String() + `","state":"triaged","state_event_id":"` + uuid.NewString() + `","state_entered_at_unix":1786449600}`)
	q := dispatchIdentityQuerierForDemand(workItemID, stateEntryID, enteredAt, payload)

	_, err := ResolveDispatchIdentity(context.Background(), q, demandID)
	if !errors.Is(err, ErrInvalidDispatchDemand) {
		t.Fatalf("error=%v, want ErrInvalidDispatchDemand", err)
	}
}

func TestResolveDispatchIdentityLegacyRequiresOneExactEntry(t *testing.T) {
	workItemID := uuid.New()
	demandID := uuid.New()
	stateEntryID := uuid.New()
	enteredAt := time.Unix(1_786_449_600, 0).UTC()
	payload := json.RawMessage(`{"work_item_id":"` + workItemID.String() + `","state":"triaged","state_entered_at_unix":1786449600}`)

	for _, tc := range []struct {
		name      string
		matches   int
		wantError bool
	}{
		{name: "unique", matches: 1},
		{name: "ambiguous", matches: 2, wantError: true},
		{name: "missing", matches: 0, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := dispatchIdentityQuerierForDemand(workItemID, stateEntryID, enteredAt, payload)
			q.rows = append(q.rows, dispatchIdentityTestRow{scan: func(dest ...any) error {
				*(dest[0].(*int)) = tc.matches
				return nil
			}})
			got, err := ResolveDispatchIdentity(context.Background(), q, demandID)
			if tc.wantError {
				if !errors.Is(err, ErrInvalidDispatchDemand) {
					t.Fatalf("error=%v, want ErrInvalidDispatchDemand", err)
				}
				return
			}
			if err != nil || got.Explicit || got.StateEntryID != stateEntryID {
				t.Fatalf("identity=%+v error=%v", got, err)
			}
		})
	}
}

func TestResolveDispatchIdentityPropagatesStateEntryQueryFailure(t *testing.T) {
	workItemID := uuid.New()
	demandID := uuid.New()
	queryErr := errors.New("database unavailable")
	q := &dispatchIdentityTestQuerier{rows: []pgx.Row{
		dispatchIdentityTestRow{scan: func(dest ...any) error {
			*(dest[0].(*int64)) = 20
			*(dest[1].(*uuid.UUID)) = workItemID
			*(dest[2].(*json.RawMessage)) = json.RawMessage(`{}`)
			return nil
		}},
		dispatchIdentityTestRow{scan: func(...any) error { return queryErr }},
	}}

	_, err := ResolveDispatchIdentity(context.Background(), q, demandID)
	if !errors.Is(err, queryErr) || errors.Is(err, ErrInvalidDispatchDemand) {
		t.Fatalf("error=%v, want operational query error", err)
	}
}

func TestCausallyAdmitsDemandRequiresExactRunningTransition(t *testing.T) {
	demandID := uuid.New()
	exact := DispatchStateEntry{
		Kind: domain.EventWorkItemTransitioned, State: domain.WorkItemRunning,
		Payload: json.RawMessage(`{"dispatch_event_id":"` + demandID.String() + `"}`),
	}
	if !CausallyAdmitsDemand(exact, demandID) {
		t.Fatal("exact reviewer transition was not admitted")
	}
	for name, entry := range map[string]DispatchStateEntry{
		"wrong event":   {Kind: domain.EventWorkItemCreated, State: domain.WorkItemRunning, Payload: exact.Payload},
		"wrong state":   {Kind: domain.EventWorkItemTransitioned, State: domain.WorkItemPlanned, Payload: exact.Payload},
		"wrong demand":  {Kind: domain.EventWorkItemTransitioned, State: domain.WorkItemRunning, Payload: json.RawMessage(`{"dispatch_event_id":"` + uuid.NewString() + `"}`)},
		"missing field": {Kind: domain.EventWorkItemTransitioned, State: domain.WorkItemRunning, Payload: json.RawMessage(`{}`)},
		"malformed":     {Kind: domain.EventWorkItemTransitioned, State: domain.WorkItemRunning, Payload: json.RawMessage(`{`)},
	} {
		t.Run(name, func(t *testing.T) {
			if CausallyAdmitsDemand(entry, demandID) {
				t.Fatal("non-causal state entry was admitted")
			}
		})
	}
}

func dispatchIdentityQuerierForDemand(workItemID, stateEntryID uuid.UUID, enteredAt time.Time, payload json.RawMessage) *dispatchIdentityTestQuerier {
	return &dispatchIdentityTestQuerier{rows: []pgx.Row{
		dispatchIdentityTestRow{scan: func(dest ...any) error {
			*(dest[0].(*int64)) = 20
			*(dest[1].(*uuid.UUID)) = workItemID
			*(dest[2].(*json.RawMessage)) = payload
			return nil
		}},
		dispatchIdentityTestRow{scan: func(dest ...any) error {
			*(dest[0].(*uuid.UUID)) = stateEntryID
			*(dest[1].(*int64)) = 10
			*(dest[2].(*string)) = domain.EventWorkItemTransitioned
			*(dest[3].(*string)) = string(domain.WorkItemTriaged)
			*(dest[4].(*time.Time)) = enteredAt
			*(dest[5].(*json.RawMessage)) = json.RawMessage(`{"to":"triaged"}`)
			return nil
		}},
	}}
}
