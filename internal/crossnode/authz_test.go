package crossnode

import (
	"errors"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestCrossnodeAuthorizationIsTargetAndOperationScoped(t *testing.T) {
	path := "/v1/work-items/7b1fc532-14f2-4be5-81a5-4719dd11d453/transition"
	actor := domain.Token{Scopes: []string{QueueWriteScope("m4", OperationClassWorkItemsWrite), OriginScope("sender")}}
	if err := AuthorizeQueueWrite(actor, "m4", "sender", path); err != nil {
		t.Fatalf("expected exact queue scope to authorize: %v", err)
	}
	if err := AuthorizeQueueWrite(actor, "peer", "sender", path); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("wrong target error = %v, want scope denial", err)
	}
	if err := AuthorizeQueueWrite(actor, "m4", "sender", "/v1/approvals/7b1fc532-14f2-4be5-81a5-4719dd11d453/decide"); !errors.Is(err, ErrInvalidCommandPath) {
		t.Fatalf("disallowed path error = %v, want invalid command path", err)
	}
	if err := AuthorizeQueueWrite(actor, "m4", "impostor", path); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("unbound origin error = %v, want scope denial", err)
	}
}

func TestCrossnodeAuthorizationRejectsRootLegacyAndRevokedTokens(t *testing.T) {
	path := "/v1/work-items"
	revokedAt := time.Now()
	tests := []domain.Token{
		{IsRoot: true, Scopes: []string{QueueWriteScope("m4", OperationClassWorkItemsWrite)}},
		{},
		{Scopes: []string{QueueWriteScope("m4", OperationClassWorkItemsWrite)}, RevokedAt: &revokedAt},
	}
	for _, actor := range tests {
		if err := AuthorizeQueueWrite(actor, "m4", "sender", path); err == nil {
			t.Fatalf("AuthorizeQueueWrite(%+v) succeeded, want denial", actor)
		}
	}
}

func TestQueueDrainAndAckScopesAreDistinctAndTargetScoped(t *testing.T) {
	drain := domain.Token{Scopes: []string{QueueDrainScope("m4")}}
	if err := AuthorizeQueueDrain(drain, "m4"); err != nil {
		t.Fatalf("drain exact scope: %v", err)
	}
	if err := AuthorizeQueueDrain(drain, "peer"); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("wrong drain target = %v, want scope denial", err)
	}
	if err := AuthorizeQueueAck(drain, "m4"); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("drain scope used for ack = %v, want scope denial", err)
	}

	ack := domain.Token{Scopes: []string{QueueAckScope("m4")}}
	if err := AuthorizeQueueAck(ack, "m4"); err != nil {
		t.Fatalf("ack exact scope: %v", err)
	}
}

func TestOutcomeScopesAreOriginAndHostScopedAndRejectRoot(t *testing.T) {
	remote := domain.Token{Scopes: []string{QueueOutcomeReadScope("origin-a")}}
	if err := AuthorizeQueueOutcomeRead(remote, "origin-a"); err != nil {
		t.Fatalf("exact outcome read scope: %v", err)
	}
	if err := AuthorizeQueueOutcomeRead(remote, "origin-b"); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("wrong origin read = %v, want scope denial", err)
	}
	local := domain.Token{Scopes: []string{OutcomeObserveScope("queue-a", "origin-a")}}
	if err := AuthorizeOutcomeObserve(local, "queue-a", "origin-a"); err != nil {
		t.Fatalf("exact observe scope: %v", err)
	}
	if err := AuthorizeOutcomeObserve(local, "queue-b", "origin-a"); !errors.Is(err, ErrCommandScopeDenied) {
		t.Fatalf("wrong queue host = %v, want scope denial", err)
	}
	root := domain.Token{IsRoot: true, Scopes: []string{QueueOutcomeReadScope("origin-a")}}
	if err := AuthorizeQueueOutcomeRead(root, "origin-a"); !errors.Is(err, ErrCommandRootForbidden) {
		t.Fatalf("root outcome read = %v, want root forbidden", err)
	}
}
