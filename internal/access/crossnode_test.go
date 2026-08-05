package access

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// The cross-node reducers are the "B" half of the D+B authorization rule
// recorded on work item 319c8dc8. They gate acting on an object homed on
// another node. The node's peer bearer is transport authority, not caller
// authority, so an actor that clears these still has to clear the anchor
// requirement and the home's own check — but an actor that does NOT clear them
// must never reach the network at all.

func crossNodeActor(scopes ...string) domain.Token {
	return domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: scopes}
}

func TestRemoteReadRequiresExplicitScope(t *testing.T) {
	if !RemoteReadAllowed(crossNodeActor(ScopeCrossNodeWorkItemsRead)) {
		t.Fatal("actor holding crossnode.work_items.read was denied")
	}
	if !RemoteMutationAllowed(crossNodeActor(ScopeCrossNodeWorkItemsWrite)) {
		t.Fatal("actor holding crossnode.work_items.write was denied")
	}
}

// TestLocalAuthorityDoesNotImplyCrossNodeReach is the escalation this scope
// exists to stop. Every one of these actors has broad LOCAL authority; none of
// them has said anything about other nodes. If any passes, a token scoped to
// this node's objects silently gained reach over another node's objects the
// moment it learned a qualified reference.
func TestLocalAuthorityDoesNotImplyCrossNodeReach(t *testing.T) {
	cases := map[string]domain.Token{
		"work_items.read":               crossNodeActor(ScopeWorkItemsRead),
		"work_items.read_all":           crossNodeActor(ScopeWorkItemsReadAll),
		"work_items.write_all":          crossNodeActor(ScopeWorkItemsWriteAll),
		"work_items.tracker_write_all":  crossNodeActor(ScopeWorkItemsTrackerWriteAll),
		"every local work-item scope":   crossNodeActor(ScopeWorkItemsRead, ScopeWorkItemsReadAll, ScopeWorkItemsWrite, ScopeWorkItemsWriteAll, ScopeWorkItemsTrackerWrite, ScopeWorkItemsTrackerWriteAll, ScopeWorkItemsCreate),
		"legacy unscoped compatibility": crossNodeActor(),
		"legacy unscoped with blank":    crossNodeActor("", "   "),
		"root credential":               {IsRoot: true},
		"root with local scopes":        {IsRoot: true, Scopes: []string{ScopeWorkItemsReadAll}},
	}
	for name, actor := range cases {
		if RemoteReadAllowed(actor) {
			t.Errorf("%s: RemoteReadAllowed = true, want false", name)
		}
		if RemoteMutationAllowed(actor) {
			t.Errorf("%s: RemoteMutationAllowed = true, want false", name)
		}
	}
}

// TestCrossNodeReadAndWriteDoNotImplyEachOther pins the separation codex asked
// for: read authority never implies mutation authority, and granting write
// must not backfill read. An operator who grants one should not discover they
// granted both.
func TestCrossNodeReadAndWriteDoNotImplyEachOther(t *testing.T) {
	readOnly := crossNodeActor(ScopeCrossNodeWorkItemsRead)
	if RemoteMutationAllowed(readOnly) {
		t.Error("crossnode read scope granted mutation authority")
	}
	writeOnly := crossNodeActor(ScopeCrossNodeWorkItemsWrite)
	if RemoteReadAllowed(writeOnly) {
		t.Error("crossnode write scope backfilled read authority")
	}
	both := crossNodeActor(ScopeCrossNodeWorkItemsRead, ScopeCrossNodeWorkItemsWrite)
	if !RemoteReadAllowed(both) || !RemoteMutationAllowed(both) {
		t.Error("actor holding both cross-node scopes was denied one of them")
	}
}

// TestCrossNodeScopesAreNotPrefixMatched guards the naming: a scope that merely
// starts with or contains the cross-node string is a different scope. Prefix
// matching here would let an unrelated grant widen into cross-node reach.
func TestCrossNodeScopesAreNotPrefixMatched(t *testing.T) {
	for _, scope := range []string{
		"crossnode.work_items",
		"crossnode.work_items.read_all",
		"crossnode.work_items.readx",
		"crossnode",
		"xcrossnode.work_items.read",
		"crossnode.work_items.read:m4",
	} {
		if RemoteReadAllowed(crossNodeActor(scope)) {
			t.Errorf("scope %q granted cross-node read", scope)
		}
	}
}

// TestCrossNodeScopeOnAnIneligibleCredentialIsRefused is XNODE-P1-B1. Holding
// the exact scope string is necessary and not sufficient — the scope has to sit
// on a credential that could legitimately carry it. The earlier version checked
// only the scope, so a root or revoked token that DID carry it passed, which
// contradicted the documented contract and is the fail-open direction.
func TestCrossNodeScopeOnAnIneligibleCredentialIsRefused(t *testing.T) {
	revokedAt := time.Now()
	both := []string{ScopeCrossNodeWorkItemsRead, ScopeCrossNodeWorkItemsWrite}
	cases := map[string]domain.Token{
		"root carrying both cross-node scopes": {ID: uuid.New(), IsRoot: true, Scopes: both},
		"revoked token carrying both scopes":   {ID: uuid.New(), RevokedAt: &revokedAt, Scopes: both},
		"unidentified actor with both scopes":  {ID: uuid.Nil, Scopes: both},
		"revoked root with both scopes":        {ID: uuid.New(), IsRoot: true, RevokedAt: &revokedAt, Scopes: both},
	}
	for name, actor := range cases {
		if RemoteReadAllowed(actor) {
			t.Errorf("%s: RemoteReadAllowed = true, want false", name)
		}
		if RemoteMutationAllowed(actor) {
			t.Errorf("%s: RemoteMutationAllowed = true, want false", name)
		}
	}
}

// TestCrossNodeScopeAdmitsScopedNonHumanActors pins the other side of the same
// rule. Source is deliberately unconstrained: agent and system actors are
// exactly who performs cross-node execution, so narrowing to human credentials
// would make the scope unusable for the thing it exists to authorize.
func TestCrossNodeScopeAdmitsScopedNonHumanActors(t *testing.T) {
	for _, source := range []domain.Source{domain.SourceAgent, domain.SourceSystem, domain.SourceHuman} {
		actor := domain.Token{ID: uuid.New(), Source: source, Scopes: []string{ScopeCrossNodeWorkItemsRead}}
		if !RemoteReadAllowed(actor) {
			t.Errorf("source %s with the exact scope was denied cross-node read", source)
		}
	}
}
