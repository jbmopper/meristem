// Package grants contains deterministic reducers for delegated token grants.
//
// It is intentionally pure: callers resolve projection-dependent facts such as
// work_item ancestry, then pass those facts in. The reducer decides grant,
// deny, or escalate without reading clocks, databases, or process state.
package grants

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

type Template string

const (
	TemplateSameTreeReadProgress Template = "same_tree_read_progress"
	TemplateSameTreeWorker       Template = "same_tree_worker"
)

type Disposition string

const (
	DispositionGrant    Disposition = "grant"
	DispositionDeny     Disposition = "deny"
	DispositionEscalate Disposition = "escalate"
)

type TreeRelation string

const (
	TreeSame       TreeRelation = "same"
	TreeDescendant TreeRelation = "descendant"
	TreeOutside    TreeRelation = "outside"
	TreeUnknown    TreeRelation = "unknown"
)

type Request struct {
	Parent             domain.Token
	Template           Template
	RequestedSource    domain.Source
	RequestedTreeRoot  uuid.UUID
	RequestedScopes    []string
	TreeRelation       TreeRelation
	HumanReviewStatus  domain.HumanReviewStatus
	ApprovalAuthority  bool
	RequestedLogsScope bool
}

type Decision struct {
	Disposition Disposition
	Reason      string
	Scopes      []string
}

func Reduce(req Request) Decision {
	if req.Parent.ID == uuid.Nil {
		return deny("parent token is required")
	}
	if req.Parent.RevokedAt != nil {
		return deny("parent token is revoked")
	}
	if req.Parent.IsRoot {
		return escalate("root delegation requires operator path")
	}
	if req.Parent.Source != domain.SourceAgent {
		return escalate("only agent tokens may self-delegate subactors")
	}
	if req.RequestedSource != "" && req.RequestedSource != domain.SourceAgent {
		return escalate("subactor source must remain agent")
	}
	if len(req.Parent.Scopes) == 0 {
		return escalate("legacy unscoped parent token cannot self-delegate")
	}
	if req.ApprovalAuthority {
		return escalate("approval authority cannot be self-delegated")
	}
	if req.RequestedLogsScope || containsLogsScope(req.RequestedScopes) {
		return escalate("logs visibility requires human approval")
	}
	if req.RequestedTreeRoot == uuid.Nil {
		return deny("requested work_items.tree root is required")
	}
	switch req.TreeRelation {
	case TreeSame, TreeDescendant:
	case TreeOutside:
		return escalate("requested tree root is outside parent assignment")
	default:
		return escalate("requested tree relation is unknown")
	}

	templateScopes, writeTemplate, ok := templateScopeSet(req.Template, req.RequestedTreeRoot)
	if !ok {
		return deny(fmt.Sprintf("unknown grant template %q", req.Template))
	}
	if len(req.RequestedScopes) > 0 && !sameScopeSet(req.RequestedScopes, templateScopes) {
		return escalate("requested scopes do not match the named grant template")
	}
	if writeTemplate && req.HumanReviewStatus != domain.HumanReviewApproved {
		return escalate("write-capable subactor grants require explicit human approval")
	}
	if !parentCoversTemplate(req.Parent.Scopes, templateScopes, req.RequestedTreeRoot) {
		return escalate("requested grant is not a subset of parent token scopes")
	}
	return Decision{
		Disposition: DispositionGrant,
		Reason:      "grant matches named template, same-tree policy, and parent scope subset",
		Scopes:      canonicalScopes(templateScopes),
	}
}

func templateScopeSet(template Template, root uuid.UUID) ([]string, bool, bool) {
	tree := "work_items.tree:" + root.String()
	switch template {
	case TemplateSameTreeReadProgress:
		return []string{
			access.ScopeFeedReadAssigned,
			access.ScopeWorkItemsRead,
			tree,
		}, false, true
	case TemplateSameTreeWorker:
		return []string{
			access.ScopeFeedReadAssigned,
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			tree,
		}, true, true
	default:
		return nil, false, false
	}
}

func parentCoversTemplate(parentScopes []string, grantScopes []string, root uuid.UUID) bool {
	parent := scopeSet(parentScopes)
	parentTreeOK := false
	for scope := range parent {
		if scope == "work_items.tree:"+root.String() {
			parentTreeOK = true
		}
	}
	if !parentTreeOK {
		return false
	}
	for _, scope := range grantScopes {
		if strings.HasPrefix(scope, "work_items.tree:") {
			continue
		}
		if parent[scope] {
			continue
		}
		if scope == access.ScopeFeedReadAssigned && parent[access.ScopeFeedRead] {
			continue
		}
		return false
	}
	return true
}

func containsLogsScope(scopes []string) bool {
	for _, scope := range scopes {
		if strings.HasPrefix(scope, "logs.") {
			return true
		}
	}
	return false
}

func sameScopeSet(a, b []string) bool {
	as := scopeSet(a)
	bs := scopeSet(b)
	if len(as) != len(bs) {
		return false
	}
	for scope := range as {
		if !bs[scope] {
			return false
		}
	}
	return true
}

func scopeSet(scopes []string) map[string]bool {
	out := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		out[scope] = true
	}
	return out
}

func canonicalScopes(scopes []string) []string {
	out := append([]string(nil), scopes...)
	sort.Strings(out)
	return out
}

func deny(reason string) Decision {
	return Decision{Disposition: DispositionDeny, Reason: reason}
}

func escalate(reason string) Decision {
	return Decision{Disposition: DispositionEscalate, Reason: reason}
}
