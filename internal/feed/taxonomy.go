package feed

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

const (
	KindClassLifecycle = "lifecycle"
	KindClassDecision  = "decision"
	KindClassProgress  = "progress"
	KindClassAdmin     = "admin"
)

var (
	ErrUnknownKind      = errors.New("feed: unknown event kind")
	ErrUnknownKindClass = errors.New("feed: unknown kind class")
	ErrKindNotAllowed   = errors.New("feed: kind not allowed in projection")
	ErrClassNotAllowed  = errors.New("feed: kind class not allowed in projection")
	ErrEmptyFilter      = errors.New("feed: projection filter is empty")
)

// ProjectionFilter is the reusable filter shape stored by projection
// definitions. Kinds select exact event kinds. KindClasses select the chatter
// taxonomy buckets R6 defines over the same event stream.
type ProjectionFilter struct {
	Kinds       []string `json:"kinds,omitempty"`
	KindClasses []string `json:"kind_classes,omitempty"`
}

// NormalizeProjectionFilter validates and canonicalizes f for persisted
// projection definitions. Administrative event classes are deliberately
// refused here even though the legacy default feed still includes
// deterministic_error.* events.
func NormalizeProjectionFilter(f ProjectionFilter) (ProjectionFilter, error) {
	kinds := dedupeTrimmed(f.Kinds)
	classes := dedupeTrimmed(f.KindClasses)
	if len(kinds) == 0 && len(classes) == 0 {
		return ProjectionFilter{}, ErrEmptyFilter
	}
	for _, kind := range kinds {
		if !KnownEventKind(kind) {
			return ProjectionFilter{}, fmt.Errorf("%w: %s", ErrUnknownKind, kind)
		}
		if !ProjectableKind(kind) {
			return ProjectionFilter{}, fmt.Errorf("%w: %s", ErrKindNotAllowed, kind)
		}
	}
	for _, class := range classes {
		if !KnownKindClass(class) {
			return ProjectionFilter{}, fmt.Errorf("%w: %s", ErrUnknownKindClass, class)
		}
		if !ProjectableKindClass(class) {
			return ProjectionFilter{}, fmt.Errorf("%w: %s", ErrClassNotAllowed, class)
		}
	}
	return ProjectionFilter{Kinds: kinds, KindClasses: classes}, nil
}

func KnownEventKind(kind string) bool {
	return slices.Contains(domain.AllEventKinds, kind)
}

func KnownKindClass(class string) bool {
	switch class {
	case KindClassLifecycle, KindClassDecision, KindClassProgress, KindClassAdmin:
		return true
	default:
		return false
	}
}

func ProjectableKind(kind string) bool {
	class, _, ok := StaticKindClass(kind)
	return ok && class != KindClassAdmin
}

func ProjectableKindClass(class string) bool {
	return class != KindClassAdmin && KnownKindClass(class)
}

// StaticKindClass classifies event kinds whose class is payload-independent.
// work_item.event_appended is dynamic because its inner_kind decides whether
// the item is coordination/decision chatter or execution progress.
func StaticKindClass(kind string) (class string, dynamic bool, ok bool) {
	switch kind {
	case domain.EventWorkItemCreated,
		domain.EventWorkItemTransitioned,
		domain.EventWorkItemRelationAdded,
		domain.EventWorkItemMetadataUpdated,
		domain.EventXylemExhausted,
		domain.EventSignalReceived,
		domain.EventEscalationRequested,
		domain.EventApprovalCreated,
		domain.EventApprovalExpired,
		domain.EventHTTPConnectorActionRequested,
		domain.EventHTTPConnectorActionApproved,
		domain.EventPatienceBreached,
		domain.EventDispatchRequested:
		return KindClassLifecycle, false, true
	case domain.EventWorkItemEventAppended:
		return "", true, true
	case domain.EventMessageCaptured,
		domain.EventSubactorGrantRequested,
		domain.EventSubactorGrantGranted,
		domain.EventSubactorGrantDenied,
		domain.EventSubactorGrantEscalated,
		domain.EventCultivarActivationRequested,
		domain.EventCultivarActivationGranted,
		domain.EventCultivarActivationDenied,
		domain.EventCultivarActivationEscalated,
		domain.EventApprovalDecided,
		domain.EventConvergenceVerdictRecorded,
		domain.EventConvergenceChecksProposed,
		domain.EventPolicyProfileSwitched,
		domain.EventTropismDefined,
		domain.EventCultivarDefined,
		domain.EventProjectionDefined:
		return KindClassDecision, false, true
	case domain.EventHTTPConnectorActionSent:
		return KindClassProgress, false, true
	case domain.EventTokenCreated,
		domain.EventTokenRevoked,
		domain.EventIdempotencyRecorded,
		domain.EventWorkItemAssigned,
		domain.EventWorkItemAssignmentReleased,
		domain.EventReviewLaunchReserved,
		domain.EventReviewLaunchHandleRecorded,
		domain.EventReviewLaunchResolved,
		domain.EventReviewLaunchTerminationDue,
		domain.EventDeterministicErrorReported,
		domain.EventDeterministicErrorMasked,
		domain.EventDeterministicErrorUnmasked,
		domain.EventNodeRegistered,
		domain.EventNodeRouteUpdated,
		domain.EventRegistrySnapshotObserved,
		domain.EventCommandQueued,
		domain.EventCommandAcked,
		domain.EventCommandAttempted,
		domain.EventCommandExpired,
		domain.EventCommandOutcomeObserved,
		domain.EventSpokeCursorAdvanced,
		domain.EventOAuthClientRegistered,
		domain.EventOAuthClientActorBound,
		domain.EventOAuthClientActorBindingRequested,
		domain.EventOAuthClientRevoked,
		domain.EventOAuthAuthorizationRequestCreated,
		domain.EventOAuthAuthorizationRequestCompleted,
		domain.EventOAuthAuthorizationCodeIssued,
		domain.EventOAuthAuthorizationCodeRedeemed,
		domain.EventOAuthGrantIssued,
		domain.EventOAuthGrantRefreshed,
		domain.EventOAuthGrantRevoked,
		domain.EventOAuthRefreshReuseDetected:
		return KindClassAdmin, false, true
	default:
		return "", false, false
	}
}

func ClassifyItem(item Item) (string, bool) {
	class, dynamic, ok := StaticKindClass(item.Kind)
	if !ok {
		return "", false
	}
	if !dynamic {
		return class, true
	}
	var payload struct {
		InnerKind string `json:"inner_kind"`
		Kind      string `json:"kind"`
	}
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return KindClassProgress, true
	}
	inner := strings.TrimSpace(payload.InnerKind)
	if inner == "" {
		inner = strings.TrimSpace(payload.Kind)
	}
	if strings.HasPrefix(inner, "coordination.") {
		return KindClassDecision, true
	}
	return KindClassProgress, true
}

func ClassifyEvent(kind string, payload any) (string, bool) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", false
	}
	return ClassifyItem(Item{Kind: kind, Payload: raw})
}

func (f ProjectionFilter) Matches(item Item) bool {
	if slices.Contains(f.Kinds, item.Kind) {
		return true
	}
	class, ok := ClassifyItem(item)
	return ok && slices.Contains(f.KindClasses, class)
}

func (f ProjectionFilter) QueryKinds() []string {
	kinds := make([]string, 0, len(domain.AllEventKinds))
	seen := map[string]bool{}
	add := func(kind string) {
		if !seen[kind] {
			seen[kind] = true
			kinds = append(kinds, kind)
		}
	}
	for _, kind := range f.Kinds {
		add(kind)
	}
	for _, kind := range domain.AllEventKinds {
		class, dynamic, ok := StaticKindClass(kind)
		if !ok || class == KindClassAdmin {
			continue
		}
		if slices.Contains(f.KindClasses, class) {
			add(kind)
		}
		if dynamic && (slices.Contains(f.KindClasses, KindClassDecision) || slices.Contains(f.KindClasses, KindClassProgress)) {
			add(kind)
		}
	}
	return kinds
}

func dedupeTrimmed(values []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}
