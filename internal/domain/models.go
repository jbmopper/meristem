package domain

import (
	"time"

	"github.com/google/uuid"
)

const (
	EventTokenCreated                 = "token.created"
	EventTokenRevoked                 = "token.revoked"
	EventIdempotencyRecorded          = "idempotency.recorded"
	EventMessageCaptured              = "message.captured"
	EventWorkItemCreated              = "work_item.created"
	EventWorkItemTransitioned         = "work_item.transitioned"
	EventWorkItemEventAppended        = "work_item.event_appended"
	EventWorkItemRelationAdded        = "work_item.relation_added"
	EventWorkItemMetadataUpdated      = "work_item.metadata_updated"
	EventXylemExhausted               = "xylem.exhausted"
	EventSignalReceived               = "signal.received"
	EventDeterministicErrorReported   = "deterministic_error.reported"
	EventDeterministicErrorMasked     = "deterministic_error.masked"
	EventDeterministicErrorUnmasked   = "deterministic_error.unmasked"
	EventEscalationRequested          = "escalation.requested"
	EventSubactorGrantRequested       = "subactor_grant.requested"
	EventSubactorGrantGranted         = "subactor_grant.granted"
	EventSubactorGrantDenied          = "subactor_grant.denied"
	EventSubactorGrantEscalated       = "subactor_grant.escalated"
	EventCultivarActivationRequested  = "cultivar_activation.requested"
	EventCultivarActivationGranted    = "cultivar_activation.granted"
	EventCultivarActivationDenied     = "cultivar_activation.denied"
	EventCultivarActivationEscalated  = "cultivar_activation.escalated"
	EventApprovalCreated              = "approval.created"
	EventApprovalDecided              = "approval.decided"
	EventApprovalExpired              = "approval.expired"
	EventHTTPConnectorActionRequested = "http_connector.action_requested"
	EventHTTPConnectorActionApproved  = "http_connector.action_approved"
	EventHTTPConnectorActionSent      = "http_connector.action_sent"
	// EventPatienceBreached records that a non-terminal work_item has been
	// in its current state longer than the configured patience budget for
	// that state. Recorded by `meristem worker --once` (see internal/worker).
	// The worker uses it as a deterministic tripwire: after recording the
	// breach observation and the budget against which it was measured, the same
	// scan routes the breached state epoch through its mechanical escalation
	// path.
	//
	// The deterministic event_id is keyed on (work_item_id, state, kind, payload)
	// where payload includes the state observed; rerunning the worker for an
	// item still breached in the same state produces the same id and dedupes
	// to one row. A subsequent transition out of the state ends the breach;
	// re-entering would breach again on the next budget elapse, which is a
	// new (state, payload) and therefore a new event.
	EventPatienceBreached = "patience.breached"
	// EventConvergenceVerdictRecorded records the output of a deterministic
	// convergence reduction (see internal/convergence and docs/spec.md →
	// "Convergence Patterns"). The payload carries the reducer identity and
	// version, the digest of the signals it reduced over, the raw signals,
	// and the resulting verdict (accept | reject | escalate). Per principle
	// #12 the *reduction* is what advances the lifecycle, and it must be
	// replayable from the log alone — so a stricter future reducer can
	// re-fold the same signals. The persistence slice projects this kind into
	// convergence_verdicts; worker emission remains separately gated.
	EventConvergenceVerdictRecorded = "convergence.verdict_recorded"
	// EventConvergenceChecksProposed records a scribe proposal for a parent
	// work_item's suggested convergence checks. The deterministic reducer
	// records its disposition separately as convergence.verdict_recorded.
	EventConvergenceChecksProposed = "convergence.checks_proposed"
	// EventDispatchRequested records that the deterministic worker observed a
	// work_item ready for agent attention and placed it on the dispatch feed.
	// The subject is the target work_item; the payload names the handling
	// cultivar and the state epoch so re-scans converge on one dispatch entry
	// per item/state/cultivar combination.
	EventDispatchRequested = "dispatch.requested"

	// EventPolicyProfileSwitched records an operator switching the active
	// safety policy profile (bring-up vs steady). The subject is the
	// singleton policy_profile aggregate; the payload carries from/to and
	// the target profile's fingerprint so the audit answers "what envelope
	// was active when" without recomputing.
	EventPolicyProfileSwitched = "policy_profile.switched"

	EventTropismDefined    = "tropism.defined"
	EventCultivarDefined   = "cultivar.defined"
	EventProjectionDefined = "projection.defined"

	// EventNodeRegistered records a fleet node joining the registry with its
	// stable, DNS-safe node_id and reachability descriptors (ingress base_url,
	// optional direct peer route, relay chain, status). The subject is the
	// node aggregate; the projection is the `nodes` table. See
	// docs/network-layer-spec.md §2 "Naming" and §6 stage 0.
	EventNodeRegistered = "node.registered"
	// EventNodeRouteUpdated records a change to how a registered node is
	// reached — its direct_url, relay_via chain, and/or status — without
	// re-registering it. §2b of the network spec models route changes as
	// node.route_updated events folded into the same `nodes` row.
	EventNodeRouteUpdated = "node.route_updated"
)

// AllEventKinds enumerates every event kind the system knows how to append.
// Adding a new event constant above MUST also add it here. Downstream
// consumers (notably internal/feed's drift guard) rely on this slice to
// force a feed-policy decision for every kind: if a kind is not classified
// as either feed-visible or feed-excluded, the test fails.
//
// Order is the declaration order above; treat the slice as immutable.
var AllEventKinds = []string{
	EventTokenCreated,
	EventTokenRevoked,
	EventIdempotencyRecorded,
	EventMessageCaptured,
	EventWorkItemCreated,
	EventWorkItemTransitioned,
	EventWorkItemEventAppended,
	EventWorkItemRelationAdded,
	EventWorkItemMetadataUpdated,
	EventXylemExhausted,
	EventSignalReceived,
	EventDeterministicErrorReported,
	EventDeterministicErrorMasked,
	EventDeterministicErrorUnmasked,
	EventEscalationRequested,
	EventSubactorGrantRequested,
	EventSubactorGrantGranted,
	EventSubactorGrantDenied,
	EventSubactorGrantEscalated,
	EventCultivarActivationRequested,
	EventCultivarActivationGranted,
	EventCultivarActivationDenied,
	EventCultivarActivationEscalated,
	EventApprovalCreated,
	EventApprovalDecided,
	EventApprovalExpired,
	EventHTTPConnectorActionRequested,
	EventHTTPConnectorActionApproved,
	EventHTTPConnectorActionSent,
	EventPatienceBreached,
	EventConvergenceVerdictRecorded,
	EventConvergenceChecksProposed,
	EventDispatchRequested,
	EventPolicyProfileSwitched,
	EventTropismDefined,
	EventCultivarDefined,
	EventProjectionDefined,
	EventNodeRegistered,
	EventNodeRouteUpdated,
}

const (
	SubjectToken               = "token"
	SubjectIdempotencyKey      = "idempotency_key"
	SubjectMessage             = "message"
	SubjectWorkItem            = "work_item"
	SubjectSignal              = "signal"
	SubjectDeterministicError  = "deterministic_error"
	SubjectEscalation          = "escalation"
	SubjectSubactorGrant       = "subactor_grant"
	SubjectCultivarActivation  = "cultivar_activation"
	SubjectApproval            = "approval"
	SubjectHTTPConnectorAction = "http_connector_action"
	SubjectTropism             = "tropism"
	SubjectCultivar            = "cultivar"
	SubjectProjection          = "projection"
	// SubjectConvergence is the subject kind for a convergence verdict. The
	// subject_id is the work_item being reduced; the attempt lives in the event
	// payload, so (work_item_id, attempt, payload) remains the deterministic
	// event identity while the projection can index verdicts by work_item_id.
	SubjectConvergence = "convergence"
	// SubjectPolicyProfile is the singleton aggregate for the active safety
	// policy profile; every switch event shares one well-known subject id
	// (see internal/policyprofile.SubjectID).
	SubjectPolicyProfile = "policy_profile"
	// SubjectNode is the subject kind for a fleet node in the registry. The
	// subject_id is a deterministic UUID derived from the node_id (see
	// internal/nodes.NodeSubjectID) so every event about one node shares a
	// subject while the projection keys on the DNS-safe node_id text.
	SubjectNode = "node"
)

// Verdict is the disposition produced by a deterministic convergence
// reduction. It is the only thing that advances a work_item's lifecycle
// out of a convergence loop: the candidate (a model's patch, plan, or
// answer) never advances directly — the reducer's verdict does. See
// docs/spec.md → "Convergence Patterns" and internal/convergence.
type Verdict string

const (
	// VerdictAccept means the reducer judged the candidate converged: the
	// work_item may advance toward done.
	VerdictAccept Verdict = "accept"
	// VerdictReject means the candidate failed the reduction. Whether this
	// is terminal or triggers a retry is the patience budget's call, not
	// the verdict's.
	VerdictReject Verdict = "reject"
	// VerdictEscalate means the reducer could not dispose on the signals it
	// had (a tie, missing signals, an exhausted budget). The escalation rule
	// — fail, request approval, or hand to a human — decides what happens.
	VerdictEscalate Verdict = "escalate"
)

// Valid reports whether v is one of the three accepted verdicts.
func (v Verdict) Valid() bool {
	switch v {
	case VerdictAccept, VerdictReject, VerdictEscalate:
		return true
	}
	return false
}

// Token is the active client credential projection. The raw bearer secret is
// never stored; Hash is the SHA-256 digest of the random token string.
type Token struct {
	ID        uuid.UUID
	Name      string
	Hash      []byte
	IsRoot    bool
	Scopes    []string
	Source    Source
	CreatedAt time.Time
	RevokedAt *time.Time
}

// WorkItemState is the lifecycle state projected for a work item.
type WorkItemState string

const (
	WorkItemCaptured         WorkItemState = "captured"
	WorkItemTriaged          WorkItemState = "triaged"
	WorkItemPlanned          WorkItemState = "planned"
	WorkItemAwaitingApproval WorkItemState = "awaiting_approval"
	WorkItemRunning          WorkItemState = "running"
	WorkItemBlocked          WorkItemState = "blocked"
	WorkItemDone             WorkItemState = "done"
	WorkItemFailed           WorkItemState = "failed"
	WorkItemCanceled         WorkItemState = "canceled"
)

// Valid reports whether s is one of the lifecycle states in docs/spec.md.
func (s WorkItemState) Valid() bool {
	switch s {
	case WorkItemCaptured, WorkItemTriaged, WorkItemPlanned,
		WorkItemAwaitingApproval, WorkItemRunning, WorkItemBlocked,
		WorkItemDone, WorkItemFailed, WorkItemCanceled:
		return true
	}
	return false
}

// Terminal reports whether no further automatic transition is expected.
func (s WorkItemState) Terminal() bool {
	switch s {
	case WorkItemDone, WorkItemFailed, WorkItemCanceled:
		return true
	}
	return false
}

// CanTransition reports whether a v0 manual transition is legal. v0 is
// intentionally permissive enough for owner/agent-driven bootstrap work while
// still rejecting moves out of terminal states.
func CanTransition(from, to WorkItemState) bool {
	if !from.Valid() || !to.Valid() {
		return false
	}
	if from == to {
		return true
	}
	if from.Terminal() {
		return false
	}
	return true
}

// HumanReviewStatus records the owner's review disposition for a work item.
// It is metadata used by reconcilers and worker scaffolding; it is distinct
// from the lifecycle state and from future approval rows.
type HumanReviewStatus string

const (
	HumanReviewBlocked      HumanReviewStatus = "blocked"
	HumanReviewWavedThrough HumanReviewStatus = "waved_through"
	HumanReviewApproved     HumanReviewStatus = "approved"
)

// Valid reports whether s is one of the accepted human review statuses.
func (s HumanReviewStatus) Valid() bool {
	switch s {
	case HumanReviewBlocked, HumanReviewWavedThrough, HumanReviewApproved:
		return true
	}
	return false
}

type EscalationRule string

const (
	EscalationRuleHandToHuman EscalationRule = "hand_to_human"
)

func (r EscalationRule) Valid() bool {
	switch r {
	case EscalationRuleHandToHuman:
		return true
	default:
		return false
	}
}

// WorkItem is the current-state projection for work_items.
type WorkItem struct {
	ID                         uuid.UUID
	Title                      string
	Body                       string
	State                      WorkItemState
	StateReason                *string
	SuggestedConvergenceChecks []string
	HumanReviewStatus          HumanReviewStatus
	CreatedBy                  *uuid.UUID
	CreatedAt                  time.Time
	StateEnteredAt             time.Time
	UpdatedAt                  time.Time
}

// Message is the v0 text-only inbox projection.
type Message struct {
	ID           uuid.UUID
	Source       Source
	ActorTokenID *uuid.UUID
	WorkItemID   *uuid.UUID
	Text         string
	CreatedAt    time.Time
}

// Signal is the projection for one accepted POST /v1/signals reception.
// See docs/signals.md for the wider contract; this is the read-side type
// services consume after the projector has run.
type Signal struct {
	ID              uuid.UUID
	ReceivedAt      time.Time
	ActorTokenID    *uuid.UUID
	Source          Source
	SignalKind      string
	DedupeKey       *string
	Fingerprint     []byte
	WorkSpec        []byte
	WorkItemID      *uuid.UUID
	CreatedWorkItem bool
}

// DeterministicErrorSeverity is the operator-facing priority of a
// deterministic-layer error report.
type DeterministicErrorSeverity string

const (
	DeterministicErrorInfo     DeterministicErrorSeverity = "info"
	DeterministicErrorWarning  DeterministicErrorSeverity = "warning"
	DeterministicErrorError    DeterministicErrorSeverity = "error"
	DeterministicErrorCritical DeterministicErrorSeverity = "critical"
)

// Valid reports whether s is one of the accepted deterministic error
// severities.
func (s DeterministicErrorSeverity) Valid() bool {
	switch s {
	case DeterministicErrorInfo, DeterministicErrorWarning, DeterministicErrorError, DeterministicErrorCritical:
		return true
	}
	return false
}

// NodeStatus is the reachability disposition projected for a fleet node.
// Status is free-form on the write path (the projector only requires it to be
// non-empty) so future statuses do not need a code change to be folded; these
// constants name the values stage 0 uses.
type NodeStatus string

const (
	// NodeStatusActive marks a node that is registered and expected to be
	// reachable over at least one approved route.
	NodeStatusActive NodeStatus = "active"
	// NodeStatusUnreachable marks a node with no currently reachable route
	// (e.g. ingress down during interim bring-up). It stays in the registry.
	NodeStatusUnreachable NodeStatus = "unreachable"
)

// Node is the current-state projection for one fleet node in the registry.
// Truth is the node.registered / node.route_updated event stream; this is the
// read-side type after the projector has folded them into the `nodes` table.
type Node struct {
	NodeID    string
	BaseURL   *string
	DirectURL *string
	RelayVia  []string
	Status    NodeStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// DeterministicError is the current-state projection for deterministic-layer
// error reports. Masking hides a report from active error views without
// deleting or mutating the underlying events.
type DeterministicError struct {
	ID         uuid.UUID
	Component  string
	Code       string
	Message    string
	Severity   DeterministicErrorSeverity
	Details    []byte
	ReportedBy *uuid.UUID
	ReportedAt time.Time
	UpdatedAt  time.Time
	Masked     bool
	MaskReason *string
	MaskedBy   *uuid.UUID
	MaskedAt   *time.Time
}
