package workitems

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

const (
	// ReviewVerdictInnerKind is the single caller-authored event that closes
	// the reviewer rootstock's machine check. The worker derives the checklist
	// signal from this event; callers must never append that signal directly.
	ReviewVerdictInnerKind = "review.verdict_recorded"
	ReviewVerdictCheck     = "event:review.verdict_recorded"
	ReviewVerdictCheckKind = "checklist.item:" + ReviewVerdictCheck
)

// ReviewVerdict is the closed reviewer decision vocabulary advertised by
// reviewer@1. It is intentionally distinct from convergence.Verdict: this is
// a reviewer's attributed input, not the deterministic reducer's output.
type ReviewVerdict string

const (
	ReviewVerdictAccepted            ReviewVerdict = "accepted"
	ReviewVerdictAcceptedWithFinding ReviewVerdict = "accepted_with_finding"
	ReviewVerdictBlockingFinding     ReviewVerdict = "blocking_finding"
)

// ReviewVerdictDetail is the typed portion of a review verdict payload plus
// the authority claims used by the verdict gate (ee916614 slice 2).
// AssignmentEventID is the work_item.assigned event the reviewer believes
// authorizes it (uuid.Nil: none named); ReviewedCommit is the exact artifact
// the reviewer read (empty: none named).
type ReviewVerdictDetail struct {
	Verdict           ReviewVerdict
	AssignmentEventID uuid.UUID
	ReviewedCommit    string
}

// ParseReviewVerdict validates the typed portion of a review verdict payload.
// Extra fields remain available for bounded review evidence. payload_version
// follows the repository convention that absent means version 1.
func ParseReviewVerdict(payload any) (ReviewVerdict, error) {
	detail, err := ParseReviewVerdictDetail(payload)
	return detail.Verdict, err
}

// ParseReviewVerdictDetail is ParseReviewVerdict plus the optional
// assignment_event_id generation claim. A present-but-malformed claim fails
// closed rather than degrading to "no claim".
func ParseReviewVerdictDetail(payload any) (ReviewVerdictDetail, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict payload: %v", ErrInvalidRequest, err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil || object == nil {
		return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict payload must be an object", ErrInvalidRequest)
	}
	var header struct {
		PayloadVersion    *int          `json:"payload_version"`
		Verdict           ReviewVerdict `json:"verdict"`
		AssignmentEventID *string       `json:"assignment_event_id"`
		ReviewedCommit    string        `json:"reviewed_commit"`
	}
	if err := json.Unmarshal(encoded, &header); err != nil {
		return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict payload must be an object: %v", ErrInvalidRequest, err)
	}
	version := 1
	if rawVersion, present := object["payload_version"]; present {
		if string(rawVersion) == "null" || header.PayloadVersion == nil {
			return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict payload_version must be an integer", ErrInvalidRequest)
		}
		version = *header.PayloadVersion
	}
	if version != 1 {
		return ReviewVerdictDetail{}, fmt.Errorf("%w: unsupported review verdict payload_version %d", ErrInvalidRequest, version)
	}
	detail := ReviewVerdictDetail{ReviewedCommit: header.ReviewedCommit}
	if rawClaim, present := object["assignment_event_id"]; present {
		if string(rawClaim) == "null" || header.AssignmentEventID == nil {
			return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict assignment_event_id must be a UUID string", ErrInvalidRequest)
		}
		parsed, err := uuid.Parse(*header.AssignmentEventID)
		if err != nil || parsed == uuid.Nil {
			return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict assignment_event_id must be a UUID string", ErrInvalidRequest)
		}
		detail.AssignmentEventID = parsed
	}
	switch header.Verdict {
	case ReviewVerdictAccepted, ReviewVerdictAcceptedWithFinding, ReviewVerdictBlockingFinding:
		detail.Verdict = header.Verdict
		return detail, nil
	case "":
		return ReviewVerdictDetail{}, fmt.Errorf("%w: review verdict is required", ErrInvalidRequest)
	default:
		return ReviewVerdictDetail{}, fmt.Errorf("%w: unknown review verdict %q", ErrInvalidRequest, header.Verdict)
	}
}

// ChecklistPass is the deterministic review-verdict-to-check mapping.
func (v ReviewVerdict) ChecklistPass() bool {
	return v == ReviewVerdictAccepted || v == ReviewVerdictAcceptedWithFinding
}
