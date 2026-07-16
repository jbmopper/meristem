package workitems

import (
	"encoding/json"
	"fmt"
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

// ParseReviewVerdict validates the typed portion of a review verdict payload.
// Extra fields remain available for bounded review evidence. payload_version
// follows the repository convention that absent means version 1.
func ParseReviewVerdict(payload any) (ReviewVerdict, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: review verdict payload: %v", ErrInvalidRequest, err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil || object == nil {
		return "", fmt.Errorf("%w: review verdict payload must be an object", ErrInvalidRequest)
	}
	var header struct {
		PayloadVersion *int          `json:"payload_version"`
		Verdict        ReviewVerdict `json:"verdict"`
	}
	if err := json.Unmarshal(encoded, &header); err != nil {
		return "", fmt.Errorf("%w: review verdict payload must be an object: %v", ErrInvalidRequest, err)
	}
	version := 1
	if rawVersion, present := object["payload_version"]; present {
		if string(rawVersion) == "null" || header.PayloadVersion == nil {
			return "", fmt.Errorf("%w: review verdict payload_version must be an integer", ErrInvalidRequest)
		}
		version = *header.PayloadVersion
	}
	if version != 1 {
		return "", fmt.Errorf("%w: unsupported review verdict payload_version %d", ErrInvalidRequest, version)
	}
	switch header.Verdict {
	case ReviewVerdictAccepted, ReviewVerdictAcceptedWithFinding, ReviewVerdictBlockingFinding:
		return header.Verdict, nil
	case "":
		return "", fmt.Errorf("%w: review verdict is required", ErrInvalidRequest)
	default:
		return "", fmt.Errorf("%w: unknown review verdict %q", ErrInvalidRequest, header.Verdict)
	}
}

// ChecklistPass is the deterministic review-verdict-to-check mapping.
func (v ReviewVerdict) ChecklistPass() bool {
	return v == ReviewVerdictAccepted || v == ReviewVerdictAcceptedWithFinding
}
