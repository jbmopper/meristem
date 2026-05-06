package errorreporting

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

const (
	// ScopeLogsRead allows an accessor to see deterministic log/error
	// records in active views. Summary fields are visible; details are
	// filtered field-by-field by the scopes below.
	ScopeLogsRead = "logs.read"
	// ScopeLogsReadDetails allows unclassified/internal detail fields.
	ScopeLogsReadDetails = "logs.read_details"
	// ScopeLogsReadRestricted allows restricted/private/encrypted detail
	// fields. It implies logs.read_details.
	ScopeLogsReadRestricted = "logs.read_restricted"
	// ScopeLogsReadMasked allows include_masked views.
	ScopeLogsReadMasked = "logs.read_masked"
	// ScopeLogsReadAll is an operator/debug convenience scope. Root tokens
	// get the same effective policy without needing the literal scope.
	ScopeLogsReadAll = "logs.read_all"

	detailsVisibilityKey = "_visibility"
)

// AccessPolicy is the deterministic read-time privacy/access reducer for
// log-like deterministic error reports. It is intentionally independent of
// any transport so REST, MCP, CLI, and future web views all see the same
// filtered data for the same token/scopes.
type AccessPolicy struct {
	CanRead                  bool
	CanReadDetails           bool
	CanReadRestrictedDetails bool
	CanReadMasked            bool
}

// PolicyForToken reduces a token's scopes into deterministic log visibility.
// The root token is the owner's break-glass/audit credential and can see every
// active, masked, internal, and restricted field.
func PolicyForToken(tok domain.Token) AccessPolicy {
	if tok.IsRoot {
		return AccessPolicy{
			CanRead:                  true,
			CanReadDetails:           true,
			CanReadRestrictedDetails: true,
			CanReadMasked:            true,
		}
	}
	scopes := scopeSet(tok.Scopes)
	readAll := scopes[ScopeLogsReadAll]
	readRestricted := readAll || scopes[ScopeLogsReadRestricted]
	readDetails := readRestricted || scopes[ScopeLogsReadDetails]
	readMasked := readAll || scopes[ScopeLogsReadMasked]
	read := readAll || scopes[ScopeLogsRead] || readDetails || readRestricted || readMasked
	return AccessPolicy{
		CanRead:                  read,
		CanReadDetails:           readDetails,
		CanReadRestrictedDetails: readRestricted,
		CanReadMasked:            readMasked,
	}
}

// Filter returns the accessor-visible view of a deterministic error record.
// It never mutates the caller's copy.
func (p AccessPolicy) Filter(item domain.DeterministicError) domain.DeterministicError {
	item.Details = p.FilterDetails(item.Details)
	return item
}

// FilterDetails applies field-level visibility labels to a details object.
//
// Details may carry an optional top-level "_visibility" object:
//
//	{
//	  "event_kind": "work_item.created",
//	  "raw_payload": "...",
//	  "_visibility": {"event_kind": "public", "raw_payload": "restricted"}
//	}
//
// The visibility object is policy metadata, not diagnostic data, so it is
// never returned in filtered views. Unlabelled fields default to "internal":
// visible to logs.read_details and above, hidden from a base logs.read view.
func (p AccessPolicy) FilterDetails(raw []byte) []byte {
	fields, labels, ok := decodeDetailsWithLabels(raw)
	if !ok {
		return []byte(`{}`)
	}
	out := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		if key == detailsVisibilityKey {
			continue
		}
		label := "internal"
		if labels != nil {
			if tagged := strings.TrimSpace(labels[key]); tagged != "" {
				label = strings.ToLower(tagged)
			}
		}
		if p.allowsDetailLabel(label) {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return []byte(`{}`)
	}
	filtered, err := json.Marshal(out)
	if err != nil {
		return []byte(`{}`)
	}
	return filtered
}

func (p AccessPolicy) allowsDetailLabel(label string) bool {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "public":
		return p.CanRead
	case "", "internal":
		return p.CanReadDetails
	case "restricted", "private", "sensitive", "encrypted":
		return p.CanReadRestrictedDetails
	default:
		// Unknown explicit labels fail closed unless the accessor has the
		// restricted/details-all scope.
		return p.CanReadRestrictedDetails
	}
}

func decodeDetailsWithLabels(raw []byte) (map[string]json.RawMessage, map[string]string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return map[string]json.RawMessage{}, nil, true
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, false
	}
	var labels map[string]string
	if labelRaw, ok := fields[detailsVisibilityKey]; ok {
		_ = json.Unmarshal(labelRaw, &labels)
	}
	return fields, labels, true
}

func scopeSet(scopes []string) map[string]bool {
	out := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope != "" {
			out[scope] = true
		}
	}
	return out
}
