package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/wayline/internal/signals"
)

// Pinned schema constants. The drift guard in
// work_spec_schema_parity_test.go asserts these stay in step with
// docs/schemas/wayline.work_spec.v1.json.
const (
	workSpecSchemaVersion   = "wayline.work_spec.v1"
	workSpecPriorityPattern = `^P[0-3]$`
)

var priorityPattern = regexp.MustCompile(workSpecPriorityPattern)

// Allowed-key and required-key sets the validator consults. Lifted to
// package level so the parity test can inspect them; the validator
// continues to consume them via the existing helpers
// (rejectUnknownKeys, requiredString, etc.). Mutating any of these maps
// at runtime is a bug.
var (
	workSpecAllowedKeys = map[string]bool{
		"schema_version":       true,
		"kind":                 true,
		"dedupe_key":           true,
		"title":                true,
		"priority":             true,
		"objective":            true,
		"details":              true,
		"source":               true,
		"target":               true,
		"acceptance_criteria":  true,
		"validation":           true,
		"constraints":          true,
		"labels":               true,
		"implementation_notes": true,
	}

	workSpecRequiredKeys = []string{
		"schema_version",
		"kind",
		"title",
		"priority",
		"acceptance_criteria",
	}

	workSpecSourceAllowedKeys = map[string]bool{
		"kind":         true,
		"identifier":   true,
		"external_ref": true,
	}

	workSpecSourceRequiredKeys = []string{
		"kind",
		"identifier",
	}

	workSpecTargetAllowedKeys = map[string]bool{
		"repo":       true,
		"path":       true,
		"line_start": true,
		"line_end":   true,
	}

	workSpecValidationAllowedKeys = map[string]bool{
		"commands": true,
		"notes":    true,
	}
)

const workSpecAcceptanceCriteriaMinItems = 1

type receiveSignalRequest struct {
	Kind      string              `json:"kind"`
	DedupeKey string              `json:"dedupe_key"`
	Source    signalSourceRequest `json:"source"`
	WorkSpec  json.RawMessage     `json:"work_spec"`
}

type signalSourceRequest struct {
	Kind        string `json:"kind"`
	Identifier  string `json:"identifier"`
	ExternalRef string `json:"external_ref"`
}

type signalResponse struct {
	Idempotency signalIdempotencyResponse `json:"idempotency"`
	Dedupe      signalDedupeResponse      `json:"dedupe"`
	Resource    signalResourceResponse    `json:"resource"`
	WorkItem    signalWorkItemResponse    `json:"work_item"`
	Events      signalEventsResponse      `json:"events"`
	Fingerprint string                    `json:"fingerprint"`
}

// signalIdempotencyResponse intentionally omits a "replayed" boolean. The
// idempotency middleware caches the response body verbatim and serves it
// on a retry, so any "replayed" field set in the body would be a frozen
// lie on every cache hit. Clients detect replays via the
// `Idempotency-Replayed` HTTP header, which the middleware sets when
// returning a cached response. See docs/signals.md "Endpoint" → status
// codes for the contract.
type signalIdempotencyResponse struct {
	Key string `json:"key"`
}

type signalDedupeResponse struct {
	Key             string `json:"key,omitempty"`
	CreatedWorkItem bool   `json:"created_work_item"`
}

type signalResourceResponse struct {
	Kind string    `json:"kind"`
	ID   uuid.UUID `json:"id"`
}

type signalWorkItemResponse struct {
	ID uuid.UUID `json:"id"`
}

type signalEventsResponse struct {
	SignalReceived  uuid.UUID  `json:"signal_received"`
	WorkItemCreated *uuid.UUID `json:"work_item_created,omitempty"`
}

func (s *Server) handleReceiveSignal(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeReceiveSignalRequest(w, r)
	if !ok {
		return
	}
	if err := validateWorkSpec(req.WorkSpec); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_work_spec", err.Error())
		return
	}
	if s.signals == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "signals_unavailable", "signals service is not configured")
		return
	}
	result, err := s.signals.Receive(r.Context(), actor, signals.ReceiveInput{
		SignalKind: req.Kind,
		DedupeKey:  req.DedupeKey,
		Source: signals.SourceMetadata{
			Kind:        strings.TrimSpace(req.Source.Kind),
			Identifier:  strings.TrimSpace(req.Source.Identifier),
			ExternalRef: strings.TrimSpace(req.Source.ExternalRef),
		},
		WorkSpec: req.WorkSpec,
	})
	if err != nil {
		writeSignalError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, signalResponseFrom(result, r.Header.Get("Idempotency-Key")))
}

func decodeReceiveSignalRequest(w http.ResponseWriter, r *http.Request) (receiveSignalRequest, bool) {
	defer func() { _ = r.Body.Close() }()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req receiveSignalRequest
	if err := dec.Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return receiveSignalRequest{}, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return receiveSignalRequest{}, false
	}
	return req, true
}

func signalResponseFrom(result signals.ReceiveResult, idempotencyKey string) signalResponse {
	var workItemCreated *uuid.UUID
	if result.WorkItemEventID != uuid.Nil {
		id := result.WorkItemEventID
		workItemCreated = &id
	}
	return signalResponse{
		Idempotency: signalIdempotencyResponse{
			Key: idempotencyKey,
		},
		Dedupe: signalDedupeResponse{
			Key:             result.DedupeKey,
			CreatedWorkItem: result.CreatedWorkItem,
		},
		Resource: signalResourceResponse{
			Kind: "signal",
			ID:   result.SignalID,
		},
		WorkItem: signalWorkItemResponse{
			ID: result.WorkItemID,
		},
		Events: signalEventsResponse{
			SignalReceived:  result.SignalEventID,
			WorkItemCreated: workItemCreated,
		},
		Fingerprint: "sha256:" + result.Fingerprint,
	}
}

func writeSignalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, signals.ErrSignalKindRequired):
		writeAPIError(w, http.StatusBadRequest, "signal_kind_required", err.Error())
	case errors.Is(err, signals.ErrWorkSpecRequired):
		writeAPIError(w, http.StatusBadRequest, "work_spec_required", err.Error())
	case errors.Is(err, signals.ErrWorkSpecInvalid):
		writeAPIError(w, http.StatusBadRequest, "invalid_work_spec", err.Error())
	case errors.Is(err, signals.ErrWorkSpecMissingTitle):
		writeAPIError(w, http.StatusBadRequest, "work_spec_missing_title", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "signal_receive_failed", "could not receive signal")
	}
}

func validateWorkSpec(raw json.RawMessage) error {
	obj, err := decodeObject(raw, "work_spec")
	if err != nil {
		return err
	}
	if err := rejectUnknownKeys(obj, workSpecAllowedKeys, "work_spec"); err != nil {
		return err
	}
	if value, err := requiredString(obj, "schema_version", "work_spec"); err != nil {
		return err
	} else if value != workSpecSchemaVersion {
		return fmt.Errorf("work_spec.schema_version must be %s", workSpecSchemaVersion)
	}
	if _, err := requiredString(obj, "kind", "work_spec"); err != nil {
		return err
	}
	if _, err := requiredString(obj, "title", "work_spec"); err != nil {
		return err
	}
	if priority, err := requiredString(obj, "priority", "work_spec"); err != nil {
		return err
	} else if !priorityPattern.MatchString(priority) {
		return fmt.Errorf("work_spec.priority must match P0, P1, P2, or P3")
	}
	if err := optionalString(obj, "dedupe_key", "work_spec"); err != nil {
		return err
	}
	if err := optionalString(obj, "objective", "work_spec"); err != nil {
		return err
	}
	if err := optionalString(obj, "details", "work_spec"); err != nil {
		return err
	}
	if err := validateStringArray(obj, "acceptance_criteria", "work_spec", true, workSpecAcceptanceCriteriaMinItems); err != nil {
		return err
	}
	if err := validateSourceObject(obj, "source", "work_spec"); err != nil {
		return err
	}
	if err := validateTargetObject(obj, "target", "work_spec"); err != nil {
		return err
	}
	if err := validateValidationObject(obj, "validation", "work_spec"); err != nil {
		return err
	}
	for _, field := range []string{"constraints", "labels", "implementation_notes"} {
		if err := validateStringArray(obj, field, "work_spec", false, 0); err != nil {
			return err
		}
	}
	return nil
}

func decodeObject(raw json.RawMessage, path string) (map[string]json.RawMessage, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, fmt.Errorf("%s is required", path)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}
	if obj == nil {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}
	return obj, nil
}

func rejectUnknownKeys(obj map[string]json.RawMessage, allowed map[string]bool, path string) error {
	for key := range obj {
		if !allowed[key] {
			return fmt.Errorf("%s.%s is not allowed", path, key)
		}
	}
	return nil
}

func requiredString(obj map[string]json.RawMessage, field string, path string) (string, error) {
	raw, ok := obj[field]
	if !ok {
		return "", fmt.Errorf("%s.%s is required", path, field)
	}
	value, err := decodeString(raw, path+"."+field)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s.%s must be a non-empty string", path, field)
	}
	return value, nil
}

func optionalString(obj map[string]json.RawMessage, field string, path string) error {
	raw, ok := obj[field]
	if !ok {
		return nil
	}
	value, err := decodeString(raw, path+"."+field)
	if err != nil {
		return err
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s.%s must be a non-empty string", path, field)
	}
	return nil
}

func decodeString(raw json.RawMessage, path string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a string", path)
	}
	return value, nil
}

func validateStringArray(obj map[string]json.RawMessage, field string, path string, required bool, minItems int) error {
	raw, ok := obj[field]
	if !ok {
		if required {
			return fmt.Errorf("%s.%s is required", path, field)
		}
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return fmt.Errorf("%s.%s must be an array of strings", path, field)
	}
	if len(values) < minItems {
		return fmt.Errorf("%s.%s must contain at least %d item(s)", path, field, minItems)
	}
	for i, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s.%s[%d] must be a non-empty string", path, field, i)
		}
	}
	return nil
}

func validateSourceObject(obj map[string]json.RawMessage, field string, path string) error {
	raw, ok := obj[field]
	if !ok {
		return nil
	}
	source, err := decodeObject(raw, path+"."+field)
	if err != nil {
		return err
	}
	if err := rejectUnknownKeys(source, workSpecSourceAllowedKeys, path+"."+field); err != nil {
		return err
	}
	if _, err := requiredString(source, "kind", path+"."+field); err != nil {
		return err
	}
	if _, err := requiredString(source, "identifier", path+"."+field); err != nil {
		return err
	}
	return optionalString(source, "external_ref", path+"."+field)
}

func validateTargetObject(obj map[string]json.RawMessage, field string, path string) error {
	raw, ok := obj[field]
	if !ok {
		return nil
	}
	target, err := decodeObject(raw, path+"."+field)
	if err != nil {
		return err
	}
	if err := rejectUnknownKeys(target, workSpecTargetAllowedKeys, path+"."+field); err != nil {
		return err
	}
	if err := optionalString(target, "repo", path+"."+field); err != nil {
		return err
	}
	if err := optionalString(target, "path", path+"."+field); err != nil {
		return err
	}
	for _, numericField := range []string{"line_start", "line_end"} {
		if err := optionalPositiveInteger(target, numericField, path+"."+field); err != nil {
			return err
		}
	}
	return nil
}

func validateValidationObject(obj map[string]json.RawMessage, field string, path string) error {
	raw, ok := obj[field]
	if !ok {
		return nil
	}
	validation, err := decodeObject(raw, path+"."+field)
	if err != nil {
		return err
	}
	if err := rejectUnknownKeys(validation, workSpecValidationAllowedKeys, path+"."+field); err != nil {
		return err
	}
	if err := validateStringArray(validation, "commands", path+"."+field, false, 0); err != nil {
		return err
	}
	return validateStringArray(validation, "notes", path+"."+field, false, 0)
}

func optionalPositiveInteger(obj map[string]json.RawMessage, field string, path string) error {
	raw, ok := obj[field]
	if !ok {
		return nil
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s.%s must be an integer", path, field)
	}
	if value < 1 {
		return fmt.Errorf("%s.%s must be at least 1", path, field)
	}
	return nil
}
