package api

// Schema/validator parity guard for docs/schemas/meristem.work_spec.v1.json.
// The JSON Schema lists only the canonical schema_version; the HTTP validator
// additionally accepts legacy maristem.work_spec.v1 and wayline.work_spec.v1
// (see internal/api/signals.go).
//
// The hand-rolled validator in signals.go is a faithful but separate
// transcription of the JSON Schema. Without a guard, the two drift: a
// new optional field added to the schema is silently rejected by the
// validator (fail-closed), and a new required field added to the
// schema is silently accepted (fail-open, the dangerous direction).
//
// This test reads the schema file as the source of truth and asserts
// that the validator's lifted data structures
// (workSpecAllowedKeys, workSpecRequiredKeys, the per-object allowed
// sets, the schema_version constant, the priority pattern, and the
// acceptance_criteria minItems) match what the schema declares. It
// also performs a runtime drift check: for each key the validator
// claims is required, omit it from an otherwise-valid work_spec and
// confirm validateWorkSpec returns an error. That catches the case
// where the data lists a required key but the validator code forgets
// to enforce it.
//
// Path note: go test cds to the package directory before running, so
// the relative path below resolves from internal/api/ up to the
// repository root.

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
)

const workSpecSchemaPath = "../../docs/schemas/meristem.work_spec.v1.json"

type workSpecSchemaNode struct {
	Type                 string                         `json:"type,omitempty"`
	Const                string                         `json:"const,omitempty"`
	Pattern              string                         `json:"pattern,omitempty"`
	AdditionalProperties json.RawMessage                `json:"additionalProperties,omitempty"`
	Required             []string                       `json:"required,omitempty"`
	Properties           map[string]*workSpecSchemaNode `json:"properties,omitempty"`
	Items                *workSpecSchemaNode            `json:"items,omitempty"`
	MinItems             int                            `json:"minItems,omitempty"`
	Minimum              int                            `json:"minimum,omitempty"`
}

func loadWorkSpecSchema(t *testing.T) *workSpecSchemaNode {
	t.Helper()
	bytes, err := os.ReadFile(workSpecSchemaPath)
	if err != nil {
		t.Fatalf("read %s: %v (parity test must run from internal/api with the repo root reachable)", workSpecSchemaPath, err)
	}
	var node workSpecSchemaNode
	if err := json.Unmarshal(bytes, &node); err != nil {
		t.Fatalf("parse %s: %v", workSpecSchemaPath, err)
	}
	return &node
}

// requireAdditionalPropertiesFalse asserts that the schema's
// additionalProperties is the literal JSON `false`. The validator's
// rejectUnknownKeys is only sound when the schema also forbids
// unknown keys; if the schema flips to `true` (or to a sub-schema)
// the parity story breaks and the test should fail loudly.
func requireAdditionalPropertiesFalse(t *testing.T, raw json.RawMessage, where string) {
	t.Helper()
	if string(raw) != "false" {
		t.Fatalf("%s: additionalProperties must be the literal `false`; got %q. The validator's reject-unknown logic depends on this.", where, string(raw))
	}
}

func sortedKeys(m map[string]*workSpecSchemaNode) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedSet(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func sortedMapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestWorkSpec_SchemaTopLevelShape(t *testing.T) {
	schema := loadWorkSpecSchema(t)

	requireAdditionalPropertiesFalse(t, schema.AdditionalProperties, "work_spec")

	schemaKeys := sortedKeys(schema.Properties)
	validatorKeys := sortedMapKeys(workSpecAllowedKeys)
	if !equalStringSlices(schemaKeys, validatorKeys) {
		t.Fatalf("work_spec allowed-key drift:\n  schema (docs/schemas/meristem.work_spec.v1.json): %v\n  validator (workSpecAllowedKeys):                  %v", schemaKeys, validatorKeys)
	}

	schemaRequired := sortedSet(schema.Required)
	validatorRequired := sortedSet(workSpecRequiredKeys)
	if !equalStringSlices(schemaRequired, validatorRequired) {
		t.Fatalf("work_spec required-key drift:\n  schema:    %v\n  validator: %v", schemaRequired, validatorRequired)
	}
}

func TestWorkSpec_SchemaVersionConstMatchesValidator(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	sv, ok := schema.Properties["schema_version"]
	if !ok || sv == nil {
		t.Fatal("schema is missing properties.schema_version")
	}
	if sv.Const != workSpecSchemaVersion {
		t.Fatalf("schema_version const drift:\n  schema:    %q\n  validator: %q", sv.Const, workSpecSchemaVersion)
	}
}

func TestWorkSpec_PriorityPatternMatchesValidator(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	priority, ok := schema.Properties["priority"]
	if !ok || priority == nil {
		t.Fatal("schema is missing properties.priority")
	}
	if priority.Pattern != workSpecPriorityPattern {
		t.Fatalf("priority pattern drift:\n  schema:    %q\n  validator: %q", priority.Pattern, workSpecPriorityPattern)
	}
}

func TestWorkSpec_AcceptanceCriteriaMinItemsMatchesValidator(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	ac, ok := schema.Properties["acceptance_criteria"]
	if !ok || ac == nil {
		t.Fatal("schema is missing properties.acceptance_criteria")
	}
	if ac.MinItems != workSpecAcceptanceCriteriaMinItems {
		t.Fatalf("acceptance_criteria.minItems drift: schema=%d validator=%d", ac.MinItems, workSpecAcceptanceCriteriaMinItems)
	}
}

func TestWorkSpec_SourceObjectShape(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	src, ok := schema.Properties["source"]
	if !ok || src == nil {
		t.Fatal("schema is missing properties.source")
	}
	requireAdditionalPropertiesFalse(t, src.AdditionalProperties, "work_spec.source")

	if !equalStringSlices(sortedKeys(src.Properties), sortedMapKeys(workSpecSourceAllowedKeys)) {
		t.Fatalf("work_spec.source allowed-key drift:\n  schema:    %v\n  validator: %v", sortedKeys(src.Properties), sortedMapKeys(workSpecSourceAllowedKeys))
	}
	if !equalStringSlices(sortedSet(src.Required), sortedSet(workSpecSourceRequiredKeys)) {
		t.Fatalf("work_spec.source required-key drift:\n  schema:    %v\n  validator: %v", sortedSet(src.Required), sortedSet(workSpecSourceRequiredKeys))
	}
}

func TestWorkSpec_TargetObjectShape(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	target, ok := schema.Properties["target"]
	if !ok || target == nil {
		t.Fatal("schema is missing properties.target")
	}
	requireAdditionalPropertiesFalse(t, target.AdditionalProperties, "work_spec.target")

	if !equalStringSlices(sortedKeys(target.Properties), sortedMapKeys(workSpecTargetAllowedKeys)) {
		t.Fatalf("work_spec.target allowed-key drift:\n  schema:    %v\n  validator: %v", sortedKeys(target.Properties), sortedMapKeys(workSpecTargetAllowedKeys))
	}
	if len(target.Required) != 0 {
		t.Fatalf("work_spec.target schema added required keys but validator treats none as required: %v", target.Required)
	}
}

func TestWorkSpec_ValidationObjectShape(t *testing.T) {
	schema := loadWorkSpecSchema(t)
	v, ok := schema.Properties["validation"]
	if !ok || v == nil {
		t.Fatal("schema is missing properties.validation")
	}
	requireAdditionalPropertiesFalse(t, v.AdditionalProperties, "work_spec.validation")

	if !equalStringSlices(sortedKeys(v.Properties), sortedMapKeys(workSpecValidationAllowedKeys)) {
		t.Fatalf("work_spec.validation allowed-key drift:\n  schema:    %v\n  validator: %v", sortedKeys(v.Properties), sortedMapKeys(workSpecValidationAllowedKeys))
	}
	if len(v.Required) != 0 {
		t.Fatalf("work_spec.validation schema added required keys but validator treats none as required: %v", v.Required)
	}
}

// TestWorkSpec_RequiredKeysActuallyEnforced is the behavior-side guard.
// The structural tests above prove the schema and the validator's data
// agree on which keys are required. This test proves the validator's
// code actually rejects requests missing those keys, by omitting each
// in turn from an otherwise-valid work_spec and asserting an error.
//
// Without this, the data could lie: someone could add a required key
// to the schema and to workSpecRequiredKeys but forget the matching
// requiredString call in validateWorkSpec, and every other test would
// pass while invalid signals slipped through.
func TestWorkSpec_RequiredKeysActuallyEnforced(t *testing.T) {
	baseline := map[string]any{
		"schema_version":      workSpecSchemaVersion,
		"kind":                "repair",
		"title":               "fix the worker retry budget",
		"priority":            "P2",
		"acceptance_criteria": []string{"the budget is honored"},
	}

	if err := validateWorkSpec(mustMarshal(t, baseline)); err != nil {
		t.Fatalf("baseline must be valid; if this fires, the test setup itself drifted from the schema: %v", err)
	}

	for _, key := range workSpecRequiredKeys {
		key := key
		t.Run(fmt.Sprintf("missing/%s", key), func(t *testing.T) {
			variant := make(map[string]any, len(baseline))
			for k, v := range baseline {
				if k == key {
					continue
				}
				variant[k] = v
			}
			err := validateWorkSpec(mustMarshal(t, variant))
			if err == nil {
				t.Fatalf("validateWorkSpec accepted a work_spec missing required key %q; the validator code forgot to enforce it (workSpecRequiredKeys lists it but no requiredString call rejects its absence)", key)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	bytes, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal helper: %v", err)
	}
	return bytes
}
