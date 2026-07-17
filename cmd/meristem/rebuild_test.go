package main

import (
	"strings"
	"testing"
)

func TestLooksLikeIdentifier(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"meristem_rebuild", true},
		{"MeristemRebuild", true},
		{"_underscore_lead", true},
		{"trailing_digits_99", true},
		{"99_leading_digit", false},
		{"with-dash", false},
		{"with space", false},
		{"with;semi", false},
		{"with\"quote", false},
		{"with'apostrophe", false},
		{"with$dollar", false},
		{strings.Repeat("a", 63), true},
		{strings.Repeat("a", 64), false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := looksLikeIdentifier(c.in); got != c.want {
				t.Errorf("looksLikeIdentifier(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"tokens", `"tokens"`},
		{"work_items", `"work_items"`},
		// quoteIdent is the suspenders to looksLikeIdentifier's belt;
		// even if a malformed name slipped past validation, doubling
		// embedded quotes prevents identifier injection.
		{`a"b`, `"a""b"`},
		{`a""b`, `"a""""b"`},
	}
	for _, c := range cases {
		if got := quoteIdent(c.in); got != c.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Pin the sibling-list invariant called out in rebuild.go: the v0
// projection-table list and the registered projector kinds must move
// together. If a future change adds a projection table here without
// wiring its projector (or vice versa), this test surfaces the drift
// before it silently turns into a "rebuild always passes" lie.
func TestProjectionTables_Coverage(t *testing.T) {
	want := map[string]bool{
		"tokens":                         true,
		"work_items":                     true,
		"work_item_assignment_state":     true,
		"work_item_relations":            true,
		"messages":                       true,
		"message_parts":                  true,
		"idempotency_keys":               true,
		"signals":                        true,
		"deterministic_errors":           true,
		"convergence_verdicts":           true,
		"active_policy_profile":          true,
		"tropisms":                       true,
		"cultivars":                      true,
		"projections":                    true,
		"approvals":                      true,
		"http_connector_actions":         true,
		"nodes":                          true,
		"registry_snapshot_state":        true,
		"command_queue":                  true,
		"crossnode_outcome_observations": true,
		"crossnode_outcome_cursors":      true,
		"spoke_state":                    true,
		"oauth_clients":                  true,
		"oauth_authorization_codes":      true,
		"oauth_authorization_requests":   true,
		"oauth_grants":                   true,
		"oauth_access_tokens":            true,
		"oauth_refresh_tokens":           true,
	}
	if len(projectionTables) != len(want) {
		t.Errorf("projectionTables length drift: have %d, expected %d", len(projectionTables), len(want))
	}
	got := make(map[string]bool, len(projectionTables))
	for _, t := range projectionTables {
		got[t] = true
	}
	for w := range want {
		if !got[w] {
			t.Errorf("projectionTables missing expected entry: %q", w)
		}
	}
	for g := range got {
		if !want[g] {
			t.Errorf("projectionTables has unexpected entry: %q (update the test if intentional)", g)
		}
	}
}
