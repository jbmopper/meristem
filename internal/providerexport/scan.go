package providerexport

import (
	"bytes"
	"regexp"
	"strings"
)

// RedactionPolicyID identifies the builtin scanner rule set. A rule change
// is a new id (@2) registered alongside, never a silent mutation — the
// reducer compares declared vs applied ids byte-for-byte.
const RedactionPolicyID = "builtin:secret_deny@1"

// scanRule is one deterministic content check. Rules operate on raw bytes;
// no locale, no clock, no environment.
type scanRule struct {
	name    string
	pattern *regexp.Regexp
}

// Rule set for builtin:secret_deny@1. Deliberately small and high-precision:
// the path-level deny list is the primary control, this is the content
// tripwire behind it.
var scanRules = []scanRule{
	{"pem_private_key", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"aws_access_key_id", regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)},
	{"meristem_bearer_token", regexp.MustCompile(`\bmrs_[A-Za-z0-9_-]{20,}\b`)},
	{"github_token", regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}\b`)},
	{"generic_env_secret", regexp.MustCompile(`(?i)^\s*(?:export\s+)?[A-Z0-9_]*(?:SECRET|PASSWORD|API_KEY)[A-Z0-9_]*\s*=\s*\S{8,}`)},
	{"generic_env_secret_colon", regexp.MustCompile(`(?i)^\s*"?[A-Z0-9_]*(?:SECRET|PASSWORD|API_KEY)[A-Z0-9_]*"?\s*:\s*\S{8,}`)},
}

// ScanContent runs the builtin:secret_deny@1 rules over one file's bytes.
// It returns passed=false with the first failing rule's name. Binary-safe:
// rules are applied per line for the anchored rule, whole-buffer otherwise.
func ScanContent(path string, content []byte) (passed bool, rule string) {
	_ = path // path-level control is Plan's job; kept for rule evolution
	for _, r := range scanRules {
		if strings.HasPrefix(r.name, "generic_env_secret") {
			for _, line := range bytes.Split(content, []byte("\n")) {
				if r.pattern.Match(line) {
					return false, r.name
				}
			}
			continue
		}
		if r.pattern.Match(content) {
			return false, r.name
		}
	}
	return true, ""
}
