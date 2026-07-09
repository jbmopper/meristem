// Package agentbudget defines the provider-agnostic execution budget for a
// spawned agent or provider-backed worker, and the deterministic reducer that
// judges whether an observed run stayed within it (work item 3d5526e4).
//
// It is intentionally pure, following internal/providercontext: callers resolve
// the observable facts of a run (elapsed runtime, MCP tool-call count, context
// manifest size, artifact count) and pass them in. The reducer decides
// accept or reject without reading clocks, databases, or process state.
//
// Budgets are ceilings, not identities: a zero field means "meristem does not
// enforce this dimension", so an unbudgeted run reduces to accept. Word counts
// are the meristem-observable proxy for prompt/response size; model-side token
// usage is unobservable here and is labelled best-effort by callers unless a
// provider reports it in a structured artifact.
package agentbudget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Budget is the structural budget payload. Every field is a ceiling on a
// meristem-observable dimension of an agent run; a zero value disables
// enforcement of that dimension. All fields are structural: two budgets with
// the same field values share a BudgetHash.
type Budget struct {
	PayloadVersion int `json:"payload_version,omitempty"`
	// MaxContextFiles caps the number of files in the exported context
	// manifest handed to the agent (observable: manifest entry count).
	MaxContextFiles int `json:"max_context_files"`
	// MaxPromptWords caps the assembled prompt/context size in words
	// (observable proxy for prompt tokens, which are best-effort here).
	MaxPromptWords int `json:"max_prompt_words"`
	// MaxResponseWords caps the agent's produced output in words when
	// meristem captures it (best-effort for providers that do not return
	// structured output).
	MaxResponseWords int `json:"max_response_words"`
	// MaxTurns caps the number of agent turns when meristem drives the loop.
	MaxTurns int `json:"max_turns"`
	// MaxRuntimeSeconds caps wall-clock elapsed runtime (observable).
	MaxRuntimeSeconds int `json:"max_runtime_seconds"`
	// MaxToolCalls caps the number of MCP tool calls the agent makes
	// (observable through the scoped MCP proxy).
	MaxToolCalls int `json:"max_tool_calls"`
	// MaxArtifacts caps the number of artifacts the agent produces
	// (observable: artifact count).
	MaxArtifacts int `json:"max_artifacts"`
}

// Valid reports the first field that is negative. Zero is allowed everywhere
// (it disables that dimension); only negative ceilings are malformed.
func (b Budget) Valid() error {
	for _, f := range []struct {
		name string
		val  int
	}{
		{"max_context_files", b.MaxContextFiles},
		{"max_prompt_words", b.MaxPromptWords},
		{"max_response_words", b.MaxResponseWords},
		{"max_turns", b.MaxTurns},
		{"max_runtime_seconds", b.MaxRuntimeSeconds},
		{"max_tool_calls", b.MaxToolCalls},
		{"max_artifacts", b.MaxArtifacts},
	} {
		if f.val < 0 {
			return fmt.Errorf("agentbudget: %s must be >= 0, got %d", f.name, f.val)
		}
	}
	return nil
}

// Enforced reports whether any dimension has a positive ceiling. A budget with
// every field zero enforces nothing, so callers use this to decide whether to
// surface a budget at all (an unbudgeted handoff states no budget line).
func (b Budget) Enforced() bool {
	return b.MaxContextFiles > 0 ||
		b.MaxPromptWords > 0 ||
		b.MaxResponseWords > 0 ||
		b.MaxTurns > 0 ||
		b.MaxRuntimeSeconds > 0 ||
		b.MaxToolCalls > 0 ||
		b.MaxArtifacts > 0
}

// BudgetHash is the canonical budget identity: sha256 over the struct marshal
// (stable key order, no maps), mirroring providerexport.PolicyHash. The same
// budget always hashes the same, so re-canonicalizing an unchanged budget
// yields the same id and a launch scaffold built from it is reproducible.
func BudgetHash(b Budget) string {
	raw, err := json.Marshal(b)
	if err != nil {
		// A struct of ints cannot fail to marshal; treat as programmer error
		// rather than propagating an impossible branch.
		panic(fmt.Sprintf("agentbudget: marshal budget: %v", err))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
