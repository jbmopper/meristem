package convergence

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/jbmopper/meristem/internal/events"
)

// digestSignals returns the SHA-256 (hex) of the canonical JSON encoding of
// the signal slice. It reuses events.CanonicalJSON — the same canonicalizer
// the deterministic event id depends on — so the digest is stable across map
// ordering, struct field order, and integer/float rendering. Identical
// signals always produce an identical digest, by construction.
//
// A nil and an empty slice both digest to the encoding of []; the recorded
// Reduction normalizes nil to []Signal{} so the recorded form matches.
func digestSignals(signals []Signal) (string, error) {
	if signals == nil {
		signals = []Signal{}
	}
	canonical, err := events.CanonicalJSON(signals)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// digestReductionInputs returns the digest for every reducer input. For
// parameter-free reducers it preserves the historical signal-only digest. For
// configured reducers it folds the reducer config and signal slice together so
// changing a threshold or required checklist cannot reuse an old verdict.
func digestReductionInputs(signals []Signal, reducerConfig map[string]any) (string, error) {
	if len(reducerConfig) == 0 {
		return digestSignals(signals)
	}
	if signals == nil {
		signals = []Signal{}
	}
	canonical, err := events.CanonicalJSON(map[string]any{
		"reducer_config": reducerConfig,
		"signals":        signals,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
