package main

// LCP3-R1-B3 regression: the default cursor directory is keyed by the
// VALIDATED registration UUID and never by the listener name — registration
// only requires names to be non-empty, so a hostile name ("../..", an
// absolute path, separators) must have NO influence on where cursors land.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestDefaultListenerCursorDirIsUUIDKeyed(t *testing.T) {
	home := t.TempDir()
	base := filepath.Join(home, ".meristem", "listener")
	id := uuid.MustParse("7a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")

	got := defaultListenerCursorDir(home, id)
	if got != filepath.Join(base, id.String()) {
		t.Fatalf("cursor dir = %q, want %q", got, filepath.Join(base, id.String()))
	}
	rel, err := filepath.Rel(base, got)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		t.Fatalf("cursor dir escapes the base: rel=%q err=%v", rel, err)
	}
	// The signature is the regression: no name parameter exists, so hostile
	// names ("../../etc", "/abs", "a/b") cannot reshape the root. This pins
	// the property against a future "convenience" re-introduction of the
	// name into the path.
	for _, hostile := range []string{"../../etc", "/abs/path", "a/b", `..\..\win`} {
		if strings.Contains(got, hostile) {
			t.Fatalf("cursor dir %q influenced by a name-shaped input %q", got, hostile)
		}
	}
}
