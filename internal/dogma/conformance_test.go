package dogma

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestTechniquesConformanceMapCoversAGENTSBullets(t *testing.T) {
	root := repoRoot(t)
	agents := readFile(t, filepath.Join(root, "AGENTS.md"))
	doc := readFile(t, filepath.Join(root, "docs", "dogma-conformance.md"))

	techniques := extractTechniques(t, agents)
	entries := extractConformanceEntries(t, doc)

	for _, technique := range techniques {
		entry, ok := entries[technique]
		if !ok {
			t.Errorf("AGENTS.md Techniques bullet %q is missing from docs/dogma-conformance.md", technique)
			continue
		}
		assertConcreteEntry(t, technique, entry, root)
	}
	for title := range entries {
		if !contains(techniques, title) {
			t.Errorf("docs/dogma-conformance.md has entry %q that is not an AGENTS.md Techniques bullet", title)
		}
	}
}

func TestEventsAppendOnlyMigrationGuardRemainsMapped(t *testing.T) {
	root := repoRoot(t)
	migration := readFile(t, filepath.Join(root, "migrations", "0001_init.up.sql"))
	for _, required := range []string{
		"events_reject_mutation",
		"events_no_update",
		"events_no_delete",
		"events_no_truncate",
	} {
		if !strings.Contains(migration, required) {
			t.Errorf("migrations/0001_init.up.sql is missing append-only guard %q", required)
		}
	}
}

func extractTechniques(t *testing.T, agents string) []string {
	t.Helper()
	section := markdownSection(t, agents, "## Techniques")
	re := regexp.MustCompile(`(?m)^- \*\*(.+?)\*\*`)
	matches := re.FindAllStringSubmatch(section, -1)
	if len(matches) == 0 {
		t.Fatal("no Techniques bullets found in AGENTS.md")
	}
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		out = append(out, strings.TrimSpace(match[1]))
	}
	return out
}

func extractConformanceEntries(t *testing.T, doc string) map[string]string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^### (.+)$`)
	matches := re.FindAllStringSubmatchIndex(doc, -1)
	if len(matches) == 0 {
		t.Fatal("no conformance entries found")
	}
	out := make(map[string]string, len(matches))
	for i, match := range matches {
		title := strings.TrimSpace(doc[match[2]:match[3]])
		start := match[1]
		end := len(doc)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		if _, exists := out[title]; exists {
			t.Fatalf("duplicate conformance entry for %q", title)
		}
		out[title] = strings.TrimSpace(doc[start:end])
	}
	return out
}

func assertConcreteEntry(t *testing.T, technique, entry, root string) {
	t.Helper()
	if !strings.Contains(entry, "- Check:") {
		t.Errorf("%q entry is missing '- Check:'", technique)
	}
	if !strings.Contains(entry, "- Evidence:") {
		t.Errorf("%q entry is missing '- Evidence:'", technique)
	}
	lower := strings.ToLower(entry)
	for _, placeholder := range []string{"tbd", "todo", "placeholder"} {
		if strings.Contains(lower, placeholder) {
			t.Errorf("%q entry contains placeholder marker %q", technique, placeholder)
		}
	}

	refs := referencedRepoFiles(entry)
	if len(refs) == 0 {
		t.Errorf("%q entry references no repo files", technique)
	}
	for _, ref := range refs {
		if _, err := os.Stat(filepath.Join(root, ref)); err != nil {
			t.Errorf("%q entry references missing file %s: %v", technique, ref, err)
		}
	}
}

func referencedRepoFiles(entry string) []string {
	re := regexp.MustCompile("`([^`]+)`")
	matches := re.FindAllStringSubmatch(entry, -1)
	var out []string
	seen := map[string]bool{}
	for _, match := range matches {
		ref := match[1]
		if i := strings.Index(ref, "::"); i >= 0 {
			ref = ref[:i]
		}
		if !looksLikeRepoFile(ref) || seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, ref)
	}
	return out
}

func looksLikeRepoFile(ref string) bool {
	for _, suffix := range []string{".go", ".md", ".sql"} {
		if strings.HasSuffix(ref, suffix) {
			return true
		}
	}
	return false
}

func markdownSection(t *testing.T, doc, heading string) string {
	t.Helper()
	start := strings.Index(doc, heading)
	if start < 0 {
		t.Fatalf("missing heading %q", heading)
	}
	rest := doc[start+len(heading):]
	if next := strings.Index(rest, "\n## "); next >= 0 {
		rest = rest[:next]
	}
	return rest
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
