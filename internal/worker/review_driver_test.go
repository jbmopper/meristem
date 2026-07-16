package worker

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// Regression for work item 9a683452: a multibyte rune straddling the
// truncation boundary must not produce invalid UTF-8.
func TestReviewChildTruncationRuneSafe(t *testing.T) {
	// Every rune is 3 bytes, so any byte-anchored cut point is likely to
	// land mid-rune.
	multibyte := strings.Repeat("語", 600)

	title := reviewChildTitle(multibyte)
	if !utf8.ValidString(title) {
		t.Fatalf("reviewChildTitle produced invalid UTF-8: %q", title[:24])
	}
	if len(title) > 120 {
		t.Fatalf("title length = %d, want <= 120", len(title))
	}
	if !strings.HasSuffix(title, "...") {
		t.Fatalf("truncated title missing ellipsis: %q", title)
	}

	body := reviewChildBody(reviewCandidate{Body: multibyte}, reviewEvidence{}, "reviewer@1")
	if !utf8.ValidString(body) {
		t.Fatal("reviewChildBody produced invalid UTF-8")
	}

	// Short inputs pass through untouched.
	if got := reviewChildTitle("short"); got != "Review implementation: short" {
		t.Fatalf("short title = %q", got)
	}
}

func TestTruncateRuneSafe(t *testing.T) {
	cases := []struct {
		s   string
		max int
	}{
		{"ascii only", 5},
		{"héllo wörld", 3},
		{strings.Repeat("é", 50), 17},
		{strings.Repeat("語", 10), 8},
		{"", 4},
		{"ab", 10},
	}
	for _, tc := range cases {
		got := truncateRuneSafe(tc.s, tc.max)
		if len(got) > tc.max {
			t.Errorf("truncateRuneSafe(%q, %d) length %d exceeds max", tc.s, tc.max, len(got))
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateRuneSafe(%q, %d) = invalid UTF-8", tc.s, tc.max)
		}
		if !strings.HasPrefix(tc.s, got) {
			t.Errorf("truncateRuneSafe(%q, %d) = %q is not a prefix", tc.s, tc.max, got)
		}
	}
}

func TestReviewChildBodyRequiresOneTypedVerdict(t *testing.T) {
	body := reviewChildBody(reviewCandidate{}, reviewEvidence{}, "reviewer@1")
	for _, want := range []string{
		`"verdict_inner_kind":"review.verdict_recorded"`,
		`"derived_check_signal_kind":"checklist.item:event:review.verdict_recorded"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("review child body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `"check_signal":`) {
		t.Fatalf("review child body still instructs a caller-authored check signal:\n%s", body)
	}
}
