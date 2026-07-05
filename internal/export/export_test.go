package export

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestScrubReplacesFreeTextWithLengthMarkers(t *testing.T) {
	in := map[string]any{
		"title": "secret plan for the weekend",
		"state": "captured",
		"to": map[string]any{
			"body":   "private prose",
			"checks": []any{"cmd:go test ./..."},
		},
		"reason": "",
	}
	out, ok := Scrub(in).(map[string]any)
	if !ok {
		t.Fatal("scrub changed top-level shape")
	}
	if out["title"] != "[scrubbed 27 chars]" {
		t.Errorf("title = %v, want length-preserving marker", out["title"])
	}
	if out["state"] != "captured" {
		t.Errorf("structural field mutated: %v", out["state"])
	}
	nested := out["to"].(map[string]any)
	if nested["body"] != "[scrubbed 13 chars]" {
		t.Errorf("nested body = %v", nested["body"])
	}
	if nested["checks"].([]any)[0] != "cmd:go test ./..." {
		t.Errorf("non-text structure mutated: %v", nested["checks"])
	}
	if out["reason"] != "" {
		t.Errorf("empty string should survive as-is, got %v", out["reason"])
	}
	if strings.Contains(strings.ToLower(strFromAny(out)), "secret") {
		t.Error("scrubbed output still contains source prose")
	}
}

func TestScrubReducesNonStringNarrativeToShapeMarker(t *testing.T) {
	in := map[string]any{"inner": map[string]any{"deep": "prose"}}
	out := Scrub(in).(map[string]any)
	s, ok := out["inner"].(string)
	if !ok || !strings.HasPrefix(s, "[scrubbed") {
		t.Errorf("inner narrative object should reduce to a shape marker, got %v", out["inner"])
	}
}

func TestKindAllowlistExcludesAuditAndInboxKinds(t *testing.T) {
	for _, kind := range []string{"token.created", "token.revoked", "idempotency.recorded", "message.captured"} {
		if KindAllowlist[kind] {
			t.Errorf("kind %s must never be exported", kind)
		}
	}
}

func TestValidateCorpusRejectsPrivateLeaks(t *testing.T) {
	corpus := strings.Join([]string{
		mustJSONLine(t, map[string]any{
			"kind":           "work_item.created",
			"actor_token_id": "private-token-id",
			"payload": map[string]any{
				"title": "archive-token-name",
			},
		}),
		mustJSONLine(t, map[string]any{
			"kind": "message.captured",
			"payload": map[string]any{
				"text": "verbatim owner archive body",
			},
		}),
	}, "\n")

	report, err := ValidateCorpus([]byte(corpus), []string{"archive-token-name"}, []string{"verbatim owner archive body"})
	if err == nil {
		t.Fatal("ValidateCorpus should reject private leaks")
	}
	if report.ActorTokenIDLeaks != 1 {
		t.Errorf("ActorTokenIDLeaks = %d, want 1", report.ActorTokenIDLeaks)
	}
	if report.NonAllowlistedKinds != 1 {
		t.Errorf("NonAllowlistedKinds = %d, want 1", report.NonAllowlistedKinds)
	}
	if report.TokenNameLeaks != 1 {
		t.Errorf("TokenNameLeaks = %d, want 1", report.TokenNameLeaks)
	}
	if report.MessageBodyLeaks != 1 {
		t.Errorf("MessageBodyLeaks = %d, want 1", report.MessageBodyLeaks)
	}
	if report.Valid {
		t.Error("report.Valid = true for rejected corpus")
	}
}

func TestValidateCorpusAcceptsScrubbedAllowlistedCorpus(t *testing.T) {
	corpus := mustJSONLine(t, map[string]any{
		"kind": "work_item.created",
		"payload": map[string]any{
			"title": "[scrubbed 24 chars]",
			"state": "captured",
		},
	})
	report, err := ValidateCorpus([]byte(corpus+"\n"), []string{"archive-token-name"}, []string{"verbatim owner archive body"})
	if err != nil {
		t.Fatalf("ValidateCorpus rejected scrubbed corpus: %v", err)
	}
	if !report.Valid {
		t.Error("report.Valid = false")
	}
	if report.LinesChecked != 1 {
		t.Errorf("LinesChecked = %d, want 1", report.LinesChecked)
	}
}

func mustJSONLine(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal test JSON: %v", err)
	}
	return string(b)
}

func strFromAny(v any) string {
	sb := strings.Builder{}
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				sb.WriteString(k)
				walk(val)
			}
		case []any:
			for _, val := range t {
				walk(val)
			}
		case string:
			sb.WriteString(t)
		}
	}
	walk(v)
	return sb.String()
}
