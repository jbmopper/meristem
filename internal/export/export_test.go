package export

import (
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
