package events

import "testing"

func TestCanonicalJSONSortsTopLevelKeys(t *testing.T) {
	got, err := CanonicalJSON(map[string]any{"b": 2, "a": 1, "c": 3})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":1,"b":2,"c":3}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSONSortsNestedKeys(t *testing.T) {
	in := map[string]any{
		"z": map[string]any{"y": 1, "x": 2},
		"a": []any{map[string]any{"d": 1, "c": 2}},
	}
	got, err := CanonicalJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":[{"c":2,"d":1}],"z":{"x":2,"y":1}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSONPreservesArrayOrder(t *testing.T) {
	got, err := CanonicalJSON([]any{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `[3,1,2]` {
		t.Errorf("array order should be preserved, got %s", got)
	}
}

func TestCanonicalJSONNilAndNullCollapseToEmptyObject(t *testing.T) {
	got, err := CanonicalJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{}` {
		t.Errorf("nil should canonicalize to {}, got %s", got)
	}

	got, err = CanonicalJSON(any(nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{}` {
		t.Errorf("typed nil should canonicalize to {}, got %s", got)
	}
}

func TestCanonicalJSONPreservesIntegerPrecision(t *testing.T) {
	// 9007199254740993 = 2^53 + 1, the smallest positive integer not
	// exactly representable as float64. If json.Decode were silently
	// widening to float64 we'd lose the trailing 1.
	got, err := CanonicalJSON(map[string]any{"big": int64(9007199254740993)})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"big":9007199254740993}` {
		t.Errorf("integer precision lost: got %s", got)
	}
}

func TestCanonicalJSONStableForStructLikeInputs(t *testing.T) {
	type payload struct {
		Z int `json:"z"`
		A int `json:"a"`
	}
	got, err := CanonicalJSON(payload{Z: 1, A: 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"a":2,"z":1}` {
		t.Errorf("struct fields should canonicalize sorted, got %s", got)
	}
}
