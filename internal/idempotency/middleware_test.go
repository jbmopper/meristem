package idempotency

import (
	"testing"

	"github.com/google/uuid"
)

func TestLockKeyForIsDeterministic(t *testing.T) {
	tokenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	first := lockKeyFor(tokenID, "POST /v1/signals", "key-1")
	second := lockKeyFor(tokenID, "POST /v1/signals", "key-1")
	if first != second {
		t.Fatalf("lockKeyFor not stable: %d != %d", first, second)
	}
}

func TestLockKeyForSeparatesIdentityComponents(t *testing.T) {
	tokenA := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	tokenB := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	scopeA := "POST /v1/signals"
	scopeB := "POST /v1/inbox/messages"
	keyA := "key-a"
	keyB := "key-b"

	base := lockKeyFor(tokenA, scopeA, keyA)

	cases := []struct {
		name string
		got  int64
	}{
		{"different token", lockKeyFor(tokenB, scopeA, keyA)},
		{"different scope", lockKeyFor(tokenA, scopeB, keyA)},
		{"different key", lockKeyFor(tokenA, scopeA, keyB)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got == base {
				t.Fatalf("collision against base: %d", tc.got)
			}
		})
	}
}

func TestLockKeyForCoversFullInt64Range(t *testing.T) {
	// Sanity that the cast preserves sign bits — if we accidentally
	// masked off the high bit, every key would land in [0, math.MaxInt64].
	tokenID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	sawNegative := false
	sawPositive := false
	for i := 0; i < 64; i++ {
		k := lockKeyFor(tokenID, "POST /v1/signals", string(rune('a'+i)))
		if k < 0 {
			sawNegative = true
		}
		if k >= 0 {
			sawPositive = true
		}
	}
	if !sawNegative || !sawPositive {
		t.Fatalf("expected both negative and non-negative keys; sawNegative=%v sawPositive=%v", sawNegative, sawPositive)
	}
}
