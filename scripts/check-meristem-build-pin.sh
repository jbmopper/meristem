#!/usr/bin/env bash
# Secret-free launcher preflight for the shared Meristem binary and reviewed-v1
# pin. Call this before reading any bearer-token file.

set -euo pipefail

fail() {
  # Deliberately do not print argv, paths, or pin contents.
  printf 'meristem build-pin check failed: %s\n' "$1" >&2
  exit 64
}

[[ "$#" -eq 2 ]] || fail "expected one binary and one pin"
meristem_bin="$1"
pin_file="$2"

[[ -x "$meristem_bin" ]] || fail "shared artifact is missing or not executable"
[[ -r "$pin_file" ]] || fail "reviewed-v1 pin is missing or unreadable"

pin_bytes="$(LC_ALL=C wc -c < "$pin_file")"
pin_lines="$(LC_ALL=C wc -l < "$pin_file")"
pin_commit="$(LC_ALL=C sed -n '1p' "$pin_file")"
commit_pattern='^[0-9a-f]{40}$'

# Accept a raw 40-byte SHA or the release script's canonical SHA+LF. Reject
# extra lines, CRLF, whitespace, uppercase, and all other payloads.
if ! { [[ "$pin_bytes" -eq 40 && "$pin_lines" -eq 0 ]] || [[ "$pin_bytes" -eq 41 && "$pin_lines" -eq 1 ]]; }; then
  fail "reviewed-v1 pin is malformed"
fi
[[ "$pin_commit" =~ $commit_pattern ]] || fail "reviewed-v1 pin is malformed"

# Require a dedicated, versioned capability command. Historical binaries
# ignored trailing `version` arguments, so `version --commit` could echo a
# plausible SHA even though the binary had no dynamic guard at all.
if ! compiled_output="$("$meristem_bin" build-guard-status 2>/dev/null && printf /)"; then
  fail "build guard capability is unavailable"
fi
compiled_output="${compiled_output%/}"
protocol='meristem-build-guard-v1'
expected_prefix="$protocol "
[[ "$compiled_output" == "$expected_prefix"* ]] || fail "build guard capability is malformed"
compiled_commit="${compiled_output#"$expected_prefix"}"
# The appended marker preserves trailing newlines inside command substitution.
# Accept exactly one canonical LF from the Go CLI and reject extra output.
compiled_bytes="${#compiled_commit}"
if [[ "$compiled_bytes" -eq 41 && "$compiled_commit" == *$'\n' ]]; then
  compiled_commit="${compiled_commit%$'\n'}"
else
  fail "compiled fingerprint is malformed"
fi
[[ "$compiled_commit" =~ $commit_pattern ]] || fail "compiled fingerprint is malformed"
[[ "$compiled_commit" == "$pin_commit" ]] || fail "compiled fingerprint does not match the reviewed-v1 pin"
