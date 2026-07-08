# Owner demo: `meristem export-context`

The deterministic workspace exporter (work item `accd39bb`) materializes an
allow/deny slice of a repository into a workspace directory, scanning every
included file for secrets first. It reads **git commit objects only** — never
the working tree — so the same `(commit, policy)` yields a byte-identical
bundle on any machine. It makes **no meristem API calls** and appends no
events; it is the operator-side tool. The `provider_context.generated` append
is a later slice.

## One command

Export a safe slice of the meristem repo itself — the docs and the README —
into `/tmp/ctx/workspace`:

```sh
meristem export-context \
  --work-item accd39bb-eb95-493f-ade7-efc858ebe6d8 \
  --provider cursor-cli \
  --repo . --ref HEAD \
  --allow docs/ --allow README.md \
  --out /tmp/ctx/workspace
```

It prints a summary like:

```
source ref:     HEAD
source commit:  75332d897b8fe0a18885c5645b62d9db1f101568
policy hash:    sha256:dc7c63b7a8d89c248f3da02ef8465b9dd5ceff1fceff39c1a549ee65bef17738
redaction:      builtin:secret_deny@1
paths included: 45
paths omitted:  0
bundle digest:  sha256:27a3d899bd25f6e9fba4591fd3ce9678154d76d86c49469459b8e7f110b9ecc6
workspace:      /tmp/ctx/workspace
manifest:       /tmp/ctx/manifest.json
```

Omit `--out` for a dry run: the plan, scan, and manifest are computed and the
summary is printed, but nothing is written to disk.

## What to look for

**The workspace** (`/tmp/ctx/workspace`) holds the materialized files with git's
0644/0755 bit preserved, plus an included-only `manifest.json` copy.

**The operator-side manifest** (`/tmp/ctx/manifest.json`, written *next to* the
workspace) is the durable audit record. Inspect these fields:

- `source_commit` — the full 40-hex commit every blob was read from. Nothing
  came from the working tree.
- `policy_hash` — `sha256` of the structural policy (the narrative `message`
  field is excluded), so the same allow/deny set always hashes the same.
- `bundle_digest` — `sha256` over `path NUL mode NUL sha256(content) LF` for
  every included file in path order. This is the bundle's identity, independent
  of filesystem, OS, tar framing, or timestamps.
- `included[]` — each exported path with its mode, size, `sha256`, and
  `redaction_passed` (always `true` in an emitted bundle — the scanner is
  fail-closed, so a file that trips a rule aborts the whole export instead of
  shipping).
- `omitted[]` — paths that were **allow-matched but excluded**, each with a
  reason (`denied_path`, `symlink`, `submodule`, `non_utf8_path`). Paths
  *outside* the allowlist are never listed: naming `.env.production` would leak
  that it exists. This full `omitted[]` list stays operator-side; the copy
  embedded in the workspace carries `included[]` only.

**Verify the digest is reproducible.** Run the command twice into two
destinations and compare — the workspace path is not part of the manifest, so
both the `bundle_digest` and the manifest bytes are identical:

```sh
meristem export-context --work-item accd39bb-eb95-493f-ade7-efc858ebe6d8 \
  --provider cursor-cli --allow docs/ --allow README.md --out /tmp/a/workspace
meristem export-context --work-item accd39bb-eb95-493f-ade7-efc858ebe6d8 \
  --provider cursor-cli --allow docs/ --allow README.md --out /tmp/b/workspace
diff /tmp/a/manifest.json /tmp/b/manifest.json && echo "byte-identical"
```

## Deliberate failure: the builtin deny always wins

The builtin secret deny list (`.meristem/`, `.env*`, `*.token`, …) is appended
to every policy and **deny beats allow**. Explicitly allowing `.meristem/`
does not exfiltrate it — every candidate under it is denied, so the export
includes zero paths and materializes nothing:

```sh
meristem export-context --work-item accd39bb-eb95-493f-ade7-efc858ebe6d8 \
  --provider cursor-cli --allow .meristem/
# paths included: 0   ← builtin deny neutralized the allow; denied blobs are
#                        never even read from git
```

A malformed allow entry is refused outright, before the repo is touched, with a
non-zero exit:

```sh
meristem export-context --work-item accd39bb-eb95-493f-ade7-efc858ebe6d8 \
  --provider cursor-cli --allow ../outside
# error: policy path entry "../outside" is root, absolute, or traverses
```

And if a file *inside* the allowed set contains secret-shaped content (a PEM
private key, an AWS key id, a meristem token), the scan aborts the entire
export — no partial workspace is left behind and no manifest is written.
