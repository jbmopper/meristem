#!/usr/bin/env bash
# Prepare an isolated git worktree for a local meristem assistant.
#
# The worktree gets its own branch, while .meristem remains anchored in the
# primary checkout through a symlink. That keeps local token/state files in one
# place without sharing the editable source checkout.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEFAULT_BASE_DIR="$(cd "$REPO_ROOT/.." && pwd)"

target=""
dest=""
branch=""
base_ref="${MERISTEM_WORKTREE_BASE_REF:-v1}"
base_dir="${MERISTEM_AGENT_WORKTREE_BASE:-$DEFAULT_BASE_DIR}"

usage() {
  cat <<'USAGE'
usage:
  scripts/prepare-agent-worktree.sh --target NAME [options]

options:
  --target NAME       Assistant target, for example codex or claude-code-gui.
  --path PATH         Worktree path. Default: ../meristem-NAME.
  --branch NAME      Branch to check out. Default: codex/NAME-worktree for
                     codex*, claude/NAME-worktree for claude*, otherwise
                     agent/NAME-worktree.
  --base REF         Base ref for a new branch. Default: v1.
  --base-dir PATH    Directory for the default path. Default: repo parent.
  -h, --help         Show this help.

The script does not copy secrets. If the primary checkout has .meristem/, the
worktree receives a .meristem symlink back to the primary checkout.
USAGE
}

die() {
  printf 'prepare-agent-worktree: %s\n' "$*" >&2
  exit 1
}

sanitize_target() {
  case "$1" in
    ''|*[^A-Za-z0-9_.-]*)
      die "invalid target name '$1'; use only letters, numbers, dot, underscore, and dash"
      ;;
  esac
}

default_branch_for() {
  case "$1" in
    codex*) printf 'codex/%s-worktree\n' "$1" ;;
    claude*) printf 'claude/%s-worktree\n' "$1" ;;
    *) printf 'agent/%s-worktree\n' "$1" ;;
  esac
}

while (($#)); do
  case "$1" in
    --target)
      target="${2:?--target requires a value}"
      shift 2
      ;;
    --target=*)
      target="${1#--target=}"
      shift
      ;;
    --path)
      dest="${2:?--path requires a value}"
      shift 2
      ;;
    --path=*)
      dest="${1#--path=}"
      shift
      ;;
    --branch)
      branch="${2:?--branch requires a value}"
      shift 2
      ;;
    --branch=*)
      branch="${1#--branch=}"
      shift
      ;;
    --base)
      base_ref="${2:?--base requires a value}"
      shift 2
      ;;
    --base=*)
      base_ref="${1#--base=}"
      shift
      ;;
    --base-dir)
      base_dir="${2:?--base-dir requires a value}"
      shift 2
      ;;
    --base-dir=*)
      base_dir="${1#--base-dir=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$target" ]] || {
  usage >&2
  exit 2
}
sanitize_target "$target"

if [[ -z "$dest" ]]; then
  dest="$base_dir/meristem-$target"
fi
if [[ -z "$branch" ]]; then
  branch="$(default_branch_for "$target")"
fi

case "$dest" in
  /*) ;;
  *) dest="$PWD/$dest" ;;
esac

if [[ -e "$dest" ]]; then
  if [[ ! -e "$dest/.git" ]]; then
    die "$dest exists but is not a git worktree"
  fi
  printf 'worktree exists: %s\n' "$dest"
else
  mkdir -p "$(dirname "$dest")"
  if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/$branch"; then
    git -C "$REPO_ROOT" worktree add "$dest" "$branch"
  else
    git -C "$REPO_ROOT" worktree add -b "$branch" "$dest" "$base_ref"
  fi
fi

if [[ -e "$REPO_ROOT/.meristem" ]]; then
  if [[ -L "$dest/.meristem" ]]; then
    existing_target="$(readlink "$dest/.meristem")"
    [[ "$existing_target" == "$REPO_ROOT/.meristem" ]] ||
      die "$dest/.meristem points at $existing_target, expected $REPO_ROOT/.meristem"
  elif [[ -e "$dest/.meristem" ]]; then
    die "$dest/.meristem already exists and is not the primary-state symlink"
  else
    ln -s "$REPO_ROOT/.meristem" "$dest/.meristem"
    printf 'linked state: %s/.meristem -> %s/.meristem\n' "$dest" "$REPO_ROOT"
  fi
else
  printf 'warning: primary checkout has no .meristem directory to link\n' >&2
fi

printf 'target: %s\n' "$target"
printf 'worktree: %s\n' "$dest"
printf 'branch: %s\n' "$branch"
printf 'base: %s\n' "$base_ref"
