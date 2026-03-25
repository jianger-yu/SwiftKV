#!/usr/bin/env bash
set -euo pipefail

DEFAULT_MESSAGE="chore: sync project updates"
MESSAGE="$DEFAULT_MESSAGE"
TAG=""
TARGET_BRANCH=""
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage: ./push-to-github.sh [options]

Options:
  -m, --message <msg>   Commit message
  -t, --tag <tag>       Create and push annotated tag
  -b, --branch <name>   Target branch (default: current branch)
  -n, --dry-run         Run checks and stage preview only
  -h, --help            Show help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    -m|--message)
        MESSAGE="$2"
        shift 2
        ;;
    -t|--tag)
        TAG="$2"
        shift 2
        ;;
    -b|--branch)
        TARGET_BRANCH="$2"
        shift 2
        ;;
    -n|--dry-run)
        DRY_RUN=1
        shift
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
done

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: current directory is not a git repository."
    exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ -z "$TARGET_BRANCH" ]]; then
    TARGET_BRANCH="$CURRENT_BRANCH"
fi

if [[ "$CURRENT_BRANCH" != "$TARGET_BRANCH" ]]; then
    echo "Warning: current branch is '$CURRENT_BRANCH', target is '$TARGET_BRANCH'."
fi

if ! git remote get-url origin >/dev/null 2>&1; then
    echo "Error: remote 'origin' is not configured."
    exit 1
fi

echo "== Pre-check: badger data should not be tracked =="
if git ls-files | grep -E '^badger-' >/dev/null 2>&1; then
    echo "Error: badger-* directories are tracked by git. Please untrack them first."
    exit 1
fi

echo "== Stage changes =="
git add -A

if git diff --cached --quiet; then
    echo "No staged changes. Nothing to commit."
    exit 0
fi

echo "== Staged files =="
git diff --cached --name-status

echo "== Large file warning (>10MB) =="
while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    if [[ -f "$file" ]]; then
        size=$(wc -c <"$file" | tr -d '[:space:]')
        if [[ "$size" -gt 10485760 ]]; then
            echo "Warning: large file staged: $file (${size} bytes)"
        fi
    fi
done < <(git diff --cached --name-only)

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "Dry-run enabled. Skip commit/push."
    exit 0
fi

echo "== Commit =="
git commit -m "$MESSAGE"

echo "== Push branch =="
git push -u origin "$TARGET_BRANCH"

if [[ -n "$TAG" ]]; then
    if git rev-parse "$TAG" >/dev/null 2>&1; then
        echo "Error: tag '$TAG' already exists locally."
        exit 1
    fi
    echo "== Create tag $TAG =="
    git tag -a "$TAG" -m "Release $TAG"
    echo "== Push tag $TAG =="
    git push origin "$TAG"
fi

echo "Done."