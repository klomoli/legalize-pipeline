#!/usr/bin/env bash
# One-shot: seed opolex-laws-es from legalize-dev/legalize-es so the BOE
# bootstrap (which would otherwise hit BOE for every law) is skipped.
#
# Usage:
#   scripts/setup-opolex.sh <github-owner>/<repo>  <local-dest-dir>
#
# Example:
#   scripts/setup-opolex.sh klomoli/opolex-laws-es ../opolex-laws-es
set -euo pipefail

slug="${1:?owner/repo arg}"
dest="${2:?dest path}"

if [ -e "$dest" ]; then
  echo "Destination $dest already exists — refusing to overwrite." >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git clone --depth=1 https://github.com/legalize-dev/legalize-es.git "$tmp/seed"

mkdir -p "$dest"
cd "$dest"
git init -b main
shopt -s extglob
cp -r "$tmp/seed"/es*/ .

printf '{"last_summary_date": "%s", "runs": []}\n' "$(date -I)" > state.json

cat > README.md <<'EOF'
# opolex-laws-es

OpoLex-internal corpus of Spanish legislation (BOE) in Markdown form.

Initially seeded from <https://github.com/legalize-dev/legalize-es> (BOE
content is public domain under Art. 13 LPI). Subsequent commits are produced
by our own run of <https://github.com/legalize-dev/legalize-pipeline> (MIT)
on a GitHub Action.
EOF

git add .
GIT_AUTHOR_DATE='2026-05-07T00:00:00Z' GIT_COMMITTER_DATE='2026-05-07T00:00:00Z' \
  git -c user.name=opolex-bot -c user.email=bot@opolex.app \
  commit -m '[bootstrap] seed from BOE via legalize-pipeline'

git remote add origin "git@github.com:${slug}.git"
git push -u origin main

echo
echo "Seed complete. Files: $(find . -name '*.md' | wc -l)."
