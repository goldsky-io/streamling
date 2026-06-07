#!/usr/bin/env bash
# Usage: prune-rolling-release.sh <release-tag> <keep-count>
#
# Deletes assets on the named release belonging to all but the most recent
# <keep-count> distinct SHAs (by asset created_at). Requires gh + GH_TOKEN
# and GITHUB_REPOSITORY in the environment.
#
# Asset names are expected to look like:
#   streamling-<os>-<arch>-<40-char-sha>[.tar.gz|.zip]
# Assets whose names don't end in a 40-char hex SHA (optionally followed by a
# file extension) are left alone.

set -euo pipefail

tag="${1:?usage: prune-rolling-release.sh <release-tag> <keep-count>}"
keep="${2:?usage: prune-rolling-release.sh <release-tag> <keep-count>}"
repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

# Fetch all assets on the release as a JSON array of {name, created_at}.
assets_json=$(gh api "repos/${repo}/releases/tags/${tag}" --jq '[.assets[] | {name, created_at}]')

# Determine which SHAs to keep: most recent <keep> distinct SHAs, by max created_at per SHA.
keepers=$(jq -r --argjson keep "$keep" '
  map(select(.name | test("-[0-9a-f]{40}(\\.[A-Za-z0-9.]+)?$"))) |
  map(. + {sha: (.name | capture("-(?<s>[0-9a-f]{40})(\\.[A-Za-z0-9.]+)?$").s)}) |
  group_by(.sha) |
  map({sha: .[0].sha, latest: (map(.created_at) | max)}) |
  sort_by(.latest) | reverse |
  .[0:$keep] | map(.sha) | .[]
' <<<"$assets_json")

# Delete any sha-suffixed asset whose trailing SHA isn't in keepers.
echo "$assets_json" | jq -r '.[].name' | while IFS= read -r name; do
  # Skip assets that don't end in a 40-char SHA (optionally with an extension).
  if [[ ! "$name" =~ -([0-9a-f]{40})(\.[A-Za-z0-9.]+)?$ ]]; then
    continue
  fi
  sha="${BASH_REMATCH[1]}"
  if ! grep -qx "$sha" <<<"$keepers"; then
    echo "Deleting $name"
    gh release delete-asset "$tag" "$name" --yes -R "$repo"
  fi
done
