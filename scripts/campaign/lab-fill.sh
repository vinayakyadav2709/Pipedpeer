#!/usr/bin/env bash
# lab-fill.sh: the four published lines of lab.sh.
#   lab-fill.sh fill <in> <out> <pubkey-line> <commit> <site> [pipedpeer-version]
#       write <in> to <out> with CAMPAIGN_KEY, CAMPAIGN_COMMIT, SITE and
#       PIPEDPEER_VERSION filled in (publish.sh, serve.sh)
#   lab-fill.sh check [<published> [<source>]]
#       fail unless the published copy (default landing/public/lab.sh) is the
#       source (default scripts/campaign/lab.sh) apart from those four lines
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../.." && pwd)
die() { echo "lab-fill.sh: $*" >&2; exit 1; }

# Only the four assignment lines: the same tokens elsewhere detect an unfilled copy.
normalize() {
  sed -e 's/^CAMPAIGN_KEY=".*"$/CAMPAIGN_KEY="__CAMPAIGN_PUBKEY__"/' \
      -e 's/^CAMPAIGN_COMMIT=".*"$/CAMPAIGN_COMMIT="__CAMPAIGN_COMMIT__"/' \
      -e 's/^PIPEDPEER_VERSION=".*"$/PIPEDPEER_VERSION=""/' \
      -e 's|^SITE=".*"$|SITE="https://vinayakyadav2709.github.io/Pipedpeer"|' "$1"
}

case "${1:-}" in
fill)
  [[ $# -ge 6 ]] || die "usage: fill <in> <out> <pubkey-line> <commit> <site> [version]"
  in=$2 out=$3 key=$4 commit=$5 site=$6 version=${7:-}
  [[ $key =~ ^ssh-ed25519\ [A-Za-z0-9+/=]+ ]] || die "not an ed25519 public key: $key"
  key=$(awk '{print $1" "$2}' <<<"$key")   # no comment: it may hold a hostname
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || die "not a full commit sha: $commit"
  [[ $site =~ ^https?://[A-Za-z0-9.:/_-]+$ ]] || die "not a site URL: $site"
  [[ $version =~ ^[A-Za-z0-9._-]*$ ]] || die "not a version: $version"
  normalize "$in" | sed -e "s|^CAMPAIGN_KEY=\".*\"\$|CAMPAIGN_KEY=\"$key\"|" \
      -e "s/^CAMPAIGN_COMMIT=\".*\"\$/CAMPAIGN_COMMIT=\"$commit\"/" \
      -e "s/^PIPEDPEER_VERSION=\".*\"\$/PIPEDPEER_VERSION=\"$version\"/" \
      -e "s|^SITE=\".*\"\$|SITE=\"${site%/}\"|" >"$out.new"
  for l in "CAMPAIGN_KEY=\"$key\"" "CAMPAIGN_COMMIT=\"$commit\"" "SITE=\"${site%/}\""; do
    grep -qxF "$l" "$out.new" || { rm -f "$out.new"; die "could not fill $l"; }
  done
  chmod 644 "$out.new"; mv -f "$out.new" "$out"
  ;;
check)
  pub=${2:-$repo/landing/public/lab.sh} src=${3:-$here/lab.sh}
  if diff -u <(normalize "$src") <(normalize "$pub") >&2; then
    echo "lab-fill: $pub matches $src"
  else
    die "$pub differs from $src beyond the published lines; run scripts/campaign/publish.sh (or copy lab.sh there)"
  fi
  ;;
*) die "usage: lab-fill.sh fill <in> <out> <key> <commit> <site> [version] | check [<published> [<source>]]" ;;
esac
