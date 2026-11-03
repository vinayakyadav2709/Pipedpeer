#!/usr/bin/env bash
# publish.sh: prepare the headless lab day for publishing on GitHub Pages.
# Run at home, in a clean checkout whose HEAD is pushed to GitHub (the lab
# PCs download the code at exactly that commit). Never run in the lab.
#
#   scripts/campaign/publish.sh [--version <pipedpeer nightly version>]
#       0. builds pipedpeer at HEAD (linux amd64 and arm64, version
#          lab-<commit12>) and uploads it with checksums.txt as the GitHub
#          prerelease lab-<commit12>: the lab runs the campaign's own build,
#          which has what no nightly has yet (E4's headcount split, the
#          shim's per-step items). --version <nightly> skips this and pins
#          that nightly instead;
#       1. creates the campaign SSH key ~/.config/pipedpeer-lab/campaign_ed25519
#          if missing (never in the repo);
#       2. asks for the campaign passphrase, or generates and prints one;
#       3. encrypts the private key and ~/.config/pipedpeer-lab/r2.env into
#          landing/public/lab-secrets.enc;
#       4. writes landing/public/lab.sh: scripts/campaign/lab.sh with the public
#          key, HEAD's commit and the pipedpeer version filled in;
#       5. prints what to commit.
#   scripts/campaign/publish.sh selftest
#       downloads the published lab.sh and lab-secrets.enc, checks lab.sh is
#       the local copy, and decrypts the bundle with the passphrase you type.
#
# The bundle sits on a public site: the passphrase is its only protection.
# After the lab: revoke the R2 token, delete the campaign key (both files), and
# publish again (or delete the two files) so the old bundle is gone from Pages.
# Env: LAB_SITE (default https://vinayakyadav2709.github.io/Pipedpeer),
# LAB_CONFIG (default ~/.config/pipedpeer-lab), LAB_PASSPHRASE_FILE (tests).
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../.." && pwd)
site=${LAB_SITE:-https://vinayakyadav2709.github.io/Pipedpeer}
cfg=${LAB_CONFIG:-$HOME/.config/pipedpeer-lab}
key=$cfg/campaign_ed25519
# Upload credentials for the bundle's "r2.env": the ingest Worker's
# (ingest/README.md), else R2 S3 keys.
r2=$cfg/ingest.env
[[ -f $r2 ]] || r2=$cfg/r2.env
pub_dir=$repo/landing/public
die() { echo "publish.sh: $*" >&2; exit 1; }

# A passphrase you can type on a lab keyboard: 6 groups of 5 from a
# 32-letter alphabet without look-alikes, 150 bits.
gen_pass() {
  python3 -c '
import secrets
a = "abcdefghijkmnpqrstuvwxyz23456789"
print("-".join("".join(secrets.choice(a) for _ in range(5)) for _ in range(6)))'
}

read_pass() {  # read_pass <prompt> -> PASS
  if [[ -n ${LAB_PASSPHRASE_FILE:-} ]]; then PASS=$(<"$LAB_PASSPHRASE_FILE"); return; fi
  IFS= read -rsp "$1" PASS </dev/tty; echo >/dev/tty
}

strong_enough() {
  local p=$1
  (( ${#p} >= 24 )) && return 0
  # Six or more words separated by spaces or dashes.
  local words; read -ra words <<<"${p//-/ }"
  (( ${#words[@]} >= 6 )) && return 0
  return 1
}

encrypt() {  # encrypt <out>: tar of the key and r2.env, AES-256-CBC, PBKDF2 600k
  local work rc=0; work=$(mktemp -d)
  install -m 600 "$key" "$work/campaign_ed25519"
  install -m 600 "$r2" "$work/r2.env"
  tar -C "$work" -czf - campaign_ed25519 r2.env |
    openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -salt -out "$1.new" -pass fd:3 3< <(printf '%s\n' "$PASS") || rc=1
  rm -rf "$work"
  ((rc == 0)) || die "encryption failed"
  mv -f "$1.new" "$1"
}

decrypt_check() {  # decrypt_check <bundle> <pubkey file>
  local work why=""; work=$(mktemp -d)
  if ! openssl enc -d -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -in "$1" -pass fd:3 3< <(printf '%s\n' "$PASS") 2>/dev/null |
       tar -xzf - -C "$work" 2>/dev/null; then
    why="cannot decrypt $1 with that passphrase"
  elif [[ $(ssh-keygen -y -f "$work/campaign_ed25519" | awk '{print $2}') != "$(awk '{print $2}' "$2")" ]]; then
    why="the key in the bundle is not the published key"
  elif ! grep -qE '^(INGEST_TOKEN|R2_SECRET_ACCESS_KEY)=.' "$work/r2.env"; then
    why="r2.env in the bundle has neither INGEST_TOKEN nor R2_SECRET_ACCESS_KEY"
  fi
  rm -rf "$work"
  [[ -z $why ]] || die "$why"
  echo "bundle OK: the key matches the published public key, r2.env is complete"
}

publish() {
  local version=""
  while (($#)); do
    case $1 in
      --version) version=${2:-}; shift 2 ;;
      *) die "usage: publish.sh [--version <v>] | selftest" ;;
    esac
  done
  for t in git openssl ssh-keygen python3 tar; do command -v "$t" >/dev/null || die "$t not found"; done
  [[ -f $r2 ]] || die "no $cfg/ingest.env (INGEST_URL=, INGEST_TOKEN=; ingest/README.md) and no r2.env"
  grep -qE '^(INGEST_TOKEN|R2_SECRET_ACCESS_KEY)=.' "$r2" || die "$r2 has neither INGEST_TOKEN nor R2_SECRET_ACCESS_KEY"
  local commit; commit=$(git -C "$repo" rev-parse HEAD)
  if [[ -n $(git -C "$repo" status --porcelain -- scripts docs/benchmarks pipedpeer-demo) ]]; then
    die "uncommitted changes under scripts/, docs/benchmarks/ or pipedpeer-demo/: the lab would not get them; commit first"
  fi
  if ! git -C "$repo" branch -r --contains "$commit" 2>/dev/null | grep -q .; then
    echo "WARNING: $commit is on no remote branch you have fetched; push it before the lab, or codeload cannot serve it" >&2
  fi

  [[ -n $version ]] || version=$(release_build "$commit") || die "building or uploading the lab build failed"

  mkdir -p "$cfg"; chmod 700 "$cfg"
  if [[ ! -f $key ]]; then
    ssh-keygen -q -t ed25519 -N '' -C pipedpeer-lab-campaign -f "$key"
    echo "created the campaign key $key"
  fi
  [[ -f $key.pub ]] || ssh-keygen -y -f "$key" >"$key.pub"
  chmod 600 "$key" "$r2"

  echo "Campaign passphrase: type one (6+ words or 24+ characters), or press Enter to generate one."
  read_pass "passphrase: "
  if [[ -z $PASS ]]; then
    PASS=$(gen_pass)
    echo
    echo "    PASSPHRASE:  $PASS"
    echo
    echo "Write it down (not in the repo); the operator types it once, on pc00."
  else
    strong_enough "$PASS" || die "too weak: use 6+ words or 24+ characters (Enter generates one)"
    local again; again=$PASS; read_pass "again: "; [[ $PASS == "$again" ]] || die "the two did not match"
  fi

  mkdir -p "$pub_dir"
  encrypt "$pub_dir/lab-secrets.enc"
  decrypt_check "$pub_dir/lab-secrets.enc" "$key.pub"
  PASS=""
  bash "$here/lab-fill.sh" fill "$here/lab.sh" "$pub_dir/lab.sh" "$(cat "$key.pub")" "$commit" "$site" "$version"
  bash "$here/lab-fill.sh" check "$pub_dir/lab.sh" >/dev/null

  cat <<EOF

Published for commit $commit${version:+, pipedpeer $version}.
Commit and push to dev (the Pages workflow deploys landing/ from dev):

  git add landing/public/lab.sh landing/public/lab-secrets.enc
  git commit -m "Publish the lab day for $commit"
  git push origin dev

Once the Pages run finishes (a few minutes), check what is online:

  scripts/campaign/publish.sh selftest

The operator types on every lab PC:   curl -fsSL $site/lab.sh | sudo bash
and on the head PC:                   curl -fsSL $site/lab.sh | sudo bash -s -- head

After the lab: rotate the ingest token, delete $key and $key.pub,
remove landing/public/lab-secrets.enc from dev, and delete the release
($version; results/lab/OPERATOR.md, "After the lab").
EOF
}

# release_build <commit>: build pipedpeer at <commit> for the lab and upload
# it as the GitHub prerelease lab-<commit12>; prints the version. A release
# is used rather than Pages: no binary goes into git history, and
# install-pipedpeer.sh verifies it against checksums.txt as it does a nightly.
# Rerunning replaces the assets (--clobber), so it is idempotent.
release_build() {
  local commit=$1 tag work gh_repo=vinayakyadav2709/Pipedpeer arch
  tag="lab-${commit:0:12}"
  command -v go >/dev/null || { echo "publish.sh: go not found; it builds the lab's pipedpeer" >&2; return 1; }
  command -v gh >/dev/null || { echo "publish.sh: gh not found; it uploads the lab's pipedpeer" >&2; return 1; }
  work=$(mktemp -d)
  # Build from the commit itself, not the working tree.
  git -C "$repo" archive "$commit" src | tar -x -C "$work" || { rm -rf "$work"; return 1; }
  for arch in amd64 arm64; do
    (cd "$work/src" && CGO_ENABLED=0 GOOS=linux GOARCH=$arch GOFLAGS=-buildvcs=false \
      go build -trimpath -ldflags "-s -w -X main.version=$tag" -o "$work/pipedpeer-linux-$arch" .) >&2 || { rm -rf "$work"; return 1; }
  done
  (cd "$work" && sha256sum pipedpeer-linux-amd64 pipedpeer-linux-arm64 >checksums.txt)
  if gh release view "$tag" -R "$gh_repo" >/dev/null 2>&1; then
    gh release upload "$tag" -R "$gh_repo" --clobber "$work"/pipedpeer-linux-* "$work/checksums.txt" >&2 || { rm -rf "$work"; return 1; }
  else
    gh release create "$tag" -R "$gh_repo" --prerelease --target "$commit" --title "Lab campaign build $tag" \
      --notes "pipedpeer built at $commit for the headless lab day (scripts/campaign/HEADLESS.md). Not a release." \
      "$work"/pipedpeer-linux-* "$work/checksums.txt" >&2 || { rm -rf "$work"; return 1; }
  fi
  echo "uploaded pipedpeer $tag (GitHub release $tag)" >&2
  rm -rf "$work"
  echo "$tag"
}

selftest() {
  local work; work=$(mktemp -d)
  # shellcheck disable=SC2064  # work is fixed now
  trap "rm -rf '$work'" EXIT
  curl -fsSL "$site/lab.sh" -o "$work/lab.sh" || die "cannot download $site/lab.sh"
  curl -fsSL "$site/lab-secrets.enc" -o "$work/lab-secrets.enc" || die "cannot download $site/lab-secrets.enc"
  cmp -s "$work/lab.sh" "$pub_dir/lab.sh" || die "lab.sh online differs from landing/public/lab.sh (Pages not deployed yet?)"
  echo "lab.sh online is landing/public/lab.sh"
  cmp -s "$work/lab-secrets.enc" "$pub_dir/lab-secrets.enc" || die "lab-secrets.enc online differs from the local one"
  echo "lab-secrets.enc online is the local one"
  grep -q '^CAMPAIGN_KEY="__' "$work/lab.sh" && die "the online lab.sh is not filled in"
  local commit; commit=$(sed -n 's/^CAMPAIGN_COMMIT="\(.*\)"$/\1/p' "$work/lab.sh")
  curl -fsSIL "https://codeload.github.com/vinayakyadav2709/Pipedpeer/tar.gz/$commit" >/dev/null ||
    die "codeload cannot serve $commit: push it"
  echo "code tarball for $commit is downloadable"
  local v; v=$(sed -n 's/^PIPEDPEER_VERSION="\(.*\)"$/\1/p' "$work/lab.sh")
  if [[ $v == lab-* ]]; then
    local rel=https://github.com/vinayakyadav2709/Pipedpeer/releases/download/$v
    curl -fsSL "$rel/checksums.txt" -o "$work/sums" && curl -fsSL "$rel/pipedpeer-linux-amd64" -o "$work/pipedpeer-linux-amd64" ||
      die "release $v is not downloadable"
    (cd "$work" && grep ' pipedpeer-linux-amd64$' sums | sha256sum -c --quiet) || die "release $v: checksum mismatch"
    chmod +x "$work/pipedpeer-linux-amd64"
    [[ $("$work/pipedpeer-linux-amd64" --version 2>&1) == *"$v"* ]] || die "release $v: the binary does not report $v"
    echo "pipedpeer $v is downloadable and checks out"
  fi
  sed -n 's/^CAMPAIGN_KEY="\(.*\)"$/\1/p' "$work/lab.sh" >"$work/pub"
  read_pass "campaign passphrase: "
  decrypt_check "$work/lab-secrets.enc" "$work/pub"
  echo "selftest passed"
}

case "${1:-}" in
  selftest) selftest ;;
  *) publish "$@" ;;
esac
