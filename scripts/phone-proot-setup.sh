#!/usr/bin/env bash
# Phone as a full node: Debian in proot-distro plus single-user Nix, inside
# Termux, without root. Run it in Termux on the phone (or in
# termux/termux-docker as the "system" user).
#
# Nix in proot gets the public aarch64-linux binary cache, so the job env
# (python314 + numpy, pandas, torch, scikit-learn at our pin) is fetched, not
# built. proot has no namespaces, so Nix runs with sandbox = false and no
# syscall filter (builds are not isolated from each other on the phone).
#
#   scripts/phone-proot-setup.sh            install proot-distro, Debian, Nix
#   scripts/phone-proot-setup.sh smoke      also build the CPU job env at our pin
#   scripts/phone-proot-setup.sh gaps       list env paths cache.nixos.org lacks
#
# Knobs (environment):
#   DISTRO       proot-distro alias (default debian)
#   NIX_VERSION  Nix release to install (default 2.31.3, the lab image's)
#   NIX_TARBALL  local nix-<version>-<system>.tar.xz to use instead of downloading
set -euo pipefail

DISTRO=${DISTRO:-debian}
NIX_VERSION=${NIX_VERSION:-2.31.3}
# Keep in step with nixpkgsRef in src/internal/nixgen/flake.go.
NIXPKGS=github:NixOS/nixpkgs/56c02bc00adcf003215cc4bd996d6efaf4cff188
SYSTEM=$(uname -m)-linux
ENV_EXPR='(builtins.getFlake "'$NIXPKGS'").legacyPackages.'$SYSTEM'.python314.withPackages (ps: with ps; [numpy pandas torch scikit-learn joblib])'

die() { echo "phone-proot-setup: $*" >&2; exit 1; }

case "${PREFIX:-}" in
*com.termux*) ;;
*) die "not in Termux (PREFIX=${PREFIX:-unset}); run this in the Termux app" ;;
esac
[ "$(id -u)" != 0 ] || die "run as the Termux user, not root (proot-distro refuses root)"
[ "$(uname -m)" = aarch64 ] || echo "phone-proot-setup: warning: $(uname -m), not aarch64 (a test container, not a phone)" >&2

# distro runs a command as proot's fake root inside the distro; in_distro runs
# a bash script from stdin. proot-distro passes Termux's PATH through, and
# Termux binaries cannot run there, so PATH is reset to the distro's.
distro() {
	proot-distro login "$DISTRO" -- /usr/bin/env USER=root HOME=/root \
		PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin "$@"
}
in_distro() { distro bash -s; }

if ! command -v proot-distro >/dev/null; then
	pkg install -y proot-distro
fi

if ! distro true 2>/dev/null; then
	proot-distro install "$DISTRO"
fi

# The Nix tarball is fetched and unpacked by Termux's curl and xz, so the
# distro needs no apt (no 10 MB of package lists, no CA package).
if ! distro test -x /root/.nix-profile/bin/nix; then
	command -v xz >/dev/null || pkg install -y xz-utils
	if [ -n "${NIX_TARBALL:-}" ]; then
		xz -dc "$NIX_TARBALL"
	else
		curl -fsSL "https://releases.nixos.org/nix/nix-$NIX_VERSION/nix-$NIX_VERSION-$SYSTEM.tar.xz" | xz -d
	fi | distro tar -x -C /root
	in_distro <<EOF
set -euo pipefail
# proot cannot create namespaces or install a seccomp filter; the installer's
# own CA bundle (cacert in the profile) serves TLS, the image has none.
mkdir -p /etc/nix && mkdir -p -m 0755 /nix
cat > /etc/nix/nix.conf <<'CONF'
sandbox = false
filter-syscalls = false
build-users-group =
experimental-features = nix-command flakes
ssl-cert-file = /root/.nix-profile/etc/ssl/certs/ca-bundle.crt
CONF
/root/nix-$NIX_VERSION-$SYSTEM/install --no-daemon --no-channel-add --no-modify-profile
rm -rf /root/nix-$NIX_VERSION-$SYSTEM
EOF
fi
distro /root/.nix-profile/bin/nix --version

case "${1:-}" in
smoke)
	in_distro <<EOF
set -euo pipefail
/root/.nix-profile/bin/nix build --impure --no-link --print-out-paths --expr '$ENV_EXPR' |
	while read -r p; do "\$p/bin/python3" -c 'import numpy, pandas, torch, sklearn; print("env ok", torch.__version__, numpy.__version__)'; done
EOF
	;;
gaps)
	# Every path in "will be built" is missing from cache.nixos.org.
	in_distro <<EOF
/root/.nix-profile/bin/nix build --impure --no-link --dry-run --expr '$ENV_EXPR' 2>&1 | sed -n '/will be built/,/will be fetched/p'
EOF
	;;
"") ;;
*) die "unknown mode $1 (want: smoke, gaps)" ;;
esac
