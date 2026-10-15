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
# On kernels 6.6+ it may build its own proot into $PREFIX/opt/pipedpeer-proot
# (see below); whatever runs proot-distro later must then export
# PD_PROOT_BIN=$PREFIX/opt/pipedpeer-proot/bin/proot.
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

# Kernels 6.6+ have fchmodat2, and glibc 2.39+ (Debian trixie, Nix's glibc)
# calls it for chmod. termux/proot translates its path only from 7fca80c
# (2026-10-12); Termux's proot 5.1.107.96 predates it, so the call reaches the
# kernel with the guest path and fails ("cp: preserving permissions ...: No
# such file or directory"; the Nix installer dies there). On those kernels,
# until Termux ships a newer proot, build termux/proot at a fixed commit into
# our own prefix and point proot-distro at it with PD_PROOT_BIN.
PROOT_COMMIT=b8c1f5c5c6730dc9d521d5bd5ce1cfeedecde087
PROOT_DIR=$PREFIX/opt/pipedpeer-proot
proot_lacks_fchmodat2() {
	local kernel pkgver
	kernel=$(uname -r | grep -oE '^[0-9]+\.[0-9]+')
	[ "$(printf '%s\n' 6.6 "$kernel" | sort -V | head -1)" = 6.6 ] || return 1
	pkgver=$(proot --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | tail -1)
	[ -n "$pkgver" ] || return 0
	[ "$(printf '%s\n' "$pkgver" 5.1.107.96 | sort -V | tail -1)" = 5.1.107.96 ]
}
if [ -x "$PROOT_DIR/bin/proot" ]; then
	export PD_PROOT_BIN=$PROOT_DIR/bin/proot
elif proot_lacks_fchmodat2; then
	echo "phone-proot-setup: kernel $(uname -r) has fchmodat2 and this Termux proot cannot translate it; building termux/proot ${PROOT_COMMIT:0:7}" >&2
	pkg install -y clang make patch libtalloc libandroid-shmem
	src=$(mktemp -d "$TMPDIR/proot-src.XXXXXX")
	curl -fsSL "https://codeload.github.com/termux/proot/tar.gz/$PROOT_COMMIT" | tar -xz -C "$src" --strip-components=1
	# Our patches on top (phone-proot/*.patch next to this script), e.g. no
	# chmod on procfs: SELinux logs every attempt on /proc/<pid>/ns.
	for p in "$(dirname "$0")"/phone-proot/*.patch; do
		if [ -e "$p" ]; then patch -s -d "$src" -p1 < "$p"; fi
	done
	# Same flags as Termux's own build (termux-packages packages/proot/build.sh).
	# make hands CPPFLAGS to a shell, so the quotes around VERSION are escaped.
	CPPFLAGS="-DARG_MAX=131072 -DVERSION=\\\"5.1.107.96+${PROOT_COMMIT:0:7}\\\"" \
		make -s -C "$src/src" install PREFIX="$PROOT_DIR" PROOT_WITH_LIBANDROID_SHMEM=true \
		PROOT_UNBUNDLE_LOADER="$PROOT_DIR/libexec/proot"
	rm -rf "$src"
	export PD_PROOT_BIN=$PROOT_DIR/bin/proot
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
