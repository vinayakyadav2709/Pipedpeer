#!/usr/bin/env bash
# shellcheck disable=SC2015,SC1090,SC2034
# PN1 case table: pin Ray and Dask to exact versions on the fake lab.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

ORIG_PATH="$PATH"
ORIG_HOME="$HOME"

pass=0
failed=0
problems=()
dirs=()

cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" != 1 ]]; then
		for d in "${dirs[@]}"; do rm -rf "$d"; done
	fi
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi
	problems=()
}

newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-pins-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

setup_lab() {
	local dir="$1"
	export PATH="$ORIG_PATH"
	export HOME="$ORIG_HOME"
	unset STUB_PIP_FAIL_PKG STUB_PIP_LOG STUB_RAY_VER STUB_DASK_VER
	fakelab_init "$dir" 2 || exit 90
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=0.2
	export LAB_POLL_S=1

	# pipedpeer stub
	cat >"$dir/bin/pipedpeer" <<'PP-EOF'
#!/usr/bin/env bash
pc="$(basename "$(cd "${HOME:-}" && pwd)")"
case "${1:-}" in
	status)
		printf 'daemon running (pid=123 node-id=node-%s port=4242 version=0.0.1-test.20261108-abc1234)\n' "$pc"
		;;
	run)
		printf '%s\n' "${STUB_RUN_VERSIONS:-VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.4.1+cpu ray=none dask=none}"
		;;
	*)
		exit 0
		;;
esac
PP-EOF
	chmod +x "$dir/bin/pipedpeer"

	# python3 wrapper stub to intercept `pip index versions`
	cat >"$dir/bin/python3" <<'PY-EOF'
#!/usr/bin/env bash
if [[ "$*" == *"-m pip index versions"* ]]; then
	pkg="${!#}"  # last argument is the package name
	# Or find package name if arguments like --python-version follow
	for arg in "$@"; do
		case "$arg" in
			ray|dask|numpy|pandas|scikit-learn|sklearn|joblib|torch)
				pkg="$arg"
				;;
		esac
	done

	if [[ -n "${STUB_PIP_LOG:-}" ]]; then
		printf '%s %s\n' "$pkg" "$*" >>"${STUB_PIP_LOG}"
	fi

	if [[ -n "${STUB_PIP_FAIL_PKG:-}" && "${STUB_PIP_FAIL_PKG}" == "$pkg" ]]; then
		exit 1
	fi

	case "$pkg" in
		ray)
			ver="${STUB_RAY_VER:-2.38.0}"
			printf 'ray (%s)\nAvailable versions: %s, 2.37.0, 2.36.0\n' "$ver" "$ver"
			exit 0
			;;
		dask)
			ver="${STUB_DASK_VER:-2024.9.0}"
			printf 'dask (%s)\nAvailable versions: %s, 2024.8.0, 2024.7.0\n' "$ver" "$ver"
			exit 0
			;;
		torch)
			printf 'torch (2.4.1)\nAvailable versions: 2.4.1, 2.4.0\n'
			exit 0
			;;
		scikit-learn|sklearn)
			printf 'scikit-learn (1.5.2)\nAvailable versions: 1.5.2, 1.5.1\n'
			exit 0
			;;
		numpy)
			printf 'numpy (1.26.4)\nAvailable versions: 1.26.4, 1.26.3\n'
			exit 0
			;;
		pandas)
			printf 'pandas (2.2.3)\nAvailable versions: 2.2.3, 2.2.2\n'
			exit 0
			;;
		joblib)
			printf 'joblib (1.4.2)\nAvailable versions: 1.4.2, 1.4.1\n'
			exit 0
			;;
		*)
			printf '%s (1.0.0)\nAvailable versions: 1.0.0\n' "$pkg"
			exit 0
			;;
	esac
fi
exec /usr/bin/python3 "$@"
PY-EOF
	chmod +x "$dir/bin/python3"
}

# --- Row 1 & 2: index offers ray 2.x and dask -> requirements holds ray==2.x, dask==...; a re-run keeps it
t1() {
	local d rc=0
	d="$(newdir)"
	setup_lab "$d"
	export STUB_RAY_VER="2.38.0"
	export STUB_DASK_VER="2024.9.0"
	export STUB_PIP_LOG="$d/pip.log"
	export PINS_WHEEL_CHECK=1

	bash "$REPO/scripts/campaign/pins.sh" >"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "pins.sh exited $rc"
	grep -qE '^ray(\[default\])?==2\.38\.0' "$d/requirements.txt" || pnote "requirements.txt lacks ray==2.38.0 pin"
	grep -qE '^dask(\[distributed\])?==2024\.9\.0' "$d/requirements.txt" || pnote "requirements.txt lacks dask==2024.9.0 pin"

	# Re-run: must keep the same pins and not re-resolve
	rm -f "$d/pip.log"
	rc=0
	bash "$REPO/scripts/campaign/pins.sh" >>"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "re-run exited $rc"
	grep -qE '^ray(\[default\])?==2\.38\.0' "$d/requirements.txt" || pnote "re-run lost ray==2.38.0 pin"
	grep -qE '^dask(\[distributed\])?==2024\.9\.0' "$d/requirements.txt" || pnote "re-run lost dask==2024.9.0 pin"
	if [[ -f "$d/pip.log" ]] && grep -qE '^(ray|dask) ' "$d/pip.log"; then
		pnote "re-run queried index for ray/dask again"
	fi
	verdict "index offers ray 2.x and dask -> requirements holds ray==2.x, dask==...; a re-run keeps it"
}

# --- Row 3: second PC -> same pins read from versions.csv, not resolved again
t2() {
	local d rc=0
	d="$(newdir)"
	setup_lab "$d"
	export STUB_PIP_LOG="$d/pip.log"
	export PINS_WHEEL_CHECK=1

	# Pre-populate versions.csv with previously resolved ray and dask pins
	cat >"$d/versions.csv" <<'EOF'
system,python,numpy,pandas,sklearn,joblib,torch,ray,dask,pipedpeer
pipedpeer,3.12.7,1.26.4,2.2.3,1.5.2,1.4.2,2.4.1+cpu,2.35.0,2024.8.0,0.0.1
EOF

	bash "$REPO/scripts/campaign/pins.sh" >"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "pins.sh exited $rc"
	grep -qE '^ray(\[default\])?==2\.35\.0' "$d/requirements.txt" || pnote "requirements.txt did not use ray==2.35.0 from versions.csv"
	grep -qE '^dask(\[distributed\])?==2024\.8\.0' "$d/requirements.txt" || pnote "requirements.txt did not use dask==2024.8.0 from versions.csv"
	if [[ -f "$d/pip.log" ]] && grep -qE '^(ray|dask) ' "$d/pip.log"; then
		pnote "pins.sh re-queried index when versions.csv already had pins"
	fi
	verdict "second PC: same pins read from versions.csv, not resolved again"
}

# --- Row 4: second PC -> same pins read from requirements.txt, not resolved again
t3() {
	local d rc=0
	d="$(newdir)"
	setup_lab "$d"
	export STUB_PIP_LOG="$d/pip.log"
	export PINS_WHEEL_CHECK=1

	# Pre-populate requirements.txt with previously resolved pins
	cat >"$d/requirements.txt" <<'EOF'
numpy==1.26.4
pandas==2.2.3
scikit-learn==1.5.2
joblib==1.4.2
--extra-index-url https://download.pytorch.org/whl/cpu
torch==2.4.1+cpu
ray[default]==2.36.0
dask[distributed]==2024.7.0
pyarrow
EOF

	bash "$REPO/scripts/campaign/pins.sh" >"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "pins.sh exited $rc"
	grep -qE '^ray(\[default\])?==2\.36\.0' "$d/requirements.txt" || pnote "requirements.txt did not retain ray==2.36.0"
	grep -qE '^dask(\[distributed\])?==2024\.7\.0' "$d/requirements.txt" || pnote "requirements.txt did not retain dask==2024.7.0"
	if [[ -f "$d/pip.log" ]] && grep -qE '^(ray|dask) ' "$d/pip.log"; then
		pnote "pins.sh re-queried index when requirements.txt already had pins"
	fi
	verdict "second PC: same pins read from requirements.txt, not resolved again"
}

# --- Row 5: resolve fails for ray -> step fails, names the package
t4() {
	local d rc=0 err=""
	d="$(newdir)"
	setup_lab "$d"
	export STUB_PIP_FAIL_PKG="ray"
	export PINS_WHEEL_CHECK=1

	err="$(bash "$REPO/scripts/campaign/pins.sh" 2>&1)" || rc=$?
	((rc != 0)) || pnote "pins.sh succeeded despite ray resolve failure"
	grep -qi 'ray' <<<"$err" || pnote "error output did not name ray: [$err]"
	verdict "resolve fails for ray: step fails, names ray"
}

# --- Row 6: resolve fails for dask -> step fails, names the package
t5() {
	local d rc=0 err=""
	d="$(newdir)"
	setup_lab "$d"
	export STUB_PIP_FAIL_PKG="dask"
	export PINS_WHEEL_CHECK=1

	err="$(bash "$REPO/scripts/campaign/pins.sh" 2>&1)" || rc=$?
	((rc != 0)) || pnote "pins.sh succeeded despite dask resolve failure"
	grep -qi 'dask' <<<"$err" || pnote "error output did not name dask: [$err]"
	verdict "resolve fails for dask: step fails, names dask"
}

# --- Row 7: versions.csv has ray and dask lines (fields recorded)
t6() {
	local d rc=0
	d="$(newdir)"
	setup_lab "$d"
	export STUB_RAY_VER="2.38.0"
	export STUB_DASK_VER="2024.9.0"
	export PINS_WHEEL_CHECK=1

	bash "$REPO/scripts/campaign/pins.sh" >"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "pins.sh exited $rc"
	local ra da
	ra="$(awk -F, '$1=="pipedpeer" {print $8}' "$d/versions.csv" 2>/dev/null || true)"
	da="$(awk -F, '$1=="pipedpeer" {print $9}' "$d/versions.csv" 2>/dev/null || true)"
	[[ "$ra" == "2.38.0" ]] || pnote "versions.csv ray field got [$ra], want [2.38.0]"
	[[ "$da" == "2024.9.0" ]] || pnote "versions.csv dask field got [$da], want [2024.9.0]"
	verdict "versions.csv has ray and dask lines"
}

# --- Row 8: plain numpy pin (ordinary use) still matches the closure
t7() {
	local d rc=0
	d="$(newdir)"
	setup_lab "$d"
	export PINS_WHEEL_CHECK=1

	bash "$REPO/scripts/campaign/pins.sh" >"$d/pins.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "pins.sh exited $rc"
	grep -qxF 'numpy==1.26.4' "$d/requirements.txt" || pnote "numpy was not pinned to closure version 1.26.4"
	local np
	np="$(awk -F, '$1=="pipedpeer" {print $3}' "$d/versions.csv" 2>/dev/null || true)"
	[[ "$np" == "1.26.4" ]] || pnote "versions.csv numpy got [$np], want [1.26.4]"
	verdict "plain numpy pin (ordinary use) still matches the closure"
}

t1
t2
t3
t4
t5
t6
t7

printf 'PASS %d/%d\n' "$pass" "$((pass + failed))"
[[ "$pass" -eq "$((pass + failed))" ]]
