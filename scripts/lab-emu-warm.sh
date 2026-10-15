#!/usr/bin/env bash
# lab-emu.sh with a warm Nix store: a fresh lab starts with the environments an
# earlier lab of the same set built, instead of rebuilding and reshipping them
# (a torch environment is 8-15 min). Same verbs and knobs as lab-emu.sh; benches
# use it with BL_EMU=scripts/lab-emu-warm.sh.
#
#   up [N]   lab-emu.sh up, then copy the warm set into every container's store
#   down     save container 1's new store paths into the warm set, then lab-emu.sh down
#   save     save only (the lab keeps running)
#   load     load only (into the running lab)
#   other verbs go to lab-emu.sh unchanged
#
# The warm set is a host-side file:// binary cache, so it is keyed by store
# path: a path is loaded only if this lab's own build produced that exact path
# before, and a job whose environment differs still builds it. "New" paths are
# those container 1 has and the bare lab image has not; envs.json (the CLI's
# environment index) goes with them, so the CLI reuses them without evaluating.
#
# Knobs (environment), plus everything lab-emu.sh reads:
#   LAB_COLD=1     cold mode: plain lab-emu.sh, nothing loaded or saved. Cold
#                  benchmark cases (bench-w10 cold, sweep.sh cold) must run cold.
#   LAB_WARM_SET   which set (default LAB_NAME); keep one set per binary when
#                  binaries build different environments (bench-before, ...)
#   LAB_EMU        lab-emu.sh to wrap (default scripts/lab-emu.sh)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"

emu="${LAB_EMU:-$repo_root/scripts/lab-emu.sh}"
lab_name="${LAB_NAME:-emu}"
prefix="pp$lab_name"
label="pipedpeer.lab=$lab_name"
image=pp-emu # lab-emu.sh's image
set_name="${LAB_WARM_SET:-$lab_name}"
[[ "$set_name" =~ ^[a-z0-9-]+$ ]] || { echo "lab-warm: LAB_WARM_SET must be [a-z0-9-]" >&2; exit 2; }
cache="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/lab-warm"
store="$cache/store"
set_file="$cache/sets/$set_name"
envs_file="$cache/sets/$set_name.envs.json"
envs_in=/var/lib/pipedpeer/pipedpeer/envs.json # XDG_DATA_HOME in lab-emu.sh
nixc=(nix --extra-experimental-features nix-command --option build-users-group '')

log() { echo "lab-warm: $*" >&2; }

# Store paths of the bare lab image, cached per image id.
base_paths() {
	local id f
	id="$($rt image inspect -f '{{.Id}}' "$image")"
	f="$cache/base-${id#sha256:}"
	[[ -s "$f" ]] || $rt run --rm "$image" "${nixc[@]}" path-info --all | sort >"$f"
	cat "$f"
}

cmd_save() {
	local c="$prefix-1" t0=$SECONDS new=() p
	if [[ "$($rt inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != true ]]; then
		log "warning: $c is not running; nothing saved"
		return 0
	fi
	mkdir -p "$store" "$cache/sets"
	# New paths the host cache lacks (narinfo name = the path's hash part).
	while read -r p; do
		[[ -e "$store/$(basename "$p" | cut -d- -f1).narinfo" ]] || new+=("$p")
	done < <(comm -13 <(base_paths) <($rt exec "$c" "${nixc[@]}" path-info --all | sort))
	if ((${#new[@]})); then
		log "saving ${#new[@]} new store paths from $c (set $set_name)..."
		if ! { $rt exec "$c" rm -rf /tmp/ppwarm &&
			$rt exec "$c" "${nixc[@]}" copy --quiet --to 'file:///tmp/ppwarm?compression=zstd' "${new[@]}" &&
			$rt cp "$c:/tmp/ppwarm/." "$store/"; }; then
			$rt exec "$c" rm -rf /tmp/ppwarm || true
			log "warning: saving from $c failed; set $set_name unchanged"
			return 1
		fi
		$rt exec "$c" rm -rf /tmp/ppwarm
	fi
	# The set: every non-base path the cache now holds (another set may have
	# saved some of them first).
	touch "$set_file"
	comm -13 <(base_paths) <($rt exec "$c" "${nixc[@]}" path-info --all | sort) |
		while read -r p; do
			[[ -e "$store/$(basename "$p" | cut -d- -f1).narinfo" ]] && echo "$p"
		done | sort -u - "$set_file" >"$set_file.new"
	mv "$set_file.new" "$set_file"
	if $rt exec "$c" test -s "$envs_in"; then
		$rt cp "$c:$envs_in" "$envs_file"
	fi
	log "set $set_name: $(wc -l <"$set_file" 2>/dev/null || echo 0) paths, $(du -sh "$store" | cut -f1) cache; save took $((SECONDS - t0)) s"
}

# load_one <container> <staging dir>: copy the set into one container's store.
load_one() {
	local c="$1" stage="$2"
	local rc=0
	$rt exec "$c" rm -rf /tmp/ppwarm &&
		$rt cp "$stage/." "$c:/tmp/ppwarm" &&
		$rt exec "$c" sh -c 'xargs "$@" < /tmp/ppwarm/paths' sh \
			"${nixc[@]}" copy --quiet --no-check-sigs --from file:///tmp/ppwarm &&
		{ [[ ! -s "$envs_file" ]] || $rt cp "$envs_file" "$c:$envs_in"; } || rc=1
	$rt exec "$c" rm -rf /tmp/ppwarm || true
	return $rc
}

cmd_load() {
	local t0=$SECONDS stage c pids=() names=() i p h nar
	if [[ ! -s "$set_file" ]]; then
		log "no warm set '$set_name' yet; this lab starts cold (down saves one)"
		return 0
	fi
	# Stage every narinfo (nix copy resolves the closure in the source, base
	# paths included) but only this set's NARs (hard links, same filesystem).
	stage="$(mktemp -d "$cache/stage.XXXXXX")"
	mkdir -p "$stage/nar"
	cp "$store/nix-cache-info" "$stage/"
	find "$store" -maxdepth 1 -name '*.narinfo' -exec ln -t "$stage" {} +
	while read -r p; do
		h="$(basename "$p" | cut -d- -f1)"
		if [[ ! -e "$store/$h.narinfo" ]]; then
			log "warning: $p is in set $set_name but not in the cache; skipped"
			continue
		fi
		nar="$(grep -m1 '^URL: ' "$store/$h.narinfo" | cut -c6-)"
		ln "$store/$nar" "$stage/$nar"
		echo "$p" >>"$stage/paths"
	done <"$set_file"
	log "loading set $set_name ($(wc -l <"$stage/paths") paths, $(du -shL "$stage" | cut -f1)) into the lab..."
	for c in $($rt ps --filter "label=$label" --format '{{.Names}}' | sort -V); do
		load_one "$c" "$stage" &
		pids+=($!)
		names+=("$c")
	done
	for i in "${!pids[@]}"; do
		wait "${pids[$i]}" || { log "warning: loading into ${names[$i]} failed; it builds or receives the environment as usual"; }
	done
	rm -rf "$stage"
	log "load took $((SECONDS - t0)) s"
	return 0
}

verb="${1:-}"
if [[ "${LAB_COLD:-0}" == 1 ]]; then
	exec "$emu" "$@"
fi
case "$verb" in
up | down | save | load)
	pp_lab_runtime || exit 1
	;;
esac
case "$verb" in
up)
	"$emu" "$@"
	cmd_load
	;;
down)
	cmd_save || log "warning: save failed (exit $?); the next lab starts colder"
	"$emu" down
	;;
save) cmd_save ;;
load) cmd_load ;;
*) exec "$emu" "$@" ;;
esac
