#!/usr/bin/env bash
# collect-bug.sh: `collect.sh --bug`, sourced by collect.sh (REPO and LAB set).
# One folder with everything needed to debug a failed campaign run, and no
# secrets; see collect.sh's header. Best effort by design: a PC that does not
# answer, or analyse.py failing, is written down in SUMMARY.txt, never fatal.

# _bug_pc_script: the bash run on each PC; prints "## <section>" blocks.
# shellcheck disable=SC2016  # expanded on the PC
_bug_pc_script() {
	printf 'export LAB_RAY_SESSION=%q\n' "${LAB_RAY_SESSION:-/tmp/ray/session_latest}"
	cat <<'PC'
sec() { local t="$1" n="$2"; shift 2; echo; echo "## $t"; { "$@" 2>&1 || echo "(exit $?)"; } | tail -n "$n"; }
st="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
cgmem() {
  local p v1 f d s r pcg
  r="${PP_CG_ROOT:-/sys/fs/cgroup}"
  pcg="${PP_PROC_CGROUP:-/proc/self/cgroup}"
  p="$(awk -F: '$1 == "0" { print $3 }' "$pcg")"
  v1="$(awk -F: '$2 ~ /(^|,)memory(,|$)/ { print $3 }' "$pcg")"
  echo "cgroup: v2 path [${p:-none}] v1 memory path [${v1:-none}]"
  for d in "$p" "$v1"; do
    [ -n "$d" ] || continue
    while :; do
      # one name at a time: /bin/sh is dash and does not expand {a,b}
      for s in current max peak events pressure usage_in_bytes max_usage_in_bytes limit_in_bytes failcnt oom_control; do
        for f in "$r$d"/memory."$s" "$r/memory$d"/memory."$s"; do
          [ -r "$f" ] && { echo "--- $f"; cat "$f"; }
        done
      done
      if [ -z "$d" ] || [ "$d" = / ]; then break; fi
      d="${d%/*}"
    done
  done
}
sec host 40 sh -c 'hostname; date -Is; uname -a; grep -E "^(PRETTY_NAME|VERSION_ID)=" /etc/os-release; echo "nproc=$(nproc)"; grep Cpus_allowed_list /proc/self/status; uptime'
sec "pipedpeer --version" 5 pipedpeer --version
sec "pipedpeer status" 80 pipedpeer status
sec memory 400 sh -c 'free -m; echo "--- /proc/pressure/memory"; cat /proc/pressure/memory 2>/dev/null || echo "no PSI"; '"$(declare -f cgmem)"'; cgmem'
sec "kernel OOM lines" 60 sh -c '{ sudo -n journalctl -k --no-pager -n 5000 2>/dev/null || sudo -n dmesg 2>/dev/null || dmesg; } | grep -iE "oom|killed process|out of memory" || echo "none found"'
sec "largest processes (RSS kB)" 30 ps -eo pid,rss,etimes,comm,args --sort=-rss
sec disk 20 df -h "$HOME" /nix /tmp
sec "failed user units" 30 systemctl --user --failed --no-pager
sec "journal pipedpeer.service" 300 journalctl --user -u pipedpeer.service --no-pager -n 300
sec "journal lab-ray" 150 journalctl --user -u lab-ray --no-pager -n 150
sec "journal lab-dask" 150 journalctl --user -u lab-dask-worker -u lab-dask-scheduler --no-pager -n 150
sec "warmup.log" 120 cat "$st/warmup.log"
sec "warmup.log.prev" 60 cat "$st/warmup.log.prev"
for f in raylet.out gcs_server.out monitor.log; do
  sec "ray $f" 200 cat "$LAB_RAY_SESSION/logs/$f"
done
sec "ray worker logs" 10 sh -c 'ls "$LAB_RAY_SESSION/logs" | grep -c "^worker-" || true'
sec "runs on this PC (newest 20)" 20 sh -c 'ls -t ~/pipedpeer-lab/runs'
PC
}

# _bug_redact <dir>: invite codes and secret values in every text file. A name
# is treated as a secret when it contains key, token, secret, password or
# credential and is set with = or :, so monkey=1 is redacted too: a false
# redaction is acceptable, a leak is not. A bare word such as keyboard stays.
_bug_redact() {
	local f
	while IFS= read -r -d '' f; do
		grep -Iq . "$f" 2>/dev/null || continue
		sed -E -i \
			-e 's/(Authorization:[[:space:]]*(Bearer[[:space:]]+)?)[^[:space:],]+/\1<redacted>/Ig' \
			-e 's/(Bearer[[:space:]]+)[^[:space:],]+/\1<redacted>/Ig' \
			-e 's#(://)[^/[:space:]@]+:[^/[:space:]@]+@#\1<redacted>@#g' \
			-e 's/([A-Za-z0-9_.-]*(key|token|secret|password|credential)[A-Za-z0-9_.-]*["'\'']?[[:space:]]*[=:][[:space:]]*["'\'']?)[^[:space:]"'\'',}]+/\1<redacted>/Ig' \
			-e 's/(pipedpeer connect[[:space:]]+)["'\'']?[^[:space:]"'\'']+["'\'']?/\1<redacted>/g' \
			"$f"
	done < <(find "$1" -type f -print0)
}

collect_bug() {
	local to="$1" sum f rel why pc out rc analysis_rc=0 n_pc=0 n_miss=0
	if [[ -d "$to" && -n "$(ls -A "$to" 2>/dev/null)" ]]; then
		echo "Error: target directory '$to' exists and is not empty" >&2
		return 1
	fi
	mkdir -p "$to/results" "$to/pcs"
	sum="$to/SUMMARY.txt"
	{
		echo "Pipedpeer campaign bug report, $(date -Is)"
		echo "lab dir: $LAB"
	} >"$sum"

	# the lab dir, minus what can hold a secret
	if [[ -d "$LAB" ]]; then
		cp -a "$LAB" "$to/results/lab"
		while IFS= read -r -d '' f; do
			rel="${f#"$to/results/lab/"}"
			why=""
			case "${f##*/}" in
				r2.env) why="R2 credentials" ;;
				cluster.json) why="cluster token" ;;
				*.pem | *.key | id_* | pipedpeer-lab | pipedpeer-lab-pc00) why="key file" ;;
			esac
			if [[ -z "$why" ]] && grep -Iq 'PRIVATE KEY' "$f" 2>/dev/null; then why="contains a private key"; fi
			if [[ -z "$why" ]] && grep -IqE '^(RCLONE_CONFIG_[A-Z0-9_]*(SECRET|KEY)[A-Z_]*|AWS_SECRET_ACCESS_KEY)=' "$f" 2>/dev/null; then why="holds credentials"; fi
			if [[ -n "$why" ]]; then
				rm -f "$f"
				echo "left out: results/lab/$rel ($why)" >>"$sum"
			fi
		done < <(find "$to/results/lab" -type f -print0)
	else
		echo "lab dir missing: $LAB" >>"$sum"
	fi

	# the driver
	{
		echo "## git"
		git -C "$REPO" rev-parse HEAD 2>&1 || true
		git -C "$REPO" status --short 2>&1 | head -50 || true
		echo "## pipedpeer"
		local ppb="${PP_BINARY:-$REPO/bin/pipedpeer}"
		[[ -x "$ppb" ]] || ppb="$(command -v pipedpeer || true)"
		if [[ -n "$ppb" ]]; then "$ppb" --version 2>&1 || echo "$ppb --version failed"; else echo "no pipedpeer binary on the driver"; fi
		echo "## tools"
		local t
		for t in bash python3 rsync parallel-ssh pssh uv nix rclone go podman docker; do
			command -v "$t" >/dev/null 2>&1 || { echo "$t: not installed"; continue; }
			echo "$t: $("$t" --version 2>&1 | head -1)"
		done
		echo "## os"
		uname -a
		grep -E '^(PRETTY_NAME|VERSION_ID)=' /etc/os-release 2>/dev/null || true
		echo "## memory"
		free -m 2>&1 || true
	} >"$to/driver.txt" 2>&1

	# every PC in hosts.tsv, excluded ones too
	if [[ -f "$LAB/hosts.tsv" ]]; then
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/lib.sh"
		[[ -f "$LAB/ssh_config" ]] || lab_ssh_config_write >/dev/null 2>&1 || true
		local b64
		b64="$(_bug_pc_script | base64 -w0)"
		while IFS= read -r pc; do
			n_pc=$((n_pc + 1))
			rc=0
			# </dev/null: ssh reads stdin, which is the hosts.tsv loop here
			out="$(lab_ssh "$pc" "echo $b64 | base64 -d | timeout 120 bash" 2>&1 </dev/null)" || rc=$?
			if ((rc != 0)) && [[ "$out" != *"## host"* ]]; then
				n_miss=$((n_miss + 1))
				printf 'no answer from %s (ssh exit %s)\n%s\n' "$pc" "$rc" "$out" >"$to/pcs/$pc.txt"
				echo "not reached: $pc (ssh exit $rc)" >>"$sum"
				lab_log "collect --bug: no answer from $pc (exit $rc)"
			else
				printf '%s\n' "$out" >"$to/pcs/$pc.txt"
				((rc == 0)) || echo "partial: $pc (exit $rc; report kept)" >>"$sum"
			fi
		done < <(awk -F'\t' '!/^#/ && NF >= 2 { print $1 }' "$LAB/hosts.tsv")
	else
		echo "no hosts.tsv: no PC reports" >>"$sum"
	fi
	echo "PCs: $n_pc, reached: $((n_pc - n_miss))" >>"$sum"

	# analyse.py, if it can
	python3 "$REPO/scripts/campaign/analyse.py" --lab "$to/results/lab" >"$to/analysis.txt" 2>&1 || analysis_rc=$?
	if ((analysis_rc == 0)); then
		echo "analyse: ok" >>"$sum"
	else
		echo "analyse: failed (exit $analysis_rc): $(grep -m1 -iE 'error|no ' "$to/analysis.txt" || head -1 "$to/analysis.txt")" >>"$sum"
	fi

	_bug_redact "$to"
	# last guard: no private key anywhere
	while IFS= read -r f; do
		rm -f "$f"
		echo "left out: ${f#"$to/"} (private key found after redaction)" >>"$sum"
	done < <(grep -rIl 'PRIVATE KEY' "$to" 2>/dev/null || true)

	(
		cd "$to" || exit 1
		find . -type f ! -name MANIFEST.sha256 -print0 | LC_ALL=C sort -z | xargs -0 sha256sum >MANIFEST.sha256
	)
	echo "BUG-BUNDLE $to"
	echo "Send this whole folder to the project owner and stop; do not try to fix the PCs or the scripts."
}
