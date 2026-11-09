#!/usr/bin/env bash
# install.sh <version>: install a pinned pipedpeer build on every lab PC,
# connect them all to pc00's cluster, and record install.csv (brief S6).
#
# Two modes:
#   default        - rsync scripts/install-pipedpeer.sh to each PC and run it
#                    `--version <version> -y` there (the GitHub installer).
#   PP_BINARY=path - an unreleased build (the Docker lab): rsync the binary to
#                    ~/.local/bin/pipedpeer and run `pipedpeer setup -y` on
#                    each PC. The expected version is what `$PP_BINARY
#                    --version` prints.
#
# INSTALL_CMD overrides the per-PC install command; `{version}` in it is
# replaced by <version> (the tests stub the installer out).
#
# Afterwards every PC's `pipedpeer status` first line must show
# `version=<wanted>` (else FAILED, reason `version <$got> != <$want>`).
# `pipedpeer connect` on pc01.. is retried by lab_fanout's own three
# attempts; a PC that still exits with "partly connected" (2) counts ok when
# pc00's status shows it healthy. pc00 never connects to itself: it is the
# introducer. Two PCs reporting the same node id stop the run
# (DUPLICATE-NODE-ID <id> <pcs>, exit 1).
#
# pc00 also gets an ssh key (~/.ssh/pipedpeer-lab-pc00) whose pub is appended
# to every PC's authorized_keys, marked ` # pipedpeer-lab` exactly like
# lab-pc.sh's key lines, so lab-pc.sh undo removes it too (the ssh baseline
# and torchrun need pc00 -> pcX without a password). The key is created with
# an empty comment so the marked line is `<type> <base64> # pipedpeer-lab`.
#
# Output: `INSTALL ok=<a> failed=<b>`; exit 0 only when b=0, and then
# logs/install/FAILED lists the failing ids, one per line (reasons go to
# stdout and campaign.log). install.csv is always written
# (id,pipedpeer_version,connect_exit,node_id; pc00's connect_exit is `self`).

set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'EOF'
usage: install.sh <version>

Install a pinned pipedpeer build on every PC of $LAB/hosts.tsv, connect
them to pc00, and write $LAB/install.csv. Prints INSTALL ok=<a> failed=<b>.

Environment:
  PP_BINARY    install this local pipedpeer binary (unreleased build) with
               `pipedpeer setup -y` on each PC, after rsyncing it to
               ~/.local/bin/pipedpeer; the expected version is what
               "$PP_BINARY --version" prints
  INSTALL_CMD  override the per-PC install command; {version} in it is
               replaced by <version> (tests stub the installer out)
EOF
}

# _status_field <status> <key>: value of `<key>=<token>` on the daemon line
# (`daemon running (pid=... node-id=... port=... version=...)`).
_status_field() {
	local key="$2"
	printf '%s\n' "$1" | head -1 | sed -n "s/.*${key}=\\([^ )]*\\).*/\\1/p"
}

# _status_pc_healthy <status> <hostname>: $host's table row says healthy
# (STATE is column 2; the table ends at the first blank line).
_status_pc_healthy() {
	printf '%s\n' "$1" | awk -v h="$2" '
		/^$/ { in_table = 0 }
		/^NAME[[:space:]]+STATE/ { in_table = 1; next }
		in_table && $1 == h && $2 == "healthy" { found = 1 }
		END { exit !found }
	'
}

main() {
	local version="${1:-}"
	case "$version" in
		"" | -h | --help) usage; exit 0 ;;
	esac
	if [[ "$#" -ne 1 ]]; then
		usage >&2
		exit 2
	fi
	if [[ -n "${PP_BINARY:-}" && ! -x "$PP_BINARY" ]]; then
		echo "PP_BINARY=$PP_BINARY is not executable" >&2
		exit 2
	fi
	if [[ -z "${PP_BINARY:-}" && ! -f "$REPO/scripts/install-pipedpeer.sh" ]]; then
		echo "no $REPO/scripts/install-pipedpeer.sh" >&2
		exit 2
	fi

	[[ -f "$LAB/hosts.tsv" ]] || {
		lab_log "install: no hosts.tsv"
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		exit 1
	}
	local count
	count="$(awk -F'\t' '$1 !~ /^#/ && NF >= 2 { c++ } END { print c + 0 }' "$LAB/hosts.tsv")"
	[[ "$count" -gt 0 ]] || {
		echo "no PC rows in $LAB/hosts.tsv" >&2
		exit 1
	}
	local hostfile restfile stepdir="$LAB/logs/install"
	mkdir -p "$stepdir"
	rm -f "$stepdir/FAILED"
	hostfile="$(lab_hosts "$count")"
	local -a ids=()
	mapfile -t ids <"$hostfile"

	# the version the status lines must show
	local want_version="$version"
	if [[ -n "${PP_BINARY:-}" ]]; then
		want_version="$("$PP_BINARY" --version 2>/dev/null | head -1 || true)"
		if [[ "$want_version" == *version=* ]]; then
			want_version="${want_version#*version=}"
			want_version="${want_version%%[[:space:]]*}"
		fi
		# `pipedpeer --version` prints "pipedpeer version <v>"
		if [[ "$want_version" == *" version "* ]]; then
			want_version="${want_version##* version }"
			want_version="${want_version%%[[:space:]]*}"
		fi
		[[ -n "$want_version" ]] || {
			lab_log "install: no version from '$PP_BINARY --version'"
			echo "cannot read a version from $PP_BINARY --version" >&2
			exit 2
		}
	fi

	# ship the installer (or the binary) and install on every PC
	local pc out rc sv sn
	rc=0
	lab_fanout install-dirs "$hostfile" 'mkdir -p ~/pipedpeer-lab ~/.local/bin' || rc=$?
	if ((rc != 0)); then
		lab_log "install: could not create ~/pipedpeer-lab on every PC"
	fi
	for pc in "${ids[@]}"; do
		if [[ -n "${PP_BINARY:-}" ]]; then
			lab_rsync "$PP_BINARY" "$pc:.local/bin/pipedpeer"
		else
			lab_rsync "$REPO/scripts/install-pipedpeer.sh" "$pc:pipedpeer-lab/install-pipedpeer.sh"
		fi
	done
	local instcmd="bash ~/pipedpeer-lab/install-pipedpeer.sh --version $version -y"
	if [[ -n "${PP_BINARY:-}" ]]; then
		# /usr/local/bin like the released installer: ~/.local/bin is not on
		# the PATH of a non-interactive ssh command on Ubuntu.
		instcmd="sudo install -m 0755 ~/.local/bin/pipedpeer /usr/local/bin/pipedpeer && pipedpeer setup -y"
	elif [[ -n "${INSTALL_CMD:-}" ]]; then
		instcmd="${INSTALL_CMD//\{version\}/$version}"
	fi
	rc=0
	lab_fanout install "$hostfile" "$instcmd" || rc=$?
	if ((rc != 0)); then
		lab_log "install: every PC failed the install step"
		echo "every PC failed the install step" >&2
	fi
	# Record who installed Nix, so the last session's undo removes the Nix this
	# install brought (and only that). lab-pc.sh setup left a copy of itself in
	# its state dir (/tmp/lab-pc.sh may be gone after a reboot). A PC where this
	# fails is warned about; the install goes on, and undo then leaves its Nix.
	local lab_pc_self="${LAB_PC_SELF:-/var/lib/pipedpeer-lab/lab-pc.sh}" mark_failed=""
	rc=0
	lab_fanout install-mark "$hostfile" "sudo bash $lab_pc_self mark" || rc=$?
	if [[ -f "$LAB/logs/install-mark/FAILED" ]]; then
		mark_failed="$(tr '\n' ' ' <"$LAB/logs/install-mark/FAILED")"
	elif ((rc != 0)); then
		mark_failed="every PC"
	fi
	if [[ -n "$mark_failed" ]]; then
		lab_log "install: WARNING: lab-pc.sh mark failed on ${mark_failed}; undo will leave Nix there if the install brought it"
		echo "WARNING: lab-pc.sh mark failed on ${mark_failed}" >&2
	fi
	# A PC the install fan-out could not get through stays failed even when a
	# later status query answers (the version it reports is not the one this
	# run installed). These PCs are skipped in the status scan below.
	local -A fail_v=()
	if [[ -f "$stepdir/FAILED" ]]; then
		while IFS= read -r pc; do
			[[ -n "$pc" ]] || continue
			fail_v[$pc]="install step failed"
		done <"$stepdir/FAILED"
		lab_log "install: the install step failed for ids in $stepdir/FAILED"
	fi

	# per PC: version and node id from `pipedpeer status`; wrong version is
	# a failure (recorded, reported at the end), missing status too.
	local -A status_v=() status_n=() status_cx=()
	local out sv sn rc
	for pc in "${ids[@]}"; do
		[[ -n "${fail_v[$pc]:-}" ]] && continue
		out="$(lab_ssh "$pc" 'pipedpeer status')" && rc=0 || rc=$?
		if ((rc != 0)); then
			fail_v[$pc]="status: exit $rc"
			continue
		fi
		sv="$(_status_field "$out" version)"
		sn="$(_status_field "$out" node-id)"
		if [[ -z "$sv" ]]; then
			fail_v[$pc]="no version= in status"
			continue
		fi
		if [[ -z "$sn" ]]; then
			fail_v[$pc]="no node-id= in status"
			continue
		fi
		status_v[$pc]="$sv"
		status_n[$pc]="$sn"
		if [[ "$sv" != "$want_version" ]]; then
			fail_v[$pc]="version $sv != $want_version"
		fi
	done

	# node ids must be unique: two PCs claiming one node id is a hard stop.
	local -A seen=() dup_pcs=()
	local node
	for pc in "${ids[@]}"; do
		node="${status_n[$pc]:-}"
		[[ -n "$node" ]] || continue
		if [[ -n "${seen[$node]:-}" ]]; then
			dup_pcs[$node]="${seen[$node]},$pc"
		else
			seen[$node]="$pc"
		fi
	done
	if ((${#dup_pcs[@]} > 0)); then
		local dupid
		for node in "${!dup_pcs[@]}"; do
			dupid="${dup_pcs[$node]}"
			lab_log "install: DUPLICATE-NODE-ID $node $dupid"
			printf 'DUPLICATE-NODE-ID %s %s\n' "$node" "$dupid"
		done
		exit 1
	fi

	# connect everyone but pc00 to pc00's cluster. The connect exit code is
	# echoed to the stderr file (the only file pssh keeps), and the command
	# itself exits with it, so lab_fanout retries exit 1/2/... three times.
	local code="" inv
	rc=0
	inv="$(lab_ssh pc00 'pipedpeer invite')" || rc=$?
	if ((rc != 0)); then
		fail_v[pc00]="invite: exit $rc"
	elif [[ -z "$inv" ]]; then
		fail_v[pc00]="invite: no output"
	else
		code="$(printf '%s\n' "$inv" | sed -n 's/^pipedpeer connect //p' | head -1)"
		if [[ -z "$code" ]]; then
			fail_v[pc00]="invite: no 'pipedpeer connect' line"
		else
			lab_log "install: got a connect code from pc00"
			restfile="$LAB/hosts-rest.txt"
			awk -v drop="pc00" '$1 != drop' "$hostfile" >"$restfile"
			local conncmd
			printf -v conncmd \
				"pipedpeer connect %q >/dev/null; rc2=\$?; echo \"CONNECT_EXIT=\$rc2\" >&2; exit \"\$rc2\"" \
				"$code"
			rc=0
			lab_fanout connect "$restfile" "$conncmd" || rc=$?
			if ((rc != 0)); then
				lab_log "install: connect fan-out ended with rc=$rc (all hosts failed)"
			fi
			local ce leader host
			for pc in "${ids[@]}"; do
				[[ "$pc" == pc00 ]] && continue
				ce="$(sed -n 's/^CONNECT_EXIT=//p' "$LAB/logs/connect/err/$pc" 2>/dev/null | tail -1 || true)"
				if [[ -z "$ce" ]]; then
					fail_v[$pc]="connect: no CONNECT_EXIT (logs/connect/err/$pc)"
					continue
				fi
				case "$ce" in
					0) status_cx[$pc]="0" ;;
					*[^0-9]*) status_cx[$pc]="$ce" ;;
					*)
						status_cx[$pc]="$ce"
						# "partly connected" (2) or unknown failure counts ok
						# only when pc00's status now shows the PC healthy
						if leader="$(lab_ssh pc00 'pipedpeer status')" \
							&& host="$(awk -F'\t' -v p="$pc" '$1 == p { print $4; exit }' "$LAB/hosts.tsv")" \
							&& _status_pc_healthy "$leader" "$host"; then
							lab_log "install: $pc connected partly (exit $ce) but $host is healthy on pc00"
						else
							fail_v[$pc]="connect exit $ce"
						fi
						;;
				esac
			done
		fi
	fi
	for pc in "${ids[@]}"; do
		[[ -n "${status_cx[$pc]:-}" ]] && continue
		[[ "$pc" == pc00 ]] && continue
		[[ -n "${fail_v[$pc]:-}" ]] && continue
		fail_v[$pc]="no connect result"
	done

	# pc00's ssh key on every PC, marked like lab-pc.sh's key lines.
	local pubkey keycmd
	if pubkey="$(lab_ssh pc00 'test -f ~/.ssh/pipedpeer-lab-pc00.pub && cat ~/.ssh/pipedpeer-lab-pc00.pub')" \
		&& [[ -n "$pubkey" ]]; then
		:
	else
		pubkey="$(lab_ssh pc00 'mkdir -p ~/.ssh && ssh-keygen -q -t ed25519 -N "" -C "" -f ~/.ssh/pipedpeer-lab-pc00 && cat ~/.ssh/pipedpeer-lab-pc00.pub')" || pubkey=""
	fi
	if [[ -n "$pubkey" ]]; then
		printf -v keycmd \
			"mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && { grep -qxF %q ~/.ssh/authorized_keys || printf '%%s\\n' %q >> ~/.ssh/authorized_keys; }" \
			"$pubkey # pipedpeer-lab" "$pubkey # pipedpeer-lab"
		rc=0
		if ! lab_fanout pc00key "$hostfile" "$keycmd"; then
			lab_log "install: pc00 ssh key could not be added on every PC; the ssh baseline needs it"
			echo "WARNING: pc00 ssh key not installed on every PC; ssh baseline will fail" >&2
		fi
	else
		lab_log "install: pc00 ssh key could not be created; the ssh baseline needs it"
		echo "WARNING: pc00 ssh key could not be created; ssh baseline will fail" >&2
	fi

	# wait until each PC's warmup.log last line is done (WU1).
	# A failed build, a timeout, or a log unchanged for stall_s (default 15 min)
	# goes in fail_v[pc] with the last log line; the other PCs go on.
	# Writes $LAB/warmup-times.tsv (pc<TAB>seconds<TAB>state).
	local -a warmup_pending=()
	local -A warmup_sec=()
	local -A warmup_state=()
	local -A warmup_last_mtime=()
	local -A warmup_miss=()
	local miss_max="${CAMPAIGN_WARMUP_MISS_MAX:-3}"
	local -A warmup_last_change=()
	local -A warmup_last_line=()
	local poll_s="${CAMPAIGN_WARMUP_POLL_S:-${LAB_POLL_S:-15}}"
	local timeout_s="${CAMPAIGN_WARMUP_TIMEOUT_S:-3600}"
	local stall_s="${CAMPAIGN_WARMUP_STALL_S:-900}"
	local t_wait_start t_now
	t_wait_start="$(date +%s)"
	t_now="$t_wait_start"

	for pc in "${ids[@]}"; do
		if [[ -n "${fail_v[$pc]:-}" ]]; then
			warmup_state[$pc]="failed"
			warmup_sec[$pc]="-"
		else
			warmup_pending+=("$pc")
			warmup_last_mtime[$pc]=""
			warmup_last_change[$pc]="$t_wait_start"
			warmup_last_line[$pc]=""
		fi
	done

	local probe_cmd
	# shellcheck disable=SC2016
	probe_cmd='log="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/warmup.log"
if [[ ! -f "$log" ]]; then
	echo "MISSING"
	exit 0
fi
mtime="$(stat -c %Y "$log" 2>/dev/null || date -r "$log" +%s 2>/dev/null || echo 0)"
head_line="$(head -n 1 "$log" 2>/dev/null | tr -d "\r\n")"
tail_line="$(tail -n 1 "$log" 2>/dev/null | tr -d "\r\n")"
printf "OK\t%s\t%s\t%s\n" "$mtime" "$head_line" "$tail_line"
'
	local pending_file="$LAB/warmup-pending.txt"
	local out p_mtime p_head p_tail start_tok start_ep sec unch elap
	while ((${#warmup_pending[@]} > 0)); do
		printf '%s\n' "${warmup_pending[@]}" >"$pending_file"
		lab_fanout warmup-poll "$pending_file" "$probe_cmd" >/dev/null 2>&1 || true

		local -a next_pending=()
		for pc in "${warmup_pending[@]}"; do
			out="$(cat "$LAB/logs/warmup-poll/$pc" 2>/dev/null || true)"
			if [[ -z "$out" || "$out" == MISSING* ]]; then
				# one failed probe is not a verdict: the PC is dropped after
				# $miss_max consecutive empty or MISSING probes
				warmup_miss[$pc]=$((${warmup_miss[$pc]:-0} + 1))
				if ((warmup_miss[$pc] < miss_max)); then
					lab_log "install: warmup $pc: probe ${warmup_miss[$pc]}/$miss_max empty or log missing; retrying"
					next_pending+=("$pc")
					continue
				fi
				warmup_state[$pc]="missing"
				warmup_sec[$pc]="-"
				fail_v[$pc]="warmup: log missing"
				lab_log "install: warmup $pc: log missing"
				continue
			fi

			if [[ "$out" == OK$'\t'* ]]; then
				warmup_miss[$pc]=0
				IFS=$'\t' read -r _ p_mtime p_head p_tail <<<"$out"
				if [[ "$p_mtime" != "${warmup_last_mtime[$pc]:-}" ]]; then
					warmup_last_mtime[$pc]="$p_mtime"
					warmup_last_change[$pc]="$t_now"
					warmup_last_line[$pc]="$p_tail"
				fi

				if [[ "$p_tail" == "done" ]]; then
					start_tok="${p_head%% *}"
					start_ep="$(date -d "$start_tok" +%s 2>/dev/null || echo 0)"
					sec=0
					if ((p_mtime > 0 && start_ep > 0 && p_mtime >= start_ep)); then
						sec=$((p_mtime - start_ep))
					fi
					warmup_state[$pc]="done"
					warmup_sec[$pc]="$sec"
					warmup_last_line[$pc]="done"
					continue
				fi

				if [[ "$p_tail" == failed* ]]; then
					start_tok="${p_head%% *}"
					start_ep="$(date -d "$start_tok" +%s 2>/dev/null || echo 0)"
					sec="-"
					if ((p_mtime > 0 && start_ep > 0 && p_mtime >= start_ep)); then
						sec=$((p_mtime - start_ep))
					fi
					warmup_state[$pc]="failed"
					warmup_sec[$pc]="$sec"
					warmup_last_line[$pc]="$p_tail"
					fail_v[$pc]="warmup: $p_tail"
					lab_log "install: warmup $pc: $p_tail"
					continue
				fi

				unch=$((t_now - warmup_last_change[$pc]))
				if ((unch >= stall_s)); then
					warmup_state[$pc]="stalled"
					warmup_sec[$pc]="$unch"
					fail_v[$pc]="warmup: stalled (last: ${p_tail:-none})"
					lab_log "install: warmup $pc: stalled after ${unch}s (last: ${p_tail:-none})"
					continue
				fi

				next_pending+=("$pc")
			else
				warmup_state[$pc]="failed"
				warmup_sec[$pc]="-"
				fail_v[$pc]="warmup: unreadable probe output"
				lab_log "install: warmup $pc: unreadable probe output"
			fi
		done
		warmup_pending=("${next_pending[@]}")
		((${#warmup_pending[@]} == 0)) && break

		t_now="$(date +%s)"
		elap=$((t_now - t_wait_start))
		if ((elap >= timeout_s)); then
			for pc in "${warmup_pending[@]}"; do
				warmup_state[$pc]="timeout"
				warmup_sec[$pc]="$elap"
				fail_v[$pc]="warmup: timed out after ${timeout_s}s (last: ${warmup_last_line[$pc]:-none})"
				lab_log "install: warmup $pc: timed out after ${timeout_s}s (last: ${warmup_last_line[$pc]:-none})"
			done
			warmup_pending=()
			break
		fi

		sleep "$poll_s"
		t_now="$(date +%s)"
	done
	rm -f "$pending_file"

	# warmup-times.tsv
	local wt="$LAB/warmup-times.tsv"
	{
		printf 'pc\tseconds\tstate\n'
		for pc in "${ids[@]}"; do
			printf '%s\t%s\t%s\n' "$pc" "${warmup_sec[$pc]:--}" "${warmup_state[$pc]:-failed}"
		done
	} >"$wt"
	lab_log "install: wrote $wt"

	# install.csv
	local csv="$LAB/install.csv"
	{
		printf 'id,pipedpeer_version,connect_exit,node_id\n'
		for pc in "${ids[@]}"; do
			printf '%s,%s,%s,%s\n' "$pc" "${status_v[$pc]:--}" "${status_cx[$pc]:-self}" "${status_n[$pc]:--}"
		done
	} >"$csv"
	lab_log "install: wrote $csv"

	# result: FAILED id per line in the file, reasons on stdout and in the log
	local ok n_failed reason
	n_failed=0
	ok=0
	for pc in "${ids[@]}"; do
		reason="${fail_v[$pc]:-}"
		if [[ -n "$reason" ]]; then
			printf '%s\n' "$pc" >>"$stepdir/FAILED"
			n_failed=$((n_failed + 1))
			printf 'FAILED %s: %s\n' "$pc" "$reason"
			lab_log "install: FAILED $pc: $reason"
		else
			ok=$((ok + 1))
		fi
	done
	printf 'INSTALL ok=%d failed=%d\n' "$ok" "$n_failed"
	lab_log "install: ok=$ok failed=$n_failed"
	((n_failed == 0)) || exit 1
}

main "$@"
