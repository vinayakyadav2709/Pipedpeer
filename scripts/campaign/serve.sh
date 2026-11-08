#!/usr/bin/env bash
# serve.sh <driver_ip> <lab_cidr>: publish what a lab PC needs before it can
# join, over plain HTTP from the driver (brief S4, docs/benchmarks/lab-campaign.md):
#
#   lab-pc.sh    lab-pc.sh with this driver's ip and the lab subnet filled in
#   driver.pub   the driver's public key, which lab-pc.sh installs
#   SETUP-LINE.txt   the one line a person pastes on each PC
#
# Then it serves the directory until stopped. Port 8000 is the number the
# setup line and the campaign docs use; SERVE_PORT exists so a test can take
# a free port instead of fighting whatever is on this one.
#
# The key is generated if absent, in the invoking user's ~/.ssh, because
# every later ssh in the campaign uses it (lib.sh's ssh_config names it) and
# a campaign that starts with a hand-made key is one more thing to get wrong.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$here/lib.sh"

usage() {
	cat <<'EOF'
usage: serve.sh <driver_ip> <lab_cidr>

Stages $LAB/serve/ (lab-pc.sh with the placeholders filled, driver.pub,
SETUP-LINE.txt), prints the sha256 and the setup line, then serves it on
http://<driver_ip>:<SERVE_PORT> until stopped.

  driver_ip   the address the lab PCs reach this machine on
  lab_cidr    the lab subnet, e.g. 192.168.1.0/24

Environment:
  SERVE_PORT  port to serve on (default 8000)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi
if [[ $# -ne 2 ]]; then
	usage >&2
	exit 2
fi

driver_ip="$1"
lab_cidr="$2"
port="${SERVE_PORT:-8000}"
serve_dir="$LAB/serve"
src="$REPO/scripts/campaign/lab-pc.sh"

case "$driver_ip" in
	*[0-9]) ;;
	*)
		echo "serve.sh: '$driver_ip' does not look like an address" >&2
		exit 2
		;;
esac

if [[ ! -f "$src" ]]; then
	echo "serve.sh: no $src to publish" >&2
	lab_log "serve: missing $src"
	exit 1
fi

mkdir -p "$serve_dir"

# The two placeholders are the only things a PC cannot know: which address to
# call home, and which subnet to let through its firewall.
sed -e "s|__DRIVER_IP__|$driver_ip|g" -e "s|__LAB_CIDR__|$lab_cidr|g" \
	"$src" >"$serve_dir/lab-pc.sh"
chmod +x "$serve_dir/lab-pc.sh"

key="$HOME/.ssh/pipedpeer-lab"
if [[ ! -f "$key.pub" ]]; then
	mkdir -p "$HOME/.ssh"
	if [[ -f "$key" ]]; then
		ssh-keygen -y -f "$key" >"$key.pub"
		lab_log "serve: rebuilt $key.pub from the private key"
	else
		ssh-keygen -t ed25519 -N '' -f "$key" >/dev/null
		lab_log "serve: generated the driver key $key"
	fi
	chmod 600 "$key"
fi
cp "$key.pub" "$serve_dir/driver.pub"

if [[ ! -s "$LAB/campaign_id" ]]; then
	printf 'lab-%s\n' "$(date +%F)" >"$LAB/campaign_id"
	lab_log "serve: campaign_id=$(cat "$LAB/campaign_id")"
fi

sha256="$(sha256sum "$serve_dir/lab-pc.sh" | cut -d' ' -f1)"
setup_line="wget -qO /tmp/lab-pc.sh http://$driver_ip:$port/lab-pc.sh && echo \"$sha256  /tmp/lab-pc.sh\" | sha256sum -c && sudo bash /tmp/lab-pc.sh setup"
printf '%s\n' "$setup_line" >"$serve_dir/SETUP-LINE.txt"

cat <<EOF
serving $serve_dir on http://$driver_ip:$port
lab-pc.sh sha256: $sha256
driver key:       $key.pub -> driver.pub
campaign_id:      $(cat "$LAB/campaign_id")
setup line (run on each PC, after copying driver.pub to it):
$setup_line
EOF
lab_log "serve: serving $serve_dir on http://$driver_ip:$port (lab-pc.sh sha256 $sha256)"

exec python3 -m http.server "$port" --bind "$driver_ip" --directory "$serve_dir"
