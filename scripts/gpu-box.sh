#!/usr/bin/env bash
# Rent a two-GPU machine, prove the second GPU earns its rank, and delete it.
#
# One claim in this project has never been tested because no machine here can
# test it: that giving a node one rank per accelerator makes a multi-GPU box
# train faster. Two T4s for an hour settles it, and an hour is all this keeps
# the machine for.
#
#   scripts/gpu-box.sh up        # create it and print what it cost to start
#   scripts/gpu-box.sh run       # deploy this build and run the benchmark
#   scripts/gpu-box.sh down      # delete it (also runs on any failure in `up`)
#   scripts/gpu-box.sh status    # is one running, and for how long
#
# It never deletes anything it did not create: every action is scoped to the
# instance name below, and `down` refuses a machine missing the label this
# script sets.
set -euo pipefail

NAME="${PP_GPU_BOX:-pipedpeer-gpubench}"
ZONE="${PP_GPU_ZONE:-us-central1-a}"
MACHINE="${PP_GPU_MACHINE:-n1-standard-8}"
ACCEL="${PP_GPU_ACCEL:-type=nvidia-tesla-t4,count=2}"
# A published image with the NVIDIA driver already installed. Building the
# driver on boot is twenty minutes of a rented machine's hour.
IMAGE_FAMILY="${PP_GPU_IMAGE_FAMILY:-common-cu123-ubuntu-2204-py310}"
IMAGE_PROJECT="${PP_GPU_IMAGE_PROJECT:-deeplearning-platform-release}"
LABEL="purpose=pipedpeer-gpubench"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

need_gcloud() {
	command -v gcloud >/dev/null || { echo "gcloud is not installed" >&2; exit 2; }
}

exists() {
	gcloud compute instances describe "$NAME" --zone "$ZONE" >/dev/null 2>&1
}

case "${1:-}" in
up)
	need_gcloud
	if exists; then
		echo "$NAME already exists in $ZONE — use 'run', or 'down' first."
		exit 0
	fi
	echo "creating $NAME ($MACHINE, $ACCEL) in $ZONE ..."
	# Spot, because this is a benchmark: if it is reclaimed mid-run the answer
	# is to run it again, not to pay four times as much for certainty.
	# --no-restart-on-failure so a reclaimed instance stays gone rather than
	# quietly coming back and billing until someone notices.
	gcloud compute instances create "$NAME" \
		--zone "$ZONE" \
		--machine-type "$MACHINE" \
		--accelerator "$ACCEL" \
		--image-family "$IMAGE_FAMILY" \
		--image-project "$IMAGE_PROJECT" \
		--boot-disk-size 100GB \
		--maintenance-policy TERMINATE \
		--provisioning-model SPOT \
		--no-restart-on-failure \
		--metadata install-nvidia-driver=True \
		--labels "$LABEL"
	echo
	echo "Created. It is billing from now. Two things to know:"
	echo "  - 'scripts/gpu-box.sh down' deletes it. Nothing else does."
	echo "  - spot instances can be reclaimed at any time; that is fine here."
	;;

run)
	need_gcloud
	exists || { echo "no $NAME in $ZONE — run 'up' first." >&2; exit 1; }
	echo "waiting for ssh ..."
	for _ in $(seq 1 60); do
		if gcloud compute ssh "$NAME" --zone "$ZONE" --command "true" >/dev/null 2>&1; then
			break
		fi
		sleep 10
	done
	echo "waiting for the GPU driver ..."
	gcloud compute ssh "$NAME" --zone "$ZONE" --command '
		for _ in $(seq 1 60); do
			nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null && break
			sleep 10
		done'

	echo "building and deploying this checkout ..."
	CGO_ENABLED=0 go build -C "$repo_root/src" -o "$repo_root/bin/pipedpeer" .
	gcloud compute scp "$repo_root/bin/pipedpeer" "$NAME:~/pipedpeer" --zone "$ZONE"
	gcloud compute scp "$repo_root/scripts/bench-multigpu.sh" \
		"$repo_root/pipedpeer-demo/04_torch_ddp.py" "$NAME:~/" --zone "$ZONE"

	gcloud compute ssh "$NAME" --zone "$ZONE" --command '
		set -e
		chmod +x ~/pipedpeer ~/bench-multigpu.sh
		mkdir -p ~/bin && mv ~/pipedpeer ~/bin/pipedpeer
		export PATH=$HOME/bin:$PATH
		nvidia-smi --query-gpu=index,name --format=csv
		~/bin/pipedpeer setup -y
		sleep 5
		PIPEDPEER=$HOME/bin/pipedpeer BENCH_SCRIPT=$HOME/04_torch_ddp.py \
			bash ~/bench-multigpu.sh'
	echo
	echo "Done. Remember: scripts/gpu-box.sh down"
	;;

down)
	need_gcloud
	if ! exists; then
		echo "no $NAME in $ZONE — nothing to delete."
		exit 0
	fi
	# Refuse anything this script did not create. A name collision with a real
	# machine is the one mistake here that would be expensive.
	labels="$(gcloud compute instances describe "$NAME" --zone "$ZONE" \
		--format 'value(labels)' 2>/dev/null || true)"
	case "$labels" in
		*pipedpeer-gpubench*) ;;
		*)
			echo "REFUSING: $NAME in $ZONE is not labelled $LABEL, so this script" >&2
			echo "did not create it. Delete it by hand if that is really what you want." >&2
			exit 1
			;;
	esac
	gcloud compute instances delete "$NAME" --zone "$ZONE" --quiet
	echo "deleted $NAME"
	;;

status)
	need_gcloud
	if ! exists; then
		echo "no $NAME in $ZONE — nothing is billing."
		exit 0
	fi
	gcloud compute instances describe "$NAME" --zone "$ZONE" \
		--format 'table(name, status, machineType.basename(), creationTimestamp)'
	echo
	echo "Still billing. 'scripts/gpu-box.sh down' when finished."
	;;

*)
	sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
