# Image for the network scenarios (scripts/lab-net.sh): daemons only, no Nix,
# which is all membership and path tests need. The binary is bind-mounted.
FROM docker.io/library/debian:stable-slim
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends iproute2 iptables curl procps jq ca-certificates >/dev/null \
 && rm -rf /var/lib/apt/lists/*
COPY ppnodes router.sh /usr/local/bin/
CMD ["sleep", "infinity"]
