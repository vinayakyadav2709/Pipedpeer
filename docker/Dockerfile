FROM debian:bookworm-slim

# Nix, single-user: the installer refuses root installs unless the /nix dir and
# the nixbld build group/users already exist (multi-user layout), so create them
# first, then install --no-daemon.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates xz-utils bash git bzip2 gzip passwd procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -m 0755 /nix \
    && groupadd -r nixbld \
    && for i in $(seq 1 10); do \
         useradd -c "Nix build user $i" -d /var/empty -g nixbld -G nixbld -M -N -r -s /sbin/nologin nixbld$i 2>/dev/null || true; \
       done \
    && curl -sSL https://nixos.org/nix/install -o /tmp/nix-install.sh \
    && sh /tmp/nix-install.sh --no-daemon --yes \
    && rm /tmp/nix-install.sh

ENV PATH="/root/.nix-profile/bin:/usr/local/bin:/usr/bin:/bin"
ENV NIX_CONFIG="experimental-features = nix-command flakes"

COPY bin/pipedpeer /usr/local/bin/pipedpeer

WORKDIR /workspace
CMD ["sleep", "infinity"]