FROM docker.io/nixos/nix:2.31.3

# crun runs the sandboxed jobs; curl serves the health checks.
RUN nix-env -iA nixpkgs.crun nixpkgs.curl
ENV PATH=/usr/local/bin:$PATH

# The cluster token arrives as LAB_TOKEN (scripts/lab-up.sh); written to the
# one config file before setup so the daemon starts inside that cluster.
CMD ["sh", "-c", "if [ -n \"$LAB_TOKEN\" ]; then mkdir -p \"$XDG_DATA_HOME/pipedpeer\" && umask 077 && printf '{\"token\":\"%s\"}\\n' \"$LAB_TOKEN\" > \"$XDG_DATA_HOME/pipedpeer/cluster.json\"; fi; /usr/local/bin/pipedpeer setup -y && echo \"Worker $(cat /etc/hostname) online :4242\" && exec sleep infinity"]
