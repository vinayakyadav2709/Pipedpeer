FROM nixos/nix:2.31.3

# Install crun OCI runtime for sandboxed job execution
RUN nix-env -iA nixpkgs.crun

CMD ["sh", "-c", "/usr/local/bin/pipedpeer setup -y --port 38081 && echo 'Worker online :38081' && exec sleep infinity"]
