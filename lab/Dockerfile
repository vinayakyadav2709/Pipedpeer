FROM nixos/nix:2.31.3

CMD ["sh", "-c", "/usr/local/bin/pipedpeer setup -y --port 38081 && echo 'Worker online :38081' && exec sleep infinity"]
