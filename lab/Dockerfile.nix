FROM docker.io/nixos/nix:2.31.3

ENV PATH="/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:$PATH"

RUN nix-env -iA nixpkgs.openssh nixpkgs.bash nixpkgs.coreutils nixpkgs.python3 nixpkgs.bubblewrap nixpkgs.util-linux nixpkgs.iproute2

RUN mkdir -p /var/run/sshd /etc/ssh /var/empty /root/.ssh /var/log && chmod 700 /root/.ssh && /nix/var/nix/profiles/default/bin/ssh-keygen -A

# Fix Shadow/Passwd and Group
RUN echo 'root::19761:0:99999:7:::' > /etc/shadow && \
    echo 'root:x:0:0:root:/root:/nix/var/nix/profiles/default/bin/bash' > /etc/passwd && \
    echo 'sshd:x:74:74:Nix build user:/var/empty:/nix/var/nix/profiles/default/bin/sh' >> /etc/passwd && \
    echo 'sshd:x:74:' > /etc/group

# SSH Config
RUN echo 'PermitRootLogin yes' > /etc/ssh/sshd_config && \
    echo 'PermitEmptyPasswords yes' >> /etc/ssh/sshd_config && \
    echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config && \
    echo 'UsePAM no' >> /etc/ssh/sshd_config

# CRITICAL NIX CONFIG FOR REMOTE BUILDS
RUN echo "experimental-features = nix-command flakes" > /etc/nix/nix.conf && \
    echo "sandbox = false" >> /etc/nix/nix.conf && \
    echo "filter-syscalls = false" >> /etc/nix/nix.conf && \
    echo "trusted-users = root" >> /etc/nix/nix.conf && \
    echo "require-trusted-for-uploads = false" >> /etc/nix/nix.conf

EXPOSE 22

# Start Nix Daemon in background, then SSHD
CMD ["sh", "-c", "/nix/var/nix/profiles/default/bin/ssh-keygen -A && (/nix/var/nix/profiles/default/bin/nix-daemon &) && /nix/var/nix/profiles/default/bin/sshd -D -o SetEnv=PATH=/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin"]