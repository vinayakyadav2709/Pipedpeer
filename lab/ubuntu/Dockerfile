# A lab PC as the campaign sees it: stock Ubuntu with systemd, sudo, ufw and a
# normal `lab` account (password "lab": test only). No openssh-server and no
# curl on purpose: lab-pc.sh must install them, as it does on a desktop.
ARG UBUNTU=24.04
FROM ubuntu:${UBUNTU}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends \
      systemd systemd-sysv sudo wget ufw python3 openssh-client iproute2 procps kmod iputils-ping ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -s /bin/bash -G sudo lab \
 && echo 'lab:lab' | chpasswd
STOPSIGNAL SIGRTMIN+3
CMD ["/lib/systemd/systemd"]
