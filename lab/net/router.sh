#!/bin/sh
# Router rules for lab-net scenarios. Runs inside the router container.
#   router.sh drop       forward nothing (no route between the subnets)
#   router.sh udp-only   forward UDP, drop TCP and ICMP (the college router)
#   router.sh nat <net>  a home router for site <net>: masquerade it out of every
#                        other interface, drop unsolicited packets to the router
set -eu
sysctl -qw net.ipv4.ip_forward=1
iptables -F FORWARD
iptables -P FORWARD DROP
case "$1" in
drop) ;;
udp-only) iptables -A FORWARD -p udp -j ACCEPT ;;
nat)
	iptables -A FORWARD -j ACCEPT
	iptables -t nat -A POSTROUTING -s "$2" ! -d "$2" -j MASQUERADE
	iptables -P INPUT DROP
	iptables -A INPUT -i lo -j ACCEPT
	iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
	;;
*) echo "router.sh: unknown mode $1" >&2; exit 2 ;;
esac
