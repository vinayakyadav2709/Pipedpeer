#!/bin/sh
# A home router in a box: NATs one private site onto the lab's "public" net.
#
#   router.sh <private-subnet> <public-gateway> cone|symmetric
#
# cone: plain masquerade. Linux keeps the source port when it is free, so the
# mapping is endpoint-independent and the filter is address-and-port
# dependent (a port-restricted cone, like most home routers).
# symmetric: masquerade fully-random, a fresh external port per destination.
#
# Unsolicited packets to the router itself are dropped in `input`, before
# conntrack confirms them. Otherwise the entry a peer's early probe leaves
# behind clashes with our own later outbound mapping and the kernel moves it
# to another port, which turns a cone into a symmetric NAT by accident.
set -eu
priv="$1" gw="$2" kind="$3"
case "$kind" in
cone) flags="" ;;
symmetric) flags="fully-random" ;;
*) echo "router: unknown NAT kind '$kind' (cone|symmetric)" >&2; exit 2 ;;
esac

sysctl -qw net.ipv4.ip_forward=1
ip route replace default via "$gw"

nft -f - <<EOF
table ip lab {
	chain input {
		type filter hook input priority 0; policy drop;
		ct state established,related accept
		iif lo accept
	}
	chain forward {
		type filter hook forward priority 0; policy accept;
		# No shortcut between sites: other private ranges are only reachable
		# through their own router's public address.
		ip saddr $priv ip daddr { 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 } drop
	}
	chain postrouting {
		type nat hook postrouting priority 100;
		ip saddr $priv ip daddr != $priv masquerade $flags
	}
}
EOF
echo "router: $priv behind a $kind NAT, upstream $gw"
exec sleep infinity
