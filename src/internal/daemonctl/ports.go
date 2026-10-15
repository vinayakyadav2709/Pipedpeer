package daemonctl

// DefaultPort is the daemon's HTTP API port (TCP). LAN discovery uses the same
// number over UDP (discovery.broadcastPort); direct QUIC links use 4243/udp
// (internet.DefaultPort) and the introducer 4244/udp (clustercfg.DefaultPort).
// The whole block moved here from 38080/38099/38447/38445 in one hard cut.
const DefaultPort = 4242
