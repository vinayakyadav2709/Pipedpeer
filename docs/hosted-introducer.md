# A hosted introducer (Oracle Cloud Always Free)

Every daemon is an introducer for its own cluster, so a cluster normally needs
no server: a new machine's invite code names members it can reach. A hosted
introducer helps when no member is reachable from the others, for example every
machine behind a different home or campus router. It is a member that
introduces and takes no jobs: it never receives code, closures or results, and
no job traffic passes through it ([ADR-0004](adr/0004-introducer-only-rendezvous.md),
[ADR-0005](adr/0005-no-relay.md)).

Until one is running, Tailscale is the stopgap: put every machine on one
tailnet and they meet as if on one LAN.

## 1. Create the VM

1. Sign up for Oracle Cloud's Always Free tier.
2. Create a compute instance: any Always Free shape (an Ampere A1 arm64 or an
   AMD micro x86_64 both work), Ubuntu image, a public IPv4 address.
3. Note the public IP.

## 2. Open two UDP ports

The introducer needs only UDP; the daemon API port (4242/tcp) stays closed.

| Port | Why |
|---|---|
| 4244/udp | The introducer |
| 4243/udp | Direct links (so members can verify and gossip with it) |

Open them in **both** places:

- **Oracle security list** (Networking → Virtual cloud networks → your VCN →
  Security lists → Default): add two ingress rules, source `0.0.0.0/0`,
  protocol UDP, destination ports `4244` and `4243`.
- **The VM's own firewall.** Oracle's Ubuntu images ship iptables rules that
  drop everything not allowed; open the ports there too:

  ```sh
  sudo iptables -I INPUT -p udp --dport 4244 -j ACCEPT
  sudo iptables -I INPUT -p udp --dport 4243 -j ACCEPT
  sudo netfilter-persistent save
  ```

## 3. Install, connect, start as introducer only

Use the same build as the rest of the cluster (`pipedpeer --version` on any
member):

```sh
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <version>
```

The VM's public IP is not on any of its interfaces (Oracle maps it 1:1 onto a
private address), so tell pipedpeer what it is; it is stored as
`public_address` in `cluster.json` and put into every invite the VM prints:

```sh
pipedpeer setup --public-address <public ip>
```

On any existing member, print an invite and run the line it prints on the VM:

```sh
pipedpeer invite              # on a member
pipedpeer connect pp1-...     # on the VM
```

Then make the VM an introducer that takes no jobs, started at boot:

```sh
pipedpeer start --introducer-only --boot
```

`connect` may say "partly" on the VM if the members are not reachable from it;
that is expected, since the members reach it, not the other way round.

## 4. Tell the members about it

Print a fresh invite on the VM; because of `--public-address` it names the
VM's public IP. Run its `connect` line once on each existing member and use it
for new machines; members then remember the VM's address.

```sh
pipedpeer invite              # on the VM
```

## 5. Check it

On any member:

```sh
pipedpeer status              # the VM appears in the machine list
pipedpeer doctor              # each introducer: answering, or the last error
```

On the VM, `pipedpeer status` lists the members it has introduced. If a member
says the introducer has not answered, the cloud security list or the VM's
iptables is the usual cause.

## Removing it

```sh
pipedpeer uninstall --yes     # on the VM
```

Members keep its address as one of several introducers and stop using it once
it stops answering.
