# Peer cache, warm-up and phone setup: input cases

The inputs the peer cache, the setup warm-up and the guided phone setup parse or branch on. Every case that exists today comes first, with a real sample. Cases expected from other nix versions, phones and Android releases follow as synthetic rows. Each row gives the simplest handling that holds and what it costs. Tests build their case tables from this file.

## 1. `nix path-info --json --sigs <path>` (peer side, describing a local path)

The command is run without `--json-format`. The private store pins nix 2.31.3, which rejects that flag. Newer nix prints a deprecation warning on stderr, so only stdout is parsed.

| # | Source | Shape | Handling | Cost |
|---|---|---|---|---|
| 1.1 | Real: Determinate Nix 3.24.1 (nix 2.35.3), no flag | An object keyed by the full path: `{"/nix/store/byi2…-bash-5.3p15":{"ca":null,"deriver":"/nix/store/yxs8…-bash-5.3p15.drv","narHash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q=","narSize":1856848,"references":["/nix/store/0d8g…-glibc-2.42-67","/nix/store/byi2…-bash-5.3p15"],"signatures":["cache.nixos.org-1:L1O9…Cw=="],"storeDir":"/nix/store","version":1}}` | Take the key as the path. Convert the SRI `narHash` to `sha256:<nix32>`. Keep full paths for the references and deriver. | none |
| 1.2 | Real: same nix, a fixed-output source path | `"ca":"fixed:r:sha256:04bw…yak3"`, `"deriver":null`, `"references":[]`, `"signatures":[]` | Pass `CA` through unchanged. `null` becomes `""`. | none |
| 1.3 | Real: `--json-format 2` (a future default) | `{"info":{"byi2…-bash-5.3p15":{…,"deriver":"yxs8…drv","references":["0d8g…-glibc-2.42-67",…]}},"storeDir":"/nix/store","version":2}`. Keys, deriver and references are basenames. | Detect the top-level `info` and prefix `storeDir + "/"` to the basenames. | one branch |
| 1.4 | Real: format 2, CA path | `"ca":{"hash":"sha256-YyoP…fBE=","method":"nar"}` | method `nar` → `fixed:r:<algo>:<nix32>`, `flat` → `fixed:<algo>:<nix32>`, `text` → `text:<algo>:<nix32>`. Any other method: warn and drop the path (it becomes a miss). | one switch |
| 1.5 | Synthetic: nix ≤ 2.18 | An array: `[{"path":"/nix/store/…","narHash":"sha256:<nix32>",…}]`. The hash may already be nix32. | When the first byte is `[`, read `path` from each element. Accept both hash encodings. | one branch |
| 1.6 | Synthetic: the path is invalid or not registered | Non-zero exit, `error: path '…' is not valid` | Count it as a miss (404). Log at debug level, since a miss is normal. | none |
| 1.7 | Synthetic: `narSize` 0 or missing, or an unparsable hash | — | Drop the path with a warning (nix rejects NarSize 0). | none |
| 1.8 | Synthetic: an input-addressed path with no signatures | `"signatures":[]`, `"ca":null` | The peer signs it lazily (`nixsign.EnsureSigned`) and re-reads it. If signing fails, warn and serve it unsigned. The asking side then rejects it, so it counts as a miss. | one sign per path, once |

## 2. narinfo (from a peer to the local daemon, then on to nix)

| # | Source | Text | Handling |
|---|---|---|---|
| 2.1 | Real: cache.nixos.org, bash (the sample, not the daemon's output) | `StorePath`, `URL: nar/0i3p…p634.nar.zst`, `Compression: zstd`, `FileHash`, `FileSize`, `NarHash: sha256:0i3p…p634`, `NarSize: 1856848`, `References: 0d8g…-glibc-2.42-67 byi2…-bash-5.3p15` (basenames), `Deriver: yxs8…drv` (basename), `Sig: cache.nixos.org-1:L1O9…` | The sample above, kept for its field set. |
| 2.1a | Rendered by the daemon from a peer's narinfo | as 2.1 | It sets `URL: nar/<storehash>.nar.zst` and `Compression: zstd`, and omits `FileHash`/`FileSize` because the NAR is compressed while streaming. |
| 2.2 | Rendered by a peer, CA path | as 2.1 with `CA: fixed:r:sha256:…` and no `Sig` | Accepted only when the store path recomputed from the CA hash, name and references equals `StorePath`. A CA that is unsupported or gives another path is treated as unsigned: it needs a trusted signature (2.3), else it is rejected. |
| 2.3 | Rendered by a peer, signed by a cluster key or by a key in nix's `trusted-public-keys` (cache.nixos.org's included) | `Sig: <node>-1:<b64>` | Accepted when the signature verifies against a key in `nixsign.TrustedKeysFile()` or nix's `trusted-public-keys`. The fingerprint is `1;<StorePath>;<NarHash>;<NarSize>;<full refs comma-joined>`. |
| 2.4 | Synthetic: signed by a key nobody trusts | | Reject it, warn with the peer and key name, and try the next peer. |
| 2.5 | Synthetic: the `StorePath` hash differs from the hash asked for | | Reject and warn. This guards against a confused or hostile peer. |
| 2.6 | Synthetic: a peer from an older build | 404 on `/v1/store/narinfo/…` | Count it as a miss, with no warning: the route is additive (ADR-0006). |
| 2.7 | Synthetic: the peer is unreachable or times out | | Warn at most once per peer per 30 s (the same limit applies to rejected narinfos). Count it as a miss. |

## 3. Binary-cache requests nix makes to `http://127.0.0.1:<port>/nix-cache`

| # | Request | Answer |
|---|---|---|
| 3.1 | `GET /nix-cache/nix-cache-info` | 200 `StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 60\n`. cache.nixos.org answers Priority 40 (real), so nix asks it first. |
| 3.2 | `GET /nix-cache/<32 nix32 chars>.narinfo` | 200 with the first acceptable peer answer, or 404. |
| 3.3 | `GET /nix-cache/nar/<hash>.nar.zst` | The peer's zstd stream, passed through. Retried on the other peers that answered the narinfo with the same `NarHash` and are not recorded as failed; a peer with a different `NarHash`, or one that never answered the narinfo, is not tried. Else 404; 503 when a source answered 503. A source whose stream breaks mid-body is dropped. |
| 3.4 | `GET /nix-cache/<bad>.narinfo` (wrong length, or a char outside `0123456789abcdfghijklmnpqrsvwxyz`) | 400. No fan-out. |
| 3.5 | `HEAD` of any of these (nix probes with HEAD for some stores) | As GET without the body. |
| 3.6 | `PUT`, `POST`, `DELETE` or any write from loopback | 405 with `Allow: GET, HEAD`. The cache is read-only. A `Host` header that does not name loopback falls through to the token check (401), so a rebinding web page cannot read the cache. |
| 3.7 | From a non-loopback address without the cluster token | 401 (the normal token middleware). |
| 3.8 | `/nix-cache/log/…`, `/nix-cache/*.ls`, `realisations/…`, any other unknown name | 404 |
| 3.9 | Peers of another nix system (an `aarch64-linux` phone asked by an `x86_64-linux` PC) | Not asked. Store paths differ by system. Arch-independent sources are then not shared across arches, which should cost little because cache.nixos.org has them (not measured). |
| 3.10 | A peer with no arch reported (a build older than the cache) | Not asked. It has no `/v1/store` and would only miss. |
| 3.11 | A same-arch peer that is a phone (Termux) | Asked and served like any node. There is no phone special case. A pool-only node has no nix, so it serves nothing and never asks; an introducer-only node asks nothing. |
| 3.12 | A peer whose status is not healthy, or that has no host or port | Not asked. |
| 3.13 | A peer on the same machine (same boot id), such as a second daemon on one host | Not asked, so two daemons on one host do not share paths. |
| 3.14 | A peer-layer request (`/v1/store/narinfo`, `/v1/store/nar`) while that route's pool of 16 is full | Waits up to 8 s for a slot, then 503. The narinfo and NAR routes have separate pools, so long NAR streams do not block narinfo lookups. The asker counts it as busy, not as a miss: nix gets 503 (retried, not cached), never 404. |
| 3.15 | A 17th concurrent `/nix-cache` lookup that would fan out | Waits up to 3 s for a slot, then 503, not remembered (nix does not cache a 5xx). |
| 3.16 | A `/nix-cache` request with `Origin` or `Sec-Fetch-Site` | Not exempt from the token: 401. |
| 3.17 | `/health` and other API routes over a spliced connection | Handled as for any caller; only `/nix-cache` loses its loopback exemption. |
| 3.16 | A peer answers the narinfo, then fails or vanishes before the NAR, on a multi-user nix-daemon (which ignores the client's `fallback`) | The build fails once with the cache options; pipedpeer runs it again without them (`peercache.RunWithFallback`), logs the first error, and returns the second. A build that carried no cache options is not retried. |
| 3.17 | A request on loopback that arrived through the QUIC forwarder from a remote peer | The forwarder marks the connection (`direct.Spliced`), so it loses the no-token exemption and gets the normal token check (401 without the token). |

## 4. Finding the Wireless debugging ports (mDNS, done in Go inside Termux)

Real, SM-G781B, Android 13, Play Store Termux:
- Termux's `adb` has no mDNS: `adb mdns services` → `unknown host service 'mdns:check'`, even with `ADB_MDNS_OPENSCREEN=1`.
- `net.Interfaces` fails with netlinkrib permission denied, so libraries that enumerate interfaces (grandcat/zeroconf) fail.
- A listener from `net.ListenMulticastUDP("udp4", nil, 224.0.0.251:5353)` that sends a plain PTR query (no QU bit) for `_adb-tls-pairing._tcp.local.` and `_adb-tls-connect._tcp.local.` received SRV answers for both (pairing 41913, connect 42965).
- A QU query from an ephemeral port found the connect service but missed the pairing one.
- Dialing `127.0.0.1:<port>` works. A 512-way concurrent port scan found nothing, probably from the fd limit.

| # | Source | Answer | Handling | Cost |
|---|---|---|---|---|
| 4.1 | Real: pairing dialog open | SRV `adb-<serial>-<id>._adb-tls-pairing._tcp.local.` port 41913, plus SRV `…_adb-tls-connect._tcp.local.` port 42965 | pairing = 41913, connect = 42965. Pair and connect on `127.0.0.1:<port>`. | one multicast listener, 4 s per query round |
| 4.2 | Real: pairing dialog closed, Wireless debugging on | only the connect SRV | Keep re-querying every 2 s, up to 60 s, while telling the user to tap "Pair device with pairing code". Then ask for the port. | the wait |
| 4.3 | Synthetic: another phone on the same Wi-Fi also advertises | two SRVs of each type | Keep only ports where a TCP dial to `127.0.0.1:<port>` succeeds within 300 ms; a port on another phone is not open locally. | one dial per candidate |
| 4.4 | Synthetic: the answer is in Extra, not Answer | SRV in the additional section | Read SRV from Answer and Extra together. | none |
| 4.5 | Synthetic: Wi-Fi off, or Wireless debugging off | no answer | Say "turn on Wi-Fi and Wireless debugging" and re-query. Then ask for the port. | none |
| 4.6 | Synthetic: the multicast listen fails (port 5353 taken, SELinux) | error | Warn with the error and ask for the ports, as `scripts/phone-phantom-off.sh` does. | none |
| 4.7 | Synthetic: a truncated or garbage packet on 5353 | unpack error | Skip it (debug log), keep listening. | none |
| 4.8 | Real: after `adb pair`, the connect port | `adb connect 127.0.0.1:42965` → `connected to 127.0.0.1:42965` | Run phantom-off over `adb -s 127.0.0.1:<port> shell …` | none |
| 4.9 | Synthetic: `adb pair` with a wrong or expired code | `Failed: Wrong password or connection was dropped.` | Ask for the code again (3 tries), then the Action line. | none |
| 4.10 | Synthetic: already paired (setup re-run) | connect SRV found, `adb connect` → `connected` with no pairing | Skip pairing. | none |

## 5. Phone state probes

| # | Source | Probe → value | Meaning |
|---|---|---|---|
| 5.1 | Real: SM-G781B, Android 13 (`ro.build.version.sdk` = 33), read over USB adb, before the change | `getprop persist.sys.fflag.override.settings_enable_monitor_phantom_procs` → empty; `settings get global settings_enable_monitor_phantom_procs` → `true`; `device_config get activity_manager max_phantom_processes` → `null` | The killer is on. SDK 33 has no "Disable child process restrictions" toggle, so the adb route applies. |
| 5.2 | Real: the same phone | `com.termux` versionName `googleplay.2026.10.09`, installer `com.android.vending` | Play Store Termux. Boot scripts are built in (`TermuxBootReceiver`). |
| 5.3 | Real: Termux itself (no adb) | `settings get …` / `cmd settings` → exception; no `device_config` service | Termux cannot read the state. It is read only through adb. |
| 5.4 | Real: after the adb route from Termux | `settings_enable_monitor_phantom_procs` → `false`, `max_phantom_processes` → `2147483647`, `set_sync_disabled_for_tests persistent` | Off. |
| 5.5 | Synthetic: SDK < 31 | | No phantom killer. Skip the step. |
| 5.6 | Synthetic: SDK ≥ 34 with the developer toggle on | `getprop persist.sys.fflag.override.settings_enable_monitor_phantom_procs` → `false`, if Termux can read it | Off. There is no adb on this route. When the prop cannot be read, setup takes the user's confirmation that the toggle is set, and doctor reports `unknown`. |
| 5.7 | Synthetic: after a reboot (Wireless debugging is off again) | no adb connection, no connect SRV | In a terminal, doctor asks the user to switch Wireless debugging on briefly. The pairing persists, so no code is needed. Doctor checks, then switches it off again. With no terminal, or if no connect SRV appears within 60 s, the result is `unknown`: doctor warns, and does not fail. |
| 5.8 | Synthetic: after a reboot without `set_sync_disabled_for_tests persistent` | via adb, `max_phantom_processes` → `null` | `on`. Doctor points to `pipedpeer setup`. |
| 5.9 | Synthetic: Wireless debugging is off when the step starts (no connect SRV dials) | the user turns it on at pipedpeer's request | When the step ends, pipedpeer runs `adb shell settings put global adb_wifi_enabled 0` as its last command, leaving the phone as it found it. The command may end with the transport closing. "Off" is confirmed when the connect port stops dialing within 3 s; otherwise pipedpeer warns "turn off Wireless debugging in Developer options". |
| 5.10 | Synthetic: Wireless debugging is already on when the step starts (a connect SRV dials) | | It is left on: the user turned it on for their own reasons. The step only runs `adb disconnect`. |
| 5.11 | Synthetic: `adb shell settings put global adb_wifi_enabled 0` over a Wi-Fi transport | the transport may close before an exit status arrives | The exit status is not relied on; 5.9's dial check decides. |

## 6. Installer arguments (`scripts/install-pipedpeer.sh`)

| # | Args | Result |
|---|---|---|
| 6.1 | `--version v0.9.1` | as today |
| 6.2 | `--version v0.9.1 --connect pp1-abc` | setup runs with `--connect pp1-abc` |
| 6.3 | `--connect=pp1-abc -y` | setup gets `--connect pp1-abc` appended after the other arguments (`-y` first) |
| 6.4 | `--connect` with no value, or followed by another flag | exits 1 with "--connect requires a code" |
| 6.5 | `--bogus` | exits 1 as today |
| 6.6 | Piped through `curl … \| bash` with a terminal | setup reads from `/dev/tty` |
| 6.7 | No terminal (CI, `ssh host 'curl … \| bash'` without `-t`) | setup gets stdin as today; prompts fall back to their non-interactive answer |

## 7. Warm-up log (`userdir.State()/warmup.log`)

| # | Last line / header | `Running()` | `Done(v, nvidia)` |
|---|---|---|---|
| 7.1 | no file | false | false |
| 7.2 | header `… build=v torch=CPU`, last `done` | false | true for (v, false) |
| 7.3 | same, asked with another build | false | false: rebuild after an upgrade |
| 7.4 | same, asked with nvidia = true | false | false |
| 7.5 | last `failed (exit 1); details above` | false | false |
| 7.6 | a progress line, modified < 1 h ago | true | false |
| 7.7 | a progress line, modified > 1 h ago | false | false |
| 7.8 | empty file | false | false |
| 7.9 | old header without `build=` (written before the `build=` header existed), last `done` | false | false: one rebuild after the upgrade, which is cheap because nix has everything |
