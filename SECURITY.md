# Security

## Threat model, stated plainly

Pipedpeer is a tool for running code on machines you control, on a network you
control. It assumes every peer on the network is trusted.

**There is no authentication, authorization, or transport encryption.** The
daemon's HTTP API (default port `38080`) will accept a job — arbitrary code —
from anyone who can reach it. Anyone with network access to a daemon can:

- upload and execute code on that machine,
- read job outputs and workspace files,
- deregister nodes.

This is a deliberate trade-off for a LAN tool at this stage of the project, not
an oversight. Authentication and encrypted peer-to-peer transport are planned
alongside internet-wide operation.

## What you must do

- Run Pipedpeer only on a network you trust and control.
- Never expose the daemon port to the internet or to a shared/public network.
- Use firewall rules to restrict the daemon port to known peer addresses.
- Do not run a daemon on a machine holding secrets you would not hand to any
  other peer on that network.

## Reporting a vulnerability

Report suspected vulnerabilities privately by opening a
[GitHub security advisory](https://github.com/vinayakyadav2709/Pipedpeer/security/advisories/new).
Please do not open a public issue for a vulnerability.

Findings that amount to "an unauthenticated peer on the LAN can run code" are
the documented design above rather than a new vulnerability — but reports about
the *scope* of that exposure (for example, reachability beyond the intended
network, or a path that escapes the sandbox) are very welcome.
