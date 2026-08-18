# Security Policy

SAGE is a gateway: **it holds signing keys and every relay it sends spends staked
POKT.** A configured `gateway_private_key_hex` (and any `owned_apps_private_keys_hex`)
*is* an on-chain identity — not a username-and-password that can be rotated behind
an account. That shapes both how vulnerabilities should be reported and how the
gateway must be operated.

## Reporting a vulnerability

**Do not open a public issue for anything security-sensitive** — especially not
anything involving key exposure, unauthorized relaying, or stake drain.

Report privately through GitHub's [private vulnerability
reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository (**Security → Report a vulnerability**). If you cannot use
that, contact the Pocket Network Foundation security team.

Please include:

- what an attacker can do, and what they need in order to do it;
- a minimal reproduction (config + request), **with all private keys redacted** —
  send a key length, never a key;
- affected version (the `version` field logged at startup, or `git describe`) and
  platform.

We aim to acknowledge within a few business days and to coordinate a fix and
disclosure timeline with you.

### Never include a private key in a report

If you believe a key has been exposed, treat it as compromised: **re-key and
re-stake** rather than sending the old one anywhere. A 64-hex secp256k1 key in an
issue, a log paste, or a screenshot is itself the incident.

## Operational security model

Most of the risk here is operational, not a code bug. The design already defends
these; the failure mode is turning the defenses off.

SAGE listens on **three separate ports by design** — do not collapse them onto one
public edge:

| Port (default) | Serves | Exposure |
| --- | --- | --- |
| `3069` (`router_config.port`) | relays (`/v1`), health (`/health`, `/ready`) | public **only behind an authenticating, rate-limiting edge** |
| `9091` (`admin_config.addr`) | admin API (`/admin/*`) | **loopback** — unauthenticated |
| `9090` (`metrics_config.prometheus_addr`) | Prometheus (`/metrics`) | scrape-only |

| Rule | Why it matters |
| --- | --- |
| **Never expose the relay port directly.** | SAGE authenticates no one and rate-limits nothing: it has no API keys, no quotas, and no per-client accounting. Every relay it accepts is signed with your gateway key and spends staked POKT, so an unauthenticated `3069` on the open internet is a funnel for draining your stake at line rate. This is a deliberate division of labour — the edge authenticates, SAGE relays — and it only holds if the edge is actually there. Put an authenticating, rate-limiting proxy in front, and scope what it accepts to the services you intend to serve. |
| **Keep the admin API on loopback.** | `/admin/*` is unauthenticated by design and can toggle feature flags and per-service behavior. On a public bind, anyone can reconfigure the gateway. Put a TLS-terminating authenticating proxy in front if you must reach it remotely. |
| **Leave `pprof_addr` empty unless you need it.** | pprof hands out heap dumps, and a heap dump holds in-memory signing keys. It is off unless explicitly set — keep it off in production, or bind it somewhere only you can reach. |
| **Keep `local/` and any keyed config out of git.** | They hold private keys. `local/` is gitignored; keep it that way and never paste a config that contains a key. |
| **Prefer an env var / secret store over an inline key in the config file.** | Keeps the key out of a file on disk where possible. |
| **Never print or log a key.** | Not in errors, not in issues, not in a paste. Emit a length, never the string. |

The three-port model and the pprof/admin reasoning are documented in the README's
port table and in `ARCHITECTURE.md`. If you find a way around any of these gates,
that is exactly the kind of report this policy is for.

## Scope

- **In scope:** anything that exposes a signing key, lets a third party relay or
  reconfigure the gateway, bypasses the admin/pprof/port boundaries, or
  forges/replays a signed relay or a supplier response.
- **Out of scope:** the security of the upstream full node or relay miner you
  point at (report those to their projects), and supplier behavior on the
  network.
