# Provisioning Docker TLS with Komodo (Opportunity 4a)

Automate the Docker mutual-TLS cert lifecycle with Komodo, instead of generating
and `scp`-ing certs to every host by hand (the toil in
[docker-tls-setup-guide.md](../docker-tls-setup-guide.md)).

> **Why we still need Docker mTLS at all.** The Periphery-transport spike (see
> [komodo-integration.md](../komodo-integration.md), Opportunity 4) concluded the
> launcher should keep talking to each worker's Docker daemon **directly** over
> `tcp://host:2376` with mutual TLS. So the certs still have to exist — this recipe
> automates *who creates, distributes, and rotates them* (Komodo), not whether
> they're needed.

## Two independent consumers of each worker's Docker

This is the key mental model:

| Consumer | How it reaches Docker | Needs Docker TLS? |
|----------|-----------------------|-------------------|
| **Komodo Periphery** (deploys stacks) | local **unix socket** on the host | No |
| **The launcher** (creates run containers) | **`tcp://host:2376`** from Host A | **Yes — mTLS** |

Periphery never uses the TCP/TLS endpoint, so you can install Periphery first
(with its own PKI/passkey auth) and *then* use Komodo to provision the Docker TCP
TLS that only the launcher consumes. They don't depend on each other.

## What lives where (matches docker-tls-setup-guide.md paths)

- **Each worker (host-b, host-c):** `ca.pem`, `server-cert.pem`, `server-key.pem`
  in `/etc/docker/tls/`, plus `daemon.json` + a systemd override enabling
  `tcp://0.0.0.0:2376` with `tlsverify`.
- **Host A (control plane / launcher):** `ca.pem`, `client-cert.pem`,
  `client-key.pem` at the paths your `dagster.yaml` `tls:` block points to (e.g.
  `/etc/dagster/certs/`). The launcher validates these exist at startup
  (`_build_docker_client` raises `FileNotFoundError`), so a missing/rotated-away
  file fails loudly.

## Step 1 — Store the cert material as Komodo Secrets

Generate the CA and per-host/client certs once (see the manual guide), then load
them into Komodo as **secret Variables** (or the Core `[secrets]` block). Secret
values are masked in logs and **never returned through the API**.

```
CA_PEM                # ca.pem (the CA cert; the CA *key* stays offline, not in Komodo)
HOST_B_SERVER_CERT    # server-cert.pem for host-b
HOST_B_SERVER_KEY     # server-key.pem for host-b   (secret)
HOST_C_SERVER_CERT
HOST_C_SERVER_KEY     # (secret)
DAGSTER_CLIENT_CERT   # client-cert.pem for Host A
DAGSTER_CLIENT_KEY    # client-key.pem for Host A   (secret)
```

PEM blobs are multi-line; keep the full `-----BEGIN…END-----` content. The CA
*private* key (`ca-key.pem`) signs certs — keep it offline, **not** in Komodo.

## Step 2 — Render certs + configure dockerd on each worker

The host-level change (write certs, `daemon.json`, systemd override, reload
dockerd) can't be done cleanly from inside a container, so use a Komodo **Action**
or a **Repo + Procedure** that runs on the worker. See
[`actions/provision-docker-tls.ts`](actions/provision-docker-tls.ts).

The `daemon.json` and the systemd `-H fd:// -H tcp://0.0.0.0:2376` override (and
the classic "both specify `-H`" conflict) are exactly as in
[docker-tls-setup-guide.md §6](../docker-tls-setup-guide.md) — the Action just
applies them programmatically and `systemctl reload docker`.

> **Secret hygiene:** do **not** echo key material into a server terminal — a PTY
> stream can land in logs. Prefer Komodo's secret **interpolation/replacers** so
> the value is substituted server-side and masked, write keys to a temp file with
> `umask 077`, `install -m 0400`, and shred the temp file. The example Action
> notes where this matters.

## Step 3 — Render the client certs onto Host A

The launcher needs `ca.pem` + `client-cert.pem` + `client-key.pem` on Host A at
the `dagster.yaml` `tls:` paths. Render them from the same Secrets via the
control-plane stack (mount/interpolate) or a small Action targeting Host A. After
this, `dagster-multihost check-env` and a launch will find them.

## Step 4 — Rotation

Certs expire. To rotate:

1. Generate new leaf certs (CA can stay) and update the Komodo Secrets.
2. Re-run the worker provisioning Procedure → new certs written + `reload docker`
   (reload, not restart, keeps running containers up).
3. Re-render the client certs on Host A. The launcher opens a fresh client per
   connection, so it picks up the new certs without a control-plane restart.

Schedule a reminder before the leaf-cert expiry (`-days` in the manual guide).
Komodo can run the rotation Procedure on a schedule.

## Bootstrap / chicken-and-egg

You can't manage a host with Komodo until Periphery is installed there — and
Periphery uses the **local Docker socket**, which needs no TLS. So the order is:

1. Install Docker + Periphery on the host (Periphery auth = passkey/PKI, unrelated
   to Docker TLS).
2. Add the host as a Komodo **Server**.
3. Run this provisioning Procedure to stand up the Docker **TCP+mTLS** endpoint
   that the launcher uses.

The first run is the only "new host" step; everything after (rotation, re-render)
is a Procedure run.

## Security checklist

- `server-key.pem` / `client-key.pem` are **secrets** — `0400`, root-owned,
  masked in Komodo.
- Keep `ca-key.pem` **offline**, never in Komodo.
- IP-allowlist `2376` to Host A only (ufw/iptables; see the manual guide).
- Don't log key material; use secret interpolation, not terminal echo.
