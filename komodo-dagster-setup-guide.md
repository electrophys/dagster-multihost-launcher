# Komodo + Dagster: Setup & Configuration Guide

End-to-end guide to running a multi-host Dagster deployment where **[Komodo](https://komo.do/)**
deploys and controls the long-lived services (the control plane and the
code-location gRPC servers) as Docker Compose stacks, and the
**`MultiHostDockerRunLauncher`** in this repo launches each Dagster run as a
container on the right host.

This guide is the map; it links to the focused guides for the details:

- [docker-setup-guide.md](docker-setup-guide.md) — install/configure Docker per host
- [docker-tls-setup-guide.md](docker-tls-setup-guide.md) — generate the Docker mTLS certs (manual)
- [komodo/tls-provisioning.md](komodo/tls-provisioning.md) — automate the cert lifecycle with Komodo
- [komodo/README.md](komodo/README.md) + [komodo/](komodo/) — example Resource Sync TOML and Actions
- [komodo-integration.md](komodo-integration.md) — the design rationale behind these choices
- [CLAUDE.md](CLAUDE.md) — the launcher config reference and CLI summary

---

## 1. What you're building

Two control planes that own different lifetimes — they complement, not compete:

| Layer | Owner | Lifetime |
|-------|-------|----------|
| Long-lived **services** (webserver, daemon, postgres; code-location gRPC servers) | **Komodo** (Compose stacks, GitOps) | hours–weeks |
| Ephemeral **run** containers (one per Dagster run) | **`MultiHostDockerRunLauncher`** | seconds–hours |

```
                 ┌──────────── Komodo Core (UI · API · Resource Sync · Builds · Procedures) ───────────┐
                 │                       deploys & controls the stacks below                            │
                 └───────Periphery───────────────────Periphery───────────────────Periphery─────────────┘
                 ┌──────────▼─────────┐      ┌────────▼──────────┐       ┌─────────▼─────────┐
                 │ Host A (control)   │      │ Host B (worker)   │       │ Host C (worker)   │
                 │ stack: webserver,  │      │ stack: code-loc   │       │ stack: code-loc   │
                 │ daemon, postgres   │      │ gRPC server (etl) │       │ gRPC server (anl) │
                 │                    │      └────────▲──────────┘       └─────────▲─────────┘
                 │ launcher (in daemon)│  run containers (ephemeral) via Docker TCP+mTLS :2376
                 │      └──────────────┼───────────────┴────────────────────────────┘
                 └────────────────────┘
```

**Key model — each worker's Docker has two independent consumers:**

- **Komodo Periphery** deploys the code-location stack over the host's **local
  Docker socket** (no TLS).
- **The launcher** creates run containers over **`tcp://host:2376` with mutual
  TLS** from Host A.

They don't depend on each other, which is why TLS only matters for the launcher's
path (see [§5](#5-docker--tls-per-host)).

---

## 2. Plan the topology

Write this down first — every later step refers back to it
([docker-setup-guide.md §0](docker-setup-guide.md) has a fuller template):

| Item | Example |
|------|---------|
| Host A (control plane) IP | `10.0.1.1` |
| Host B / C (workers) IPs | `10.0.1.2`, `10.0.1.3` |
| Docker TLS port (workers) | `2376` (inbound from Host A only) |
| Code-location gRPC ports | `4001`, `4002` (inbound from Host A only) |
| Postgres port | `5432` (inbound on Host A from workers) |
| Code locations → host | `etl → host-b`, `analytics → host-c` |

The "code location → host" mapping is the spine: it must match in **three**
places — `dagster.yaml` `docker_hosts`, the Komodo stacks, and `workspace.yaml`.
`komodo-verify` ([§8](#8-keep-everything-in-lockstep)) guards two of them.

---

## 3. Install Komodo

Follow the official docs — don't hand-roll it:

1. **Komodo Core** (UI + API + Mongo) on a host that can reach all workers —
   typically Host A. See https://komo.do/docs/setup.
2. **Periphery** on **every** host (A, B, C). Periphery talks to the local Docker
   socket. v2.0+ uses auto-rotating PKI keys; older uses a passkey + IP allowlist.
   See https://komo.do/docs/setup/connect-servers.
3. In the Komodo UI, add each host as a **Server** and confirm it reports healthy.

Create a **Komodo API key** (Settings → for your user or a Service User) if you
plan to drive Komodo from scripts; the wiring here uses Procedures/Actions inside
Komodo, which need no external key.

---

## 4. Install Docker per host

Install Docker on every host and decide root vs rootless (build run images in the
same context the daemon serves). Full steps:
[docker-setup-guide.md](docker-setup-guide.md).

Create the Docker networks the stacks and run containers attach to (one per
worker, e.g. `host_b_dagster_network`). Run containers must be able to reach
Postgres on Host A by **real IP** (not a compose service name).

---

## 5. Docker + TLS per host

The launcher dials each worker's Docker daemon at `tcp://host:2376` with mutual
TLS, so each worker must expose that endpoint and Host A must hold client certs.

1. **Generate the CA + per-host server certs + Host A client certs** —
   [docker-tls-setup-guide.md](docker-tls-setup-guide.md). (Strict TLS stacks need
   the Subject/Authority Key Identifier extensions that guide includes.)
2. **Distribute + rotate them with Komodo** instead of by hand —
   [komodo/tls-provisioning.md](komodo/tls-provisioning.md): store cert material
   as Komodo Secrets and apply `daemon.json` + the systemd `-H tcp://0.0.0.0:2376`
   override + certs via the [`provision-docker-tls`](komodo/actions/provision-docker-tls.ts)
   Action.
3. Lock the TLS port to Host A only (ufw/iptables — see the TLS guide).

The launcher validates the client cert paths at startup, so a missing/rotated-away
file fails loudly rather than silently.

---

## 6. Configure the Dagster control plane (Host A)

The control plane runs as a Komodo stack on Host A: `webserver`, `daemon`,
`postgres`. Two config files drive the launcher.

### 6.1 `dagster.yaml` — the launcher

See [dagster.yaml](dagster.yaml) for a complete example; the shape:

```yaml
run_launcher:
  module: dagster_multihost_launcher
  class: MultiHostDockerRunLauncher
  config:
    default_env_vars: [DAGSTER_POSTGRES_USER, DAGSTER_POSTGRES_PASSWORD, ...]
    default_env_file: "/etc/dagster/shared.env"   # central run-container env (Komodo-rendered)
    docker_hosts:
      - host_name: "host-b"
        docker_url: "tcp://10.0.1.2:2376"
        tls: { ca_cert: /etc/dagster/certs/ca.pem,
               client_cert: /etc/dagster/certs/client-cert.pem,
               client_key: /etc/dagster/certs/client-key.pem }
        location_names: ["etl"]
        network: "host_b_dagster_network"
        env_file: "/etc/dagster/host-b.env"            # per-host run-container env
        inherit_env_from_container: "code-{location}"  # inherit the gRPC server's env
```

Env precedence (low→high): `default_env_file` < host `env_file` < inherited from
the code-location server < `default_env_vars` + host `env_vars` < Dagster
internals. Full reference in [CLAUDE.md](CLAUDE.md).

### 6.2 `workspace.yaml` — where the code locations live

Point Dagster at each remote gRPC server (host:port) so the webserver/daemon can
load definitions. See [workspace.yaml](workspace.yaml). The location names here
must match `dagster.yaml` `docker_hosts[].location_names`.

### 6.3 Mount cert/config into the daemon

The daemon container needs `dagster.yaml`, `workspace.yaml`, the TLS client certs,
and the `*.env` files mounted. Komodo renders these onto Host A (certs via
[§5](#5-docker--tls-per-host); env via [§7](#7-central-environment--secrets)).

---

## 7. Central environment & secrets

Define env once in **Komodo Variables/Secrets** (`[[VAR]]` interpolation; mark
secrets secret — they never leave via the API) and render it everywhere:

- **Control-plane stack `.env`** — the daemon/webserver's *own* env, including the
  `{env: ...}` references in `dagster.yaml` (e.g. Postgres creds). Gate this with
  `dagster-multihost check-env` ([§8](#8-keep-everything-in-lockstep)).
- **Each code-location stack `.env`** — the gRPC server's env, including
  `DAGSTER_CURRENT_IMAGE` (image pinning, [§9](#9-image-builds--pinning)).
- **Run containers** — the launcher's `default_env_file` / per-host `env_file`
  point at Komodo-rendered files on Host A; `inherit_env_from_container` pulls the
  gRPC server's env so runs match it exactly.

One definition feeds the long-lived stacks **and** the ephemeral run containers.

---

## 8. Keep everything in lockstep

Two pre-deploy gates (wire into CI or a Komodo pre-deploy Procedure step):

```bash
# Fail if the control plane is missing env vars dagster.yaml references:
dagster-multihost check-env --env-file /etc/dagster/control-plane.env

# Fail if a host/code location in dagster.yaml is missing from / misplaced in Komodo
# (which would silently fall back to the DefaultRunLauncher):
dagster-multihost komodo-verify komodo/resource-sync.toml
```

Bootstrap the Komodo Resource Sync TOML from your existing `dagster.yaml`:

```bash
dagster-multihost komodo-export -o komodo/resource-sync.toml
```

Convention enforced by `komodo-verify`: `[[server]]` name == `host_name`,
`[[stack]]` name == code location name, stack `server` == its host. See
[komodo/resource-sync.toml](komodo/resource-sync.toml) for a filled-in example.

---

## 9. Image builds & pinning

Reproducible deploys hinge on a pinned image:

1. A Komodo **Build** builds the code-location image and tags it deterministically
   (commit SHA / semver, **never `:latest`**).
2. Set `DAGSTER_CURRENT_IMAGE` on the code-location stack to that exact tag (via a
   `[[VAR]]`).
3. Dagster reports it as the location's `container_image`; the launcher resolves,
   **trims, and validates** that reference before creating run containers — so the
   gRPC server and its run containers use the identical, well-formed image, and a
   malformed value fails with a clear error (not Docker's opaque
   `invalid reference format`).

---

## 10. The safe deploy flow

Dagster does **not** auto-reload a code location when its gRPC server restarts, so
a redeploy must quiesce → deploy → reload → restore. The CLI provides the
Dagster-aware verbs; Komodo orchestrates them:

```
drain  ──►  DeployStack  ──►  reload  ──►  restore
```

```bash
dagster-multihost drain etl --state-file /var/lib/dagster-deploy/etl.json --timeout 600
#   (Komodo) DeployStack etl
dagster-multihost reload etl
dagster-multihost restore --state-file /var/lib/dagster-deploy/etl.json
```

- `drain` stops the location's schedules/sensors, waits for active runs, and
  records what it stopped; on timeout (without `--force`) it restarts what it
  stopped and exits non-zero — halting the rollout before any redeploy.
- `restore` re-enables exactly those. Use a **stable shared state-file path** so a
  later stage reads what `drain` wrote.

Run these on **Host A** (they talk to the webserver). The committed
[safe-deploy Action](komodo/actions/safe-deploy.ts) packages this as a single
Komodo Action (state file on a fixed path, `restore` in a `finally`); a Komodo
Procedure can trigger it from the same git push that built the image. Stages run
sequentially and a non-zero exit halts the Procedure — the rollback gate.

---

## 11. Ongoing operations

- **Cleanup** — the scheduled admin asset (`build_admin_definitions`) is the
  default for pruning exited run containers (precise `dagster/managed` + age
  scoping, Dagster-native history). If you need cleanup to survive a control-plane
  outage or to run over each host's local socket, schedule
  `dagster-multihost cleanup --max-age-hours N` from a Komodo Procedure instead —
  same logic, your choice of scheduler.
- **Monitoring** — add a Komodo **Alerter** on worker host/stack health to page on
  outages and gRPC-server crashes, complementing Dagster's per-run health checks.
- **Cert rotation** — re-run the TLS provisioning Procedure
  ([komodo/tls-provisioning.md §4](komodo/tls-provisioning.md)); the launcher picks
  up new certs without a control-plane restart.

---

## 12. Verify it works

1. **Topology**: `dagster-multihost status` shows each code location `LOADED`.
2. **Routing**: launch a job in a mapped location; the run log shows
   `Creating Docker container on host '<host>' with image '<pinned tag>'`, and
   `docker ps` on that worker shows a `dagster/managed=true` container.
3. **Fallback check**: a job in an *unmapped* location is tagged
   `multihost_docker/launcher_type=default` and runs via the DefaultRunLauncher.
4. **Safe deploy**: run the `drain → DeployStack → reload → restore` flow against
   a test location and confirm schedules/sensors come back enabled.

---

## 13. Troubleshooting

| Symptom | Likely cause → fix |
|---------|--------------------|
| Runs for a location execute on Host A as subprocesses | Location not in `docker_hosts` (silent DefaultRunLauncher fallback) → `komodo-verify` |
| `invalid reference format` at container create | Malformed `DAGSTER_CURRENT_IMAGE` → the launcher now reports the bad value; pin a clean tag ([§9](#9-image-builds--pinning)) |
| Run fails on missing env var | Var not rendered into the run env → check `env_file`/inheritance; for the control plane's own env, `check-env` |
| `certificate verify failed` / connection refused on `2376` | Docker TLS not set up/rotated → [docker-tls-setup-guide.md](docker-tls-setup-guide.md), [komodo/tls-provisioning.md](komodo/tls-provisioning.md) |
| New code not picked up after redeploy | Missing reload → `dagster-multihost reload <location>` (or the safe-deploy Action) |
| Control plane down but containers piling up | Move cleanup to a Komodo Procedure running `dagster-multihost cleanup` ([§11](#11-ongoing-operations)) |

---

> Komodo's exact Resource Sync TOML fields and Action client method signatures
> vary by version — confirm the examples in [komodo/](komodo/) against
> https://komo.do/docs for the version you run.
