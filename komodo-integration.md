# Komodo Integration — Synergy Analysis & Roadmap

> Status: research / design doc for the `komodo-integration` branch.
> Goal: use [Komodo](https://komo.do/) to deploy and control the Dagster control
> plane and the remote code-location hosts (as Docker Compose stacks), and define
> how `MultiHostDockerRunLauncher` and the `dagster-multihost` CLI should fit into
> that world.

## TL;DR

Komodo and this project operate at **two different layers** and are complementary:

| Layer | Concern | Lifetime | Owner today | Owner with Komodo |
|-------|---------|----------|-------------|-------------------|
| **Service / stack** | Control plane (webserver, daemon, postgres) + code-location gRPC servers | Long-lived | `dagster-multihost` CLI (local compose only) + manual remote ops | **Komodo Stacks** (multi-host, GitOps) |
| **Run execution** | Per-Dagster-run worker containers | Ephemeral (seconds–hours) | `MultiHostDockerRunLauncher` (direct Docker TCP+mTLS) | **Stays the launcher** |

Komodo should own the *stack layer* end-to-end. The launcher keeps owning the
*run-execution layer* — routing ephemeral runs through Komodo's resource model
would be an impedance mismatch (see [Anti-patterns](#anti-patterns)). The high-value
work is at the **seam** between the two layers: image pinning, deploy
orchestration, config single-sourcing, and secret/cert handling.

## Architecture: how the layers fit

```
                    ┌──────────────────────── Komodo Core (UI + RPC API + GitOps) ──────────────────────┐
                    │  Stacks · Builds · Procedures · Resource Sync (TOML) · Variables/Secrets · Alerters │
                    └───────────┬───────────────────────────┬───────────────────────────┬───────────────┘
                       Periphery│                   Periphery│                   Periphery│
                    ┌───────────▼──────────┐      ┌──────────▼──────────┐     ┌──────────▼──────────┐
                    │ Host A (control plane)│      │ Host B (worker)     │     │ Host C (worker)     │
                    │  Stack: webserver,    │      │  Stack: code-loc    │     │  Stack: code-loc    │
                    │  daemon, postgres     │      │  gRPC server        │     │  gRPC server        │
                    └───────────┬──────────┘      └──────────▲──────────┘     └──────────▲──────────┘
                                │ daemon hosts the launcher              run containers (ephemeral)
                                │  MultiHostDockerRunLauncher ───────────┴──── Docker TCP+mTLS :2376 ──┘
                                ▼
                            Postgres
```

- **Komodo Core ↔ Periphery** is Komodo's own authenticated channel (passkey pre-2.0,
  auto-rotating PKI key-pairs in v2.0, optional outbound-from-Periphery for NAT'd hosts).
- **The launcher's data path is separate**: the daemon still opens the Docker API on
  each worker directly (the TCP+mTLS path the TLS guide documents). Komodo does not
  currently sit in that path — see [Opportunity 4](#4-tame-or-eliminate-the-mtls-pain).

## Opportunities (ranked by value × fit)

### 1. Image pinning: Komodo Build → pinned tag → both gRPC server *and* run containers

**This directly fixes the class of bug we just hit** (`invalid reference format` /
unresolved `DAGSTER_CURRENT_IMAGE`). The launcher resolves the run image from the
`dagster/image` tag or the code location's `container_image`
([launcher.py:364](dagster_multihost_launcher/launcher.py#L364)). If that value is
empty, stale, or malformed, every run on that host fails at create time.

Komodo Builds produce deterministic, versioned tags (`major.minor.patch`,
auto-incrementing patch, plus commit-hash and `latest` tags) and push to a registry.

**Pattern:**
1. Komodo Build builds the code-location image, tags it `…:<semver>` / `…:<commit>`.
2. The same pinned tag is written into the code-location **Stack**'s `.env` via a
   Komodo Variable (`DAGSTER_CURRENT_IMAGE=[[CODELOC_B_IMAGE]]`).
3. The gRPC server starts with that image **and** reports it to Dagster, so
   `container_image` on the job origin — and therefore the launcher's run
   containers — use the *identical* pinned image.

Result: the gRPC server and its run workers can never drift apart, and the image is
never ambiguous. This is the single most valuable synergy and the most on-theme for
the `feat/improve-error-handling` work that preceded this branch.

### 2. Komodo-orchestrated, Dagster-aware deploys (multi-host)

The CLI already encodes the hard-won **Dagster-aware** zero-downtime sequence —
stop schedules/sensors, drain active runs, restart, health-check, restore, cleanup
([deploy.py:124-621](dagster_multihost_launcher/cli/deploy.py#L124)). Its gap is that
it only drives **local** compose; it cannot deploy Hosts B/C.

Komodo Procedures are *staged* (stages run sequentially, executions within a stage in
parallel) and span hosts. The synergy: **Komodo does the multi-host stack mechanics;
the CLI supplies the Dagster brain.**

**Pattern — a Komodo Procedure per rollout:**
1. **Stage: build** — `RunBuild` for changed code-location images.
2. **Stage: drain** — a Komodo **Action** (TypeScript in Core) or a Periphery-run
   `Repo` script invokes `dagster-multihost` drain logic against the webserver
   GraphQL API (reuse `phase_drain`). Refactor the CLI so drain/restore are callable
   as a library, not only via `click`.
3. **Stage: deploy** — `DeployStack` for each worker, then the control-plane stack.
4. **Stage: verify + restore** — health check + re-enable schedules/sensors
   (`phase_health`, `phase_restore`).

This keeps every Dagster-specific guarantee while gaining true multi-host rollout,
GitOps triggering, and audit history.

### 3. One source of truth for topology (kill the config triplication)

Host/location/network facts are currently declared in **three** places that must
agree: `dagster.yaml` `docker_hosts` (consumed at [config.py:107](dagster_multihost_launcher/cli/config.py#L107)),
the compose files, and (soon) Komodo's Resource Sync TOML. Drift here is a silent
foot-gun.

**Pattern:** make Komodo's Resource Sync TOML (the GitOps repo) the source of truth
and **generate** the launcher's `docker_hosts` block from it (or vice-versa). The CLI
already parses both compose and `dagster.yaml` into a unified `DeployConfig`
([config.py:145](dagster_multihost_launcher/cli/config.py#L145)) — extend it with:

- `dagster-multihost komodo-export` → emit `[[server]]` / `[[stack]]` TOML stanzas
  from `dagster.yaml` + compose, for Komodo Resource Sync.
- `dagster-multihost komodo-verify` → diff live Komodo Servers/Stacks (via `/read`)
  against `dagster.yaml` `docker_hosts` and fail CI on mismatch.

### 4. Tame (or eliminate) the mTLS pain

The TLS setup the user just fought through (CA with SKI, leaf certs with AKI, per-host
SANs) exists *only* because the launcher needs the **Docker daemon TCP API exposed
with mutual TLS** on every worker. Two levels of synergy:

- **4a (near-term, safe): Komodo provisions and rotates the TLS material.** Store the
  CA / client certs as Komodo **Secrets**; have the worker Server's setup write
  `daemon.json` (enable `tcp://…:2376` + `tlsverify`) and drop the server certs. The
  painful, manual, error-prone cert distribution becomes declarative GitOps. The
  launcher is unchanged — it still dials Docker directly.
- **4b (ambitious, needs investigation): route run-container creation through
  Periphery** so the Docker TCP socket never has to be exposed at all. Periphery
  already has an authenticated channel to Core and talks to each host's local Docker.
  **Open question:** does Periphery expose a generic "create container with this
  command + env" call usable for ephemeral, per-run workers? If yes, a
  `transport="komodo"` mode on the launcher could replace direct `containers.create`
  and delete the entire mTLS surface. If Periphery only exposes Stack/Deployment-level
  operations, 4b is not viable and we stay on 4a. **Verify against the Periphery API
  before committing to this.**

### 5. Unified secrets & env injection

`default_env_vars` / per-host `env_vars` pull from the daemon's process environment
([launcher.py:300-327](dagster_multihost_launcher/launcher.py#L300)), and
`dagster.yaml` storage config uses `env:` references that must also exist on each
worker (per CLAUDE.md networking notes). Today that's scattered. Komodo Variables +
`[[VAR]]` interpolation + the Core `[secrets]` block become the **single injector**:
the same secret (postgres password, registry creds, run env) is defined once in
Komodo and rendered into the control-plane stack, each worker stack, and the run
env — eliminating the "set it on every host too" failure mode.

### 6. Monitoring & cleanup consolidation

The admin assets already report container counts and prune exited run containers by
label ([launcher.py:625-741](dagster_multihost_launcher/launcher.py#L625)). Komodo
adds host stats, log retrieval, and **Alerters**. Low-effort wins:

- Schedule the existing `cleanup_old_containers` as a Komodo **Procedure** (cron),
  consolidating ops scheduling in one place.
- Wire a Komodo **Alerter** on worker host/stack health so control-plane outages and
  gRPC-server crashes page someone, complementing Dagster's per-run health checks
  ([launcher.py:562](dagster_multihost_launcher/launcher.py#L562)).

## Anti-patterns

- **Do not model each Dagster run as a Komodo Deployment/Stack.** Komodo resources
  are declarative and persistent; Dagster runs are ephemeral and dynamically
  parameterized (each carries a generated `ExecuteRunArgs` command). Creating a
  Komodo resource per run would flood Komodo with transient state and fight its
  reconciler. Per-run launching stays in the launcher.
- **Don't let Komodo's reconciler/pruner garbage-collect run containers.** Run
  workers are tagged `dagster/managed=true` but are *not* Komodo-managed resources.
  Ensure any Komodo prune/cleanup is scoped so it doesn't kill in-flight run
  containers. (The label convention already in the launcher makes an exclusion
  filter easy.)
- **Don't duplicate cert/secret material across Komodo and `dagster.yaml`.** Pick
  Komodo as the source (Opportunity 5) and render down.

## Recommended roadmap for this branch

1. **Refactor the CLI deploy phases into an importable library** (decouple from
   `click`) so Komodo Actions/Procedures can call drain/restore/health directly.
   *Prereq for Opportunities 2.*
2. **Image-pinning convention + docs** (Opportunity 1): document the Komodo Build →
   `DAGSTER_CURRENT_IMAGE` Variable → gRPC server flow; optionally add a CLI check
   that warns when a code location's reported image isn't a pinned tag.
3. **`komodo-export` / `komodo-verify` CLI subcommands** (Opportunity 3) to keep
   `dagster.yaml` and Komodo Resource Sync TOML in lockstep.
4. **TLS-via-Komodo provisioning recipe** (Opportunity 4a): move the cert material
   into Komodo Secrets and document the GitOps `daemon.json` setup.
5. **Spike: Periphery-backed launcher transport** (Opportunity 4b) — first confirm
   the Periphery container API supports ephemeral create; only then design the mode.
6. **Komodo Procedure templates** for the full build → drain → deploy → verify
   rollout, plus a scheduled cleanup Procedure and health Alerter.

## Implemented in this branch

### Reload on remote container update (Opportunity 2, reload half)

Dagster does **not** reload a code location when its gRPC server restarts behind
the same host:port, so the reload must be triggered externally — and Komodo is
the thing that just did the redeploy.

- `DagsterGraphQLClient.reload_location(name)` calls the targeted
  `reloadRepositoryLocation` mutation (the existing `reload_workspace` reloads
  *all* locations).
- `dagster-multihost reload <location> [...]` (`--all` for the whole workspace)
  drives it from the command line.

**Komodo wiring** — make the redeploy a Procedure whose final stage reloads the
location that changed, e.g. on Host A's Periphery (which can reach the webserver):

```
Procedure: deploy-code-loc-b
  Stage 1  DeployStack   code-loc-b
  Stage 2  <run on Host A>  dagster-multihost reload etl_pipelines
```

The Stage-2 execution can be a Periphery-run Repo script, or a Komodo Action
(TypeScript) that POSTs the `reloadRepositoryLocation` mutation to the webserver
directly. Trigger the whole Procedure from the same git push (webhook) that built
the new image.

### Central env management (Opportunity 5)

The launcher now layers run-container env from files + inheritance, not just
enumerated keys (see the precedence list in `CLAUDE.md`):

- `default_env_file` (all hosts) and per-host `env_file` — point these at files
  Komodo renders from its **Variables/Secrets** (`[[VAR]]`). The *same* central
  definition feeds the control-plane stack, each code-location stack, **and** the
  run containers.
- per-host `inherit_env_from_container: "code-{location}"` — run containers
  inherit the code-location server's own `Config.Env`, so whatever Komodo
  injected into the gRPC server is exactly what its runs get, with no second
  list to maintain. Explicit `env_vars` still override.

This closes the "set it on every host too" gap called out in
[Networking Requirements](CLAUDE.md): the run worker's env is sourced from a
file/inheritance the launcher controls, rather than relying on the daemon's own
process environment for every bare `KEY`.

## Komodo facts this design relies on

- Core + per-host **Periphery** agent; RPC API at `/read`, `/write`, `/execute`
  (auth `X-Api-Key` / `X-Api-Secret`); official **Rust** and **TypeScript** clients,
  **no official Python client** (Python integration = raw HTTP).
- **Stack** = docker-compose deployment (compose can be UI-defined, on-host, or
  git-sourced); `.env` injection via Variables; per-Stack `server` targeting.
- **Build** = git repo → image with semver/commit/latest tags → registry.
- **Procedure** = sequential stages of parallel executions; **Action** = TypeScript
  run in Core with a pre-initialized Komodo client (can call external systems).
- **Resource Sync** = declarative TOML in git (`[[server]]`, `[[stack]]`, …) diffed
  against live state.
- **Variables/Secrets** = `[[VAR]]` interpolation; Core `[secrets]` never exposed via API.
- **Webhooks**: `…/listener/<github|gitlab>/<resource>/<id>/<execution>`.

> Items to verify before building: exact Periphery container-create capability
> (Opportunity 4b), and the canonical `/execute` JSON request shapes (auto-generated;
> confirm against live `docs.rs`/OpenAPI). Komodo v2.0 transport specifics (Noise
> protocol / port 9120) were unconfirmed in official docs.
