# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A composite Dagster run launcher (`MultiHostDockerRunLauncher`) that routes runs to different Docker daemons based on code location name, with automatic fallback to `DefaultRunLauncher` for non-Docker locations.

**Typical multi-host setup:**
- Host A: Dagster control plane (webserver, daemon, postgres) + admin code location
- Hosts B, C: Remote Docker daemons running code location gRPC servers; runs execute as containers
- Host D: Non-Docker code location (bare process); DefaultRunLauncher sends run to gRPC server, executes on Host D

## Development Commands

```bash
# Install dependencies (creates .venv automatically)
uv sync

# Install with dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Run a single test
uv run pytest path/to/test.py::test_function
```

## Architecture

### Run Routing Logic

The launcher routes runs by matching the code location name against the `docker_hosts` config:

1. **Mapped locations** (listed in `docker_hosts[].location_names`) → Creates Docker container on the specified remote daemon via TCP/SSH
2. **Unmapped locations** → Delegates to `DefaultRunLauncher` (sends `start_run` gRPC to the code location server; run executes there)

This allows mixing Docker and non-Docker code locations in the same Dagster instance.

### Key Files

- `dagster_multihost_launcher/launcher.py` — `MultiHostDockerRunLauncher` class implementing `RunLauncher` interface
- `dagster_multihost_launcher/admin_assets.py` — Pre-built Dagster assets for container cleanup and status monitoring
- `dagster_multihost_launcher/__init__.py` — Package exports
- `dagster.yaml` — Example config showing host routing and TLS setup
- `workspace.yaml` — Example workspace with local + remote gRPC code locations
- `integration_test/` — Working multi-host integration test across 3 physical machines

### Run Tags

The launcher tags Docker runs with:
- `multihost_docker/container_id` — Used by `terminate()` and health checks
- `multihost_docker/host_name` — Which Docker host the container is on
- `multihost_docker/launcher_type` — `docker` or `default`

### Admin Assets

`build_admin_definitions(cron_schedule, cleanup_max_age_hours)` provides a single scheduled job (`multihost_admin_job`) with two assets that run in sequence:
1. `multihost_container_status` — Reports container counts per host
2. `multihost_container_cleanup` — Removes exited containers older than the configured threshold (configurable via `multihost/cleanup_max_age_hours` run tag)

These assets must run via `DefaultRunLauncher` on Host A (not listed in `docker_hosts`). The admin container needs `dagster.yaml` and TLS certs mounted so it can rehydrate the launcher to talk to remote Docker daemons.

## Configuration Reference

In `dagster.yaml`:

```yaml
run_launcher:
  module: dagster_multihost_launcher
  class: MultiHostDockerRunLauncher
  config:
    default_env_vars: [...]       # Env vars for ALL run containers
    default_env_file: "..."       # .env file (on daemon host) for ALL run containers
    default_container_kwargs: {}  # Default containers.create() kwargs
    docker_hosts:
      - host_name: "host-b"
        docker_url: "tcp://..."   # or ssh://user@host
        tls: {...}                # Optional TLS config
        location_names: [...]     # Code locations that run here
        network: "..."            # Docker network for containers
        env_vars: [...]           # Host-specific env vars
        env_file: "..."           # Host-specific .env file
        inherit_env_from_container: "code-{location}"  # inherit code-server env
        container_kwargs: {}      # Host-specific container overrides
        registry: {...}           # Optional registry credentials
```

### Run-container environment precedence

`launch_run` builds each run container's env by layering sources, lowest to
highest precedence:

1. `default_env_file` — shared `.env` for all hosts
2. host `env_file` — per-host `.env`
3. host `inherit_env_from_container` — the code-location server container's
   `Config.Env` (supports a `{location}` placeholder; failures are non-fatal)
4. `default_env_vars` + host `env_vars` — explicit `KEY=VALUE` / `KEY`
5. Dagster-internal vars (`DAGSTER_RUN_JOB_NAME`, `DAGSTER_RUN_ID`) — always set

The `env_file` sources are designed to be rendered by an external config manager
(e.g. Komodo Variables/Secrets), giving a single central source of env vars
shared between the long-lived stacks and the ephemeral run containers.

## CLI: `dagster-multihost`

A Click CLI for managing the deployment: `status`, `pull`, `deploy`, `reload`,
`drain`, `restore`.

- `dagster-multihost reload <location> [...]` reloads specific code locations
  (`--all` reloads the whole workspace). Dagster does **not** auto-reload when a
  remote gRPC server restarts, so call this after a remote code-location
  container is redeployed — e.g. as a Komodo post-deploy Procedure step.
- `dagster-multihost drain <location> [...] --state-file PATH` stops the
  location's running schedules/sensors and waits for active runs to finish,
  recording what it stopped. `restore --state-file PATH` re-enables exactly
  those. These let an external orchestrator wrap a remote redeploy safely:
  `drain → DeployStack → reload → restore`. The Dagster-aware core lives in
  `dagster_multihost_launcher/orchestration.py` (`WorkspaceOrchestrator`), which
  the `deploy` command also uses.
- `dagster-multihost check-env [--env-file PATH]` reports env vars the control
  plane needs — every `{env: NAME}` reference in `dagster.yaml` plus the
  launcher's bare `KEY` env_vars — and which are missing. Exits non-zero if any
  are. The launcher's `env_file`/inheritance only feeds *run containers*; the
  daemon/webserver's own env (e.g. Postgres creds) must be rendered into the
  control-plane stack (e.g. via Komodo Variables). Use as a pre-deploy gate.
- `dagster-multihost komodo-export [-o FILE]` generates a Komodo Resource Sync
  TOML skeleton (servers + stacks) from `dagster.yaml`. `komodo-verify <TOML>`
  diffs `dagster.yaml` topology against that TOML and exits non-zero on drift —
  a host or code location missing from / misplaced in Komodo would otherwise
  silently fall back to the DefaultRunLauncher. Convention: `[[server]]` name ==
  `host_name`, `[[stack]]` name == code location name, stack `server` == its host.
  Example artifacts live in [`komodo/`](komodo/).

### Run image resolution & validation

`launch_run` resolves each run's image from the `dagster/image` tag, else the
code location's `container_image` (from `DAGSTER_CURRENT_IMAGE`). The value is
trimmed and validated against the Docker reference grammar, so a malformed image
(trailing newline, `https://` scheme, uppercase repository, empty tag) fails
with a clear error instead of Docker's opaque 400 `invalid reference format`.

**Pin images via the build tool.** Have your image build (e.g. a Komodo Build)
set `DAGSTER_CURRENT_IMAGE` on the code-location server to a deterministic tag
(commit SHA or semver, not `:latest`). The gRPC server and the run containers it
spawns then use the identical pinned image, and deployments are reproducible.

## Networking Requirements

- Run containers on remote hosts need to reach Postgres on Host A (use real IP, not docker-compose service name)
- Code location gRPC ports must be accessible from Host A for webserver/daemon
- Remote Docker daemon needs TCP (port 2376 with TLS) or SSH access from Host A
- DefaultRunLauncher sends the daemon's `instance_ref` (including storage config with `env:` references) to remote gRPC servers — those env vars must be set on the remote host too
- If using Docker rootless on remote hosts, TLS typically runs on the root daemon (port 2376) — images must be built in the root context (`sudo docker build`)
