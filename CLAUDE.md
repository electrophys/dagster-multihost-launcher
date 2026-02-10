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
    default_container_kwargs: {}  # Default containers.create() kwargs
    docker_hosts:
      - host_name: "host-b"
        docker_url: "tcp://..."   # or ssh://user@host
        tls: {...}                # Optional TLS config
        location_names: [...]     # Code locations that run here
        network: "..."            # Docker network for containers
        env_vars: [...]           # Host-specific env vars
        container_kwargs: {}      # Host-specific container overrides
        registry: {...}           # Optional registry credentials
```

## Networking Requirements

- Run containers on remote hosts need to reach Postgres on Host A (use real IP, not docker-compose service name)
- Code location gRPC ports must be accessible from Host A for webserver/daemon
- Remote Docker daemon needs TCP (port 2376 with TLS) or SSH access from Host A
- DefaultRunLauncher sends the daemon's `instance_ref` (including storage config with `env:` references) to remote gRPC servers — those env vars must be set on the remote host too
- If using Docker rootless on remote hosts, TLS typically runs on the root daemon (port 2376) — images must be built in the root context (`sudo docker build`)
