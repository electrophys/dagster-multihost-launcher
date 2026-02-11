# dagster-multihost-launcher

A composite Dagster run launcher that routes runs to multiple Docker daemons across different hosts, or falls back to the `DefaultRunLauncher` for non-Docker code locations.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Host A (control plane)                                      │
│                                                              │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ webserver  │  │  daemon  │  │ postgres │                  │
│  └────────────┘  └──────────┘  └──────────┘                  │
│                       │              ▲                       │
│                       │              │ (event storage)       │
│  ┌──────────────────┐ │              │                       │
│  │  admin code loc  │ │              │                       │
│  │  (cleanup/status)│ │              │                       │
│  └──────────────────┘ │              │                       │
│                       │              │                       │
└───────────────────────┼──────────────┼───────────────────────┘
                        │              │
          ┌─────────────┼──────────────┼──────────────┐
          │             ▼              │              │
          │  Host B (Docker)           │              │
          │                            │              │
          │  ┌──────────────────┐      │              │
          │  │ code loc gRPC    │      │              │
          │  │ (etl_pipelines)  │      │              │
          │  └──────────────────┘      │              │
          │                            │              │
          │  ┌──────────────────┐      │              │
          │  │ run container ◄──┼──────┘              │
          │  │ (created by      │                     │
          │  │  daemon via TCP) │                     │
          │  └──────────────────┘                     │
          │                                           │
          └───────────────────────────────────────────┘

          ┌───────────────────────────────────────────┐
          │  Host D (non-Docker)                      │
          │                                           │
          │  ┌──────────────────┐                     │
          │  │ code loc gRPC    │  (bare process)     │
          │  │ (reporting)      │                     │
          │  └──────────────────┘                     │
          │         ▲                                 │
          │         │ DefaultRunLauncher sends        │
          │         │ start_run gRPC; run executes    │
          │         │ here in the gRPC server process │
          └───────────────────────────────────────────┘
```

## How It Works

The launcher inspects each run's code location name and routes it:

1. **Code locations listed under `docker_hosts`** → creates a Docker container on the mapped remote daemon via the Docker TCP API
2. **All other code locations** → delegates to Dagster's `DefaultRunLauncher`, which sends a `start_run` gRPC call to the code location's gRPC server — the run executes as a subprocess on whichever host runs that gRPC server

This means you can mix Docker-based and non-Docker code locations freely. Any location not explicitly mapped to a Docker host automatically falls back to the DefaultRunLauncher.

## Installation

```bash
pip install dagster-multihost-launcher
```

The package must be installed in the Docker images for the **webserver** and **daemon** containers on Host A.

## Configuration

### dagster.yaml

```yaml
run_launcher:
  module: dagster_multihost_launcher
  class: MultiHostDockerRunLauncher
  config:
    default_env_vars:
      - DAGSTER_POSTGRES_USER
      - DAGSTER_POSTGRES_PASSWORD
      - DAGSTER_POSTGRES_DB
      - DAGSTER_POSTGRES_HOST

    docker_hosts:
      # Host A: containerized code locations on the control plane host.
      # Omitting docker_url connects to the local Docker daemon.
      - host_name: "host-a"
        location_names:
          - "local_pipelines"
        network: "dagster_network"

      # Host B: remote Docker host with TLS
      - host_name: "host-b"
        docker_url: "tcp://10.0.1.2:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
        location_names:
          - "etl_pipelines"
        network: "host_b_dagster_network"
        env_vars:
          - "WAREHOUSE_HOST=10.0.1.50"

    # Host D's "reporting" code location is NOT listed above, so it
    # uses DefaultRunLauncher — the run executes on Host D's gRPC server.
```

### Configuration Reference

**Top-level options:**

| Key | Type | Description |
|-----|------|-------------|
| `docker_hosts` | list | Remote Docker host configurations |
| `default_env_vars` | list[str] | Env vars passed to ALL run containers |
| `default_container_kwargs` | dict | Default `containers.create()` kwargs |
| `container_label_prefix` | str | Label prefix (default: `dagster`) |

**Per docker_host:**

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `host_name` | str | yes | Friendly name (used in tags/logs) |
| `docker_url` | str | no | Docker daemon URL (`tcp://`, `ssh://`, `unix://`). Omit to use the local daemon. |
| `location_names` | list[str] | yes | Code locations that run here |
| `tls` | dict | no | TLS config (ca_cert, client_cert, client_key, verify) |
| `network` | str | no | Docker network to attach run containers to |
| `networks` | list[str] | no | Multiple networks to attach to |
| `env_vars` | list[str] | no | Host-specific env vars |
| `container_kwargs` | dict | no | Host-specific `containers.create()` kwargs |
| `registry` | dict | no | Registry credentials (url, username, password) |

## Setting Up Remote Docker Daemons

Each remote host needs its Docker daemon exposed over TCP with TLS.

### 1. Generate TLS certificates

Use Docker's built-in TLS guide or a tool like `cfssl`. You need:
- A CA cert (`ca.pem`)
- Server cert + key for each Docker host
- Client cert + key for the Dagster daemon on Host A

### 2. Configure the Docker daemon

On each remote host, edit `/etc/docker/daemon.json`:

```json
{
  "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2376"],
  "tls": true,
  "tlscacert": "/etc/docker/tls/ca.pem",
  "tlscert": "/etc/docker/tls/server-cert.pem",
  "tlskey": "/etc/docker/tls/server-key.pem",
  "tlsverify": true
}
```

If using systemd, you may also need to override the ExecStart:

```bash
sudo systemctl edit docker.service
```

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

### 3. Alternative: SSH-based access

Instead of TLS over TCP, you can use SSH:

```yaml
docker_hosts:
  - host_name: "host-b"
    docker_url: "ssh://deploy@10.0.1.2"
    location_names: ["etl_pipelines"]
```

This requires the Dagster daemon container to have SSH client installed and the appropriate key mounted.

## Admin Code Location

The package includes pre-built assets for container cleanup and monitoring. On Host A, create an admin code location:

```python
# admin_location/__init__.py
from dagster_multihost_launcher import build_admin_definitions

defs = build_admin_definitions()  # every 5 min, cleanup after 1 min
```

This creates a single `multihost_admin_job` that first checks container status across all hosts, then cleans up old exited containers. The two assets (`multihost_container_status` → `multihost_container_cleanup`) run in sequence within the same job.

Since this code location is NOT listed under any `docker_hosts` entry, it runs via `DefaultRunLauncher` on Host A — which is where the Docker clients and TLS certs are configured.

**Important:** The admin container needs the `dagster.yaml` and TLS certs mounted, because the admin assets rehydrate the `MultiHostDockerRunLauncher` to talk to remote Docker daemons. See the example `docker-compose.yml` for the required volume mounts.

The cleanup max age is configurable per-run via the `multihost/cleanup_max_age_hours` run tag.

## Networking Considerations

### Run containers → Postgres

Run containers on remote hosts need to reach Postgres on Host A. Use Host A's real IP/hostname (not a docker-compose service name) for `DAGSTER_POSTGRES_HOST`. Make sure:
- Postgres port is published on Host A (on all interfaces, not just localhost)
- Firewall rules allow traffic from remote hosts

### DefaultRunLauncher and instance_ref

When `DefaultRunLauncher` sends a run to a remote gRPC server, it includes the daemon's `instance_ref` — the serialized storage config from `dagster.yaml`. If your storage config uses `env:` references (e.g., `env: DAGSTER_POSTGRES_HOST`), those env vars must also be set on the remote gRPC server's host. Use the same env var names but with values appropriate for that host (e.g., Host A's external IP instead of a Docker service name).

### Run containers → other services

If run containers need to talk to services in the same docker-compose stack on their host (e.g., a local Redis), attach them to the same network via the `network` config.

**Important:** Docker Compose prefixes network names with the project name. Either:
- Use an explicit `name:` in your docker-compose network definition
- Use the full prefixed name in your `dagster.yaml`

### Code location gRPC → Host A

The gRPC servers on remote hosts need their ports accessible from Host A (for the webserver and daemon to load definitions).

## Run Tags

The launcher tags each Docker run with:

| Tag | Description |
|-----|-------------|
| `multihost_docker/container_id` | Docker container ID |
| `multihost_docker/host_name` | Which Docker host the container is on |
| `multihost_docker/launcher_type` | `docker` or `default` |

These are used by `terminate()`, `check_run_worker_health()`, and the admin cleanup assets.

## Image Resolution

For Docker-launched runs, the image is resolved in this order:
1. `dagster/image` tag on the run
2. `DAGSTER_CURRENT_IMAGE` from the code location
3. Container image from the job code origin

Set `DAGSTER_CURRENT_IMAGE` in your code location's environment (see Host B docker-compose example).

## Logging

Dagster's run worker process (`dagster api execute_run`) writes structured events directly to Postgres. As long as the run container can reach Postgres, logs appear in the Dagster UI automatically — no log forwarding required.

For container-level failures (OOM, image pull errors, crashes), the `check_run_worker_health` method captures the last 25 lines of Docker logs and reports them as engine events.

## Examples

The root-level `dagster.yaml`, `workspace.yaml`, `docker-compose.yml`, and `host_b_docker-compose.yml` provide example configurations for a typical multi-host setup.

The `integration_test/` directory contains a complete working test setup across three hosts:
- `integration_test/host_a/` — Control plane (webserver, daemon, postgres, admin code location)
- `integration_test/host_b/` — Remote Docker host with TLS, running a dockerized test code location
- `integration_test/host_d/` — Bare-process host running a non-Docker gRPC server
