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
          │                                           │
          │  Runs execute on Host A via               │
          │  DefaultRunLauncher (subprocess)          │
          └───────────────────────────────────────────┘
```

## How It Works

The launcher inspects each run's code location name and routes it:

1. **Code locations listed under `docker_hosts`** → creates a Docker container on the mapped remote daemon via the Docker TCP API
2. **All other code locations** → delegates to Dagster's `DefaultRunLauncher` (spawns a subprocess on the daemon host)

This means you can mix Docker-based and non-Docker code locations freely. Any location not explicitly mapped to a Docker host automatically falls back to local execution.

## Installation

```bash
pip install -e ./dagster-multihost-launcher
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
      - host_name: "host-b"
        docker_url: "tcp://10.0.1.2:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
        location_names:
          - "etl_pipelines"
          - "ml_training"
        network: "host_b_dagster_network"
        env_vars:
          - "WAREHOUSE_HOST=10.0.1.50"

      - host_name: "host-c"
        docker_url: "tcp://10.0.1.3:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
        location_names:
          - "analytics"
        network: "host_c_dagster_network"
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
| `docker_url` | str | yes | Docker daemon URL (`tcp://`, `ssh://`, `unix://`) |
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

defs = build_admin_definitions(
    cleanup_cron="0 */6 * * *",   # clean up every 6 hours
    status_cron="0 * * * *",       # status check every hour
)
```

Since this code location is NOT listed under any `docker_hosts` entry, it automatically runs via `DefaultRunLauncher` on Host A — which is exactly where the Docker clients are configured.

The cleanup asset removes exited containers older than 24 hours (configurable via the `multihost/cleanup_max_age_hours` run tag).

## Networking Considerations

### Run containers → Postgres

Run containers on remote hosts need to reach Postgres on Host A. Use Host A's real IP/hostname (not a docker-compose service name) for `DAGSTER_POSTGRES_HOST`. Make sure:
- Postgres port (5432) is published on Host A
- Firewall rules allow traffic from Host B/C

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

See the `examples/` directory for complete setups:

- `examples/host_a/` — Control plane (webserver, daemon, postgres, admin code location)
- `examples/host_b/` — Remote Docker worker host with code locations
