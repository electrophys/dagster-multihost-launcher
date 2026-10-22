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
| `default_env_vars` | list[str] | Env vars passed to ALL run containers: `KEY=VALUE`, or bare `KEY` to copy Host A's value |
| `default_env_file` | str | `.env` file on Host A (read by the launcher) injected into ALL run containers |
| `default_container_kwargs` | dict | Default `containers.create()` kwargs |
| `log_url_template` | str | Link to each run's logs in an external viewer (e.g. Grafana), posted at launch. Placeholders: `{run_id}`, `{job_name}`, `{location}`, `{host_name}`, `{launch_ms}`. |
| `container_label_prefix` | str | Label prefix (default: `dagster`) |

**Per docker_host:**

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `host_name` | str | yes | Friendly name (used in tags/logs) |
| `docker_url` | str | no | Docker daemon URL (`tcp://`, `ssh://`, `unix://`). Omit to use the local daemon. |
| `location_names` | list[str] | yes | Code locations that run here |
| `tls` | dict | no | TLS config (ca_cert, client_cert, client_key, verify) |
| `network` | str | no | Docker network to attach run containers to. Leave this and `networks` unset to inherit the code server's networks; `"none"` for no networking. |
| `networks` | list[str] | no | Multiple networks to attach to. Overrides inheritance. |
| `env_vars` | list[str] | no | Host-specific env vars: `KEY=VALUE`, or bare `KEY` to copy Host A's value |
| `env_file` | str | no | Host-specific `.env` file, also on **Host A** (not the remote host) |
| `inherit_env_from_container` | str | no | Overrides code-server env inheritance, which is **on by default** (the server is found from the run's gRPC origin: by container name, else by published port). Give a container name — a `{location}` placeholder is supported — when the server doesn't publish its port, or `"off"` to disable inheritance. |
| `container_kwargs` | dict | no | Host-specific `containers.create()` kwargs |
| `registry` | dict | no | Registry credentials (url, username, password) |

## Environment Variables on Remote Hosts

Three kinds of process run on a remote host, and each gets its environment differently:

| Process | Gets its env from |
|---------|-------------------|
| Code-location gRPC server (Host B/C container) | Its own stack (Compose, Komodo). The launcher never sets it. |
| Run container (Host B/C) | The launcher, which builds it on Host A at launch time from the layers below |
| Run on a bare-process host (Host D) | The code server process it runs in. None of the launcher's env settings apply. |

### Run containers: where each value comes from

The launcher builds a run container's environment in the process that launches runs: the Dagster daemon on Host A with the `QueuedRunCoordinator` (as in the example config), or the webserver without it. Layers, lowest precedence first:

| # | Source | Resolved where | If missing |
|---|--------|----------------|------------|
| 1 | `default_env_file` | A file on **Host A**, read by the launcher | Warning logged, layer skipped |
| 2 | host `env_file` | Also a file on **Host A**, even though it is per-host. Nothing is read from the remote host's disk. | Warning logged, layer skipped |
| 3 | Code-server inheritance | The server container's `Config.Env` on the **remote** host, read over the Docker API. This is the env the container was created with, including its image's `ENV` lines, not anything the server process changes at runtime. | Warning logged, layer skipped |
| 4 | `default_env_vars` + host `env_vars` | `KEY=VALUE` is used literally. A bare `KEY` copies **Host A's** value of `KEY` from the launching process. | A bare `KEY` that is unset on Host A is **silently omitted** |
| 5 | `DAGSTER_RUN_JOB_NAME`, `DAGSTER_RUN_ID` | Set by the launcher | Always set |

The result is a snapshot taken at launch and written into the container's config. Editing an env file or redeploying the code server affects the next run, not runs already going. A run that's re-executed is a new launch, so it gets current values.

### Variables Dagster itself needs on the remote host

Each run worker and code server receives Host A's instance config (the `instance_ref`) with its `{env: NAME}` references still **unresolved**. They are resolved in the remote process, so `storage`, and `compute_logs` if configured, need their variables set there:

- **Run containers** need `DAGSTER_POSTGRES_*`, or whatever your `dagster.yaml` references, or the run fails at startup.
- **Code servers** need them too: sensors, schedules and their `context.log` use the same instance config.
- **Host D** needs them in its code server's environment (see `integration_test/host_d/start_grpc.ps1`).

The values must work **from the remote host**. A common trap is `DAGSTER_POSTGRES_HOST`: on Host A it is often a Compose service name such as `postgres`. Listing it as a bare `KEY` in `env_vars` copies that name into a container on Host B, where it doesn't resolve ("could not translate host name"). Set it explicitly instead, as `DAGSTER_POSTGRES_HOST=<Host A's IP>` in `env_vars`, in an env file, or in the code server's own env so that inheritance passes it on.

`DAGSTER_CURRENT_IMAGE` on the code server is not passed as env: the launcher uses it to choose the run container's image (see [Image Resolution](#image-resolution)).

### Secrets

Every value ends up in the run container's config, so anyone with access to that Docker daemon can read it with `docker inspect`. The env files sit on Host A in plain text. Keep secrets in a manager that renders the env files or the code server's env (e.g. Komodo Secrets), and limit who can reach the remote daemons. Their TLS client certificate effectively grants root.

### Checking what a run got

- **Control plane:** `dagster-multihost check-env` reports variables Host A is missing. That covers `{env: ...}` references and bare `KEY`s, which become silent omissions downstream.
- **A run container:** inspect it on its host. The container ID is in the run's `multihost_docker/container_id` tag:

  ```bash
  docker -H tcp://<host>:2376 --tlsverify ... inspect <container_id> \
    --format '{{range .Config.Env}}{{println .}}{{end}}'
  ```

- **Missing env files and failed inheritance** appear as warnings in the Dagster daemon's log on Host A, not in the run's event log.

## Deploying with Komodo

To run this under [Komodo](https://komo.do/) (deploying the control plane and
code-location stacks, with safe rollouts), see the end-to-end
[Komodo + Dagster Setup & Configuration Guide](komodo-dagster-setup-guide.md) and
the design rationale in [komodo-integration.md](komodo-integration.md).

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

### Integrating into an existing code location

If you already have a code location with its own `Definitions`, you can import the individual assets instead of using `build_admin_definitions`:

```python
from dagster_multihost_launcher import multihost_cleanup_asset, multihost_status_asset
from dagster import Definitions, ScheduleDefinition, define_asset_job

admin_job = define_asset_job(
    "admin_job",
    selection=[multihost_status_asset, multihost_cleanup_asset],
)
admin_schedule = ScheduleDefinition(job=admin_job, cron_schedule="0 */6 * * *")

defs = Definitions(
    assets=[my_asset_1, my_asset_2, multihost_status_asset, multihost_cleanup_asset],
    jobs=[my_job, admin_job],
    schedules=[my_schedule, admin_schedule],
)
```

## Networking Considerations

### Run containers → Postgres

Run containers on remote hosts need to reach Postgres on Host A. Use Host A's real IP/hostname (not a docker-compose service name) for `DAGSTER_POSTGRES_HOST`. Make sure:
- Postgres port is published on Host A (on all interfaces, not just localhost)
- Firewall rules allow traffic from remote hosts

### DefaultRunLauncher and instance_ref

When `DefaultRunLauncher` sends a run to a remote gRPC server, it includes Host A's `instance_ref`, whose `env:` references are resolved on the remote host. See [Variables Dagster itself needs on the remote host](#variables-dagster-itself-needs-on-the-remote-host).

### Run containers → other services

Run containers join the code-location server's own networks by default, so they can reach whatever the server reaches (Postgres, a local Redis in the same docker-compose stack) with nothing configured. Only network names are inherited, never the server's aliases.

Set `network` (or `networks`) on the host to override that — but note that Docker Compose prefixes network names with the project name, so an explicit value must either match a `name:` you set in the compose network definition or use the full prefixed name. Inheritance sidesteps that mismatch, which is the usual cause of a run container coming up unable to resolve Postgres. Use `network: "none"` for a run container with no networking.

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

Dagster produces two kinds of run logs, and they travel differently:

- **Event logs** — `context.log`, step events, materializations. The run worker writes these straight to Postgres, so they reach the UI from every host with nothing to configure.
- **Compute logs** — a step's raw stdout/stderr (the UI's stdout/stderr view). These are written by a *compute log manager*. Dagster's default, `LocalComputeLogManager`, keeps them on the disk of whichever host ran the step — inside a run container on Host B/C, or on Host D — so the webserver on Host A shows nothing, and a removed run container takes them with it.

Fix compute logs with a `compute_logs:` block in Host A's `dagster.yaml`. That config is part of the `instance_ref` every run worker receives, so it applies on all hosts at once. It also covers sensor and schedule `context.log` output, which the code-location server writes through the same manager. So every run image **and every code-location server** (including Host D) must have the chosen manager's package installed and its credentials in env (the `DAGSTER_POSTGRES_*` vars for the Postgres option). Otherwise runs fail to start, and sensors or schedules that call `context.log` fail to evaluate.

### Option 1: Postgres (no extra infrastructure)

`PostgresComputeLogManager`, shipped in this package, stores logs in the Dagster database every run worker already reaches:

```yaml
storage:
  postgres:
    postgres_db: &postgres_db
      username: {env: DAGSTER_POSTGRES_USER}
      # ...

compute_logs:
  module: dagster_multihost_launcher.compute_log_manager
  class: PostgresComputeLogManager
  config:
    postgres_db: *postgres_db   # or postgres_url
    upload_interval: 30         # stream in-progress logs; default: on step completion
    max_log_bytes: 10485760     # per-file cap; longer logs keep their tail
```

It creates a `multihost_compute_logs` table on first use. Install `dagster-multihost-launcher` in every code-location image and on Host D.

### Option 2: S3 via `dagster-aws`

```yaml
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket: "my-dagster-compute-logs"
    upload_interval: 30
    # endpoint_url: "http://10.0.1.1:9000"   # MinIO / Garage / SeaweedFS instead of AWS
```

Install `dagster-aws` in every code-location image, on Host D, and in the webserver image. Run containers need credentials: add `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` to `default_env_vars` (or let them be inherited from the code server's env). `dagster-multihost check-env` reports any missing on the control plane.

### Python `logging`

Records from Python loggers your code uses can go into the event log too, which needs no compute log manager at all:

```yaml
python_logs:
  python_log_level: INFO
  managed_python_loggers:
    - my_project
```

This doesn't capture `print()` or subprocess output; use `compute_logs` for those.

### Loki / Grafana (via Alloy)

Docker already captures everything the Dagster UI misses — sensor `print()`s, run-worker startup errors, code-server and daemon logs, and step output lost to a crash or truncated by the compute log manager. Dagster echoes captured step output to the process's real stdout too, so container logs hold the complete picture. [`loki/`](loki/) has ready-to-adapt configs to get it into Loki keyed by run:

- [`loki/docker-host.alloy`](loki/docker-host.alloy) — for Hosts A–C, in two parts:
  - **Run containers** are read from Docker's `json-file` logs on disk, not through the Docker API. The API source only starts tailing a container about 10s after it starts (it batches target updates), and it only lists running containers, so it misses every run shorter than that. A `json-file` log stays on disk until the container is removed, and the launcher keeps exited run containers until cleanup. The launcher's labels (`dagster/run_id`, `job_name`, `location_name`, `host_name`) come from each line's `attrs`, which Docker writes when the container is created with `labels-regex: ^dagster/`. The example [`dagster.yaml`](dagster.yaml) sets that through `default_container_kwargs.log_config`. **A run container without it is not collected.**
  - **Everything else** (code servers, webserver, daemon) is tailed through the Docker API, with the run ID parsed from Dagster's console lines where present.

  `dagster_run_id` becomes **structured metadata**, not an index label, because it is unique per run (needs Loki 3.x). Alloy needs `/var/run/docker.sock` and `/var/lib/docker/containers` mounted read-only.
- [`loki/bare-process-host.alloy`](loki/bare-process-host.alloy) — for Host D. Tails the code server's redirected output file, tagging lines with the run ID from Dagster's console format.
- [`loki/grafana-datasource.yaml`](loki/grafana-datasource.yaml) — a Loki data source whose derived field links each line's run ID back to the run in the Dagster UI.

Find a run's logs with `{host=~".+"} | dagster_run_id="<run id>"`.

To link the other way, set `log_url_template` on the launcher. Every run — Docker or DefaultRunLauncher — gets an engine event at launch whose `logs` metadata links to the URL, with `{run_id}`, `{job_name}`, `{location}`, `{host_name}` and `{launch_ms}` filled in (URL-encoded; other braces are left alone, so Grafana's JSON-in-URL works). The example [`dagster.yaml`](dagster.yaml) has one that opens Grafana Explore on the query above.

The easiest way to make your own: run the query in Grafana Explore, use **Share → Copy link**, then replace the run ID with `{run_id}` and the start time with `{launch_ms}`.

Caveats:
- Containers other than run containers are discovered every 10s and read from their start, so only one that starts and exits within about 20s is missed. Code servers and the daemon are long-lived, so this rarely matters.
- In a step's raw `print()` output, the run ID is only known from the container label. On Host D such lines have no run ID; use the Dagster UI's stdout/stderr view for those.

### Container-level failures

For failures outside the run worker (OOM, image pull errors, crashes), `check_run_worker_health` captures the last 25 lines of Docker logs and reports them as engine events.

## Examples

The root-level `dagster.yaml`, `workspace.yaml`, `docker-compose.yml`, and `host_b_docker-compose.yml` provide example configurations for a typical multi-host setup.

The `integration_test/` directory contains a complete working test setup across three hosts:
- `integration_test/host_a/` — Control plane (webserver, daemon, postgres, admin code location)
- `integration_test/host_b/` — Remote Docker host with TLS, running a dockerized test code location
- `integration_test/host_d/` — Bare-process host running a non-Docker gRPC server
