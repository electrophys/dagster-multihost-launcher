# Docker Setup Guide — Control Plane and Remote Worker Hosts

This guide walks through installing and configuring Docker for a `MultiHostDockerRunLauncher`
deployment, step by step, on **both** host roles:

- **Host A — the control plane.** Runs the Dagster webserver, daemon, and Postgres. The
  daemon talks to its own local Docker daemon (for local code locations) and to remote
  Docker daemons over the network (for remote code locations).
- **Hosts B, C, … — remote workers.** Run code-location gRPC servers. Their Docker daemons
  are driven *remotely* by the Dagster daemon on Host A, which creates run containers on them
  over the Docker TCP API.

> **TLS certificates.** This guide covers Docker installation, daemon configuration,
> networking, and verification. For the full step-by-step generation and distribution of the
> mutual-TLS certificates that secure the remote TCP connection, see
> [docker-tls-setup-guide.md](docker-tls-setup-guide.md). This guide tells you *where* the
> certs go and *when* you need them; that guide tells you *how* to create them.

The commands below target **Ubuntu / Debian** with systemd. Adjust the package manager and
paths for other distributions.

---

## 0. Plan your topology first

Before touching any host, write down the following — every later step refers back to it:

| Item | Example | Notes |
|------|---------|-------|
| Host A IP (reachable by workers) | `10.0.1.1` | Workers connect to Postgres here |
| Host B IP (reachable by Host A) | `10.0.1.2` | Host A connects to Docker + gRPC here |
| Host C IP | `10.0.1.3` | |
| Docker TLS port (remote daemons) | `2376` | Inbound on workers, from Host A only |
| gRPC code-location ports | `4001`, `4002`, … | Inbound on workers, from Host A only |
| Postgres port | `5432` (or `5433` external) | Inbound on Host A, from workers |

**Port summary — who must reach what:**

```
Host A daemon ──tcp/2376 (Docker TLS)──►  Host B / C   (create run containers)
Host A webserver+daemon ──tcp/4001+ (gRPC)──►  Host B / C   (load code locations)
Host B / C run containers ──tcp/5432 (Postgres)──►  Host A   (write run events/logs)
```

---

## 1. Install Docker Engine (all hosts)

Do this on Host A **and** every remote worker. Use the official Docker repository, not the
distro's bundled `docker.io` package, so you get a current Engine and Compose v2.

```bash
# Remove any old/conflicting packages
sudo apt remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true

# Set up Docker's apt repository
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Engine, CLI, containerd, Buildx, and Compose v2
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Verify and enable on boot:

```bash
sudo systemctl enable --now docker
sudo docker run --rm hello-world
```

**Optional — run docker without sudo** (the user is added to the `docker` group; note this
grants root-equivalent access):

```bash
sudo usermod -aG docker "$USER"
newgrp docker   # or log out and back in
```

> **Rootless Docker?** If you run Docker rootless on the remote workers, keep reading but note
> the caveats in [§4.5](#45-rootless-docker-caveats). In short: terminate TLS on the **root**
> daemon (port 2376) and build run images in the **root** context.

---

## 2. Configure Host A (the control plane)

On Host A, Docker is used in two ways:

1. The Dagster daemon creates **local** run containers (for any `docker_hosts` entry that
   points at the local daemon — i.e. with no `docker_url`, or `docker_url:
   unix:///var/run/docker.sock`).
2. The Dagster webserver/daemon/admin services themselves run as containers via Compose.

You do **not** need to expose Host A's Docker daemon over TCP — Host A is the client to the
remote daemons, not a server.

### 2.1 Give the Dagster daemon access to the local Docker socket

The daemon container creates local run containers through the host's Docker socket, so mount
it into the daemon (and admin) container. This is already wired in the example
[docker-compose.yml](docker-compose.yml):

```yaml
  daemon:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock   # talk to local Docker daemon
      - ./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml
      - ./workspace.yaml:/opt/dagster/dagster_home/workspace.yaml
      - ./certs:/certs:ro                            # client certs for remote daemons
```

> **Skip the local daemon entirely?** If Host A runs no Docker code locations of its own
> (everything is on remote workers or uses the `DefaultRunLauncher`), you can omit the socket
> mount and leave the local `host-a` entry out of `docker_hosts`. You still need the
> `./certs` mount for the remote daemons.

### 2.2 Place the client TLS certs

Host A authenticates to every remote daemon with a single **client** certificate. Put the
three files where Compose mounts them (`./certs` → `/certs` inside the containers):

```bash
mkdir -p ~/dagster-deployment/certs
cp ca.pem client-cert.pem client-key.pem ~/dagster-deployment/certs/
chmod 0400 ~/dagster-deployment/certs/client-key.pem
chmod 0444 ~/dagster-deployment/certs/ca.pem ~/dagster-deployment/certs/client-cert.pem
```

(Generating these files is covered in [docker-tls-setup-guide.md](docker-tls-setup-guide.md),
Steps 1 and 3.)

### 2.3 Create the local Docker network

If you map any code locations to the local Host A daemon, the run containers attach to a
network — create it (or let Compose create `dagster_network`, as in the example):

```bash
docker network create dagster_network
```

### 2.4 Expose Postgres to the workers

Run containers on remote hosts write run events/logs straight to Postgres on Host A, so
Postgres must be reachable from the workers — **published on all interfaces, not just
localhost**, and using Host A's real IP in the workers' config (never a Compose service
name). The example [docker-compose.yml](docker-compose.yml) already publishes it:

```yaml
  postgres:
    ports:
      - "5432:5432"   # 0.0.0.0:5432 — reachable from remote hosts
```

> The integration test publishes Postgres on `5433` externally to avoid clashing with a local
> Postgres; pick whatever is free and use that port in the workers' `DAGSTER_POSTGRES_PORT`.

### 2.5 Firewall (Host A)

Allow the workers to reach Postgres, and allow your own machine to reach the webserver:

```bash
sudo ufw allow from 10.0.1.2 to any port 5432 proto tcp comment "Postgres from host-b"
sudo ufw allow from 10.0.1.3 to any port 5432 proto tcp comment "Postgres from host-c"
sudo ufw allow 3000/tcp comment "Dagster webserver UI"
```

### 2.6 Bring up the control plane

```bash
cd ~/dagster-deployment
docker compose up -d --build
docker compose ps
```

---

## 3. Make run images available on the remote workers

This step is easy to miss. When the launcher creates a run container on Host B, **Docker on
Host B must be able to find the image.** The launcher does not build or push images for you —
it only runs `containers.create()` on the remote daemon.

The image is resolved (in order) from the `dagster/image` run tag, the
`DAGSTER_CURRENT_IMAGE` env var on the code location, or the job's code origin. Whatever it
resolves to must exist on the worker. You have two options:

- **Use a registry.** Push the image to a registry both hosts can reach, and configure
  `registry:` credentials on the `docker_hosts` entry so the daemon pulls it. This is the
  recommended approach for more than one worker.
- **Build the image locally on the worker.** Build it on Host B itself (and tag it to match
  `DAGSTER_CURRENT_IMAGE`), so the remote daemon already has it. Simple for a single worker.

Set `DAGSTER_CURRENT_IMAGE` on each code-location server to the image runs should use — see
the worker Compose example in [host_b_docker-compose.yml](host_b_docker-compose.yml):

```yaml
    environment:
      DAGSTER_CURRENT_IMAGE: "etl_pipelines:latest"
```

---

## 4. Configure each remote worker (Hosts B, C, …)

The goal here is to expose the worker's Docker daemon on TCP **with mutual TLS** so only Host A
can drive it, then run the code-location gRPC servers.

### 4.1 Install the server TLS certs

Each worker needs three files: the CA cert, and its **own** server cert + key. Put them in
`/etc/docker/tls/` (this matches the `daemon.json` below). Distribution is covered in
[docker-tls-setup-guide.md](docker-tls-setup-guide.md) Step 5; the short version:

```bash
sudo mkdir -p /etc/docker/tls
# copy ca.pem, this host's server cert + key into place, then:
sudo mv /tmp/ca.pem                  /etc/docker/tls/ca.pem
sudo mv /tmp/server-host-b-cert.pem  /etc/docker/tls/server-cert.pem
sudo mv /tmp/server-host-b-key.pem   /etc/docker/tls/server-key.pem
sudo chown -R root:root /etc/docker/tls
sudo chmod 0400 /etc/docker/tls/server-key.pem
sudo chmod 0444 /etc/docker/tls/ca.pem /etc/docker/tls/server-cert.pem
```

> The server cert's **Subject Alternative Names must include the exact IP/hostname** Host A
> uses in `docker_url` (e.g. `IP:10.0.1.2`). A mismatch is the #1 cause of TLS handshake
> failures — see the TLS guide, Step 2.3.

### 4.2 Configure `daemon.json` for TLS

```bash
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "tls": true,
  "tlsverify": true,
  "tlscacert": "/etc/docker/tls/ca.pem",
  "tlscert": "/etc/docker/tls/server-cert.pem",
  "tlskey": "/etc/docker/tls/server-key.pem"
}
EOF
```

> **Do not put `"hosts"` in `daemon.json` on a systemd host.** The `-H` flag in the systemd
> unit and the `hosts` key in `daemon.json` conflict and Docker will refuse to start. Set the
> listening addresses via the systemd override below instead.

### 4.3 Tell Docker to listen on TCP (systemd override)

```bash
sudo systemctl edit docker.service
```

Add:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd -H fd:// -H tcp://0.0.0.0:2376
```

The empty `ExecStart=` clears the default; the second line keeps the local socket
(`fd://`, via systemd socket activation) **and** adds the TLS-protected TCP listener on 2376.

Reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

Confirm it's listening:

```bash
sudo ss -tlnp | grep 2376
# LISTEN 0 4096 *:2376 *:* users:(("dockerd",...))
```

### 4.4 Create the run-container network

Run containers created by Host A attach to a network on this worker. Create it with an
**explicit name** so Compose's project-name prefix doesn't change it out from under your
`dagster.yaml`:

```bash
docker network create host_b_dagster_network
```

Or, in the worker's Compose file, pin the name (as the integration test does):

```yaml
networks:
  dagster_network:
    name: dagster_network   # explicit — no <project>_ prefix
    driver: bridge
```

Your `dagster.yaml` `network:` value for this host must match this exact name.

### 4.5 Rootless Docker caveats

If the worker runs Docker rootless:

- The **root** daemon is what typically listens on TCP 2376 with TLS — terminate TLS there,
  not on the rootless user daemon.
- Build run images in the **root** context (`sudo docker build …`) so the daemon that creates
  run containers can see them. An image built only in the rootless user's context is invisible
  to the root daemon.

### 4.6 Firewall (workers)

Lock the Docker TLS port and the gRPC ports to Host A only:

```bash
sudo ufw allow from 10.0.1.1 to any port 2376 proto tcp comment "Docker TLS from Host A"
sudo ufw allow from 10.0.1.1 to any port 4001 proto tcp comment "gRPC code location from Host A"
# repeat for each gRPC port this worker exposes (4002, ...)
```

Even though mutual TLS already rejects unauthorized clients, restricting the port at the
firewall is good defense in depth. **Never expose port 2376 to the public internet.**

### 4.7 Start the code-location gRPC servers

Bring up the worker's code locations (see [host_b_docker-compose.yml](host_b_docker-compose.yml)).
Each server needs:

- `DAGSTER_CURRENT_IMAGE` set to the run image ([§3](#3-make-run-images-available-on-the-remote-workers)).
- Postgres connection env pointing at **Host A's real IP** and the published port — not a
  Compose service name, because run containers won't share this stack's network with Host A.

```yaml
    environment:
      DAGSTER_CURRENT_IMAGE: "etl_pipelines:latest"
      DAGSTER_POSTGRES_HOST: "10.0.1.1"
      DAGSTER_POSTGRES_PORT: "5432"
```

> **`DefaultRunLauncher` env vars.** For any code location that falls back to the
> `DefaultRunLauncher` (runs execute inside the gRPC server process, not a container), the
> daemon ships its `instance_ref` — including storage config with `env:` references — to the
> remote server. Those same env var **names** must be set on the worker, with values correct
> for that host (e.g. Host A's external IP).

```bash
docker compose up -d --build
docker compose ps
```

---

## 5. Verify connectivity from Host A

### 5.1 Docker CLI over TLS

```bash
docker \
  --tlsverify \
  --tlscacert ~/dagster-deployment/certs/ca.pem \
  --tlscert  ~/dagster-deployment/certs/client-cert.pem \
  --tlskey   ~/dagster-deployment/certs/client-key.pem \
  -H tcp://10.0.1.2:2376 \
  info
```

You should see Host B's Docker system info. A handshake error usually means the IP isn't in
the server cert's SANs, or the CA doesn't match.

### 5.2 Python (exactly how the launcher connects)

```python
import docker
from docker.tls import TLSConfig

tls = TLSConfig(
    ca_cert="/home/you/dagster-deployment/certs/ca.pem",
    client_cert=(
        "/home/you/dagster-deployment/certs/client-cert.pem",
        "/home/you/dagster-deployment/certs/client-key.pem",
    ),
    verify=True,
)
client = docker.DockerClient(base_url="tcp://10.0.1.2:2376", tls=tls)
print(client.info()["Name"])        # -> host-b's hostname
print(client.containers.list())
```

### 5.3 gRPC and Postgres reachability

```bash
# From Host A — code location gRPC port open?
nc -zv 10.0.1.2 4001

# From a worker — can it reach Postgres on Host A?
nc -zv 10.0.1.1 5432
```

### 5.4 End-to-end

In the Dagster UI, launch a run for a code location mapped to a remote host. Confirm:

- A run container appears on the worker: `docker -H tcp://10.0.1.2:2376 … ps`
- The run's tags include `multihost_docker/host_name` and `multihost_docker/container_id`
- Logs stream into the UI (the run container reached Postgres on Host A)

---

## 6. Alternative: SSH instead of TLS over TCP

If you'd rather not expose a TCP port, the launcher can reach a remote daemon over SSH. No
`daemon.json`/systemd changes are needed on the worker, but the Dagster daemon container must
have an SSH client installed and the key mounted, and the remote user must be in the `docker`
group.

```yaml
docker_hosts:
  - host_name: "host-b"
    docker_url: "ssh://deploy@10.0.1.2"
    location_names: ["etl_pipelines"]
    network: "host_b_dagster_network"
```

---

## 7. Wire it all into `dagster.yaml`

The cert paths are as seen **inside** the daemon container (`./certs` → `/certs`). See the
example [dagster.yaml](dagster.yaml):

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
      - DAGSTER_POSTGRES_PORT

    docker_hosts:
      # Local daemon on Host A (no docker_url -> unix socket, no TLS)
      - host_name: "host-a"
        location_names: ["local_pipelines"]
        network: "dagster_network"

      # Remote worker over TLS
      - host_name: "host-b"
        docker_url: "tcp://10.0.1.2:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
          verify: true
        location_names: ["etl_pipelines", "ml_training"]
        network: "host_b_dagster_network"
```

Any code location **not** listed under `docker_hosts` automatically uses the
`DefaultRunLauncher`.

---

## 8. Checklist

**Host A (control plane)**
- [ ] Docker installed and running
- [ ] `/var/run/docker.sock` mounted into the daemon container (if running local Docker locations)
- [ ] Client certs (`ca.pem`, `client-cert.pem`, `client-key.pem`) in `./certs`, mounted to `/certs`
- [ ] Local Docker network created (if used)
- [ ] Postgres published on all interfaces; reachable from workers
- [ ] Firewall allows workers → Postgres, and you → webserver (3000)

**Each remote worker**
- [ ] Docker installed and running
- [ ] Server certs in `/etc/docker/tls/`, with the right IP/host in the cert SANs
- [ ] `daemon.json` TLS block (no `"hosts"` key)
- [ ] systemd override adds `-H tcp://0.0.0.0:2376`; daemon restarted and listening
- [ ] Run-container network created with an explicit, matching name
- [ ] Run image present (registry pull or built locally; `DAGSTER_CURRENT_IMAGE` set)
- [ ] gRPC servers up; Postgres env points at Host A's real IP
- [ ] Firewall allows Host A → 2376 and → gRPC ports only

**Verify**
- [ ] `docker -H tcp://<worker>:2376 … info` works from Host A
- [ ] A remote-mapped run launches a container on the worker and streams logs to the UI

---

## 9. Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `certificate signed by unknown authority` | `ca.pem` differs between Host A and the worker — use the same CA on both sides |
| `x509: certificate is valid for X, not Y` | The IP/host in `docker_url` isn't in the server cert SANs — regenerate the server cert |
| `connection refused` on 2376 | Docker isn't listening on TCP — check the systemd override and `ss -tlnp \| grep 2376` |
| `context deadline exceeded` / timeout | Firewall blocking 2376 — test with `nc -zv <worker> 2376` |
| Run fails with image-not-found | Image missing on the worker — push to a registry or build it there ([§3](#3-make-run-images-available-on-the-remote-workers)) |
| Run starts but no logs in UI | Run container can't reach Postgres — check Host A IP/port and firewall, not a Compose service name |
| Docker won't start after edits | Both `daemon.json` and systemd specify `-H` — keep `hosts`/`-H` only in the systemd override |

More TLS-specific troubleshooting is in
[docker-tls-setup-guide.md](docker-tls-setup-guide.md). Inspect the daemon log with:

```bash
sudo journalctl -u docker.service --no-pager -n 50
```
