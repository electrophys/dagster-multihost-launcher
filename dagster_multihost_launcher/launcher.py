"""
MultiHost Docker Run Launcher for Dagster.

Routes runs to different Docker daemons (or the DefaultRunLauncher) based on
which code location the run belongs to.

Typical setup:
  - Host A: webserver, daemon, postgres, admin code location (default launcher)
  - Host B, C: code location gRPC servers in Docker, runs launch as containers
  - Host D: code location as bare process, runs use default launcher on Host A
"""

import logging
import re
import threading
import time
import os
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import docker
from docker.tls import TLSConfig

from dagster import (
    Field,
    MetadataValue,
    Noneable,
    StringSource,
    _check as check,
)
from dagster._core.events import EngineEventData
from dagster._core.launcher import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.launcher.default_run_launcher import DefaultRunLauncher
from dagster._core.storage.dagster_run import DagsterRun
from dagster._cli.api import ExecuteRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData

logger = logging.getLogger("dagster_multihost_launcher")

# Tags written onto DagsterRun so we can find containers later
TAG_CONTAINER_ID = "multihost_docker/container_id"
TAG_HOST_NAME = "multihost_docker/host_name"
TAG_LAUNCHER_TYPE = "multihost_docker/launcher_type"

LAUNCHER_TYPE_DOCKER = "docker"
LAUNCHER_TYPE_DEFAULT = "default"

# A gRPC origin host that is an IP (or localhost) names a machine, not a
# container, so it is no use as a container lookup during env inheritance.
_IP_ADDRESS_RE = re.compile(
    r"^(?:\d{1,3}(?:\.\d{1,3}){3}|\[?[0-9a-fA-F:]+\]?|localhost)$"
)

# Sentinel value for inherit_env_from_container that disables inheritance
# (including auto-discovery), for deployments that want run containers to get
# only what env_file/env_vars provide.
INHERIT_OFF = "off"

# Distinguishes "caller has not resolved the code server yet" from "there is no
# code server", so a launch can resolve it once and share it.
_UNRESOLVED = object()

# Docker image reference grammar (based on distribution/reference): an optional
# registry domain (host[:port] / IP[:port] / localhost), a lowercase repository
# path, and an optional :tag and/or @digest. Used to reject malformed images
# before they hit the daemon as an opaque "invalid reference format" 400.
_REF_DOMAIN = (
    r"(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+(?::[0-9]+)?"
    r"|[a-zA-Z0-9]+:[0-9]+"
    r"|localhost(?::[0-9]+)?)"
)
_REF_PATH = r"[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*)*"
_REF_TAG = r"[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}"
_REF_DIGEST = r"[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*:[0-9a-fA-F]{32,}"
_IMAGE_REFERENCE_RE = re.compile(
    rf"^(?:{_REF_DOMAIN}/)?{_REF_PATH}(?::{_REF_TAG})?(?:@{_REF_DIGEST})?$"
)


class _LazyDockerClient:
    """A Docker client that connects on first use.

    docker-py contacts the daemon when a client is created, and every process
    that loads the instance config builds the launcher: the webserver, the
    daemon, the admin job, the CLI. Connecting up front made all of them need
    every Docker host reachable, so one host down (or one missing socket or
    cert) broke the launcher everywhere, including runs for healthy hosts and
    DefaultRunLauncher runs. Connecting on first use confines a failure to the
    operations on that host, each of which already handles errors. A failed
    connection is not cached, so the next use retries.
    """

    def __init__(self, factory: Any, host_name: str):
        self._factory = factory
        self._host_name = host_name
        self._client: Optional[docker.DockerClient] = None
        self._lock = threading.Lock()

    def _get(self) -> docker.DockerClient:
        with self._lock:
            if self._client is None:
                self._client = self._factory()
            return self._client

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get(), name)

    def __repr__(self) -> str:
        state = "connected" if self._client is not None else "not connected"
        return f"<_LazyDockerClient host={self._host_name!r} {state}>"


class MultiHostDockerRunLauncher(RunLauncher, ConfigurableClass):
    """A composite run launcher that routes runs to remote Docker daemons
    or falls back to the DefaultRunLauncher, based on code location name.

    Code locations listed under ``docker_hosts`` will have their runs launched
    as Docker containers on the corresponding remote daemon. All other code
    locations use the DefaultRunLauncher (subprocess on the daemon host).
    """

    def __init__(
        self,
        docker_hosts: Optional[List[Dict[str, Any]]] = None,
        default_env_vars: Optional[List[str]] = None,
        default_env_file: Optional[str] = None,
        default_container_kwargs: Optional[Dict[str, Any]] = None,
        container_label_prefix: str = "dagster",
        log_url_template: Optional[str] = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        super().__init__()
        self._inst_data = inst_data
        self._default_env_vars = default_env_vars or []
        self._default_env_file = default_env_file
        self._default_container_kwargs = default_container_kwargs or {}
        self._container_label_prefix = container_label_prefix
        self._log_url_template = log_url_template

        # Build host map: location_name -> host config + Docker client
        self._docker_host_map: Dict[str, Dict[str, Any]] = {}
        # Also keep a name -> client map for cleanup operations
        self._docker_clients: Dict[str, docker.DockerClient] = {}

        for host_cfg in docker_hosts or []:
            host_name = host_cfg["host_name"]
            client = _LazyDockerClient(
                lambda cfg=host_cfg: self._build_docker_client(cfg), host_name
            )
            self._docker_clients[host_name] = client

            for loc in host_cfg["location_names"]:
                self._docker_host_map[loc] = {
                    "host_name": host_name,
                    "client": client,
                    "network": host_cfg.get("network"),
                    "networks": host_cfg.get("networks"),
                    "container_kwargs": host_cfg.get("container_kwargs", {}),
                    "env_vars": host_cfg.get("env_vars", []),
                    "env_file": host_cfg.get("env_file"),
                    "inherit_env_from_container": host_cfg.get(
                        "inherit_env_from_container"
                    ),
                    "registry": host_cfg.get("registry"),
                }

        # The fallback launcher for non-Docker locations
        self._default_launcher = DefaultRunLauncher()

    @staticmethod
    def _build_docker_client(host_cfg: Dict[str, Any]) -> docker.DockerClient:
        """Create a Docker client from host configuration.

        If ``docker_url`` is omitted, connects to the local Docker daemon
        via ``docker.from_env()`` (reads DOCKER_HOST or defaults to the
        local socket). This is useful for running containerized code
        locations on Host A alongside the Dagster control plane.
        """
        docker_url = host_cfg.get("docker_url")

        if not docker_url:
            try:
                return docker.from_env()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to create Docker client for host "
                    f"'{host_cfg.get('host_name')}' (local Docker daemon): {e}"
                )

        kwargs: Dict[str, Any] = {"base_url": docker_url}

        tls_cfg = host_cfg.get("tls")
        if tls_cfg:
            # Validate TLS file paths early to surface helpful errors
            host_name = host_cfg.get("host_name", docker_url or "<unknown>")
            ca = tls_cfg.get("ca_cert")
            cert = tls_cfg.get("client_cert")
            key = tls_cfg.get("client_key")

            for path, label in [
                (ca, "ca_cert"),
                (cert, "client_cert"),
                (key, "client_key"),
            ]:
                if path and not os.path.exists(path):
                    raise FileNotFoundError(
                        f"TLS {label} file '{path}' for Docker host '{host_name}' does not exist"
                    )

            kwargs["tls"] = TLSConfig(
                ca_cert=ca,
                client_cert=(cert, key),
                verify=tls_cfg.get("verify", True),
            )

        # Support ssh:// URLs (docker SDK handles this natively)
        if docker_url.startswith("ssh://"):
            kwargs["use_ssh_client"] = True

        try:
            return docker.DockerClient(**kwargs)
        except Exception as e:
            base = kwargs.get("base_url")
            raise RuntimeError(
                f"Failed to create Docker client for host '{host_cfg.get('host_name', base)}' (base_url={base}): {e}"
            )

    # -------------------------------------------------------------------------
    # ConfigurableClass interface
    # -------------------------------------------------------------------------

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls):
        tls_config_schema = {
            "ca_cert": Field(StringSource, is_required=False),
            "client_cert": Field(StringSource, is_required=True),
            "client_key": Field(StringSource, is_required=True),
            "verify": Field(bool, default_value=True, is_required=False),
        }

        registry_schema = {
            "url": Field(StringSource, is_required=True),
            "username": Field(StringSource, is_required=True),
            "password": Field(StringSource, is_required=True),
        }

        docker_host_schema = {
            "host_name": Field(
                str,
                description="Friendly name for this Docker host (used in tags and logs).",
            ),
            "docker_url": Field(
                StringSource,
                is_required=False,
                description=(
                    "Docker daemon URL. Examples: "
                    "tcp://10.0.0.2:2376, ssh://user@host, unix:///var/run/docker.sock. "
                    "If omitted, connects to the local Docker daemon (useful for "
                    "running containerized code locations on the control plane host)."
                ),
            ),
            "location_names": Field(
                [str],
                description="Code location names that should run on this host.",
            ),
            "tls": Field(
                Noneable(tls_config_schema),
                default_value=None,
                is_required=False,
                description="TLS configuration for the Docker daemon connection.",
            ),
            "network": Field(
                Noneable(str),
                default_value=None,
                is_required=False,
                description=(
                    "Docker network to attach run containers to. Leave this and "
                    "'networks' unset to inherit the code-location server's own "
                    "networks, so runs reach whatever the server reaches without "
                    "naming a Compose-generated network here. Use 'none' for a "
                    "run container with no networking."
                ),
            ),
            "networks": Field(
                Noneable([str]),
                default_value=None,
                is_required=False,
                description=(
                    "Multiple Docker networks to attach run containers to. "
                    "Overrides inheritance of the code server's networks."
                ),
            ),
            "env_vars": Field(
                [str],
                default_value=[],
                is_required=False,
                description=(
                    "Additional env vars for containers on this host. "
                    "Format: KEY=VALUE or KEY (pulled from daemon environment)."
                ),
            ),
            "env_file": Field(
                Noneable(str),
                default_value=None,
                is_required=False,
                description=(
                    "Path (on the daemon host) to a .env file whose KEY=VALUE "
                    "lines are injected into run containers for this host. "
                    "Overrides default_env_file; overridden by env_vars."
                ),
            ),
            "inherit_env_from_container": Field(
                Noneable(str),
                default_value=None,
                is_required=False,
                description=(
                    "Name of the code-location server container on this host to "
                    "inherit the environment from (its Config.Env is merged into "
                    "run containers). Supports a {location} placeholder, e.g. "
                    "'code-{location}'. Resolved over this host's Docker client. "
                    "Leave unset to auto-discover the server from the code "
                    "location's gRPC origin -- by container name, else by "
                    "published port -- which is the usual case and needs no "
                    "naming convention. Set to 'off' to disable inheritance."
                ),
            ),
            "container_kwargs": Field(
                Noneable(dict),
                default_value=None,
                is_required=False,
                description="Additional kwargs passed to docker containers.create().",
            ),
            "registry": Field(
                Noneable(registry_schema),
                default_value=None,
                is_required=False,
                description="Docker registry credentials for pulling images on this host.",
            ),
        }

        return {
            "docker_hosts": Field(
                [docker_host_schema],
                default_value=[],
                is_required=False,
                description="List of remote Docker hosts and their code location mappings.",
            ),
            "default_env_vars": Field(
                [str],
                default_value=[],
                is_required=False,
                description="Env vars passed to ALL Docker run containers.",
            ),
            "default_env_file": Field(
                Noneable(str),
                default_value=None,
                is_required=False,
                description=(
                    "Path (on the daemon host) to a .env file injected into ALL "
                    "Docker run containers. Lowest precedence; a central place for "
                    "env vars managed by an external tool (e.g. Komodo)."
                ),
            ),
            "default_container_kwargs": Field(
                Noneable(dict),
                default_value=None,
                is_required=False,
                description="Default container_kwargs for all Docker hosts.",
            ),
            "container_label_prefix": Field(
                str,
                default_value="dagster",
                is_required=False,
                description="Prefix for labels applied to run containers.",
            ),
            "log_url_template": Field(
                Noneable(str),
                default_value=None,
                is_required=False,
                description=(
                    "URL of this run's logs in an external viewer (e.g. Grafana "
                    "Explore over Loki), posted as a link on every run at launch. "
                    "Placeholders: {run_id}, {job_name}, {location}, "
                    "{host_name} (empty for DefaultRunLauncher runs) and "
                    "{launch_ms} (launch time, epoch milliseconds). Values are "
                    "URL-encoded; no other braces are interpreted, so a "
                    "Grafana URL's JSON can be pasted as-is."
                ),
            ),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def register_instance(self, instance):
        """Called by Dagster to register the instance. We forward to the
        default launcher as well so it can function properly."""
        super().register_instance(instance)
        self._default_launcher.register_instance(instance)

    # -------------------------------------------------------------------------
    # Routing logic
    # -------------------------------------------------------------------------

    def _get_location_name(self, run: DagsterRun) -> Optional[str]:
        """Extract the code location name from a run."""
        origin = run.remote_job_origin
        if origin is None:
            return None
        return origin.location_name

    def _is_docker_location(self, run: DagsterRun) -> bool:
        """Check whether this run's code location is mapped to a Docker host."""
        loc = self._get_location_name(run)
        return loc is not None and loc in self._docker_host_map

    def _get_docker_host(self, run: DagsterRun) -> Dict[str, Any]:
        """Get the Docker host config for a run. Raises if not found."""
        loc = self._get_location_name(run)
        host_info = self._docker_host_map.get(loc)
        if not host_info:
            raise Exception(
                f"No Docker host configured for code location '{loc}'. "
                f"Known Docker locations: {list(self._docker_host_map.keys())}"
            )
        return host_info

    # -------------------------------------------------------------------------
    # Environment variable helpers
    # -------------------------------------------------------------------------

    def _build_env_vars(
        self,
        run: DagsterRun,
        host_info: Dict[str, Any],
        server_container: Any = _UNRESOLVED,
    ) -> Dict[str, str]:
        """Build environment variables for a run container.

        Layered from lowest to highest precedence so a central source can be
        overridden by progressively more specific config:

        1. ``default_env_file``                  — shared file for all hosts
        2. host ``env_file``                     — per-host file
        3. the code-location server's env        — see ``inherit_env_from_container``
        4. ``default_env_vars`` + host ``env_vars`` — explicit KEY=VALUE / KEY
        5. Dagster-internal vars (always set last)

        Variables in ``env_vars`` given as just ``KEY`` (no ``=``) are pulled
        from the daemon's own process environment.

        ``server_container`` is the already-resolved code server (``None`` if
        there isn't one), so a launch shares one lookup between env and network
        inheritance; left unset, it is resolved here.
        """
        env: Dict[str, str] = {}

        # 1 + 2: env files (default, then host-specific)
        if self._default_env_file:
            env.update(self._read_env_file(self._default_env_file))
        host_env_file = host_info.get("env_file")
        if host_env_file:
            env.update(self._read_env_file(host_env_file))

        # 3: inherit env from the code-location server container on this host.
        # See _find_code_server_container for how the server is resolved.
        if server_container is _UNRESOLVED:
            server_container = self._find_code_server_container(run, host_info)
        if server_container is not None:
            env_list = (server_container.attrs.get("Config", {}) or {}).get("Env") or []
            env.update(self._env_from_list(env_list))

        # 4: explicit env vars (default then host) override files/inheritance
        all_vars = self._default_env_vars + host_info.get("env_vars", [])
        for var in all_vars:
            if "=" in var:
                key, val = var.split("=", 1)
                env[key] = val
            else:
                val = os.environ.get(var)
                if val is not None:
                    env[var] = val

        # 5: Dagster-internal env vars the run worker needs (always win)
        env["DAGSTER_RUN_JOB_NAME"] = run.job_name
        if run.run_id:
            env["DAGSTER_RUN_ID"] = run.run_id

        return env

    @staticmethod
    def _read_env_file(path: str) -> Dict[str, str]:
        """Parse a .env file into a dict.

        Skips blank lines and ``#`` comments, tolerates a leading ``export``,
        splits on the first ``=``, and strips a single layer of matching quotes
        from the value. Missing/unreadable files log a warning and yield ``{}``
        rather than failing the run.
        """
        result: Dict[str, str] = {}
        try:
            with open(path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            logger.warning("env_file '%s' not found; skipping", path)
            return result
        except OSError as e:
            logger.warning("Could not read env_file '%s': %s", path, e)
            return result

        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key:
                result[key] = val
        return result

    def _inherit_server_env(
        self,
        run: DagsterRun,
        host_info: Dict[str, Any],
        container_template: str,
    ) -> Optional[Any]:
        """Look up the code-location server container by an explicit name so run
        containers can inherit from it (e.g. whatever Komodo injected).

        ``container_template`` may contain a ``{location}`` placeholder, which is
        filled with this run's code location name. Failures (container missing,
        daemon unreachable) log a warning and yield ``None``.
        """
        location = self._get_location_name(run) or ""
        container_name = container_template.format(location=location)
        client = host_info.get("client")
        host_name = host_info.get("host_name")
        if client is None:
            return None

        try:
            return client.containers.get(container_name)
        except docker.errors.NotFound:
            logger.warning(
                "inherit_env_from_container '%s' not found on host '%s'; skipping",
                container_name,
                host_name,
            )
            return None
        except Exception as e:
            logger.warning(
                "Could not inherit env from container '%s' on host '%s': %s",
                container_name,
                host_name,
                e,
            )
            return None

    def _find_code_server_container(
        self, run: DagsterRun, host_info: Dict[str, Any]
    ) -> Optional[Any]:
        """The code-location server container this run's container should be
        modelled on, or ``None``.

        Resolved once per launch and shared by env and network inheritance:
        an explicit ``inherit_env_from_container`` names it, ``"off"`` opts out
        of inheriting anything from the server, and otherwise it is discovered
        from the run's gRPC origin.
        """
        inherit_from = host_info.get("inherit_env_from_container")
        if inherit_from and inherit_from.strip().lower() == INHERIT_OFF:
            return None
        if inherit_from:
            return self._inherit_server_env(run, host_info, inherit_from)
        return self._discover_server_container(run, host_info)

    def _discover_server_container(
        self, run: DagsterRun, host_info: Dict[str, Any]
    ) -> Optional[Any]:
        """Find this run's code-location server container without being told its
        name.

        The workspace entry for a code location carries the gRPC host and port
        the control plane dials, which identify the server two ways:

        1. the origin host, when it is a name rather than an IP — that is the
           container's name/alias on a shared Docker network
        2. the container publishing the origin port on this host — the case for
           remote locations, which must publish a port Host A can reach

        Between them, inheritance works with no config and no container-naming
        convention. Failures (no origin port, daemon unreachable, no match) log
        and yield ``None`` — the same non-fatal contract as an explicit
        ``inherit_env_from_container``.
        """
        client = host_info.get("client")
        host_name = host_info.get("host_name")
        port = self._code_server_grpc_port(run)
        if client is None or port is None:
            return None

        # 1: the origin host as a container name (shared-network code servers
        # publish no port, so the port scan below cannot find them).
        origin_host = self._code_server_grpc_host(run)
        if origin_host and not _IP_ADDRESS_RE.match(origin_host):
            try:
                container = client.containers.get(origin_host)
            except Exception:
                container = None
            if container is not None:
                logger.debug(
                    "Code server for host '%s' is '%s' (matched gRPC origin host)",
                    host_name,
                    origin_host,
                )
                return container

        # 2: the container publishing the gRPC port
        try:
            containers = client.containers.list()
        except Exception as e:
            logger.warning(
                "Could not list containers on host '%s' to discover the code "
                "server for inheritance: %s",
                host_name,
                e,
            )
            return None

        for container in containers:
            if self._publishes_host_port(container, port):
                logger.debug(
                    "Code server for host '%s' is '%s' (publishes gRPC port %s)",
                    host_name,
                    getattr(container, "name", "?"),
                    port,
                )
                return container

        logger.warning(
            "No container on host '%s' publishes gRPC port %s for code location "
            "'%s'; run containers inherit neither its env nor its networks. Set "
            "inherit_env_from_container to the server's container name, or '%s' "
            "to silence this.",
            host_name,
            port,
            self._get_location_name(run),
            INHERIT_OFF,
        )
        return None

    def _resolve_networks(
        self, host_info: Dict[str, Any], server_container: Any
    ) -> List[str]:
        """The networks to attach a run container to.

        Explicit host ``network``/``networks`` win. With neither set, the run
        joins the networks the code-location server is on, so it can reach
        Postgres and everything else the server reaches without anyone
        transcribing a Compose-generated network name into ``dagster.yaml``.
        (Set ``network: "none"`` for a run container with no networking.)

        Only network *names* are inherited, never the server's aliases: an alias
        is the code location's service DNS name, and duplicating it would let a
        run container answer gRPC lookups meant for the server.
        """
        network = host_info.get("network")
        networks = list(host_info.get("networks") or [])
        if network:
            return [network] + [n for n in networks if n != network]
        if networks:
            return networks
        if server_container is None:
            return []

        network_settings = server_container.attrs.get("NetworkSettings") or {}
        inherited = list((network_settings.get("Networks") or {}).keys())
        if inherited:
            logger.debug(
                "Run container on host '%s' inherits networks %s from '%s'",
                host_info.get("host_name"),
                inherited,
                getattr(server_container, "name", "?"),
            )
        return inherited

    @staticmethod
    def _code_server_grpc_host(run: DagsterRun) -> Optional[str]:
        """The host the control plane dials for this run's code location."""
        origin = getattr(run, "remote_job_origin", None)
        repo_origin = getattr(origin, "repository_origin", None)
        location_origin = getattr(repo_origin, "code_location_origin", None)
        host = getattr(location_origin, "host", None)
        return host if isinstance(host, str) and host else None

    @staticmethod
    def _code_server_grpc_port(run: DagsterRun) -> Optional[int]:
        """The port the control plane dials for this run's code location, from
        the workspace's gRPC origin. ``None`` for origins without one."""
        origin = getattr(run, "remote_job_origin", None)
        repo_origin = getattr(origin, "repository_origin", None)
        location_origin = getattr(repo_origin, "code_location_origin", None)
        port = getattr(location_origin, "port", None)
        try:
            return int(port) if port else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _publishes_host_port(container: Any, port: int) -> bool:
        """Whether a container publishes ``port`` on the host."""
        network_settings = container.attrs.get("NetworkSettings") or {}
        for bindings in (network_settings.get("Ports") or {}).values():
            for binding in bindings or []:
                if str(binding.get("HostPort")) == str(port):
                    return True
        return False

    @staticmethod
    def _env_from_list(env_list: List[str]) -> Dict[str, str]:
        """Turn Docker's ``["KEY=VALUE", ...]`` Config.Env into a dict."""
        result: Dict[str, str] = {}
        for item in env_list:
            if "=" in item:
                key, val = item.split("=", 1)
                result[key] = val
        return result

    # -------------------------------------------------------------------------
    # Container labels
    # -------------------------------------------------------------------------

    def _container_labels(self, run: DagsterRun, host_name: str) -> Dict[str, str]:
        # Besides cleanup, these let a log collector (e.g. Alloy) tag each run
        # container's output with the run it belongs to.
        prefix = self._container_label_prefix
        return {
            f"{prefix}/run_id": run.run_id,
            f"{prefix}/job_name": run.job_name,
            f"{prefix}/location_name": self._get_location_name(run) or "",
            f"{prefix}/host_name": host_name,
            f"{prefix}/managed": "true",
        }

    def _log_url(self, run: DagsterRun, host_name: Optional[str]) -> Optional[str]:
        """``log_url_template`` filled in for this run, or ``None`` if unset.

        Placeholders are substituted by plain replacement rather than
        ``str.format``, because log-viewer URLs (Grafana's especially) are full
        of literal braces.
        """
        if not self._log_url_template:
            return None
        values = {
            "run_id": run.run_id,
            "job_name": run.job_name,
            "location": self._get_location_name(run) or "",
            "host_name": host_name or "",
            "launch_ms": str(int(time.time() * 1000)),
        }
        url = self._log_url_template
        for key, value in values.items():
            url = url.replace("{" + key + "}", quote(value, safe=""))
        return url

    def _report_log_url(self, run: DagsterRun, host_name: Optional[str]) -> None:
        """Post a link to the run's logs in the external viewer. Non-fatal: a
        run never fails to launch over a missing link."""
        url = self._log_url(run, host_name)
        if url is None:
            return
        try:
            self._instance.report_engine_event(
                "Run logs are also available in the external log viewer",
                run,
                engine_event_data=EngineEventData(
                    metadata={"logs": MetadataValue.url(url)}
                ),
                cls=self.__class__,
            )
        except Exception as e:
            logger.warning("Could not report log URL for run %s: %s", run.run_id, e)

    # -------------------------------------------------------------------------
    # Core RunLauncher interface
    # -------------------------------------------------------------------------

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run

        if not self._is_docker_location(run):
            logger.info(
                "Location '%s' not mapped to a Docker host, using DefaultRunLauncher.",
                self._get_location_name(run),
            )
            self._instance.add_run_tags(
                run.run_id, {TAG_LAUNCHER_TYPE: LAUNCHER_TYPE_DEFAULT}
            )
            self._default_launcher.launch_run(context)
            self._report_log_url(run, None)
            return

        host_info = self._get_docker_host(run)
        client: docker.DockerClient = host_info["client"]
        host_name = host_info["host_name"]

        # Determine image: prefer DAGSTER_CURRENT_IMAGE from the code location.
        # Validated up front so a malformed reference fails with an actionable
        # message instead of Docker's opaque 400 "invalid reference format".
        image = self._resolve_run_image(run, host_name)

        # Authenticate to registry if configured
        registry = host_info.get("registry")
        if registry:
            try:
                client.login(
                    username=registry["username"],
                    password=registry["password"],
                    registry=registry["url"],
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to authenticate to registry for host '{host_name}': {e}."
                    " Check registry credentials and network connectivity."
                )

        # Build the dagster execute_run command
        job_code_origin = check.not_none(context.job_code_origin)
        command = ExecuteRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        # Resolved once and shared: the run container inherits both its env and
        # (absent explicit config) its networks from the code-location server.
        server_container = self._find_code_server_container(run, host_info)
        env_vars = self._build_env_vars(run, host_info, server_container)
        labels = self._container_labels(run, host_name)

        # Merge container kwargs: defaults < host-specific
        container_kwargs = {**self._default_container_kwargs}
        if host_info.get("container_kwargs"):
            container_kwargs.update(host_info["container_kwargs"])

        networks = self._resolve_networks(host_info, server_container)

        # Create container
        create_kwargs = {
            "image": image,
            "command": command,
            "environment": env_vars,
            "labels": labels,
            "detach": True,
            "auto_remove": False,  # we manage cleanup ourselves
            **container_kwargs,
        }

        # Attach to first network at creation time if specified
        if networks:
            create_kwargs["network"] = networks[0]

        self._instance.report_engine_event(
            f"Creating Docker container on host '{host_name}' " f"with image '{image}'",
            run,
            cls=self.__class__,
        )

        try:
            container = client.containers.create(**create_kwargs)
        except docker.errors.ImageNotFound:
            # Try pulling the image first
            self._instance.report_engine_event(
                f"Image '{image}' not found on '{host_name}', pulling...",
                run,
                cls=self.__class__,
            )
            try:
                client.images.pull(image)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to pull image '{image}' on host '{host_name}': {e}"
                )
            container = client.containers.create(**create_kwargs)
        except Exception as e:
            raise RuntimeError(
                f"Failed to create container on '{host_name}' "
                f"(image '{image}'): {e}"
            )

        # Connect to additional networks
        for net in networks[1:]:
            try:
                network_obj = client.networks.get(net)
                network_obj.connect(container)
            except docker.errors.NotFound:
                logger.warning("Network '%s' not found on host '%s'", net, host_name)

        try:
            container.start()
        except Exception as e:
            self._instance.report_engine_event(
                f"Failed to start container on '{host_name}': {e}",
                run,
                cls=self.__class__,
            )
            raise RuntimeError(f"Failed to start container on '{host_name}': {e}")

        self._instance.add_run_tags(
            run.run_id,
            {
                TAG_CONTAINER_ID: container.id,
                TAG_HOST_NAME: host_name,
                TAG_LAUNCHER_TYPE: LAUNCHER_TYPE_DOCKER,
            },
        )

        self._instance.report_engine_event(
            f"Started Docker container {container.short_id} on host '{host_name}'",
            run,
            cls=self.__class__,
        )
        self._report_log_url(run, host_name)

    def _resolve_image(self, run: DagsterRun) -> Optional[str]:
        """Try to get the image from the code location's DAGSTER_CURRENT_IMAGE
        or from the job origin."""
        try:
            origin = run.job_code_origin
            if origin and hasattr(origin, "repository_origin"):
                repo_origin = origin.repository_origin
                if hasattr(repo_origin, "container_image"):
                    return repo_origin.container_image
        except Exception:
            pass
        return None

    def _resolve_run_image(self, run: DagsterRun, host_name: str) -> str:
        """Resolve and validate the Docker image for a run.

        Resolution order: the ``dagster/image`` run tag, then the code
        location's ``container_image`` (from ``DAGSTER_CURRENT_IMAGE``). The
        result is trimmed and checked against the Docker reference grammar so a
        malformed value (trailing newline, ``https://`` scheme, uppercase repo,
        empty tag, …) raises a clear error here rather than surfacing as
        Docker's opaque 400 ``invalid reference format`` at create time.
        """
        raw = run.tags.get("dagster/image") or self._resolve_image(run)
        if not raw or not str(raw).strip():
            raise Exception(
                f"Could not determine Docker image for run {run.run_id}. "
                "Set DAGSTER_CURRENT_IMAGE in your code location or add a "
                "'dagster/image' tag to your job."
            )

        image = str(raw).strip()
        self._validate_image_reference(image, run, host_name)
        return image

    @staticmethod
    def _validate_image_reference(image: str, run: DagsterRun, host_name: str) -> None:
        """Raise a descriptive error if ``image`` is not a valid Docker
        reference. ``image`` is assumed already trimmed of surrounding
        whitespace."""
        prefix = (
            f"Invalid Docker image reference '{image}' for run {run.run_id} "
            f"on host '{host_name}': "
        )
        if "://" in image:
            raise Exception(
                prefix + "looks like a URL (contains '://'). Use a bare "
                "reference such as 'registry:5000/name:tag', not a URL."
            )
        if any(ch.isspace() for ch in image):
            raise Exception(
                prefix + "contains whitespace. Check for a stray newline or "
                "space in DAGSTER_CURRENT_IMAGE / the 'dagster/image' tag."
            )
        if not _IMAGE_REFERENCE_RE.match(image):
            raise Exception(
                prefix + "not a valid reference. Common causes: uppercase "
                "letters in the repository name (must be lowercase), an empty "
                "tag ('name:'), or a missing repository (':tag')."
            )

    def resume_run(self, context: ResumeRunContext) -> None:
        """Resume a previously interrupted run."""
        run = context.dagster_run
        launcher_type = run.tags.get(TAG_LAUNCHER_TYPE)

        if launcher_type == LAUNCHER_TYPE_DEFAULT:
            self._default_launcher.resume_run(context)
        else:
            # For Docker runs, we re-launch (there is no container to resume)
            # This creates a new container that picks up from the last checkpoint
            self.launch_run(
                LaunchRunContext(dagster_run=run, workspace=context.workspace)
            )

    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        if run is None:
            return False

        launcher_type = run.tags.get(TAG_LAUNCHER_TYPE)

        if launcher_type == LAUNCHER_TYPE_DEFAULT:
            return self._default_launcher.terminate(run_id)

        host_name = run.tags.get(TAG_HOST_NAME)
        container_id = run.tags.get(TAG_CONTAINER_ID)

        if not host_name or not container_id:
            self._instance.report_engine_event(
                "Cannot terminate: missing host or container ID in run tags.",
                run,
                cls=self.__class__,
            )
            return False

        client = self._docker_clients.get(host_name)
        if not client:
            self._instance.report_engine_event(
                f"Cannot terminate: Docker host '{host_name}' not in current config.",
                run,
                cls=self.__class__,
            )
            return False

        try:
            container = client.containers.get(container_id)
            container.stop(timeout=30)
            self._instance.report_engine_event(
                f"Stopped container {container_id[:12]} on host '{host_name}'",
                run,
                cls=self.__class__,
            )
            return True
        except docker.errors.NotFound:
            self._instance.report_engine_event(
                f"Container {container_id[:12]} not found on host '{host_name}'",
                run,
                cls=self.__class__,
            )
            return False
        except Exception as e:
            self._instance.report_engine_event(
                f"Error stopping container on '{host_name}': {e}",
                run,
                cls=self.__class__,
            )
            return False

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        launcher_type = run.tags.get(TAG_LAUNCHER_TYPE)

        if launcher_type == LAUNCHER_TYPE_DEFAULT:
            return self._default_launcher.check_run_worker_health(run)

        host_name = run.tags.get(TAG_HOST_NAME)
        container_id = run.tags.get(TAG_CONTAINER_ID)

        if not host_name or not container_id:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, msg="Missing run tags")

        client = self._docker_clients.get(host_name)
        if not client:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN,
                msg=f"Docker host '{host_name}' not in current config",
            )

        try:
            container = client.containers.get(container_id)
        except docker.errors.NotFound:
            return CheckRunHealthResult(
                WorkerStatus.FAILED,
                msg=f"Container {container_id[:12]} not found on '{host_name}'",
            )
        except Exception as e:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN,
                msg=f"Error reaching host '{host_name}': {e}",
            )

        if container.status == "running":
            return CheckRunHealthResult(WorkerStatus.RUNNING)

        # Container exited — check if it was a success or failure
        exit_code = container.attrs.get("State", {}).get("ExitCode")
        if exit_code == 0:
            return CheckRunHealthResult(WorkerStatus.SUCCESS)

        # Grab tail of logs for the error message
        try:
            tail_logs = container.logs(tail=25).decode("utf-8", errors="replace")
        except Exception:
            tail_logs = "(could not retrieve logs)"

        return CheckRunHealthResult(
            WorkerStatus.FAILED,
            msg=(
                f"Container exited with code {exit_code} on '{host_name}'.\n"
                f"Last logs:\n{tail_logs}"
            ),
        )

    # -------------------------------------------------------------------------
    # Public helpers for admin/cleanup jobs
    # -------------------------------------------------------------------------

    def get_all_docker_clients(self) -> Dict[str, docker.DockerClient]:
        """Return name->client map. Useful for admin code locations that
        perform cleanup."""
        return dict(self._docker_clients)

    def list_dagster_containers(
        self,
        host_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List Dagster-managed containers across all (or one) Docker hosts.

        Args:
            host_name: Restrict to a single host. None = all hosts.
            status: Docker status filter (running, exited, etc). None = all.

        Returns:
            List of dicts with keys: host_name, container_id, short_id,
            run_id, status, image, created, finished_at.
        """
        results = []
        clients = (
            {host_name: self._docker_clients[host_name]}
            if host_name
            else self._docker_clients
        )
        prefix = self._container_label_prefix

        for hname, client in clients.items():
            filters = {"label": f"{prefix}/managed=true"}
            if status:
                filters["status"] = status
            try:
                containers = client.containers.list(all=True, filters=filters)
            except Exception as e:
                logger.error("Failed to list containers on '%s': %s", hname, e)
                continue

            for c in containers:
                state = c.attrs.get("State", {})
                results.append(
                    {
                        "host_name": hname,
                        "container_id": c.id,
                        "short_id": c.short_id,
                        "run_id": c.labels.get(f"{prefix}/run_id", "unknown"),
                        "status": c.status,
                        "image": (
                            c.image.tags[0] if c.image.tags else str(c.image.id)[:12]
                        ),
                        "created": c.attrs.get("Created"),
                        "finished_at": state.get("FinishedAt"),
                        "exit_code": state.get("ExitCode"),
                    }
                )

        return results

    def cleanup_old_containers(
        self,
        max_age_hours: float = 24,
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """Remove exited Dagster-managed containers older than max_age_hours.

        Returns list of removed (or would-be-removed) container info dicts.
        """
        from datetime import datetime, timezone

        cutoff = time.time() - (max_age_hours * 3600)
        removed = []

        exited = self.list_dagster_containers(status="exited")
        for info in exited:
            finished_at = info.get("finished_at")
            if not finished_at:
                continue

            # Docker returns ISO format timestamps
            try:
                # Handle Docker's timestamp format (may include nanoseconds)
                if "." in finished_at:
                    ts_str = finished_at.split(".")[0]
                else:
                    ts_str = finished_at.rstrip("Z")
                finished_time = datetime.fromisoformat(ts_str).replace(
                    tzinfo=timezone.utc
                )
                if finished_time.timestamp() > cutoff:
                    continue
            except (ValueError, TypeError):
                continue

            if dry_run:
                info["action"] = "would_remove"
                removed.append(info)
                continue

            client = self._docker_clients.get(info["host_name"])
            if not client:
                continue

            try:
                container = client.containers.get(info["container_id"])
                container.remove(force=True)
                info["action"] = "removed"
                removed.append(info)
                logger.info(
                    "Removed container %s (run %s) from host '%s'",
                    info["short_id"],
                    info["run_id"],
                    info["host_name"],
                )
            except Exception as e:
                logger.error(
                    "Failed to remove container %s on '%s': %s",
                    info["short_id"],
                    info["host_name"],
                    e,
                )

        return removed
