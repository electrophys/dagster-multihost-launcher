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
import time
import os
from typing import Any, Dict, List, Optional

import docker
from docker.tls import TLSConfig

from dagster import (
    Field,
    Noneable,
    StringSource,
    _check as check,
)
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
        default_container_kwargs: Optional[Dict[str, Any]] = None,
        container_label_prefix: str = "dagster",
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        super().__init__()
        self._inst_data = inst_data
        self._default_env_vars = default_env_vars or []
        self._default_container_kwargs = default_container_kwargs or {}
        self._container_label_prefix = container_label_prefix

        # Build host map: location_name -> host config + Docker client
        self._docker_host_map: Dict[str, Dict[str, Any]] = {}
        # Also keep a name -> client map for cleanup operations
        self._docker_clients: Dict[str, docker.DockerClient] = {}

        for host_cfg in docker_hosts or []:
            client = self._build_docker_client(host_cfg)
            host_name = host_cfg["host_name"]
            self._docker_clients[host_name] = client

            for loc in host_cfg["location_names"]:
                self._docker_host_map[loc] = {
                    "host_name": host_name,
                    "client": client,
                    "network": host_cfg.get("network"),
                    "networks": host_cfg.get("networks"),
                    "container_kwargs": host_cfg.get("container_kwargs", {}),
                    "env_vars": host_cfg.get("env_vars", []),
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
            return docker.from_env()

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
                description="Docker network to attach run containers to.",
            ),
            "networks": Field(
                Noneable([str]),
                default_value=None,
                is_required=False,
                description="Multiple Docker networks to attach run containers to.",
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
        self, run: DagsterRun, host_info: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build environment variables for a run container.

        Merges default_env_vars + host-specific env_vars. Variables specified
        as just KEY (no =) are pulled from the current process environment.
        """
        import os

        env = {}
        all_vars = self._default_env_vars + host_info.get("env_vars", [])

        for var in all_vars:
            if "=" in var:
                key, val = var.split("=", 1)
                env[key] = val
            else:
                val = os.environ.get(var)
                if val is not None:
                    env[var] = val

        # Dagster-internal env vars the run worker needs
        env["DAGSTER_RUN_JOB_NAME"] = run.job_name
        if run.run_id:
            env["DAGSTER_RUN_ID"] = run.run_id

        return env

    # -------------------------------------------------------------------------
    # Container labels
    # -------------------------------------------------------------------------

    def _container_labels(self, run: DagsterRun) -> Dict[str, str]:
        prefix = self._container_label_prefix
        return {
            f"{prefix}/run_id": run.run_id,
            f"{prefix}/job_name": run.job_name,
            f"{prefix}/managed": "true",
        }

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
            return

        host_info = self._get_docker_host(run)
        client: docker.DockerClient = host_info["client"]
        host_name = host_info["host_name"]

        # Determine image: prefer DAGSTER_CURRENT_IMAGE from the code location
        image = run.tags.get("dagster/image") or self._resolve_image(run)
        if not image:
            raise Exception(
                f"Could not determine Docker image for run {run.run_id}. "
                "Set DAGSTER_CURRENT_IMAGE in your code location or add a "
                "'dagster/image' tag to your job."
            )

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
        env_vars = self._build_env_vars(run, host_info)
        labels = self._container_labels(run)

        # Merge container kwargs: defaults < host-specific
        container_kwargs = {**self._default_container_kwargs}
        if host_info.get("container_kwargs"):
            container_kwargs.update(host_info["container_kwargs"])

        # Handle networks
        network = host_info.get("network")
        networks = host_info.get("networks") or []
        if network:
            networks = [network] + [n for n in networks if n != network]

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
            raise RuntimeError(f"Failed to create container on '{host_name}': {e}")

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
