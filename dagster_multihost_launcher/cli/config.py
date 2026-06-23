"""Configuration loading from docker-compose.yml and dagster.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


@dataclass
class ServiceInfo:
    """A Docker Compose service and its relationship to Dagster."""

    name: str
    image: Optional[str] = None
    build: Optional[Dict[str, Any]] = None
    container_name: Optional[str] = None
    # Which Docker host this service's code location maps to (from dagster.yaml)
    host_name: Optional[str] = None
    # Code location names served by this service
    location_names: List[str] = field(default_factory=list)
    # Role: "webserver", "daemon", "code_location", "other"
    role: str = "other"


@dataclass
class DockerHostInfo:
    """Docker host connection info from dagster.yaml."""

    host_name: str
    docker_url: Optional[str] = None
    tls: Optional[Dict[str, Any]] = None
    location_names: List[str] = field(default_factory=list)
    network: Optional[str] = None
    registry: Optional[Dict[str, Any]] = None


@dataclass
class DeployConfig:
    """Unified configuration for the CLI."""

    services: Dict[str, ServiceInfo] = field(default_factory=dict)
    docker_hosts: Dict[str, DockerHostInfo] = field(default_factory=dict)
    # location_name -> host_name mapping
    location_to_host: Dict[str, str] = field(default_factory=dict)
    compose_file: Optional[str] = None
    compose_project: Optional[str] = None
    dagster_home: Optional[str] = None


def _detect_service_role(name: str, service_cfg: Dict[str, Any]) -> str:
    """Detect the role of a compose service from its entrypoint/command."""
    entrypoint = service_cfg.get("entrypoint", [])
    command = service_cfg.get("command", [])

    # Normalize to string for matching
    if isinstance(entrypoint, list):
        entry_str = " ".join(str(e) for e in entrypoint)
    else:
        entry_str = str(entrypoint)

    if isinstance(command, list):
        cmd_str = " ".join(str(c) for c in command)
    else:
        cmd_str = str(command)

    combined = f"{entry_str} {cmd_str}".lower()

    if "dagster-webserver" in combined or "webserver" in name.lower():
        return "webserver"
    if "dagster-daemon" in combined or (
        "daemon" in name.lower() and "dagster" in combined
    ):
        return "daemon"
    if "code-server" in combined or "grpc" in combined or "code_server" in combined:
        return "code_location"

    return "other"


def load_compose(compose_file: str) -> Dict[str, ServiceInfo]:
    """Parse docker-compose.yml and return service info."""
    path = Path(compose_file)
    if not path.exists():
        raise FileNotFoundError(f"Compose file not found: {compose_file}")

    with open(path) as f:
        data = yaml.safe_load(f)

    services = {}
    for name, cfg in (data.get("services") or {}).items():
        role = _detect_service_role(name, cfg)
        services[name] = ServiceInfo(
            name=name,
            image=cfg.get("image"),
            build=cfg.get("build"),
            container_name=cfg.get("container_name"),
            role=role,
        )

    return services


def load_dagster_yaml(dagster_home: str) -> Dict[str, DockerHostInfo]:
    """Parse dagster.yaml and extract docker_hosts config."""
    dagster_yaml = Path(dagster_home) / "dagster.yaml"
    if not dagster_yaml.exists():
        raise FileNotFoundError(f"dagster.yaml not found in: {dagster_home}")

    with open(dagster_yaml) as f:
        data = yaml.safe_load(f)

    launcher_cfg = data.get("run_launcher", {}).get("config", {})
    hosts = {}

    for host_cfg in launcher_cfg.get("docker_hosts", []):
        host_name = host_cfg["host_name"]
        hosts[host_name] = DockerHostInfo(
            host_name=host_name,
            docker_url=host_cfg.get("docker_url"),
            tls=host_cfg.get("tls"),
            location_names=host_cfg.get("location_names", []),
            network=host_cfg.get("network"),
            registry=host_cfg.get("registry"),
        )

    return hosts


def build_docker_client(host_info: DockerHostInfo):
    """Create a Docker client for a host, reusing the launcher's logic."""
    host_cfg = {
        "host_name": host_info.host_name,
    }
    if host_info.docker_url:
        host_cfg["docker_url"] = host_info.docker_url
    if host_info.tls:
        host_cfg["tls"] = host_info.tls
    return MultiHostDockerRunLauncher._build_docker_client(host_cfg)


def load_config(
    compose_file: str,
    dagster_home: Optional[str] = None,
) -> DeployConfig:
    """Load and merge configuration from compose and dagster.yaml.

    Args:
        compose_file: Path to docker-compose.yml.
        dagster_home: Path to directory containing dagster.yaml.
            Falls back to DAGSTER_HOME env var.
    """
    config = DeployConfig(compose_file=compose_file)

    # Load compose services
    config.services = load_compose(compose_file)

    # Load dagster.yaml if available
    dagster_home = dagster_home or os.environ.get("DAGSTER_HOME")
    if dagster_home:
        config.dagster_home = dagster_home
        try:
            config.docker_hosts = load_dagster_yaml(dagster_home)
        except FileNotFoundError:
            pass

    # Build location -> host mapping
    for host_name, host_info in config.docker_hosts.items():
        for loc in host_info.location_names:
            config.location_to_host[loc] = host_name

    # Try to infer compose project name from compose file directory
    config.compose_project = Path(compose_file).resolve().parent.name

    return config
