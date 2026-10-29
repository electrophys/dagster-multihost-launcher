"""Docker clients connect on first use, so building the launcher (which every
process loading the instance does) never needs a Docker host to be reachable."""

from types import SimpleNamespace

import pytest
import yaml
from dagster._core.launcher import RunLauncher, WorkerStatus
from dagster._serdes import ConfigurableClassData

from dagster_multihost_launcher.launcher import (
    TAG_CONTAINER_ID,
    TAG_HOST_NAME,
    TAG_LAUNCHER_TYPE,
    MultiHostDockerRunLauncher,
)

HOSTS = [
    {"host_name": "host-a", "location_names": ["local"]},
    {"host_name": "host-b", "location_names": ["etl"]},
]


class FakeClient:
    containers = SimpleNamespace(get=lambda container_id: "container")


def make_launcher(monkeypatch, build):
    monkeypatch.setattr(
        MultiHostDockerRunLauncher, "_build_docker_client", staticmethod(build)
    )
    return MultiHostDockerRunLauncher(docker_hosts=HOSTS)


def unreachable(cfg):
    raise RuntimeError(f"Failed to create Docker client for host '{cfg['host_name']}'")


def test_construction_does_not_connect(monkeypatch):
    calls = []
    make_launcher(monkeypatch, lambda cfg: calls.append(cfg) or FakeClient())
    assert calls == []


def test_unreachable_host_does_not_break_construction(monkeypatch):
    launcher = make_launcher(monkeypatch, unreachable)
    assert set(launcher.get_all_docker_clients()) == {"host-a", "host-b"}


def test_connects_once_on_first_use(monkeypatch):
    calls = []
    launcher = make_launcher(monkeypatch, lambda cfg: calls.append(1) or FakeClient())
    client = launcher.get_all_docker_clients()["host-b"]

    assert client.containers.get("x") == "container"
    assert client.containers.get("y") == "container"
    assert len(calls) == 1


def test_failure_surfaces_on_use_and_is_retried(monkeypatch):
    attempts = []

    def flaky(cfg):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("daemon down")
        return FakeClient()

    client = make_launcher(monkeypatch, flaky).get_all_docker_clients()["host-b"]
    with pytest.raises(RuntimeError, match="daemon down"):
        client.containers
    assert client.containers.get("x") == "container"  # host came back


def test_hosts_fail_independently(monkeypatch):
    def build(cfg):
        if cfg["host_name"] == "host-a":
            raise RuntimeError("no socket")
        return FakeClient()

    clients = make_launcher(monkeypatch, build).get_all_docker_clients()
    with pytest.raises(RuntimeError):
        clients["host-a"].containers
    assert clients["host-b"].containers.get("x") == "container"


def test_health_check_on_unreachable_host_is_unknown(monkeypatch):
    launcher = make_launcher(monkeypatch, unreachable)
    run = SimpleNamespace(
        tags={
            TAG_LAUNCHER_TYPE: "docker",
            TAG_HOST_NAME: "host-b",
            TAG_CONTAINER_ID: "abc123",
        }
    )
    result = launcher.check_run_worker_health(run)
    assert result.status == WorkerStatus.UNKNOWN


def test_rehydrates_from_config_without_reachable_daemons():
    """What the webserver does to show the instance config: build the launcher
    from dagster.yaml with real docker-py, no daemon reachable."""
    config = {
        "docker_hosts": [
            {"host_name": "host-a", "location_names": ["local"]},
            {
                "host_name": "host-a-sock",
                "docker_url": "unix:///nonexistent/docker.sock",
                "location_names": ["sock"],
            },
            {
                "host_name": "host-b",
                "docker_url": "tcp://192.0.2.1:2376",
                "location_names": ["etl"],
            },
        ]
    }
    launcher = ConfigurableClassData(
        "dagster_multihost_launcher", "MultiHostDockerRunLauncher", yaml.dump(config)
    ).rehydrate(as_type=RunLauncher)

    assert isinstance(launcher, MultiHostDockerRunLauncher)
    client = launcher.get_all_docker_clients()["host-a-sock"]
    with pytest.raises(RuntimeError, match="host-a-sock"):
        client.containers
