from types import SimpleNamespace

import pytest

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


class DummyClient:
    pass


def make_launcher(monkeypatch, docker_hosts=None, default_env_vars=None):
    monkeypatch.setattr(
        "dagster_multihost_launcher.launcher.MultiHostDockerRunLauncher._build_docker_client",
        staticmethod(lambda cfg: DummyClient()),
    )
    return MultiHostDockerRunLauncher(
        docker_hosts=docker_hosts or [], default_env_vars=default_env_vars or []
    )


def make_run(location_name=None, run_id="r1", job_name="j"):
    origin = (
        SimpleNamespace(location_name=location_name)
        if location_name is not None
        else None
    )
    return SimpleNamespace(
        run_id=run_id, job_name=job_name, remote_job_origin=origin, tags={}
    )


def test_location_mapping_and_get_docker_host(monkeypatch):
    launcher = make_launcher(
        monkeypatch,
        docker_hosts=[
            {"host_name": "h1", "location_names": ["loc_a"]},
            {"host_name": "h2", "location_names": ["loc_b"]},
        ],
    )

    run_a = make_run("loc_a")
    assert launcher._is_docker_location(run_a)
    host_info = launcher._get_docker_host(run_a)
    assert host_info["host_name"] == "h1"

    run_unknown = make_run("unknown")
    assert not launcher._is_docker_location(run_unknown)
    with pytest.raises(Exception):
        launcher._get_docker_host(run_unknown)


def test_get_location_name_none():
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = SimpleNamespace(remote_job_origin=None)
    assert launcher._get_location_name(run) is None


def test_build_env_vars_merging(monkeypatch):
    launcher = make_launcher(monkeypatch, default_env_vars=["GLOBAL_A", "X=1"])
    monkeypatch.setenv("GLOBAL_A", "val_a")
    host_info = {"env_vars": ["Y=2", "LOCAL_B"]}
    monkeypatch.setenv("LOCAL_B", "val_b")
    run = make_run("loc", run_id="r123", job_name="jobx")
    env = launcher._build_env_vars(run, host_info)

    assert env["GLOBAL_A"] == "val_a"
    assert env["X"] == "1"
    assert env["Y"] == "2"
    assert env["LOCAL_B"] == "val_b"
    assert env["DAGSTER_RUN_JOB_NAME"] == "jobx"
    assert env["DAGSTER_RUN_ID"] == "r123"
