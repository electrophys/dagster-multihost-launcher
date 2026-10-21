"""Tests for PostgresComputeLogManager, run against SQLite.

Each test pairs a "worker" manager (where a step runs and writes logs) with a
"webserver" manager on a separate local_dir, mirroring a run on Host B whose
logs are read back on Host A through the shared database only.
"""

import os

import pytest
import yaml
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.local_compute_log_manager import IO_TYPE_EXTENSION
from dagster._serdes import ConfigurableClassData

from dagster_multihost_launcher.compute_log_manager import (
    PostgresComputeLogManager,
    _url_from_config,
)

LOG_KEY = ["run-1", "compute_logs", "step_a"]


@pytest.fixture
def db_url(tmp_path):
    return f"sqlite:///{tmp_path / 'logs.db'}"


@pytest.fixture
def worker(tmp_path, db_url):
    manager = PostgresComputeLogManager(db_url, local_dir=str(tmp_path / "worker"))
    yield manager
    manager.dispose()


@pytest.fixture
def webserver(tmp_path, db_url):
    manager = PostgresComputeLogManager(db_url, local_dir=str(tmp_path / "webserver"))
    yield manager
    manager.dispose()


def write_local(manager, log_key, io_type, data: bytes):
    path = manager.local_manager.get_captured_local_path(
        log_key, IO_TYPE_EXTENSION[io_type]
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def read(manager, log_key, io_type=ComputeIOType.STDOUT):
    data, _ = manager.get_log_data_for_type(log_key, io_type, offset=0, max_bytes=None)
    return data


def test_completed_logs_are_readable_from_another_host(worker, webserver):
    with worker.open_log_stream(LOG_KEY, ComputeIOType.STDOUT) as f:
        f.write("hello from host b\n")

    assert not worker.has_local_file(
        LOG_KEY, ComputeIOType.STDOUT
    )  # staged copy cleaned up
    assert webserver.is_capture_complete(LOG_KEY)
    assert read(webserver, LOG_KEY) == b"hello from host b\n"
    assert read(webserver, LOG_KEY, ComputeIOType.STDERR) == b""


def test_partial_logs_are_visible_while_running(worker, webserver):
    write_local(worker, LOG_KEY, ComputeIOType.STDOUT, b"still going")
    worker.on_progress(LOG_KEY)

    assert not webserver.is_capture_complete(LOG_KEY)
    assert read(webserver, LOG_KEY) == b"still going"


def test_empty_partial_is_not_uploaded(worker):
    write_local(worker, LOG_KEY, ComputeIOType.STDOUT, b"")
    worker.upload_to_cloud_storage(LOG_KEY, ComputeIOType.STDOUT, partial=True)
    assert not worker.cloud_storage_has_logs(
        LOG_KEY, ComputeIOType.STDOUT, partial=True
    )


def test_complete_upload_replaces_partial(worker):
    write_local(worker, LOG_KEY, ComputeIOType.STDOUT, b"part")
    worker.upload_to_cloud_storage(LOG_KEY, ComputeIOType.STDOUT, partial=True)
    write_local(worker, LOG_KEY, ComputeIOType.STDOUT, b"part and the rest")
    worker.upload_to_cloud_storage(LOG_KEY, ComputeIOType.STDOUT)
    worker.upload_to_cloud_storage(
        LOG_KEY, ComputeIOType.STDOUT
    )  # re-upload is an upsert

    assert not worker.cloud_storage_has_logs(
        LOG_KEY, ComputeIOType.STDOUT, partial=True
    )
    assert worker.cloud_storage_has_logs(LOG_KEY, ComputeIOType.STDOUT)


def test_large_logs_keep_their_tail(tmp_path, db_url, webserver):
    worker = PostgresComputeLogManager(
        db_url, local_dir=str(tmp_path / "w"), max_log_bytes=10
    )
    write_local(worker, LOG_KEY, ComputeIOType.STDOUT, b"0123456789abcdefghij")
    worker.upload_to_cloud_storage(LOG_KEY, ComputeIOType.STDOUT)

    data = read(webserver, LOG_KEY)
    assert data.endswith(b"abcdefghij")
    assert b"0123456789" not in data
    assert b"truncated" in data


def test_delete_by_key_and_prefix(worker):
    keys = [
        ["run-1", "compute_logs", "a"],
        ["run-1", "compute_logs", "b"],
        ["run-10", "x", "c"],
    ]
    for key in keys:
        write_local(worker, key, ComputeIOType.STDOUT, b"x")
        worker.upload_to_cloud_storage(key, ComputeIOType.STDOUT)

    worker.delete_logs(log_key=keys[0])
    assert not worker.cloud_storage_has_logs(keys[0], ComputeIOType.STDOUT)
    assert worker.cloud_storage_has_logs(keys[1], ComputeIOType.STDOUT)

    worker.delete_logs(prefix=["run-1"])
    assert not worker.cloud_storage_has_logs(keys[1], ComputeIOType.STDOUT)
    # "run-1" must not match "run-10"
    assert worker.cloud_storage_has_logs(keys[2], ComputeIOType.STDOUT)


def test_log_keys_for_prefix_lists_direct_children(worker):
    for key in (
        ["r", "c", "a"],
        ["r", "c", "b"],
        ["r", "c", "deeper", "z"],
        ["r", "d", "q"],
    ):
        write_local(worker, key, ComputeIOType.STDERR, b"x")
        worker.upload_to_cloud_storage(key, ComputeIOType.STDERR)

    keys = worker.get_log_keys_for_log_key_prefix(["r", "c"], ComputeIOType.STDERR)
    assert sorted(keys) == [["r", "c", "a"], ["r", "c", "b"]]
    assert (
        worker.get_log_keys_for_log_key_prefix(["r", "c"], ComputeIOType.STDOUT) == []
    )


def test_display_path_once_complete(worker):
    assert worker.display_path_for_type(LOG_KEY, ComputeIOType.STDOUT) is None
    with worker.open_log_stream(LOG_KEY, ComputeIOType.STDOUT) as f:
        f.write("x")
    path = worker.display_path_for_type(LOG_KEY, ComputeIOType.STDOUT)
    assert path == "postgres:multihost_compute_logs/run-1/compute_logs/step_a.out"
    assert worker.download_url_for_type(LOG_KEY, ComputeIOType.STDOUT) is None


def test_rehydrates_from_dagster_yaml_config(tmp_path, db_url):
    """The dagster.yaml block passes config validation and builds a manager."""
    config = {"postgres_url": db_url, "upload_interval": 15, "local_dir": str(tmp_path)}
    manager = ConfigurableClassData(
        "dagster_multihost_launcher.compute_log_manager",
        "PostgresComputeLogManager",
        yaml.dump(config),
    ).rehydrate(as_type=PostgresComputeLogManager)

    assert manager.upload_interval == 15
    with manager.open_log_stream(LOG_KEY, ComputeIOType.STDOUT) as f:
        f.write("ok")
    assert manager.is_capture_complete(LOG_KEY)
    manager.dispose()


def test_url_from_postgres_db_quotes_credentials():
    url = _url_from_config(
        {
            "postgres_db": {
                "username": "dagster",
                "password": "p@ss/word",
                "hostname": "10.0.1.1",
                "db_name": "dagster",
                "port": 5433,
                "params": {"sslmode": "require"},
            }
        }
    )
    assert (
        url
        == "postgresql://dagster:p%40ss%2Fword@10.0.1.1:5433/dagster?sslmode=require"
    )


def test_url_config_requires_exactly_one_source():
    with pytest.raises(Exception):
        _url_from_config({})
