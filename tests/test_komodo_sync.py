from dagster_multihost_launcher.cli.config import DeployConfig, DockerHostInfo
from dagster_multihost_launcher.komodo_sync import (
    diff_topology,
    generate_resource_sync_toml,
    load_komodo_toml,
)


def _config():
    config = DeployConfig()
    config.docker_hosts = {
        "host-b": DockerHostInfo(
            host_name="host-b",
            docker_url="tcp://10.0.1.2:2376",
            location_names=["etl", "ml"],
        ),
        "host-c": DockerHostInfo(
            host_name="host-c",
            docker_url="tcp://10.0.1.3:2376",
            location_names=["analytics"],
        ),
    }
    config.location_to_host = {
        "etl": "host-b",
        "ml": "host-b",
        "analytics": "host-c",
    }
    return config


def test_generate_roundtrips_and_maps(tmp_path):
    toml_text = generate_resource_sync_toml(_config())
    path = tmp_path / "out.toml"
    path.write_text(toml_text)

    doc = load_komodo_toml(str(path))
    servers = {s["name"] for s in doc["server"]}
    stacks = {s["name"]: s["config"]["server"] for s in doc["stack"]}

    assert servers == {"host-b", "host-c"}
    assert stacks == {"etl": "host-b", "ml": "host-b", "analytics": "host-c"}


def test_diff_in_sync():
    config = _config()
    komodo = {
        "server": [{"name": "host-b"}, {"name": "host-c"}],
        "stack": [
            {"name": "etl", "config": {"server": "host-b"}},
            {"name": "ml", "config": {"server": "host-b"}},
            {"name": "analytics", "config": {"server": "host-c"}},
        ],
    }
    diff = diff_topology(config, komodo)
    assert diff.ok
    assert diff.missing_servers == []
    assert diff.missing_stacks == []


def test_diff_detects_drift():
    config = _config()
    komodo = {
        "server": [{"name": "host-b"}, {"name": "host-z"}],  # host-c missing
        "stack": [
            {"name": "etl", "server": "host-c"},  # misplaced (top-level server)
            # "ml" stack missing
            {"name": "analytics", "config": {"server": "host-c"}},
        ],
    }
    diff = diff_topology(config, komodo)
    assert not diff.ok
    assert diff.missing_servers == ["host-c"]
    assert diff.missing_stacks == ["ml"]
    assert any("etl" in m for m in diff.misplaced_stacks)
    assert diff.extra_servers == ["host-z"]
