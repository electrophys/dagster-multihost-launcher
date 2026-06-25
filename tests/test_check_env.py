import textwrap

from dagster_multihost_launcher.cli.config import (
    find_bare_env_vars,
    find_env_references,
)


def test_find_env_references_nested():
    data = {
        "storage": {
            "postgres": {
                "postgres_db": {
                    "username": {"env": "PG_USER"},
                    "password": {"env": "PG_PASS"},
                    "port": 5432,
                }
            }
        },
        "things": [
            {"token": {"env": "API_TOKEN"}},
            {"plain": "value"},
        ],
    }
    assert find_env_references(data) == {"PG_USER", "PG_PASS", "API_TOKEN"}


def test_find_env_references_empty():
    assert find_env_references({}) == set()
    assert find_env_references({"a": 1, "b": [1, 2]}) == set()


def test_find_bare_env_vars(tmp_path):
    (tmp_path / "dagster.yaml").write_text(
        textwrap.dedent(
            """
            run_launcher:
              module: dagster_multihost_launcher
              class: MultiHostDockerRunLauncher
              config:
                default_env_vars:
                  - GLOBAL_KEY
                  - LITERAL=value
                docker_hosts:
                  - host_name: host-b
                    location_names: [etl]
                    env_vars:
                      - HOST_KEY
                      - HOST_LITERAL=x
            """
        )
    )
    assert find_bare_env_vars(str(tmp_path)) == {"GLOBAL_KEY", "HOST_KEY"}
