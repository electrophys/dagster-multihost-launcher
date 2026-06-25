from types import SimpleNamespace

import docker

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


class DummyClient:
    pass


def make_launcher(monkeypatch, **kwargs):
    monkeypatch.setattr(
        "dagster_multihost_launcher.launcher.MultiHostDockerRunLauncher._build_docker_client",
        staticmethod(lambda cfg: DummyClient()),
    )
    return MultiHostDockerRunLauncher(docker_hosts=[], **kwargs)


def make_run(location_name="loc", run_id="r1", job_name="j"):
    origin = SimpleNamespace(location_name=location_name)
    return SimpleNamespace(
        run_id=run_id, job_name=job_name, remote_job_origin=origin, tags={}
    )


# -- _read_env_file --------------------------------------------------------


def test_read_env_file_parsing(tmp_path):
    p = tmp_path / "shared.env"
    p.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                "PLAIN=value",
                "export EXPORTED=exp",
                'QUOTED="quoted value"',
                "SINGLE='single'",
                "WITH_EQUALS=a=b=c",
                "  SPACED  =  trimmed  ",
                "NOEQUALS_LINE",
            ]
        )
    )
    parsed = MultiHostDockerRunLauncher._read_env_file(str(p))
    assert parsed == {
        "PLAIN": "value",
        "EXPORTED": "exp",
        "QUOTED": "quoted value",
        "SINGLE": "single",
        "WITH_EQUALS": "a=b=c",
        "SPACED": "trimmed",
    }


def test_read_env_file_missing_returns_empty():
    assert MultiHostDockerRunLauncher._read_env_file("/no/such/file.env") == {}


# -- precedence ------------------------------------------------------------


def test_env_precedence_full_chain(monkeypatch, tmp_path):
    default_file = tmp_path / "default.env"
    default_file.write_text("A=1\nB=1\n")
    host_file = tmp_path / "host.env"
    host_file.write_text("B=2\nC=2\n")

    launcher = make_launcher(
        monkeypatch,
        default_env_vars=["D=4"],
        default_env_file=str(default_file),
    )

    # Fake code-location server container providing C and D
    class FakeContainer:
        attrs = {"Config": {"Env": ["C=3", "D=3"]}}

    class FakeClient:
        def __init__(self):
            self.containers = SimpleNamespace(get=lambda name: FakeContainer())

    host_info = {
        "host_name": "host-b",
        "client": FakeClient(),
        "env_file": str(host_file),
        "inherit_env_from_container": "code-{location}",
        "env_vars": ["E=5"],
    }

    env = launcher._build_env_vars(make_run("loc_b"), host_info)

    # default_env_file < host env_file < inherit < default_env_vars < host env_vars
    assert env["A"] == "1"  # default file only
    assert env["B"] == "2"  # host file overrides default file
    assert env["C"] == "3"  # inheritance overrides host file
    assert env["D"] == "4"  # default_env_vars overrides inheritance
    assert env["E"] == "5"  # host env_vars
    assert env["DAGSTER_RUN_JOB_NAME"] == "j"
    assert env["DAGSTER_RUN_ID"] == "r1"


def test_inherit_uses_location_placeholder(monkeypatch):
    launcher = make_launcher(monkeypatch)
    seen = {}

    class FakeContainer:
        attrs = {"Config": {"Env": ["FROM_SERVER=yes"]}}

    class FakeClient:
        def __init__(self):
            self.containers = SimpleNamespace(get=self._get)

        def _get(self, name):
            seen["name"] = name
            return FakeContainer()

    host_info = {
        "host_name": "host-b",
        "client": FakeClient(),
        "inherit_env_from_container": "code-{location}",
    }
    env = launcher._build_env_vars(make_run("payments"), host_info)
    assert seen["name"] == "code-payments"
    assert env["FROM_SERVER"] == "yes"


def test_inherit_missing_container_is_non_fatal(monkeypatch):
    launcher = make_launcher(monkeypatch)

    class FakeClient:
        def __init__(self):
            self.containers = SimpleNamespace(get=self._raise)

        def _raise(self, name):
            raise docker.errors.NotFound("nope")

    host_info = {
        "host_name": "host-b",
        "client": FakeClient(),
        "inherit_env_from_container": "missing",
        "env_vars": ["KEEP=1"],
    }
    env = launcher._build_env_vars(make_run("loc"), host_info)
    assert env["KEEP"] == "1"  # run still gets its env despite the missing server
