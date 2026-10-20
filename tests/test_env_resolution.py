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


def make_run(location_name="loc", run_id="r1", job_name="j", grpc_port=None):
    origin = SimpleNamespace(location_name=location_name)
    if grpc_port is not None:
        origin.repository_origin = SimpleNamespace(
            code_location_origin=SimpleNamespace(host="10.0.1.2", port=grpc_port)
        )
    return SimpleNamespace(
        run_id=run_id, job_name=job_name, remote_job_origin=origin, tags={}
    )


def make_container(name, env, host_port=None):
    """A fake docker container publishing host_port, with Config.Env of env."""
    ports = (
        {"4000/tcp": [{"HostIp": "0.0.0.0", "HostPort": str(host_port)}]}
        if host_port is not None
        else {}
    )
    return SimpleNamespace(
        name=name,
        attrs={"Config": {"Env": env}, "NetworkSettings": {"Ports": ports}},
    )


def make_listing_client(containers):
    return SimpleNamespace(containers=SimpleNamespace(list=lambda: containers))


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


# -- env inheritance by auto-discovery -------------------------------------


def test_discovers_code_server_by_grpc_port(monkeypatch):
    """With no inherit_env_from_container set, the server is found as the
    container publishing the code location's gRPC port."""
    launcher = make_launcher(monkeypatch)
    client = make_listing_client(
        [
            make_container("unrelated", ["WRONG=1"], host_port=5432),
            make_container("whatever_its_named", ["FROM_SERVER=yes"], host_port=4001),
        ]
    )

    host_info = {"host_name": "host-b", "client": client}
    env = launcher._build_env_vars(make_run("payments", grpc_port=4001), host_info)

    assert env["FROM_SERVER"] == "yes"
    assert "WRONG" not in env


def test_explicit_container_name_overrides_discovery(monkeypatch):
    launcher = make_launcher(monkeypatch)

    named = make_container("code-payments", ["SOURCE=by_name"])
    client = SimpleNamespace(
        containers=SimpleNamespace(
            get=lambda name: named,
            list=lambda: [make_container("x", ["SOURCE=by_port"], host_port=4001)],
        )
    )

    host_info = {
        "host_name": "host-b",
        "client": client,
        "inherit_env_from_container": "code-{location}",
    }
    env = launcher._build_env_vars(make_run("payments", grpc_port=4001), host_info)
    assert env["SOURCE"] == "by_name"


def test_inherit_off_disables_inheritance(monkeypatch):
    launcher = make_launcher(monkeypatch)
    client = make_listing_client(
        [make_container("code", ["FROM_SERVER=yes"], host_port=4001)]
    )

    host_info = {
        "host_name": "host-b",
        "client": client,
        "inherit_env_from_container": "off",
        "env_vars": ["KEEP=1"],
    }
    env = launcher._build_env_vars(make_run("payments", grpc_port=4001), host_info)

    assert "FROM_SERVER" not in env
    assert env["KEEP"] == "1"


def test_discovery_no_matching_port_is_non_fatal(monkeypatch):
    launcher = make_launcher(monkeypatch)
    client = make_listing_client([make_container("other", ["NOPE=1"], host_port=9999)])

    host_info = {"host_name": "host-b", "client": client, "env_vars": ["KEEP=1"]}
    env = launcher._build_env_vars(make_run("payments", grpc_port=4001), host_info)

    assert "NOPE" not in env
    assert env["KEEP"] == "1"


def test_discovery_skipped_when_origin_has_no_port(monkeypatch):
    """Locations without a gRPC port in their origin simply skip inheritance."""
    launcher = make_launcher(monkeypatch)

    def explode():
        raise AssertionError("should not list containers without a port")

    host_info = {
        "host_name": "host-b",
        "client": SimpleNamespace(containers=SimpleNamespace(list=explode)),
    }
    env = launcher._build_env_vars(make_run("payments"), host_info)
    assert env["DAGSTER_RUN_ID"] == "r1"


def test_discovery_docker_error_is_non_fatal(monkeypatch):
    launcher = make_launcher(monkeypatch)

    def boom():
        raise docker.errors.APIError("daemon down")

    host_info = {
        "host_name": "host-b",
        "client": SimpleNamespace(containers=SimpleNamespace(list=boom)),
        "env_vars": ["KEEP=1"],
    }
    env = launcher._build_env_vars(make_run("payments", grpc_port=4001), host_info)
    assert env["KEEP"] == "1"


def test_discovers_code_server_by_origin_hostname(monkeypatch):
    """A shared-network code server publishes no port, but the workspace's gRPC
    host is its container name."""
    launcher = make_launcher(monkeypatch)
    named = make_container("admin_code_location", ["FROM_SERVER=by_name"])

    def get(name):
        assert name == "admin_code_location"
        return named

    client = SimpleNamespace(containers=SimpleNamespace(get=get, list=lambda: []))
    run = make_run("admin", grpc_port=4000)
    run.remote_job_origin.repository_origin.code_location_origin.host = (
        "admin_code_location"
    )

    env = launcher._build_env_vars(run, {"host_name": "host-a", "client": client})
    assert env["FROM_SERVER"] == "by_name"


def test_ip_origin_host_falls_through_to_port_scan(monkeypatch):
    """An IP names a machine, not a container, so discovery must not look it
    up as a container name."""
    launcher = make_launcher(monkeypatch)

    def get(name):
        raise AssertionError(f"should not look up container by IP: {name}")

    client = SimpleNamespace(
        containers=SimpleNamespace(
            get=get,
            list=lambda: [
                make_container("srv", ["FROM_SERVER=by_port"], host_port=4001)
            ],
        )
    )
    env = launcher._build_env_vars(
        make_run("payments", grpc_port=4001),  # origin host is 10.0.1.2
        {"host_name": "host-b", "client": client},
    )
    assert env["FROM_SERVER"] == "by_port"
