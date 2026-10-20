"""Network resolution for run containers: explicit config wins, otherwise the
run joins the code-location server's networks."""

from types import SimpleNamespace

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


class DummyClient:
    pass


def make_launcher(monkeypatch, **kwargs):
    monkeypatch.setattr(
        "dagster_multihost_launcher.launcher.MultiHostDockerRunLauncher._build_docker_client",
        staticmethod(lambda cfg: DummyClient()),
    )
    return MultiHostDockerRunLauncher(docker_hosts=[], **kwargs)


def make_server(networks, name="code_server"):
    """A fake code-server container attached to `networks`, each with an alias."""
    return SimpleNamespace(
        name=name,
        attrs={
            "NetworkSettings": {
                "Networks": {
                    net: {"Aliases": [f"{name}-alias"], "IPAddress": "172.18.0.2"}
                    for net in networks
                }
            }
        },
    )


def test_inherits_server_networks_when_unconfigured(monkeypatch):
    launcher = make_launcher(monkeypatch)
    server = make_server(["host_b_dagster_network", "shared_net"])

    assert launcher._resolve_networks({"host_name": "host-b"}, server) == [
        "host_b_dagster_network",
        "shared_net",
    ]


def test_inheritance_does_not_copy_aliases(monkeypatch):
    """Only network names are inherited -- an inherited alias would let the run
    container answer gRPC lookups meant for the code server."""
    launcher = make_launcher(monkeypatch)
    server = make_server(["netA"])

    resolved = launcher._resolve_networks({"host_name": "host-b"}, server)

    assert resolved == ["netA"]
    assert all(isinstance(n, str) for n in resolved)
    assert "code_server-alias" not in resolved


def test_explicit_network_overrides_inheritance(monkeypatch):
    launcher = make_launcher(monkeypatch)
    server = make_server(["inherited_net"])

    host_info = {"host_name": "host-b", "network": "explicit_net"}
    assert launcher._resolve_networks(host_info, server) == ["explicit_net"]


def test_explicit_networks_list_overrides_inheritance(monkeypatch):
    launcher = make_launcher(monkeypatch)
    server = make_server(["inherited_net"])

    host_info = {"host_name": "host-b", "networks": ["a", "b"]}
    assert launcher._resolve_networks(host_info, server) == ["a", "b"]


def test_network_is_first_and_deduped_against_networks(monkeypatch):
    launcher = make_launcher(monkeypatch)

    host_info = {
        "host_name": "host-b",
        "network": "primary",
        "networks": ["primary", "extra"],
    }
    assert launcher._resolve_networks(host_info, None) == ["primary", "extra"]


def test_no_server_and_no_config_yields_no_networks(monkeypatch):
    """Docker's own default applies, as before inheritance existed."""
    launcher = make_launcher(monkeypatch)
    assert launcher._resolve_networks({"host_name": "host-b"}, None) == []


def test_none_network_opts_out_of_inheritance(monkeypatch):
    launcher = make_launcher(monkeypatch)
    server = make_server(["inherited_net"])

    host_info = {"host_name": "host-b", "network": "none"}
    assert launcher._resolve_networks(host_info, server) == ["none"]


def test_server_with_no_networks_yields_empty(monkeypatch):
    launcher = make_launcher(monkeypatch)
    server = SimpleNamespace(name="srv", attrs={})

    assert launcher._resolve_networks({"host_name": "host-b"}, server) == []
