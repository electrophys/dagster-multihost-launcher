from types import SimpleNamespace

import docker

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


class FakeImage:
    tags = ["img:latest"]
    id = "sha256:abc"


class FakeContainer:
    def __init__(self, cid, finished_at, removed):
        self.id = cid
        self.short_id = cid[:12]
        self.status = "exited"
        self.labels = {"dagster/run_id": "run-" + cid, "dagster/managed": "true"}
        self.image = FakeImage()
        self.attrs = {
            "Created": "2020-01-01T00:00:00Z",
            "State": {"FinishedAt": finished_at, "ExitCode": 0},
        }
        self._removed = removed

    def remove(self, force=False):
        self._removed.append(self.id)


class FakeClient:
    def __init__(self, containers):
        self._containers = containers
        self.containers = SimpleNamespace(list=self._list, get=self._get)

    def _list(self, all=True, filters=None):
        return self._containers

    def _get(self, cid):
        for c in self._containers:
            if c.id == cid:
                return c
        raise docker.errors.NotFound(cid)


def _launcher(monkeypatch, containers):
    client = FakeClient(containers)
    monkeypatch.setattr(
        MultiHostDockerRunLauncher,
        "_build_docker_client",
        staticmethod(lambda cfg: client),
    )
    return MultiHostDockerRunLauncher(
        docker_hosts=[{"host_name": "h", "location_names": ["l"]}]
    )


def test_cleanup_removes_only_old_exited(monkeypatch):
    removed = []
    old = FakeContainer("old1234567890", "2000-01-01T00:00:00Z", removed)
    recent = FakeContainer("new1234567890", "2999-01-01T00:00:00Z", removed)
    launcher = _launcher(monkeypatch, [old, recent])

    result = launcher.cleanup_old_containers(max_age_hours=24, dry_run=False)

    assert removed == ["old1234567890"]
    assert {r["container_id"] for r in result} == {"old1234567890"}
    assert all(r["action"] == "removed" for r in result)


def test_cleanup_dry_run_removes_nothing(monkeypatch):
    removed = []
    old = FakeContainer("old1234567890", "2000-01-01T00:00:00Z", removed)
    launcher = _launcher(monkeypatch, [old])

    result = launcher.cleanup_old_containers(max_age_hours=24, dry_run=True)

    assert removed == []
    assert [r["action"] for r in result] == ["would_remove"]
