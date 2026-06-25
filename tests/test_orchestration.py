from dagster_multihost_launcher.orchestration import (
    SavedInstigator,
    WorkspaceOrchestrator,
    load_instigators,
    save_instigators,
)


def _sched(name, status):
    return {
        "name": name,
        "scheduleState": {"status": status},
        "repositoryOrigin": {
            "repositoryName": "repo",
            "repositoryLocationName": "loc",
        },
    }


def _sensor(name, status):
    return {
        "name": name,
        "sensorState": {"status": status},
        "repositoryOrigin": {
            "repositoryName": "repo",
            "repositoryLocationName": "loc",
        },
    }


class FakeClient:
    def __init__(self, schedules=None, sensors=None, start_results=None):
        self._schedules = schedules or []
        self._sensors = sensors or []
        self._start_results = start_results or {}
        self.stopped = []
        self.started = []

    def get_schedules(self, loc):
        return self._schedules

    def get_sensors(self, loc):
        return self._sensors

    def stop_schedule(self, name, repo, loc):
        self.stopped.append(("schedule", name))
        return True

    def stop_sensor(self, name, repo, loc):
        self.stopped.append(("sensor", name))
        return True

    def start_schedule(self, name, repo, loc):
        self.started.append(("schedule", name))
        return self._start_results.get(name, True)

    def start_sensor(self, name, repo, loc):
        self.started.append(("sensor", name))
        return self._start_results.get(name, True)


def test_stop_instigators_only_running():
    client = FakeClient(
        schedules=[_sched("s_on", "RUNNING"), _sched("s_off", "STOPPED")],
        sensors=[_sensor("sn_on", "RUNNING"), _sensor("sn_off", "STOPPED")],
    )
    orch = WorkspaceOrchestrator(client)
    stopped = orch.stop_instigators("loc")

    assert client.stopped == [("schedule", "s_on"), ("sensor", "sn_on")]
    assert {(i.kind, i.name) for i in stopped} == {
        ("schedule", "s_on"),
        ("sensor", "sn_on"),
    }


def test_restart_instigators_all_ok():
    client = FakeClient()
    orch = WorkspaceOrchestrator(client)
    instigators = [
        SavedInstigator("schedule", "s1", "repo", "loc"),
        SavedInstigator("sensor", "sn1", "repo", "loc"),
    ]
    assert orch.restart_instigators(instigators) is True
    assert client.started == [("schedule", "s1"), ("sensor", "sn1")]


def test_restart_instigators_reports_failure():
    client = FakeClient(start_results={"s1": False})
    orch = WorkspaceOrchestrator(client)
    instigators = [
        SavedInstigator("schedule", "s1", "repo", "loc"),
        SavedInstigator("sensor", "sn1", "repo", "loc"),
    ]
    # one fails -> overall False, but the other still attempted
    assert orch.restart_instigators(instigators) is False
    assert ("sensor", "sn1") in client.started


def test_state_file_roundtrip(tmp_path):
    instigators = [
        SavedInstigator("schedule", "s1", "repo", "loc_a"),
        SavedInstigator("sensor", "sn1", "repo", "loc_b"),
    ]
    path = tmp_path / "drain.json"
    save_instigators(str(path), instigators)
    assert load_instigators(str(path)) == instigators
