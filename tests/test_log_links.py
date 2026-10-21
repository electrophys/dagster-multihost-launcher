"""Hooks for external log collection: identifying labels on run containers, and
a link to the run's logs (e.g. in Grafana) posted at launch."""

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


def make_run(location_name="etl", run_id="abc-123", job_name="my_job"):
    origin = SimpleNamespace(location_name=location_name)
    return SimpleNamespace(
        run_id=run_id, job_name=job_name, remote_job_origin=origin, tags={}
    )


class RecordingInstance:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    def report_engine_event(self, message, run, engine_event_data=None, cls=None):
        if self.fail:
            raise RuntimeError("storage down")
        self.events.append((message, engine_event_data))


def test_container_labels_identify_the_run(monkeypatch):
    labels = make_launcher(monkeypatch)._container_labels(make_run(), "host-b")
    assert labels == {
        "dagster/run_id": "abc-123",
        "dagster/job_name": "my_job",
        "dagster/location_name": "etl",
        "dagster/host_name": "host-b",
        "dagster/managed": "true",
    }


def test_no_template_no_url(monkeypatch):
    assert make_launcher(monkeypatch)._log_url(make_run(), "host-b") is None


def test_template_fills_placeholders_and_keeps_literal_braces(monkeypatch):
    template = (
        'https://grafana/explore?panes={"a":{"expr":"run={run_id}"}}'
        "&loc={location}&host={host_name}&job={job_name}&from={launch_ms}"
    )
    monkeypatch.setattr("dagster_multihost_launcher.launcher.time.time", lambda: 1700.5)
    launcher = make_launcher(monkeypatch, log_url_template=template)

    url = launcher._log_url(make_run(), "host-b")
    assert url == (
        'https://grafana/explore?panes={"a":{"expr":"run=abc-123"}}'
        "&loc=etl&host=host-b&job=my_job&from=1700500"
    )


def test_values_are_url_encoded(monkeypatch):
    launcher = make_launcher(monkeypatch, log_url_template="https://x/?job={job_name}")
    assert (
        launcher._log_url(make_run(job_name="a b&c"), None)
        == "https://x/?job=a%20b%26c"
    )


def test_default_launcher_runs_get_empty_host(monkeypatch):
    launcher = make_launcher(monkeypatch, log_url_template="https://x/?h={host_name}")
    assert launcher._log_url(make_run(), None) == "https://x/?h="


def test_report_posts_url_metadata(monkeypatch):
    launcher = make_launcher(monkeypatch, log_url_template="https://x/{run_id}")
    instance = RecordingInstance()
    monkeypatch.setattr(type(launcher), "_instance", property(lambda self: instance))

    launcher._report_log_url(make_run(), "host-b")

    [(_, data)] = instance.events
    assert data.metadata["logs"].url == "https://x/abc-123"


def test_report_failure_is_non_fatal(monkeypatch):
    launcher = make_launcher(monkeypatch, log_url_template="https://x/{run_id}")
    instance = RecordingInstance(fail=True)
    monkeypatch.setattr(type(launcher), "_instance", property(lambda self: instance))

    launcher._report_log_url(make_run(), "host-b")  # must not raise
