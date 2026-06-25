from types import SimpleNamespace

import pytest

from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


def make_run(image_tag=None, container_image=None, run_id="r1"):
    repo_origin = SimpleNamespace(container_image=container_image)
    origin = SimpleNamespace(repository_origin=repo_origin)
    tags = {"dagster/image": image_tag} if image_tag else {}
    return SimpleNamespace(
        run_id=run_id,
        tags=tags,
        job_code_origin=origin,
        remote_job_origin=SimpleNamespace(location_name="loc"),
    )


VALID = [
    "myimage",
    "myimage:latest",
    "my_org/my-image:1.2.3",
    "ghcr.io/org/img:1.2.3",
    "registry.example.com/team/app:v2",
    "10.6.4.160:5000/etl:abc123",
    "localhost:5000/img",
    "alpine@sha256:" + "a" * 64,
    "repo/img:1.2.3-aarch64",
]

INVALID = [
    "MyImage",  # uppercase repo
    "myimage:",  # empty tag
    ":latest",  # no repo
    "https://reg/img:tag",  # scheme
    "my image:latest",  # internal whitespace
]


@pytest.mark.parametrize("ref", VALID)
def test_valid_references_pass(ref):
    # _validate_image_reference assumes already-trimmed input
    MultiHostDockerRunLauncher._validate_image_reference(ref, make_run(), "host-b")


@pytest.mark.parametrize("ref", INVALID)
def test_invalid_references_raise(ref):
    with pytest.raises(Exception):
        MultiHostDockerRunLauncher._validate_image_reference(
            ref.strip() if ref.strip() else ref, make_run(), "host-b"
        )


def test_resolve_run_image_strips_whitespace(monkeypatch):
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = make_run(image_tag="  ghcr.io/org/app:1.0  ")
    assert launcher._resolve_run_image(run, "host-b") == "ghcr.io/org/app:1.0"


def test_resolve_run_image_prefers_tag_over_origin():
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = make_run(image_tag="org/from-tag:1", container_image="org/from-origin:2")
    assert launcher._resolve_run_image(run, "host-b") == "org/from-tag:1"


def test_resolve_run_image_falls_back_to_origin():
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = make_run(container_image="org/from-origin:2")
    assert launcher._resolve_run_image(run, "host-b") == "org/from-origin:2"


def test_resolve_run_image_missing_raises():
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = make_run()  # no tag, no container_image
    with pytest.raises(Exception, match="Could not determine Docker image"):
        launcher._resolve_run_image(run, "host-b")


def test_resolve_run_image_rejects_malformed_with_host_context():
    launcher = MultiHostDockerRunLauncher(docker_hosts=[])
    run = make_run(image_tag="https://reg/img:tag")
    with pytest.raises(Exception, match="host-b"):
        launcher._resolve_run_image(run, "host-b")
