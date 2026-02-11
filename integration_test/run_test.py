#!/usr/bin/env python3
"""
Integration test runner for dagster-multihost-launcher.

Deploys the integration test across 3 hosts, runs materializations,
and verifies that run routing works correctly.

Usage:
    python integration_test/run_test.py \
        --host-a-ip <HOST_A_IP> \
        --host-b-ip <HOST_B_IP> \
        --host-d-ip <HOST_D_IP>

Assumes:
    - Remote hosts are reachable via SSH (key-based auth)
    - Docker TLS is already configured on Host B
    - uv and dependencies are already installed on Host D
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

# ── Paths ──────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
INTEGRATION_DIR = REPO_ROOT / "integration_test"

# Files containing IP placeholders
TEMPLATED_FILES = [
    INTEGRATION_DIR / "host_a" / "dagster.yaml",
    INTEGRATION_DIR / "host_a" / "workspace.yaml",
    INTEGRATION_DIR / "host_b" / "docker-compose.yml",
    INTEGRATION_DIR / "host_d" / "dagster.yaml",
    INTEGRATION_DIR / "host_d" / "start_grpc.ps1",
]

GRAPHQL_URL = "http://localhost:3001/graphql"

EXPECTED_LOCATIONS = ["admin", "dockerized_test_location", "bare_process_test_location"]


# ── Terminal output helpers ────────────────────────────────────────────────


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def log(prefix, msg, color=Colors.CYAN):
    print(f"{color}[{prefix:6s}]{Colors.RESET} {msg}")


def log_ok(msg):
    print(f"         {Colors.GREEN}\u2713 {msg}{Colors.RESET}")


def log_fail(msg):
    print(f"         {Colors.RED}\u2717 {msg}{Colors.RESET}")


# ── Shell helpers ──────────────────────────────────────────────────────────


def run_cmd(args, check=True, capture=False, **kwargs):
    """Run a local command with logging."""
    cmd_str = " ".join(str(a) for a in args)
    log("CMD", cmd_str, Colors.YELLOW)
    return subprocess.run(
        args,
        check=check,
        capture_output=capture,
        text=True if capture else None,
        **kwargs,
    )


def ssh_run(user, host, command, check=True):
    """Run a command on a remote host via SSH."""
    return run_cmd(
        ["ssh", "-o", "StrictHostKeyChecking=no", f"{user}@{host}", command],
        check=check,
    )


def scp_to_remote(user, host, local_paths, remote_dir):
    """Copy local paths to a remote directory via scp."""
    args = ["scp", "-o", "StrictHostKeyChecking=no", "-r"]
    args.extend(str(p) for p in local_paths)
    args.append(f"{user}@{host}:{remote_dir}")
    run_cmd(args)


# ── GraphQL helpers ────────────────────────────────────────────────────────


def graphql_query(query):
    """Execute a GraphQL query against the Dagster webserver."""
    payload = json.dumps({"query": query}).encode("utf-8")
    req = Request(
        GRAPHQL_URL, data=payload, headers={"Content-Type": "application/json"}
    )
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def launch_run(mutation):
    """Launch a run via GraphQL and return the run ID."""
    result = graphql_query(mutation)
    data = result.get("data", {})

    launch_result = data.get("launchRun", {})
    typename = launch_result.get("__typename", "")

    if typename == "LaunchRunSuccess":
        run_id = launch_result["run"]["runId"]
        return run_id

    # Error
    error_msg = launch_result.get("message", json.dumps(result, indent=2))
    raise RuntimeError(f"Failed to launch run: {error_msg}")


def wait_for_run(run_id, timeout=180):
    """Poll a run until it reaches a terminal state. Returns the final status."""
    terminal_states = {"SUCCESS", "FAILURE", "CANCELED"}
    query = (
        '{ runOrError(runId: "' + run_id + '") { '
        "... on Run { runId status } "
        "... on PythonError { message } "
        "} }"
    )

    deadline = time.time() + timeout
    while time.time() < deadline:
        result = graphql_query(query)
        run_data = result.get("data", {}).get("runOrError", {})
        status = run_data.get("status")
        if status in terminal_states:
            return status
        time.sleep(5)

    raise TimeoutError(
        f"Run {run_id} did not complete within {timeout}s (last status: {status})"
    )


def get_run_tags(run_id):
    """Fetch run tags as a dict."""
    query = (
        '{ runOrError(runId: "' + run_id + '") { '
        "... on Run { tags { key value } } "
        "} }"
    )
    result = graphql_query(query)
    tags_list = result["data"]["runOrError"]["tags"]
    return {t["key"]: t["value"] for t in tags_list}


def get_asset_metadata(asset_name):
    """Fetch the latest materialization metadata for an asset."""
    query = (
        '{ assetOrError(assetKey: { path: ["' + asset_name + '"] }) { '
        "... on Asset { assetMaterializations(limit: 1) { "
        "metadataEntries { label ... on TextMetadataEntry { text } "
        "... on IntMetadataEntry { intValue } "
        "... on JsonMetadataEntry { jsonString } } } } } }"
    )
    result = graphql_query(query)
    asset_data = result["data"]["assetOrError"]
    materializations = asset_data.get("assetMaterializations", [])
    if not materializations:
        return {}
    entries = materializations[0].get("metadataEntries", [])
    metadata = {}
    for e in entries:
        label = e["label"]
        if "text" in e:
            metadata[label] = e["text"]
        elif "intValue" in e:
            metadata[label] = e["intValue"]
        elif "jsonString" in e:
            metadata[label] = json.loads(e["jsonString"])
    return metadata


# ── IP substitution ────────────────────────────────────────────────────────


def substitute_ips(host_a_ip, host_b_ip, host_d_ip):
    """Replace placeholders in config files with real IPs."""
    log("DEPLOY", "Substituting IPs into config files...")
    for fpath in TEMPLATED_FILES:
        content = fpath.read_text()
        content = content.replace("<HOST_A_IP>", host_a_ip)
        content = content.replace("<HOST_B_IP>", host_b_ip)
        content = content.replace("<HOST_D_IP>", host_d_ip)
        fpath.write_text(content)


def restore_placeholders(host_a_ip, host_b_ip, host_d_ip):
    """Restore IP placeholders in config files."""
    log("CLEAN", "Restoring IP placeholders in config files...")
    for fpath in TEMPLATED_FILES:
        content = fpath.read_text()
        content = content.replace(host_a_ip, "<HOST_A_IP>")
        content = content.replace(host_b_ip, "<HOST_B_IP>")
        content = content.replace(host_d_ip, "<HOST_D_IP>")
        fpath.write_text(content)


# ── Deployment ─────────────────────────────────────────────────────────────


def deploy_host_a():
    """Start Host A control plane via docker compose."""
    log("DEPLOY", "Starting Host A (docker compose up --build)...")
    run_cmd(
        [
            "docker",
            "compose",
            "-f",
            str(INTEGRATION_DIR / "host_a" / "docker-compose.yml"),
            "up",
            "-d",
            "--build",
        ]
    )

    # Wait for webserver to be reachable
    log("DEPLOY", "Waiting for webserver at http://localhost:3001 ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            req = Request(
                GRAPHQL_URL,
                data=b'{"query":"{ __typename }"}',
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=5):
                pass
            log("DEPLOY", "Webserver is up.")
            return
        except (URLError, OSError):
            time.sleep(3)
    raise TimeoutError("Webserver did not become reachable within 120s")


def deploy_host_b(user, host):
    """Deploy code location to Host B (remote Docker host)."""
    log("DEPLOY", f"Deploying to Host B ({user}@{host})...")

    # Ensure remote directory exists
    ssh_run(user, host, "mkdir -p ~/dagster-test")

    # Copy files
    scp_to_remote(
        user,
        host,
        [
            INTEGRATION_DIR / "host_b",
            INTEGRATION_DIR / "dockerized_test_location",
        ],
        "~/dagster-test/",
    )

    # Build image and start containers (rootful docker)
    ssh_run(
        user,
        host,
        (
            "cd ~/dagster-test && "
            "sudo docker build -t dockerized_test_location:latest "
            "-f host_b/Dockerfile_dockerized . && "
            "sudo docker compose -f host_b/docker-compose.yml up -d"
        ),
    )


def deploy_host_d(user, host):
    """Deploy code location to Host D (Windows bare-process host)."""
    log("DEPLOY", f"Deploying to Host D ({user}@{host})...")

    # Ensure remote directory exists
    ssh_run(
        user,
        host,
        'powershell -Command "New-Item -ItemType Directory -Force -Path C:\\dagster-test"',
        check=False,
    )

    # Copy files
    scp_to_remote(
        user,
        host,
        [
            INTEGRATION_DIR / "bare_process_test_location",
            INTEGRATION_DIR / "host_d" / "dagster.yaml",
            INTEGRATION_DIR / "host_d" / "start_grpc.ps1",
        ],
        "C:/dagster-test/",
    )

    # Start gRPC server as background process
    ssh_run(
        user,
        host,
        (
            'powershell -Command "'
            "Start-Process powershell -ArgumentList '-File','C:\\dagster-test\\start_grpc.ps1'"
            '"'
        ),
    )


# ── Wait for workspace ────────────────────────────────────────────────────


def wait_for_workspace(timeout=120):
    """Poll until all expected code locations are loaded."""
    log("WAIT", "Waiting for all code locations to load...")
    query = (
        "{ workspaceOrError { ... on Workspace { "
        "locationEntries { name loadStatus } } } }"
    )

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = graphql_query(query)
            entries = result["data"]["workspaceOrError"]["locationEntries"]
            loaded = {e["name"] for e in entries if e["loadStatus"] == "LOADED"}
            missing = set(EXPECTED_LOCATIONS) - loaded
            if not missing:
                log_ok(f"All {len(EXPECTED_LOCATIONS)} locations loaded")
                return
            log("WAIT", f"  Still waiting for: {', '.join(sorted(missing))}")
        except Exception:
            pass
        time.sleep(5)

    raise TimeoutError(
        f"Not all locations loaded within {timeout}s. Missing: {', '.join(sorted(missing))}"
    )


def reload_workspace():
    """Trigger a workspace reload via GraphQL."""
    log("WAIT", "Reloading workspace...")
    query = (
        "mutation { reloadWorkspace { "
        "... on Workspace { locationEntries { name loadStatus } } } }"
    )
    graphql_query(query)


# ── Teardown ───────────────────────────────────────────────────────────────


def teardown_host_a():
    """Stop Host A services."""
    log("CLEAN", "Tearing down Host A...")
    run_cmd(
        [
            "docker",
            "compose",
            "-f",
            str(INTEGRATION_DIR / "host_a" / "docker-compose.yml"),
            "down",
        ],
        check=False,
    )


def teardown_host_b(user, host):
    """Stop Host B services."""
    log("CLEAN", f"Tearing down Host B ({user}@{host})...")
    ssh_run(
        user,
        host,
        (
            "cd ~/dagster-test && "
            "sudo docker compose -f host_b/docker-compose.yml down"
        ),
        check=False,
    )


def teardown_host_d(user, host):
    """Stop Host D gRPC server."""
    log("CLEAN", f"Tearing down Host D ({user}@{host})...")
    ssh_run(
        user,
        host,
        (
            'powershell -Command "'
            "Get-Process | Where-Object { $_.Path -like '*dagster*' } | "
            'Stop-Process -Force -ErrorAction SilentlyContinue"'
        ),
        check=False,
    )


# ── Tests ──────────────────────────────────────────────────────────────────


def test_dockerized_materialization():
    """Test 1: Docker routing — materialize on remote Docker host."""
    print()
    log(
        "TEST",
        f"{Colors.BOLD}Test 1: Dockerized materialization (Docker routing){Colors.RESET}",
    )

    mutation = (
        "mutation { launchRun(executionParams: { "
        "selector: { "
        'repositoryLocationName: "dockerized_test_location", '
        'repositoryName: "__repository__", '
        'jobName: "__ASSET_JOB", '
        'assetSelection: [{ path: ["dockerized_proof_of_execution"] }] '
        "}, "
        "executionMetadata: { tags: [] }, "
        'runConfigData: "{}" '
        "}) { __typename "
        "... on LaunchRunSuccess { run { runId } } "
        "... on PythonError { message } } }"
    )

    run_id = launch_run(mutation)
    log("TEST", f"Launched run {run_id}, waiting...")

    passed = True

    status = wait_for_run(run_id)
    if status == "SUCCESS":
        log_ok("Run succeeded")
    else:
        log_fail(f"Run status: {status}")
        passed = False

    tags = get_run_tags(run_id)

    launcher_type = tags.get("multihost_docker/launcher_type")
    if launcher_type == "docker":
        log_ok("launcher_type = docker")
    else:
        log_fail(f"launcher_type = {launcher_type} (expected docker)")
        passed = False

    host_name = tags.get("multihost_docker/host_name")
    if host_name == "docker-host-b":
        log_ok("host_name = docker-host-b")
    else:
        log_fail(f"host_name = {host_name} (expected docker-host-b)")
        passed = False

    metadata = get_asset_metadata("dockerized_proof_of_execution")
    arch = metadata.get("architecture", "")
    if "aarch64" in arch:
        log_ok(f"architecture = {arch}")
    else:
        log_fail(f"architecture = {arch} (expected aarch64)")
        passed = False

    return passed


def test_bare_process_materialization():
    """Test 2: DefaultRunLauncher fallback — materialize on bare-process host."""
    print()
    log(
        "TEST",
        f"{Colors.BOLD}Test 2: Bare process materialization (DefaultRunLauncher fallback){Colors.RESET}",
    )

    mutation = (
        "mutation { launchRun(executionParams: { "
        "selector: { "
        'repositoryLocationName: "bare_process_test_location", '
        'repositoryName: "__repository__", '
        'jobName: "__ASSET_JOB", '
        'assetSelection: [{ path: ["bare_process_proof_of_execution"] }] '
        "}, "
        "executionMetadata: { tags: [] }, "
        'runConfigData: "{}" '
        "}) { __typename "
        "... on LaunchRunSuccess { run { runId } } "
        "... on PythonError { message } } }"
    )

    run_id = launch_run(mutation)
    log("TEST", f"Launched run {run_id}, waiting...")

    passed = True

    status = wait_for_run(run_id)
    if status == "SUCCESS":
        log_ok("Run succeeded")
    else:
        log_fail(f"Run status: {status}")
        passed = False

    tags = get_run_tags(run_id)

    launcher_type = tags.get("multihost_docker/launcher_type")
    if launcher_type == "default":
        log_ok("launcher_type = default")
    else:
        log_fail(f"launcher_type = {launcher_type} (expected default)")
        passed = False

    metadata = get_asset_metadata("bare_process_proof_of_execution")
    os_name = metadata.get("os_name", "")
    if os_name == "nt":
        log_ok(f"os_name = {os_name}")
    else:
        log_fail(f"os_name = {os_name} (expected nt)")
        passed = False

    return passed


def test_admin_job():
    """Test 3: Admin job — container status + cleanup via DefaultRunLauncher."""
    print()
    log(
        "TEST",
        f"{Colors.BOLD}Test 3: Admin job (container status + cleanup){Colors.RESET}",
    )

    mutation = (
        "mutation { launchRun(executionParams: { "
        "selector: { "
        'repositoryLocationName: "admin", '
        'repositoryName: "__repository__", '
        'jobName: "multihost_admin_job" '
        "}, "
        "executionMetadata: { tags: [] }, "
        'runConfigData: "{}" '
        "}) { __typename "
        "... on LaunchRunSuccess { run { runId } } "
        "... on PythonError { message } } }"
    )

    run_id = launch_run(mutation)
    log("TEST", f"Launched run {run_id}, waiting...")

    passed = True

    status = wait_for_run(run_id)
    if status == "SUCCESS":
        log_ok("Run succeeded")
    else:
        log_fail(f"Run status: {status}")
        passed = False

    tags = get_run_tags(run_id)

    launcher_type = tags.get("multihost_docker/launcher_type")
    if launcher_type == "default":
        log_ok("launcher_type = default")
    else:
        log_fail(f"launcher_type = {launcher_type} (expected default)")
        passed = False

    return passed


# ── Main ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Integration test runner for dagster-multihost-launcher"
    )
    parser.add_argument(
        "--host-a-ip", required=True, help="IP of Host A (control plane)"
    )
    parser.add_argument(
        "--host-b-ip", required=True, help="IP of Host B (remote Docker)"
    )
    parser.add_argument(
        "--host-d-ip", required=True, help="IP of Host D (bare process)"
    )
    parser.add_argument(
        "--host-b-user", default="pi", help="SSH user for Host B (default: pi)"
    )
    parser.add_argument(
        "--host-d-user", default="ihenr", help="SSH user for Host D (default: ihenr)"
    )
    parser.add_argument(
        "--skip-deploy", action="store_true", help="Skip deployment, just run tests"
    )
    parser.add_argument(
        "--skip-teardown",
        action="store_true",
        help="Leave services running after tests",
    )
    args = parser.parse_args()

    results = []

    try:
        # Substitute IPs
        substitute_ips(args.host_a_ip, args.host_b_ip, args.host_d_ip)

        if not args.skip_deploy:
            # Deploy
            deploy_host_a()
            deploy_host_b(args.host_b_user, args.host_b_ip)
            deploy_host_d(args.host_d_user, args.host_d_ip)

            # Reload workspace so all locations are picked up
            reload_workspace()

        # Wait for all locations
        wait_for_workspace()

        # Run tests
        results.append(
            ("Dockerized materialization", test_dockerized_materialization())
        )
        results.append(
            ("Bare process materialization", test_bare_process_materialization())
        )
        results.append(("Admin job", test_admin_job()))

    except Exception as e:
        print(f"\n{Colors.RED}{Colors.BOLD}ERROR: {e}{Colors.RESET}")
        results.append(("Setup/Infrastructure", False))

    finally:
        # Teardown
        if not args.skip_teardown and not args.skip_deploy:
            print()
            teardown_host_d(args.host_d_user, args.host_d_ip)
            teardown_host_b(args.host_b_user, args.host_b_ip)
            teardown_host_a()

        # Always restore placeholders
        restore_placeholders(args.host_a_ip, args.host_b_ip, args.host_d_ip)

    # Summary
    print()
    total = len(results)
    passed = sum(1 for _, ok in results if ok)
    bar = "\u2550" * 40

    if passed == total:
        color = Colors.GREEN
    else:
        color = Colors.RED

    print(f"{color}{bar}{Colors.RESET}")
    print(f"{color}{Colors.BOLD}  {passed}/{total} tests passed{Colors.RESET}")
    print(f"{color}{bar}{Colors.RESET}")

    for name, ok in results:
        mark = f"{Colors.GREEN}\u2713" if ok else f"{Colors.RED}\u2717"
        print(f"  {mark} {name}{Colors.RESET}")

    print()
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
