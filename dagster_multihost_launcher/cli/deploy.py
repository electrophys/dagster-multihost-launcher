"""Deploy command — safely update Dagster services with zero-downtime orchestration."""

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import click
import docker
from rich.console import Console

from dagster_multihost_launcher.cli.config import (
    DeployConfig,
    ServiceInfo,
    load_config,
)
from dagster_multihost_launcher.cli.graphql_client import DagsterGraphQLClient


@dataclass
class SavedInstigator:
    """A schedule or sensor that was running before deployment."""

    kind: str  # "schedule" or "sensor"
    name: str
    repository_name: str
    location_name: str


@dataclass
class DeployState:
    """Tracks state throughout the deployment for rollback."""

    stopped_instigators: List[SavedInstigator] = field(default_factory=list)
    old_image_ids: Dict[str, str] = field(default_factory=dict)  # service -> image id
    restarted_services: List[str] = field(default_factory=list)
    health_ok: bool = False


def _get_compose_cmd(compose_file: str) -> List[str]:
    """Return the base docker compose command."""
    return ["docker", "compose", "-f", str(Path(compose_file).resolve())]


def _run_compose(
    compose_file: str,
    args: List[str],
    console: Console,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Run a docker compose subcommand."""
    cmd = _get_compose_cmd(compose_file) + args
    console.print(f"  [dim]$ {' '.join(cmd)}[/dim]")
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def _resolve_targets(config: DeployConfig, services: tuple) -> List[ServiceInfo]:
    """Resolve service names to ServiceInfo objects."""
    if services:
        targets = []
        for name in services:
            if name in config.services:
                targets.append(config.services[name])
            else:
                # Try matching by role keywords
                for svc in config.services.values():
                    if svc.role == name or svc.name == name:
                        targets.append(svc)
                        break
                else:
                    raise click.ClickException(
                        f"Service '{name}' not found in compose file. "
                        f"Available: {', '.join(config.services.keys())}"
                    )
        return targets
    # Default: all services with an image or build
    return [svc for svc in config.services.values() if svc.image or svc.build]


def _get_location_names_for_service(
    svc: ServiceInfo, config: DeployConfig
) -> List[str]:
    """Determine which code locations a service is responsible for."""
    if svc.location_names:
        return svc.location_names

    # If the service role is code_location, try to match by service name
    # against dagster.yaml location_names
    if svc.role == "code_location":
        for host_info in config.docker_hosts.values():
            for loc in host_info.location_names:
                # Match service name to location name (common convention)
                if loc == svc.name or loc == svc.container_name:
                    return [loc]

    return []


def _record_old_images(
    targets: List[ServiceInfo],
    compose_file: str,
    console: Console,
) -> Dict[str, str]:
    """Record current image IDs before updating, for cleanup later."""
    old_images = {}
    try:
        local_client = docker.from_env()
        for svc in targets:
            if svc.container_name:
                try:
                    container = local_client.containers.get(svc.container_name)
                    old_images[svc.name] = container.image.id
                except docker.errors.NotFound:
                    pass
    except Exception:
        pass

    return old_images


# -- Phase functions --


def phase_preflight(
    config: DeployConfig,
    targets: List[ServiceInfo],
    client: DagsterGraphQLClient,
    console: Console,
    yes: bool,
) -> bool:
    """Phase 1: Pre-flight checks."""
    console.print("\n[bold]Phase 1: Pre-flight[/bold]")

    # Verify webserver
    console.print("  Checking webserver connectivity...")
    if not client.is_reachable():
        console.print(f"  [red]Webserver not reachable at {client.graphql_url}[/red]")
        return False
    console.print("  [green]Webserver is reachable[/green]")

    # Show deployment plan
    console.print("\n  [bold]Deployment plan:[/bold]")
    for svc in targets:
        image_info = svc.image or "(build)"
        locs = _get_location_names_for_service(svc, config)
        loc_str = f" (locations: {', '.join(locs)})" if locs else ""
        console.print(f"    - {svc.name} [{svc.role}] {image_info}{loc_str}")

    if not yes:
        if not click.confirm("\n  Proceed with deployment?", default=True):
            return False

    return True


def phase_pull(
    config: DeployConfig,
    targets: List[ServiceInfo],
    compose_file: str,
    console: Console,
) -> bool:
    """Phase 2: Pull latest images."""
    console.print("\n[bold]Phase 2: Pull images[/bold]")

    pullable = [svc for svc in targets if svc.image]
    if not pullable:
        console.print("  No services with pullable images, skipping.")
        return True

    service_names = [svc.name for svc in pullable]
    result = _run_compose(compose_file, ["pull"] + service_names, console, check=False)

    if result.returncode != 0:
        console.print(f"  [red]Pull failed:[/red] {result.stderr}")
        return False

    console.print(f"  [green]Pulled {len(pullable)} image(s)[/green]")
    return True


def phase_drain(
    config: DeployConfig,
    targets: List[ServiceInfo],
    client: DagsterGraphQLClient,
    state: DeployState,
    console: Console,
    timeout: int,
    force: bool,
) -> bool:
    """Phase 3: Stop schedules/sensors and wait for runs to drain."""
    console.print("\n[bold]Phase 3: Drain code locations[/bold]")

    # Collect all impacted locations (skip webserver/daemon)
    all_locations = set()
    for svc in targets:
        if svc.role in ("webserver", "daemon"):
            continue
        locs = _get_location_names_for_service(svc, config)
        all_locations.update(locs)

    if not all_locations:
        console.print("  No code locations to drain (control plane only).")
        return True

    for loc_name in sorted(all_locations):
        console.print(f"\n  [cyan]Draining: {loc_name}[/cyan]")

        # Stop schedules
        try:
            schedules = client.get_schedules(loc_name)
            for sched in schedules:
                sched_state = sched.get("scheduleState", {}).get("status")
                if sched_state == "RUNNING":
                    repo = sched["repositoryOrigin"]
                    console.print(f"    Stopping schedule: {sched['name']}")
                    client.stop_schedule(
                        sched["name"],
                        repo["repositoryName"],
                        repo["repositoryLocationName"],
                    )
                    state.stopped_instigators.append(
                        SavedInstigator(
                            kind="schedule",
                            name=sched["name"],
                            repository_name=repo["repositoryName"],
                            location_name=repo["repositoryLocationName"],
                        )
                    )
        except Exception as e:
            console.print(
                f"    [yellow]Warning: could not query schedules: {e}[/yellow]"
            )

        # Stop sensors
        try:
            sensors = client.get_sensors(loc_name)
            for sensor in sensors:
                sensor_state = sensor.get("sensorState", {}).get("status")
                if sensor_state == "RUNNING":
                    repo = sensor["repositoryOrigin"]
                    console.print(f"    Stopping sensor: {sensor['name']}")
                    client.stop_sensor(
                        sensor["name"],
                        repo["repositoryName"],
                        repo["repositoryLocationName"],
                    )
                    state.stopped_instigators.append(
                        SavedInstigator(
                            kind="sensor",
                            name=sensor["name"],
                            repository_name=repo["repositoryName"],
                            location_name=repo["repositoryLocationName"],
                        )
                    )
        except Exception as e:
            console.print(f"    [yellow]Warning: could not query sensors: {e}[/yellow]")

        # Wait for active runs
        console.print(
            f"    Waiting for active runs to complete (timeout: {timeout}s)..."
        )
        try:
            active = client.get_active_runs(loc_name)
            if active:
                console.print(
                    f"    [yellow]{len(active)} active run(s) in {loc_name}[/yellow]"
                )
                for run in active:
                    console.print(f"      - {run['runId'][:12]} ({run['status']})")

                if not client.wait_for_runs_to_complete(loc_name, timeout=timeout):
                    remaining = client.get_active_runs(loc_name)
                    console.print(
                        f"    [red]Timeout: {len(remaining)} run(s) still active[/red]"
                    )
                    if not force:
                        if not click.confirm(
                            "    Continue anyway (runs may be interrupted)?",
                            default=False,
                        ):
                            return False
                    else:
                        console.print(
                            "    [yellow]--force: continuing despite active runs[/yellow]"
                        )
            else:
                console.print(f"    [green]No active runs in {loc_name}[/green]")
        except Exception as e:
            console.print(f"    [yellow]Warning: could not check runs: {e}[/yellow]")

    console.print("  [green]Drain complete[/green]")
    return True


def phase_restart(
    config: DeployConfig,
    targets: List[ServiceInfo],
    compose_file: str,
    state: DeployState,
    console: Console,
) -> bool:
    """Phase 4: Restart containers."""
    console.print("\n[bold]Phase 4: Restart containers[/bold]")

    # Sort: code locations first, then daemon, then webserver last
    role_order = {"code_location": 0, "other": 1, "daemon": 2, "webserver": 3}
    sorted_targets = sorted(targets, key=lambda s: role_order.get(s.role, 1))

    for svc in sorted_targets:
        console.print(f"  Restarting [cyan]{svc.name}[/cyan] ({svc.role})...")

        # Stop the service
        result = _run_compose(compose_file, ["stop", svc.name], console, check=False)
        if result.returncode != 0:
            console.print(f"    [yellow]Stop warning: {result.stderr.strip()}[/yellow]")

        # Recreate and start with new image
        result = _run_compose(
            compose_file,
            ["up", "-d", "--force-recreate", svc.name],
            console,
            check=False,
        )
        if result.returncode != 0:
            console.print(
                f"    [red]Failed to restart {svc.name}: {result.stderr}[/red]"
            )
            return False

        state.restarted_services.append(svc.name)
        console.print(f"    [green]Restarted {svc.name}[/green]")

    return True


def phase_health(
    config: DeployConfig,
    targets: List[ServiceInfo],
    client: DagsterGraphQLClient,
    state: DeployState,
    console: Console,
) -> bool:
    """Phase 5: Verify health."""
    console.print("\n[bold]Phase 5: Verify health[/bold]")

    # If webserver was restarted, wait for it to come back
    webserver_restarted = any(
        svc.role == "webserver"
        for svc in targets
        if svc.name in state.restarted_services
    )

    if webserver_restarted:
        console.print("  Waiting for webserver to come back online...")
        if not client.wait_for_webserver(timeout=120):
            console.print("  [red]Webserver did not come back within 120s[/red]")
            return False
        console.print("  [green]Webserver is back online[/green]")

    # Reload workspace
    console.print("  Reloading workspace...")
    try:
        client.reload_workspace()
    except Exception as e:
        console.print(f"  [yellow]Workspace reload warning: {e}[/yellow]")

    # Wait for all impacted locations to load
    all_locations = set()
    for svc in targets:
        locs = _get_location_names_for_service(svc, config)
        all_locations.update(locs)

    if all_locations:
        console.print(
            f"  Waiting for locations to load: {', '.join(sorted(all_locations))}..."
        )
        if client.wait_for_locations(list(all_locations), timeout=120):
            console.print("  [green]All locations loaded[/green]")
        else:
            console.print("  [red]Some locations failed to load[/red]")
            # Show which ones
            try:
                entries = client.get_code_locations()
                for entry in entries:
                    if entry["name"] in all_locations:
                        status = entry["loadStatus"]
                        style = "green" if status == "LOADED" else "red"
                        console.print(
                            f"    {entry['name']}: [{style}]{status}[/{style}]"
                        )
            except Exception:
                pass
            return False

    state.health_ok = True
    return True


def phase_restore(
    state: DeployState,
    client: DagsterGraphQLClient,
    console: Console,
) -> bool:
    """Phase 6: Restore schedules and sensors."""
    console.print("\n[bold]Phase 6: Restore schedules & sensors[/bold]")

    if not state.stopped_instigators:
        console.print("  Nothing to restore.")
        return True

    all_ok = True
    for instigator in state.stopped_instigators:
        console.print(
            f"  Starting {instigator.kind}: "
            f"[cyan]{instigator.name}[/cyan] ({instigator.location_name})"
        )
        try:
            if instigator.kind == "schedule":
                ok = client.start_schedule(
                    instigator.name,
                    instigator.repository_name,
                    instigator.location_name,
                )
            else:
                ok = client.start_sensor(
                    instigator.name,
                    instigator.repository_name,
                    instigator.location_name,
                )

            if ok:
                console.print("    [green]Started[/green]")
            else:
                console.print("    [yellow]May not have started correctly[/yellow]")
                all_ok = False
        except Exception as e:
            console.print(f"    [red]Failed: {e}[/red]")
            all_ok = False

    return all_ok


def phase_cleanup(
    state: DeployState,
    compose_file: str,
    console: Console,
) -> None:
    """Phase 7: Cleanup old images."""
    console.print("\n[bold]Phase 7: Cleanup old images[/bold]")

    if not state.health_ok:
        console.print(
            "  [yellow]Skipping cleanup: health checks did not pass.[/yellow]\n"
            "  Old images preserved for potential rollback."
        )
        return

    if not state.old_image_ids:
        console.print("  No old images to clean up.")
        return

    try:
        local_client = docker.from_env()
    except Exception:
        console.print("  [yellow]Cannot connect to Docker for cleanup.[/yellow]")
        return

    removed = 0
    for svc_name, old_id in state.old_image_ids.items():
        try:
            # Check if the image is still in use (same ID means no update happened)
            current = None
            try:
                container = local_client.containers.get(svc_name)
                current = container.image.id
            except docker.errors.NotFound:
                pass

            if current == old_id:
                # Same image, nothing to clean
                continue

            local_client.images.remove(old_id, force=False)
            console.print(f"  Removed old image for {svc_name}: {old_id[:12]}")
            removed += 1
        except docker.errors.APIError as e:
            if "image is being used" in str(e) or "conflict" in str(e).lower():
                console.print(
                    f"  [dim]Old image for {svc_name} still in use, skipping[/dim]"
                )
            else:
                console.print(
                    f"  [yellow]Could not remove old image for {svc_name}: {e}[/yellow]"
                )

    if removed:
        console.print(f"  [green]Removed {removed} old image(s)[/green]")
    else:
        console.print("  [dim]No old images needed removal.[/dim]")


# -- Main deploy command --


@click.command()
@click.argument("services", nargs=-1)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts.")
@click.option(
    "--timeout",
    default=600,
    show_default=True,
    help="Seconds to wait for active runs to complete before proceeding.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Continue even if active runs don't complete within timeout.",
)
@click.option(
    "--skip-cleanup",
    is_flag=True,
    help="Skip old image cleanup after deployment.",
)
@click.pass_context
def deploy(ctx, services, yes, timeout, force, skip_cleanup):
    """Safely deploy updated Dagster services.

    SERVICES are docker-compose service names to deploy. If omitted, deploys
    all services.

    The deployment workflow:

    \b
    1. Pre-flight  — verify connectivity, show plan
    2. Pull        — pull latest images
    3. Drain       — stop schedules/sensors, wait for runs
    4. Restart     — stop and recreate containers
    5. Health      — verify webserver and code locations
    6. Restore     — re-enable schedules and sensors
    7. Cleanup     — remove old images (if healthy)
    """
    console = Console()
    webserver_url = ctx.obj["webserver_url"]
    compose_file = ctx.obj["compose_file"]
    dagster_home = ctx.obj["dagster_home"]

    # Load config
    try:
        config = load_config(compose_file, dagster_home)
    except FileNotFoundError as e:
        console.print(f"[red]Config error:[/red] {e}")
        raise SystemExit(1)

    # Resolve targets
    try:
        targets = _resolve_targets(config, services)
    except click.ClickException:
        raise
    except Exception as e:
        console.print(f"[red]Error resolving services:[/red] {e}")
        raise SystemExit(1)

    if not targets:
        console.print("[yellow]No services to deploy.[/yellow]")
        return

    client = DagsterGraphQLClient(webserver_url)
    state = DeployState()

    console.print("[bold]Starting deployment...[/bold]")

    # Phase 1: Pre-flight
    if not phase_preflight(config, targets, client, console, yes):
        console.print("\n[red]Deployment aborted.[/red]")
        raise SystemExit(1)

    # Record old images for cleanup
    state.old_image_ids = _record_old_images(targets, compose_file, console)

    # Phase 2: Pull
    if not phase_pull(config, targets, compose_file, console):
        console.print("\n[red]Deployment aborted: pull failed.[/red]")
        raise SystemExit(1)

    # Phase 3: Drain
    if not phase_drain(config, targets, client, state, console, timeout, force):
        # Restore instigators before aborting
        if state.stopped_instigators:
            console.print(
                "\n[yellow]Restoring schedules/sensors before abort...[/yellow]"
            )
            phase_restore(state, client, console)
        console.print("\n[red]Deployment aborted: drain failed.[/red]")
        raise SystemExit(1)

    # Phase 4: Restart
    if not phase_restart(config, targets, compose_file, state, console):
        console.print(
            "\n[red]Deployment error during restart.[/red]\n"
            "[yellow]Some services may need manual intervention.[/yellow]"
        )
        # Still try to restore instigators and check health
        phase_health(config, targets, client, state, console)
        phase_restore(state, client, console)
        raise SystemExit(1)

    # Phase 5: Health
    if not phase_health(config, targets, client, state, console):
        console.print("\n[yellow]Health checks failed. Old images preserved.[/yellow]")
        # Still restore instigators
        phase_restore(state, client, console)
        raise SystemExit(1)

    # Phase 6: Restore
    phase_restore(state, client, console)

    # Phase 7: Cleanup
    if not skip_cleanup:
        phase_cleanup(state, compose_file, console)

    console.print("\n[bold green]Deployment complete![/bold green]")
