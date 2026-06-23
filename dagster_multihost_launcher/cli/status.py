"""Status command — show overview of Dagster deployment."""

import click
from rich.console import Console
from rich.table import Table

from dagster_multihost_launcher.cli.config import load_config
from dagster_multihost_launcher.cli.graphql_client import DagsterGraphQLClient


@click.command()
@click.pass_context
def status(ctx):
    """Show status of code locations, runs, schedules, and sensors."""
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

    # Show compose services
    if config.services:
        table = Table(title="Compose Services")
        table.add_column("Service", style="cyan")
        table.add_column("Image")
        table.add_column("Role", style="green")
        table.add_column("Container")

        for svc in config.services.values():
            image = svc.image or (f"(build: {svc.build})" if svc.build else "(none)")
            table.add_row(svc.name, str(image), svc.role, svc.container_name or "")

        console.print(table)
        console.print()

    # Show docker hosts from dagster.yaml
    if config.docker_hosts:
        table = Table(title="Docker Hosts (dagster.yaml)")
        table.add_column("Host", style="cyan")
        table.add_column("URL")
        table.add_column("TLS")
        table.add_column("Locations", style="green")

        for host in config.docker_hosts.values():
            url = host.docker_url or "(local)"
            tls = "yes" if host.tls else "no"
            locs = ", ".join(host.location_names)
            table.add_row(host.host_name, url, tls, locs)

        console.print(table)
        console.print()

    # Connect to Dagster webserver
    client = DagsterGraphQLClient(webserver_url)
    if not client.is_reachable():
        console.print(
            f"[yellow]Webserver not reachable at {webserver_url}[/yellow]\n"
            "Skipping live status. Set --webserver-url or DAGSTER_WEBSERVER_URL."
        )
        return

    # Code locations
    locations = client.get_code_locations()
    table = Table(title="Code Locations")
    table.add_column("Location", style="cyan")
    table.add_column("Status")

    for loc in locations:
        status_style = "green" if loc["loadStatus"] == "LOADED" else "red"
        table.add_row(
            loc["name"], f"[{status_style}]{loc['loadStatus']}[/{status_style}]"
        )

    console.print(table)
    console.print()

    # Active runs
    try:
        active_runs = client.get_active_runs()
    except Exception:
        active_runs = []

    if active_runs:
        table = Table(title="Active Runs")
        table.add_column("Run ID", style="cyan")
        table.add_column("Job")
        table.add_column("Status")
        table.add_column("Location")

        for run in active_runs:
            tags = {t["key"]: t["value"] for t in run.get("tags", [])}
            location = tags.get("dagster/code_location", "")
            status_color = "yellow" if run["status"] == "STARTED" else "blue"
            table.add_row(
                run["runId"][:12],
                run["jobName"],
                f"[{status_color}]{run['status']}[/{status_color}]",
                location,
            )

        console.print(table)
    else:
        console.print("[dim]No active runs.[/dim]")
    console.print()

    # Schedules and sensors per location
    for loc in locations:
        if loc["loadStatus"] != "LOADED":
            continue

        loc_name = loc["name"]

        try:
            schedules = client.get_schedules(loc_name)
        except Exception:
            schedules = []

        try:
            sensors = client.get_sensors(loc_name)
        except Exception:
            sensors = []

        if not schedules and not sensors:
            continue

        if schedules:
            table = Table(title=f"Schedules [{loc_name}]")
            table.add_column("Schedule", style="cyan")
            table.add_column("Status")

            for sched in schedules:
                state = sched.get("scheduleState", {}).get("status", "UNKNOWN")
                style = "green" if state == "RUNNING" else "dim"
                table.add_row(sched["name"], f"[{style}]{state}[/{style}]")

            console.print(table)

        if sensors:
            table = Table(title=f"Sensors [{loc_name}]")
            table.add_column("Sensor", style="cyan")
            table.add_column("Status")

            for sensor in sensors:
                state = sensor.get("sensorState", {}).get("status", "UNKNOWN")
                style = "green" if state == "RUNNING" else "dim"
                table.add_row(sensor["name"], f"[{style}]{state}[/{style}]")

            console.print(table)

        console.print()
