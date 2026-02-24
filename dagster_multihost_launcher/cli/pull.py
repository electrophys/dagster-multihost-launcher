"""Pull command — pull latest Docker images for services."""

import click
import docker
from rich.console import Console

from dagster_multihost_launcher.cli.config import (
    build_docker_client,
    load_config,
)


@click.command()
@click.argument("services", nargs=-1)
@click.pass_context
def pull(ctx, services):
    """Pull latest Docker images for specified services (or all).

    SERVICES are docker-compose service names. If omitted, pulls all services
    that have an image defined.
    """
    console = Console()
    compose_file = ctx.obj["compose_file"]
    dagster_home = ctx.obj["dagster_home"]

    try:
        config = load_config(compose_file, dagster_home)
    except FileNotFoundError as e:
        console.print(f"[red]Config error:[/red] {e}")
        raise SystemExit(1)

    # Filter to requested services or all with images
    targets = {}
    for name, svc in config.services.items():
        if services and name not in services:
            continue
        if svc.image:
            targets[name] = svc

    if not targets:
        console.print("[yellow]No services with pullable images found.[/yellow]")
        return

    # Build Docker clients for each host
    clients = {}
    for host_name, host_info in config.docker_hosts.items():
        try:
            clients[host_name] = build_docker_client(host_info)
        except Exception as e:
            console.print(
                f"[yellow]Warning: could not connect to host '{host_name}': {e}[/yellow]"
            )

    # Local client for webserver/daemon/unmatched services
    try:
        local_client = docker.from_env()
    except Exception as e:
        console.print(f"[red]Cannot connect to local Docker: {e}[/red]")
        raise SystemExit(1)

    console.print(f"[bold]Pulling images for {len(targets)} service(s)...[/bold]\n")

    for name, svc in targets.items():
        # Determine which Docker client to use
        client = local_client
        host_label = "local"

        # If this service serves a code location on a remote host, pull there
        for loc in svc.location_names:
            host_name = config.location_to_host.get(loc)
            if host_name and host_name in clients:
                client = clients[host_name]
                host_label = host_name
                break

        console.print(
            f"  Pulling [cyan]{svc.image}[/cyan] on [green]{host_label}[/green]..."
        )

        try:
            # Authenticate to registry if configured for this host
            host_name = config.location_to_host.get(
                svc.location_names[0] if svc.location_names else "", ""
            )
            host_info = config.docker_hosts.get(host_name)
            if host_info and host_info.registry:
                client.login(
                    username=host_info.registry["username"],
                    password=host_info.registry["password"],
                    registry=host_info.registry["url"],
                )

            client.images.pull(svc.image)
            console.print("    [green]Done[/green]")
        except Exception as e:
            console.print(f"    [red]Failed: {e}[/red]")

    console.print()
