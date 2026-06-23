"""Reload command — reload Dagster code locations after a remote update.

Intended to be called right after a remote code-location container is
redeployed, so the control plane picks up the new code. The natural caller is
the deploy tool that just updated the container (e.g. a Komodo post-deploy
Procedure step running ``dagster-multihost reload <location>``).
"""

import click
from rich.console import Console

from dagster_multihost_launcher.cli.graphql_client import DagsterGraphQLClient


def _print_entries(console: Console, entries) -> None:
    for entry in entries:
        status = entry.get("loadStatus", "UNKNOWN")
        style = "green" if status == "LOADED" else "yellow"
        console.print(f"  {entry.get('name', '?')}: [{style}]{status}[/{style}]")


@click.command()
@click.argument("locations", nargs=-1)
@click.option(
    "--all",
    "reload_all",
    is_flag=True,
    help="Reload the entire workspace instead of specific locations.",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for the locations to finish loading.",
)
@click.option(
    "--timeout",
    default=120,
    show_default=True,
    help="Seconds to wait for the reload/load to complete.",
)
@click.pass_context
def reload(ctx, locations, reload_all, wait, timeout):
    """Reload one or more code LOCATIONS on the Dagster control plane.

    Run this after a remote code-location container is redeployed, e.g. as a
    Komodo post-deploy step:

    \b
        dagster-multihost reload code_loc_b

    With --all (and no LOCATIONS) the entire workspace is reloaded.
    """
    console = Console()
    webserver_url = ctx.obj["webserver_url"]
    client = DagsterGraphQLClient(webserver_url)

    if not client.is_reachable():
        console.print(f"[red]Webserver not reachable at {client.graphql_url}[/red]")
        raise SystemExit(1)

    if reload_all:
        if locations:
            raise click.UsageError("Pass either LOCATIONS or --all, not both.")
        console.print("Reloading entire workspace...")
        try:
            entries = client.reload_workspace()
        except Exception as e:
            console.print(f"[red]Reload failed: {e}[/red]")
            raise SystemExit(1)
        _print_entries(console, entries)
        return

    if not locations:
        raise click.UsageError("Provide one or more LOCATIONS, or use --all.")

    failed = False
    for name in locations:
        console.print(f"Reloading [cyan]{name}[/cyan]...")
        try:
            result = client.reload_location(name, timeout=timeout)
        except Exception as e:
            console.print(f"  [red]Failed: {e}[/red]")
            failed = True
            continue

        typename = result.get("__typename")
        if typename == "WorkspaceLocationEntry":
            load_error = (result.get("locationOrLoadError") or {}).get("message")
            if load_error:
                console.print(f"  [red]Loaded with error: {load_error}[/red]")
                failed = True
            else:
                status = result.get("loadStatus", "UNKNOWN")
                style = "green" if status == "LOADED" else "yellow"
                console.print(f"  [{style}]{status}[/{style}]")
        else:
            message = result.get("message", "")
            console.print(f"  [red]{typename}: {message}[/red]")
            failed = True

    if wait and not failed:
        console.print("Waiting for locations to load...")
        if client.wait_for_locations(list(locations), timeout=timeout):
            console.print("[green]All locations loaded[/green]")
        else:
            console.print("[red]Some locations did not load in time[/red]")
            failed = True

    if failed:
        raise SystemExit(1)
    console.print("[bold green]Reload complete[/bold green]")
