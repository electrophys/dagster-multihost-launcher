"""Drain command — quiesce code locations before a remote redeploy.

Stops each location's running schedules/sensors and waits for active runs to
finish, then records what it stopped to a state file so ``restore`` can
re-enable exactly those afterwards. Intended as the pre-deploy step in a Komodo
Procedure:

    drain  ->  DeployStack  ->  reload  ->  restore

If draining times out (runs still active) without --force, the instigators that
were stopped are restarted so the system is left as it was found, and the
command exits non-zero — halting the Procedure before any redeploy.
"""

import click
from rich.console import Console

from dagster_multihost_launcher.graphql_client import DagsterGraphQLClient
from dagster_multihost_launcher.orchestration import (
    WorkspaceOrchestrator,
    save_instigators,
)


@click.command()
@click.argument("locations", nargs=-1, required=True)
@click.option(
    "--state-file",
    default=None,
    type=click.Path(),
    help="Where to record stopped schedules/sensors for `restore`. "
    "Without it, restore is not possible.",
)
@click.option(
    "--timeout",
    default=600,
    show_default=True,
    help="Seconds to wait for active runs to finish per location.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Proceed even if runs do not finish within the timeout.",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for active runs to drain.",
)
@click.pass_context
def drain(ctx, locations, state_file, timeout, force, wait):
    """Stop schedules/sensors and drain active runs for LOCATIONS."""
    console = Console()
    client = DagsterGraphQLClient(ctx.obj["webserver_url"])

    if not client.is_reachable():
        console.print(f"[red]Webserver not reachable at {client.graphql_url}[/red]")
        raise SystemExit(1)

    orch = WorkspaceOrchestrator(client, log=lambda m: console.print(f"    {m}"))
    stopped = []

    for loc in locations:
        console.print(f"[cyan]Draining {loc}[/cyan]")
        stopped.extend(orch.stop_instigators(loc))

        if not wait:
            continue

        active = client.get_active_runs(loc)
        if not active:
            console.print("    No active runs.")
            continue

        console.print(f"    {len(active)} active run(s); waiting up to {timeout}s...")
        if not orch.wait_for_runs(loc, timeout=timeout):
            remaining = client.get_active_runs(loc)
            if force:
                console.print(
                    f"    [yellow]--force: proceeding with {len(remaining)} "
                    "active run(s)[/yellow]"
                )
                continue
            # Roll back: re-enable everything we stopped, then fail.
            console.print(
                f"[red]Timed out: {len(remaining)} run(s) still active in "
                f"{loc}[/red]"
            )
            if stopped:
                console.print("Restoring stopped schedules/sensors...")
                orch.restart_instigators(stopped)
            raise SystemExit(1)

    if state_file:
        save_instigators(state_file, stopped)
        console.print(f"Recorded {len(stopped)} stopped instigator(s) to {state_file}")
    elif stopped:
        console.print(
            "[yellow]No --state-file given; `restore` will not be able to "
            "re-enable these automatically.[/yellow]"
        )

    console.print("[bold green]Drain complete[/bold green]")
