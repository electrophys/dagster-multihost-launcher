"""Restore command — re-enable schedules/sensors recorded by ``drain``.

The post-deploy counterpart to ``drain``: reads the state file written by a
prior drain and restarts exactly the instigators that were stopped. Run it as
the final stage of a Komodo deploy Procedure, after the stack is back up and the
code location has been reloaded.
"""

import os

import click
from rich.console import Console

from dagster_multihost_launcher.graphql_client import DagsterGraphQLClient
from dagster_multihost_launcher.orchestration import (
    WorkspaceOrchestrator,
    load_instigators,
)


@click.command()
@click.option(
    "--state-file",
    required=True,
    type=click.Path(),
    help="State file written by a prior `drain`.",
)
@click.option(
    "--keep-state-file",
    is_flag=True,
    help="Do not delete the state file after a successful restore.",
)
@click.pass_context
def restore(ctx, state_file, keep_state_file):
    """Re-enable schedules/sensors recorded by a previous `drain`."""
    console = Console()

    if not os.path.exists(state_file):
        console.print(
            f"[yellow]No state file at {state_file}; nothing to restore.[/yellow]"
        )
        return

    instigators = load_instigators(state_file)
    if not instigators:
        console.print("Nothing to restore.")
        if not keep_state_file:
            os.remove(state_file)
        return

    client = DagsterGraphQLClient(ctx.obj["webserver_url"])
    if not client.is_reachable():
        console.print(f"[red]Webserver not reachable at {client.graphql_url}[/red]")
        raise SystemExit(1)

    orch = WorkspaceOrchestrator(client, log=lambda m: console.print(f"  {m}"))
    ok = orch.restart_instigators(instigators)

    if not ok:
        console.print(
            "[red]Some instigators failed to restart; keeping state file "
            f"{state_file} for retry.[/red]"
        )
        raise SystemExit(1)

    if not keep_state_file:
        os.remove(state_file)
    console.print("[bold green]Restore complete[/bold green]")
