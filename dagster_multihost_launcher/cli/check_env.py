"""check-env command — verify the control plane's required env vars are present.

The launcher's ``env_file`` / inheritance feeds *run containers*. The control
plane's own process — the daemon and webserver — still needs the env vars that
``dagster.yaml`` references as ``{env: NAME}`` (e.g. Postgres credentials) and
the bare ``KEY`` entries in the launcher's ``env_vars`` (pulled from the daemon
environment at launch time).

This command surfaces those required names and checks them against the current
environment (and an optional ``--env-file``, e.g. the file Komodo renders for the
control-plane stack), so a missing var fails loudly here instead of as a runtime
error deep in a run. Run it as a pre-deploy gate.
"""

import os

import click
from rich.console import Console
from rich.table import Table

from dagster_multihost_launcher.cli.config import (
    find_bare_env_vars,
    find_env_references,
    load_dagster_yaml_raw,
)
from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


@click.command(name="check-env")
@click.option(
    "--env-file",
    default=None,
    type=click.Path(),
    help="Treat keys in this .env file as available (in addition to the "
    "current environment), e.g. the file Komodo renders for the control plane.",
)
@click.pass_context
def check_env(ctx, env_file):
    """Check that env vars required by the control plane are set.

    Collects every ``{env: NAME}`` reference in dagster.yaml plus the launcher's
    bare ``KEY`` env_vars, then reports which are missing. Exits non-zero if any
    are missing.
    """
    console = Console()
    dagster_home = ctx.obj["dagster_home"] or os.environ.get("DAGSTER_HOME")

    if not dagster_home:
        console.print(
            "[red]No dagster home; pass --dagster-home or set DAGSTER_HOME.[/red]"
        )
        raise SystemExit(1)

    try:
        raw = load_dagster_yaml_raw(dagster_home)
    except FileNotFoundError as e:
        console.print(f"[red]Config error:[/red] {e}")
        raise SystemExit(1)

    required = find_env_references(raw) | find_bare_env_vars(dagster_home)
    if not required:
        console.print("No env var references found in dagster.yaml.")
        return

    available = set(os.environ)
    source_note = "current environment"
    if env_file:
        from_file = MultiHostDockerRunLauncher._read_env_file(env_file)
        available |= set(from_file)
        source_note += f" + {env_file}"

    table = Table(title=f"Control-plane env (checked against {source_note})")
    table.add_column("Variable", style="cyan")
    table.add_column("Status")

    missing = []
    for name in sorted(required):
        present = name in available
        if not present:
            missing.append(name)
        style = "green" if present else "red"
        label = "present" if present else "MISSING"
        table.add_row(name, f"[{style}]{label}[/{style}]")

    console.print(table)

    if missing:
        console.print(
            f"\n[red]{len(missing)} required env var(s) missing:[/red] "
            f"{', '.join(missing)}"
        )
        console.print(
            "[dim]Render these into the control-plane stack's environment "
            "(e.g. via Komodo Variables/Secrets) on the daemon host.[/dim]"
        )
        raise SystemExit(1)

    console.print("\n[bold green]All required env vars present.[/bold green]")
