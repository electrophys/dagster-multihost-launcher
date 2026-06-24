"""komodo-export / komodo-verify — keep dagster.yaml and Komodo in lockstep.

``komodo-export`` generates a Komodo Resource Sync TOML skeleton from the Docker
topology in ``dagster.yaml`` (+ compose). ``komodo-verify`` diffs ``dagster.yaml``
against an existing Komodo Resource Sync TOML and fails on drift — wire it into
CI so a code location can't be deployed to a host the launcher doesn't know
about (which would silently fall back to the DefaultRunLauncher).
"""

import click
from rich.console import Console

from dagster_multihost_launcher.cli.config import load_config
from dagster_multihost_launcher.komodo_sync import (
    diff_topology,
    generate_resource_sync_toml,
    load_komodo_toml,
)


@click.command(name="komodo-export")
@click.option(
    "--output",
    "-o",
    default=None,
    type=click.Path(),
    help="Write the TOML here instead of stdout.",
)
@click.pass_context
def komodo_export(ctx, output):
    """Generate a Komodo Resource Sync TOML skeleton from dagster.yaml."""
    console = Console(stderr=True)
    try:
        config = load_config(ctx.obj["compose_file"], ctx.obj["dagster_home"])
    except FileNotFoundError as e:
        console.print(f"[red]Config error:[/red] {e}")
        raise SystemExit(1)

    if not config.docker_hosts:
        console.print(
            "[yellow]No docker_hosts found in dagster.yaml; nothing to export.[/yellow]"
        )
        raise SystemExit(1)

    toml = generate_resource_sync_toml(config)
    if output:
        with open(output, "w") as f:
            f.write(toml)
        console.print(f"[green]Wrote {output}[/green]")
    else:
        click.echo(toml)


@click.command(name="komodo-verify")
@click.argument("toml_path", type=click.Path(exists=True))
@click.pass_context
def komodo_verify(ctx, toml_path):
    """Diff dagster.yaml topology against a Komodo Resource Sync TOML_PATH.

    Exits non-zero if a host or code location in dagster.yaml is missing from, or
    placed differently in, the Komodo definition.
    """
    console = Console()
    try:
        config = load_config(ctx.obj["compose_file"], ctx.obj["dagster_home"])
    except FileNotFoundError as e:
        console.print(f"[red]Config error:[/red] {e}")
        raise SystemExit(1)

    komodo = load_komodo_toml(toml_path)
    diff = diff_topology(config, komodo)

    if diff.missing_servers:
        console.print(
            "[red]Hosts in dagster.yaml with no Komodo server:[/red] "
            + ", ".join(diff.missing_servers)
        )
    if diff.missing_stacks:
        console.print(
            "[red]Code locations with no Komodo stack:[/red] "
            + ", ".join(diff.missing_stacks)
        )
    if diff.misplaced_stacks:
        console.print("[red]Stacks on the wrong server:[/red]")
        for item in diff.misplaced_stacks:
            console.print(f"  {item}")
    if diff.extra_servers:
        console.print(
            "[dim]Komodo servers not referenced by dagster.yaml: "
            + ", ".join(diff.extra_servers)
            + "[/dim]"
        )

    if diff.ok:
        console.print("[bold green]Topology in sync.[/bold green]")
        return

    raise SystemExit(1)
