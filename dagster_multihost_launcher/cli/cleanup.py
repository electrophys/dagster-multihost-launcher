"""cleanup command — remove old exited run containers across Docker hosts.

Wraps the launcher's ``cleanup_old_containers`` (the same precise label+age logic
the admin asset uses), so the cleanup can run from either a Dagster schedule (the
admin asset) or a Komodo Procedure — without falling back to Komodo's blunter
generic container prune. It reaches each host's daemon over the same TCP+mTLS the
launcher uses, so run it where dagster.yaml and the TLS certs are available
(typically Host A).
"""

import os

import click
from rich.console import Console
from rich.table import Table

from dagster_multihost_launcher.cli.config import load_dagster_yaml_raw
from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher


@click.command()
@click.option(
    "--max-age-hours",
    default=24.0,
    show_default=True,
    help="Remove exited dagster-managed containers older than this.",
)
@click.option(
    "--host",
    default=None,
    help="Limit cleanup to a single Docker host (by host_name).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Report what would be removed without removing anything.",
)
@click.pass_context
def cleanup(ctx, max_age_hours, host, dry_run):
    """Remove exited dagster-managed run containers older than --max-age-hours."""
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

    launcher_cfg = raw.get("run_launcher", {}).get("config", {})
    docker_hosts = launcher_cfg.get("docker_hosts", [])

    if host:
        docker_hosts = [h for h in docker_hosts if h.get("host_name") == host]
        if not docker_hosts:
            console.print(f"[red]Host '{host}' not found in dagster.yaml.[/red]")
            raise SystemExit(1)

    if not docker_hosts:
        console.print("[yellow]No docker_hosts configured; nothing to clean.[/yellow]")
        return

    try:
        launcher = MultiHostDockerRunLauncher(
            docker_hosts=docker_hosts,
            container_label_prefix=launcher_cfg.get(
                "container_label_prefix", "dagster"
            ),
        )
    except Exception as e:
        console.print(f"[red]Could not connect to Docker host(s): {e}[/red]")
        raise SystemExit(1)

    removed = launcher.cleanup_old_containers(
        max_age_hours=max_age_hours, dry_run=dry_run
    )

    if not removed:
        console.print("[dim]No containers to clean up.[/dim]")
        return

    table = Table(title=("Would remove" if dry_run else "Removed") + " containers")
    table.add_column("Host", style="cyan")
    table.add_column("Container")
    table.add_column("Run ID")
    for info in removed:
        table.add_row(info["host_name"], info["short_id"], info.get("run_id", ""))
    console.print(table)
    console.print(
        f"{'Would remove' if dry_run else 'Removed'} {len(removed)} container(s)."
    )
