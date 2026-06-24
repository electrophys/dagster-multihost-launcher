"""CLI entry point for dagster-multihost."""

import click

from dagster_multihost_launcher.cli.check_env import check_env
from dagster_multihost_launcher.cli.deploy import deploy
from dagster_multihost_launcher.cli.drain import drain
from dagster_multihost_launcher.cli.komodo import komodo_export, komodo_verify
from dagster_multihost_launcher.cli.pull import pull
from dagster_multihost_launcher.cli.reload import reload
from dagster_multihost_launcher.cli.restore import restore
from dagster_multihost_launcher.cli.status import status


@click.group()
@click.option(
    "--webserver-url",
    envvar="DAGSTER_WEBSERVER_URL",
    default="http://localhost:3000",
    show_default=True,
    help="Dagster webserver URL.",
)
@click.option(
    "--compose-file",
    "-f",
    default="docker-compose.yml",
    show_default=True,
    type=click.Path(),
    help="Path to docker-compose.yml.",
)
@click.option(
    "--dagster-home",
    envvar="DAGSTER_HOME",
    default=None,
    help="Path to directory containing dagster.yaml.",
)
@click.pass_context
def cli(ctx, webserver_url, compose_file, dagster_home):
    """Manage dockerized Dagster code locations and control plane."""
    ctx.ensure_object(dict)
    ctx.obj["webserver_url"] = webserver_url
    ctx.obj["compose_file"] = compose_file
    ctx.obj["dagster_home"] = dagster_home


cli.add_command(status)
cli.add_command(pull)
cli.add_command(deploy)
cli.add_command(reload)
cli.add_command(drain)
cli.add_command(restore)
cli.add_command(check_env)
cli.add_command(komodo_export)
cli.add_command(komodo_verify)
