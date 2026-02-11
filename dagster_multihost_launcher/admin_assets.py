"""
Admin assets for the MultiHost Docker Run Launcher.

These are designed to run in a code location on Host A (the Dagster control
plane host). They use the launcher's built-in methods to inspect and clean up
containers across all configured Docker hosts.

Usage in your admin code location:

    from dagster_multihost_launcher import build_admin_definitions
    defs = build_admin_definitions()

Or compose them into your own Definitions:

    from dagster_multihost_launcher import multihost_cleanup_asset, multihost_status_asset
    from dagster import Definitions, ScheduleDefinition, define_asset_job

    admin_job = define_asset_job(
        "admin_job",
        selection=[multihost_status_asset, multihost_cleanup_asset],
    )
    admin_schedule = ScheduleDefinition(job=admin_job, cron_schedule="0 */6 * * *")

    defs = Definitions(
        assets=[multihost_status_asset, multihost_cleanup_asset],
        jobs=[admin_job],
        schedules=[admin_schedule],
    )
"""

import json
import logging
from typing import Any, Dict, List

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

logger = logging.getLogger("dagster_multihost_launcher.admin")


def _get_launcher(context: AssetExecutionContext):
    """Retrieve the MultiHostDockerRunLauncher from the current instance."""
    from dagster_multihost_launcher.launcher import MultiHostDockerRunLauncher

    launcher = context.instance.run_launcher
    if not isinstance(launcher, MultiHostDockerRunLauncher):
        raise Exception(
            f"Expected MultiHostDockerRunLauncher, got {type(launcher).__name__}. "
            "These admin assets must run on an instance configured with "
            "MultiHostDockerRunLauncher."
        )
    return launcher


@asset(
    key=AssetKey("multihost_container_status"),
    description=(
        "Reports the status of all Dagster-managed Docker containers across "
        "all configured hosts. Materializes a summary with container counts "
        "per host and status."
    ),
    group_name="admin",
    compute_kind="docker",
)
def multihost_status_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Check container status across all Docker hosts."""
    launcher = _get_launcher(context)
    all_containers = launcher.list_dagster_containers()

    # Build summary
    by_host: Dict[str, Dict[str, int]] = {}
    for c in all_containers:
        host = c["host_name"]
        status = c["status"]
        by_host.setdefault(host, {})
        by_host[host][status] = by_host[host].get(status, 0) + 1

    summary_lines = []
    for host, statuses in sorted(by_host.items()):
        parts = ", ".join(f"{s}: {n}" for s, n in sorted(statuses.items()))
        summary_lines.append(f"  {host}: {parts}")
        context.log.info("Host '%s': %s", host, parts)

    total = len(all_containers)
    running = sum(1 for c in all_containers if c["status"] == "running")
    exited = sum(1 for c in all_containers if c["status"] == "exited")

    context.log.info(
        "Total containers: %d (running: %d, exited: %d)", total, running, exited
    )

    return MaterializeResult(
        metadata={
            "total_containers": total,
            "running": running,
            "exited": exited,
            "by_host": MetadataValue.json(by_host),
            "containers": MetadataValue.json(all_containers),
        }
    )


@asset(
    key=AssetKey("multihost_container_cleanup"),
    deps=[AssetKey("multihost_container_status")],
    description=(
        "Cleans up old exited Dagster-managed Docker containers across all "
        "configured hosts. Runs after status check."
    ),
    group_name="admin",
    compute_kind="docker",
)
def multihost_cleanup_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Clean up old exited containers on all Docker hosts."""
    launcher = _get_launcher(context)

    # You can override max_age_hours via run config tags if needed
    max_age_hours = float(
        context.dagster_run.tags.get("multihost/cleanup_max_age_hours", str(1 / 60))
    )

    context.log.info("Cleaning up containers older than %.1f hours...", max_age_hours)

    removed = launcher.cleanup_old_containers(max_age_hours=max_age_hours)

    for r in removed:
        context.log.info(
            "%s container %s (run %s) on host '%s'",
            r.get("action", "removed"),
            r["short_id"],
            r["run_id"],
            r["host_name"],
        )

    context.log.info("Cleaned up %d containers.", len(removed))

    return MaterializeResult(
        metadata={
            "containers_removed": len(removed),
            "max_age_hours": max_age_hours,
            "details": MetadataValue.json(removed),
        }
    )


def build_admin_definitions(
    cron_schedule: str = "*/5 * * * *",
    cleanup_max_age_hours: float = 1 / 60,
) -> Definitions:
    """Build a complete Definitions object for the admin code location.

    Args:
        cron_schedule: Cron schedule for the admin job. Default: every 5 minutes.
        cleanup_max_age_hours: Max container age in hours before cleanup. Default: 1 minute.

    Returns:
        A Definitions object you can use directly or merge with your own.

    Usage in your admin code location's definitions.py::

        from dagster_multihost_launcher import build_admin_definitions
        defs = build_admin_definitions()
    """
    admin_job = define_asset_job(
        "multihost_admin_job",
        selection=[multihost_status_asset, multihost_cleanup_asset],
        description="Check container status then clean up old exited containers.",
        tags={"multihost/cleanup_max_age_hours": str(cleanup_max_age_hours)},
    )

    schedules = []
    if cron_schedule:
        schedules.append(
            ScheduleDefinition(
                name="multihost_admin_schedule",
                job=admin_job,
                cron_schedule=cron_schedule,
            )
        )

    return Definitions(
        assets=[multihost_status_asset, multihost_cleanup_asset],
        jobs=[admin_job],
        schedules=schedules,
    )
