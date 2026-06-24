"""Dagster-aware workspace orchestration, decoupled from the CLI.

This is the reusable core behind the ``deploy``, ``drain``, and ``restore``
commands: quiescing a code location (stopping its running schedules/sensors and
waiting for active runs to finish) and restoring it afterwards. It depends only
on the Dagster GraphQL API — no ``click``, ``rich``, or Docker — so an external
orchestrator (e.g. a Komodo Procedure/Action) can drive a safe rollout around a
remote stack redeploy:

    drain  ->  DeployStack  ->  reload  ->  restore

``drain`` persists what it stopped (see :func:`save_instigators`) so a later,
separate ``restore`` process can re-enable exactly those instigators.
"""

import json
from dataclasses import asdict, dataclass
from typing import Callable, List, Optional

from dagster_multihost_launcher.graphql_client import DagsterGraphQLClient


@dataclass
class SavedInstigator:
    """A schedule or sensor that was running before being stopped, recorded so
    it can be restarted afterwards."""

    kind: str  # "schedule" or "sensor"
    name: str
    repository_name: str
    location_name: str


def _noop(_msg: str) -> None:
    pass


class WorkspaceOrchestrator:
    """Stop/restart a code location's schedules and sensors and wait for its
    runs to drain, over the Dagster GraphQL API.

    ``log`` is an optional ``str -> None`` progress callback so callers (CLI,
    Komodo Action) can render output however they like; by default it is silent.
    """

    def __init__(
        self,
        client: DagsterGraphQLClient,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.client = client
        self._log = log or _noop

    def stop_instigators(self, location_name: str) -> List[SavedInstigator]:
        """Stop every RUNNING schedule and sensor for a location and return the
        list of what was stopped (for a later restore)."""
        stopped: List[SavedInstigator] = []

        for sched in self.client.get_schedules(location_name):
            if sched.get("scheduleState", {}).get("status") != "RUNNING":
                continue
            repo = sched["repositoryOrigin"]
            self._log(f"Stopping schedule: {sched['name']}")
            self.client.stop_schedule(
                sched["name"],
                repo["repositoryName"],
                repo["repositoryLocationName"],
            )
            stopped.append(
                SavedInstigator(
                    kind="schedule",
                    name=sched["name"],
                    repository_name=repo["repositoryName"],
                    location_name=repo["repositoryLocationName"],
                )
            )

        for sensor in self.client.get_sensors(location_name):
            if sensor.get("sensorState", {}).get("status") != "RUNNING":
                continue
            repo = sensor["repositoryOrigin"]
            self._log(f"Stopping sensor: {sensor['name']}")
            self.client.stop_sensor(
                sensor["name"],
                repo["repositoryName"],
                repo["repositoryLocationName"],
            )
            stopped.append(
                SavedInstigator(
                    kind="sensor",
                    name=sensor["name"],
                    repository_name=repo["repositoryName"],
                    location_name=repo["repositoryLocationName"],
                )
            )

        return stopped

    def restart_instigators(self, instigators: List[SavedInstigator]) -> bool:
        """Restart previously-stopped instigators. Returns True only if all
        succeeded; failures are logged and do not abort the rest."""
        all_ok = True
        for inst in instigators:
            self._log(f"Starting {inst.kind}: {inst.name} ({inst.location_name})")
            try:
                if inst.kind == "schedule":
                    ok = self.client.start_schedule(
                        inst.name, inst.repository_name, inst.location_name
                    )
                else:
                    ok = self.client.start_sensor(
                        inst.name, inst.repository_name, inst.location_name
                    )
            except Exception as e:  # noqa: BLE001 - report and continue
                self._log(f"Failed to start {inst.kind} {inst.name}: {e}")
                ok = False
            if not ok:
                all_ok = False
        return all_ok

    def wait_for_runs(self, location_name: str, timeout: int = 600) -> bool:
        """Block until the location has no active runs, or the timeout elapses.
        Returns True if drained, False on timeout."""
        return self.client.wait_for_runs_to_complete(location_name, timeout=timeout)


# -- state persistence (so drain and restore can be separate processes) ------


def save_instigators(path: str, instigators: List[SavedInstigator]) -> None:
    """Write the stopped-instigator list to a JSON state file."""
    with open(path, "w") as f:
        json.dump([asdict(i) for i in instigators], f, indent=2)


def load_instigators(path: str) -> List[SavedInstigator]:
    """Read a JSON state file written by :func:`save_instigators`."""
    with open(path) as f:
        data = json.load(f)
    return [SavedInstigator(**d) for d in data]
