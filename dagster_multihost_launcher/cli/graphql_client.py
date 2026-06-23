"""GraphQL client for Dagster webserver API."""

import json
import time
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen


class DagsterGraphQLClient:
    """Thin GraphQL client for the Dagster webserver."""

    def __init__(self, webserver_url: str, timeout: int = 30):
        self.graphql_url = f"{webserver_url.rstrip('/')}/graphql"
        self.timeout = timeout

    def query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> dict:
        """Execute a GraphQL query/mutation."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        req = Request(
            self.graphql_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=self.timeout) as resp:
            result = json.loads(resp.read())

        if "errors" in result:
            raise RuntimeError(
                f"GraphQL error: {json.dumps(result['errors'], indent=2)}"
            )
        return result

    def is_reachable(self) -> bool:
        """Check if the webserver is reachable."""
        try:
            self.query("{ __typename }")
            return True
        except (URLError, OSError, RuntimeError):
            return False

    def wait_for_webserver(self, timeout: int = 120, poll_interval: int = 3) -> bool:
        """Poll until the webserver becomes reachable."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_reachable():
                return True
            time.sleep(poll_interval)
        return False

    # -- Workspace --

    def get_code_locations(self) -> List[Dict[str, str]]:
        """Get all code locations and their load status."""
        result = self.query(
            "{ workspaceOrError { ... on Workspace { "
            "locationEntries { name loadStatus } } } }"
        )
        return result["data"]["workspaceOrError"]["locationEntries"]

    def reload_workspace(self) -> List[Dict[str, str]]:
        """Reload the workspace and return location entries."""
        result = self.query(
            "mutation { reloadWorkspace { "
            "... on Workspace { locationEntries { name loadStatus } } } }"
        )
        return result["data"]["reloadWorkspace"]["locationEntries"]

    def wait_for_locations(
        self,
        location_names: List[str],
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> bool:
        """Wait until all specified code locations are loaded."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                entries = self.get_code_locations()
                loaded = {e["name"] for e in entries if e["loadStatus"] == "LOADED"}
                if all(loc in loaded for loc in location_names):
                    return True
            except Exception:
                pass
            time.sleep(poll_interval)
        return False

    # -- Runs --

    def get_active_runs(
        self, location_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get runs in non-terminal states, optionally filtered by location."""
        query = """
        query($filter: RunsFilter) {
          runsOrError(filter: $filter) {
            ... on Runs {
              results {
                runId
                status
                jobName
                tags { key value }
              }
            }
            ... on PythonError { message }
          }
        }
        """
        variables: Dict[str, Any] = {
            "filter": {"statuses": ["STARTED", "QUEUED", "STARTING", "CANCELING"]}
        }

        result = self.query(query, variables)
        runs_data = result["data"]["runsOrError"]

        if "message" in runs_data:
            raise RuntimeError(f"Error querying runs: {runs_data['message']}")

        runs = runs_data.get("results", [])

        if location_name:
            filtered = []
            for run in runs:
                tags = {t["key"]: t["value"] for t in run.get("tags", [])}
                run_location = tags.get("dagster/code_location")
                if run_location == location_name:
                    filtered.append(run)
            return filtered

        return runs

    def wait_for_runs_to_complete(
        self,
        location_name: str,
        timeout: int = 600,
        poll_interval: int = 10,
    ) -> bool:
        """Wait until all active runs for a location reach terminal state."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            active = self.get_active_runs(location_name)
            if not active:
                return True
            time.sleep(poll_interval)
        return False

    def terminate_run(self, run_id: str) -> bool:
        """Terminate a running run."""
        result = self.query(
            'mutation { terminateRun(runId: "%s") { '
            "__typename "
            "... on TerminateRunSuccess { run { runId status } } "
            "... on PythonError { message } "
            "} }" % run_id
        )
        data = result["data"]["terminateRun"]
        return data.get("__typename") == "TerminateRunSuccess"

    # -- Schedules --

    def get_schedules(self, location_name: str) -> List[Dict[str, Any]]:
        """Get all schedules for a code location with their status."""
        result = self.query(
            '{ schedulesOrError(repositoryLocationName: "%s") { '
            "... on Schedules { results { "
            "name scheduleState { status } "
            "repositoryOrigin { repositoryName repositoryLocationName } "
            "} } "
            "... on PythonError { message } "
            "} }" % location_name
        )
        data = result["data"]["schedulesOrError"]
        if "message" in data:
            return []
        return data.get("results", [])

    def stop_schedule(
        self,
        schedule_name: str,
        repository_name: str,
        location_name: str,
    ) -> bool:
        """Stop a running schedule."""
        result = self.query(
            "mutation { stopRunningSchedule(scheduleSelector: { "
            'scheduleName: "%s", '
            'repositoryName: "%s", '
            'repositoryLocationName: "%s"'
            "}) { __typename "
            "... on ScheduleStateResult { scheduleState { status } } "
            "... on PythonError { message } "
            "} }" % (schedule_name, repository_name, location_name)
        )
        data = result["data"]["stopRunningSchedule"]
        return data.get("__typename") == "ScheduleStateResult"

    def start_schedule(
        self,
        schedule_name: str,
        repository_name: str,
        location_name: str,
    ) -> bool:
        """Start a schedule."""
        result = self.query(
            "mutation { startSchedule(scheduleSelector: { "
            'scheduleName: "%s", '
            'repositoryName: "%s", '
            'repositoryLocationName: "%s"'
            "}) { __typename "
            "... on ScheduleStateResult { scheduleState { status } } "
            "... on PythonError { message } "
            "} }" % (schedule_name, repository_name, location_name)
        )
        data = result["data"]["startSchedule"]
        return data.get("__typename") == "ScheduleStateResult"

    # -- Sensors --

    def get_sensors(self, location_name: str) -> List[Dict[str, Any]]:
        """Get all sensors for a code location with their status."""
        result = self.query(
            '{ sensorsOrError(repositoryLocationName: "%s") { '
            "... on Sensors { results { "
            "name sensorState { status } "
            "repositoryOrigin { repositoryName repositoryLocationName } "
            "} } "
            "... on PythonError { message } "
            "} }" % location_name
        )
        data = result["data"]["sensorsOrError"]
        if "message" in data:
            return []
        return data.get("results", [])

    def stop_sensor(
        self,
        sensor_name: str,
        repository_name: str,
        location_name: str,
    ) -> bool:
        """Stop a running sensor."""
        result = self.query(
            "mutation { stopSensor(instigationSelector: { "
            'name: "%s", '
            'repositoryName: "%s", '
            'repositoryLocationName: "%s"'
            "}) { __typename "
            "... on StopSensorMutationResult { instigationState { status } } "
            "... on PythonError { message } "
            "} }" % (sensor_name, repository_name, location_name)
        )
        data = result["data"]["stopSensor"]
        return data.get("__typename") == "StopSensorMutationResult"

    def start_sensor(
        self,
        sensor_name: str,
        repository_name: str,
        location_name: str,
    ) -> bool:
        """Start a sensor."""
        result = self.query(
            "mutation { startSensor(sensorSelector: { "
            'sensorName: "%s", '
            'repositoryName: "%s", '
            'repositoryLocationName: "%s"'
            "}) { __typename "
            "... on Sensor { sensorState { status } } "
            "... on PythonError { message } "
            "} }" % (sensor_name, repository_name, location_name)
        )
        data = result["data"]["startSensor"]
        return data.get("__typename") == "Sensor"
