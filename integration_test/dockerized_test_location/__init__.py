import platform
import socket

from dagster import asset, Definitions, MaterializeResult


@asset(group_name="dockerized_test", compute_kind="python")
def dockerized_proof_of_execution() -> MaterializeResult:
    """Asset that proves it executed on a remote Docker host."""
    return MaterializeResult(
        metadata={
            "hostname": socket.gethostname(),
            "architecture": platform.machine(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
        }
    )


defs = Definitions(assets=[dockerized_proof_of_execution])
