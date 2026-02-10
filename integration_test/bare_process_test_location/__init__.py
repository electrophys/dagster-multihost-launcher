import os
import platform
import socket

from dagster import asset, Definitions, MaterializeResult


@asset(group_name="bare_process_test", compute_kind="python")
def bare_process_proof_of_execution() -> MaterializeResult:
    """Asset that proves it executed on a bare-process (non-Docker) host."""
    return MaterializeResult(
        metadata={
            "hostname": socket.gethostname(),
            "architecture": platform.machine(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "os_name": os.name,
        }
    )


defs = Definitions(assets=[bare_process_proof_of_execution])
