/**
 * Komodo Action: safe rollout of one Dagster code location.
 *
 *   drain  ->  DeployStack  ->  reload  ->  restore
 *
 * Runs in Komodo Core with the pre-initialized `komodo` client. The Dagster-aware
 * steps (drain/reload/restore) run the `dagster-multihost` CLI ON HOST A via a
 * server terminal, because those verbs talk to the Dagster webserver. The
 * DeployStack step is a normal Komodo execution.
 *
 * ADAPT THIS: confirm the `komodo.execute` / server-terminal method names and
 * shapes against your Komodo version, and how terminal exit status surfaces.
 * Prerequisites: `dagster-multihost` installed on Host A, DAGSTER_WEBSERVER_URL
 * set there, and a stable STATE_FILE path shared across the terminal calls.
 */

const LOCATION = "etl"; // Dagster code location name
const STACK = "etl"; // Komodo stack name (== location by convention)
const HOST_A_SERVER = "host-a"; // Komodo server running the control plane
const STATE_FILE = `/var/lib/dagster-deploy/${LOCATION}.drain.json`;
const TIMEOUT = 600;

/**
 * Run a shell command on Host A and throw if it fails. A server terminal is a
 * PTY stream, so confirm how your Komodo version reports the exit status; the
 * `|| echo "__FAIL__"` sentinel below is a portable fallback if it doesn't.
 */
async function onHostA(command: string): Promise<void> {
  const result = await komodo.execute_server_terminal({
    server: HOST_A_SERVER,
    terminal: "dagster-deploy",
    command: `${command} || echo "__FAIL__"`,
  });
  const output = typeof result === "string" ? result : JSON.stringify(result);
  if (output.includes("__FAIL__")) {
    throw new Error(`Host A command failed: ${command}\n${output}`);
  }
}

export async function handler() {
  // 1. Quiesce: stop schedules/sensors, wait for active runs, record state.
  //    Non-zero exit (runs didn't drain) aborts here — nothing is redeployed.
  await onHostA(
    `dagster-multihost drain ${LOCATION} --state-file ${STATE_FILE} --timeout ${TIMEOUT}`,
  );

  try {
    // 2. Redeploy the code-location stack (Komodo pulls the pinned image).
    await komodo.execute({ type: "DeployStack", params: { stack: STACK } });

    // 3. Reload the code location so the control plane picks up the new code.
    await onHostA(`dagster-multihost reload ${LOCATION}`);
  } finally {
    // 4. Always re-enable what we stopped, even if deploy/reload failed.
    await onHostA(`dagster-multihost restore --state-file ${STATE_FILE}`);
  }
}
