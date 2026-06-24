/**
 * Komodo Action: provision Docker TCP+mTLS on a worker host.
 *
 * Writes the CA + server cert/key, configures dockerd to listen on
 * tcp://0.0.0.0:2376 with tlsverify, and reloads dockerd — so the launcher can
 * reach this host's Docker daemon directly (see ../tls-provisioning.md).
 *
 * SECRET HANDLING — IMPORTANT. The key material below is interpolated from
 * Komodo *secret* Variables ([[...]]). Komodo substitutes and masks these in
 * logs. Do NOT rewrite this to `echo`/print the keys, and do not pass them as
 * plain command arguments — a server-terminal PTY stream can be logged. The
 * heredocs write to root-only files; keep it that way.
 *
 * ADAPT: confirm the server-terminal / interpolation API for your Komodo
 * version, the systemd unit path, and that `[[...]]` secret interpolation is
 * applied to this Action's executed commands.
 */

const SERVER = "host-b"; // Komodo server (worker) to provision
const CERT_DIR = "/etc/docker/tls";

// Cert material from Komodo (secret) Variables. Komodo interpolates [[NAME]].
const CA_PEM = "[[CA_PEM]]";
const SERVER_CERT = "[[HOST_B_SERVER_CERT]]";
const SERVER_KEY = "[[HOST_B_SERVER_KEY]]"; // secret

/** Run a command on the worker; throw on failure (see safe-deploy.ts notes). */
async function onServer(command: string): Promise<void> {
  const result = await komodo.execute_server_terminal({
    server: SERVER,
    terminal: "provision-tls",
    command: `${command} || echo "__FAIL__"`,
  });
  const output = typeof result === "string" ? result : JSON.stringify(result);
  if (output.includes("__FAIL__")) {
    throw new Error(`provisioning step failed: ${command}\n${output}`);
  }
}

export async function handler() {
  // 1. Write certs with restrictive perms (umask 077; key stays 0400).
  await onServer(`umask 077 && mkdir -p ${CERT_DIR}`);
  await onServer(`cat > ${CERT_DIR}/ca.pem <<'PEM'\n${CA_PEM}\nPEM`);
  await onServer(`cat > ${CERT_DIR}/server-cert.pem <<'PEM'\n${SERVER_CERT}\nPEM`);
  await onServer(`cat > ${CERT_DIR}/server-key.pem <<'PEM'\n${SERVER_KEY}\nPEM`);
  await onServer(
    `chmod 0444 ${CERT_DIR}/ca.pem ${CERT_DIR}/server-cert.pem && ` +
      `chmod 0400 ${CERT_DIR}/server-key.pem && chown -R root:root ${CERT_DIR}`,
  );

  // 2. daemon.json — TLS settings only (NOT "hosts"; that conflicts with the
  //    systemd -H flag). See docker-tls-setup-guide.md §6.
  await onServer(
    `cat > /etc/docker/daemon.json <<'JSON'\n` +
      JSON.stringify(
        {
          tls: true,
          tlsverify: true,
          tlscacert: `${CERT_DIR}/ca.pem`,
          tlscert: `${CERT_DIR}/server-cert.pem`,
          tlskey: `${CERT_DIR}/server-key.pem`,
        },
        null,
        2,
      ) +
      `\nJSON`,
  );

  // 3. systemd override: listen on the socket AND tcp:2376.
  await onServer(`mkdir -p /etc/systemd/system/docker.service.d`);
  await onServer(
    `cat > /etc/systemd/system/docker.service.d/override.conf <<'UNIT'\n` +
      `[Service]\nExecStart=\nExecStart=/usr/bin/dockerd -H fd:// -H tcp://0.0.0.0:2376\n` +
      `UNIT`,
  );

  // 4. Apply. reload (not restart) where possible to keep containers running.
  await onServer(`systemctl daemon-reload && systemctl restart docker`);
  await onServer(`ss -tlnp | grep 2376`); // sanity: listening on TLS port
}
