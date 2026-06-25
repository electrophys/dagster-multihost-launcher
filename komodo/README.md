# Komodo wiring for the multi-host Dagster deployment

Example/templated artifacts that wire Komodo to this launcher. They turn the
shipped CLI verbs (`drain`, `reload`, `restore`, `check-env`,
`komodo-export`/`komodo-verify`) into an actual safe rollout.

> New here? Start with the end-to-end
> [Komodo + Dagster Setup & Configuration Guide](../komodo-dagster-setup-guide.md);
> these files are the artifacts it references.

> These are **starting points to adapt**, not drop-in files. Komodo's exact TOML
> fields and Action client signatures vary by version — confirm against your
> Komodo docs. Nothing here runs in CI.

## Files

- [`resource-sync.toml`](resource-sync.toml) — a Komodo Resource Sync (GitOps)
  definition: `[[server]]` per host, `[[stack]]` per code location, a `[[build]]`
  that pins the image, a `[[variable]]`, and a `[[procedure]]` that runs the safe
  rollout. Generate the server/stack skeleton with
  `dagster-multihost komodo-export -o komodo/resource-sync.toml`, then fill it in.
- [`actions/safe-deploy.ts`](actions/safe-deploy.ts) — a Komodo **Action**
  (TypeScript, runs in Core) that orchestrates
  `drain → DeployStack → reload → restore` for one code location.
- [`tls-provisioning.md`](tls-provisioning.md) + [`actions/provision-docker-tls.ts`](actions/provision-docker-tls.ts)
  — automate the Docker mutual-TLS cert lifecycle (store certs as Komodo Secrets,
  configure dockerd's `tcp://…:2376` endpoint, rotate) instead of distributing
  certs by hand.

## The rollout

```
drain  ──►  DeployStack  ──►  reload  ──►  restore
(stop schedules/sensors,   (Komodo redeploys   (control plane   (re-enable
 wait for runs, record      the code-location    picks up the     schedules/
 what was stopped)          stack with the       new code)        sensors)
                            pinned image)
```

Stages run sequentially; a non-zero exit halts the Procedure — the rollback
gate. `drain` restarts what it stopped and exits non-zero if runs don't finish,
so a stuck location never gets redeployed out from under active runs.

## Prerequisites on the control-plane host (Host A)

The `drain`/`reload`/`restore`/`check-env` verbs talk to the Dagster webserver,
so they must run **on Host A** (its Periphery, or via the Action's server
terminal), with:

- `dagster-multihost` installed (e.g. baked into the daemon image or a sidecar).
- `DAGSTER_WEBSERVER_URL` set (or pass `--webserver-url`).
- A **stable, shared path** for the drain state file (e.g.
  `/var/lib/dagster-deploy/`) so `restore` in a later stage reads what `drain`
  wrote — the two stages may run in different terminal invocations.

## Keeping things in sync

- `dagster-multihost komodo-verify komodo/resource-sync.toml` — fail CI if a host
  or code location in `dagster.yaml` is missing from / misplaced in this TOML
  (prevents a location silently falling back to the DefaultRunLauncher).
- `dagster-multihost check-env` — fail a pre-deploy gate if the control plane is
  missing env vars `dagster.yaml` references (render them via the Komodo
  `[[variable]]` / Secrets into the control-plane stack's `.env`).

## Image pinning

The `[[build]]` should tag images deterministically (commit SHA / semver, never
`:latest`) and set `DAGSTER_CURRENT_IMAGE` on the code-location stack to that
tag. The gRPC server and the run containers the launcher spawns then use the
identical, validated image.
