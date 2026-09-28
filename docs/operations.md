# Operations runbook

Commands run on an authorized local Docker host or existing Linux workspace. The tested local setup uses Colima with 6 CPUs and approximately 12 GiB RAM on Apple Silicon. No paid resources or trials are provisioned. Keep forwarded admin ports private.

## First run

```sh
make bootstrap          # Generates private .env; builds before runtime; creates topics/migrations; submits job; verifies tiny actual match
make demo               # Starts a fresh default 100-player, approximately four-minute run
make status
make scenario SCENARIO=latency-spike
make pause              # Pauses source delivery loops
make resume
make drain              # Waits up to 120s for checkpoint-committed lag to reach zero
make smoke              # Run after completion; verifies all four topics, outcomes and closed sessions
make reconcile
make dbt
make export-tableau     # Legacy command name: generic consistent CSV snapshots
make export-artifacts   # Copy archive, evidence and CSVs from named volumes to host
make bi-up              # Authenticated Superset at localhost:8088
make down               # Preserves named volumes and exports
```

Settings are in `.env`: seed, players (2–100), target duration (10–600 seconds), event-rate target (1–100), concurrent matches (1–3), regions, named scenario. The target rate × duration / concurrent matches is capped at 5000 to bound final manifest size. Duration is a target and pauses/backpressure extend wall time. For repeatable source identities keep the seed and run ID; never restart a partially emitted run with the same run ID. The normal runtime creates a fresh UUID each process start. Four source schedules are independent, so target event rate is approximate, not a throughput guarantee. Named scenarios: normal, latency-spike, disconnect-burst, duplicate, out-of-order, excessively-late, finish-first, invalid. Optional DUPLICATE_PROBABILITY, LATENCY_SPIKE_PROBABILITY and DISCONNECT_BURST_PROBABILITY each accept 0–1 (default 0); probabilities are applied per relevant source tick/event using seeded generators.

Use `python scripts/cloud.py restore-checkpoint file:///checkpoints/<job-id>/chk-<n>` after canceling any current job. Confirm the path and Kafka retained offsets. `make restart-processing` waits for a successful checkpoint, restarts TaskManager and waits for a newer checkpoint; it does not prove all acceptance criteria by itself.

## Archive/replay and operations profile

```sh
make archive
# Copy the printed manifest path (inside /artifacts/archive):
docker compose run --rm --no-deps toolbox python -m scripts.ops replay --manifest /artifacts/archive/date=YYYY-MM-DD/run=HASH/UUID/manifest.json --namespace replay-demo1
# Wait for expected row count, then reconcile that namespace with original source run ID:
docker compose run --rm --no-deps toolbox python -m scripts.ops reconcile --namespace replay-demo1 --run SOURCE_RUN_ID
make ops-up
# Airflow standalone generates its own admin password; inspect it privately in the workspace.
docker compose exec airflow airflow dags trigger telemetry_maintenance
# View task result in the private Airflow UI or CLI. Stop ops after use:
make ops-down
```

Airflow metadata lives in its own `airflow` database with its own owner, not the analytical schemas. Tasks use the isolated `/opt/dbt/bin/dbt` executable. Batch start pins the latest live run once; subsequent tasks use that run even if a new simulation begins. dbt validates all retained demo data. A failing task records failure and raises; a retry can rerun idempotent reconciliation/materialization. Archive retries create independent timestamped files, so disk use can grow. Keep/remove artifacts intentionally; this tool does not delete them automatically.

## Faults

| Symptom | Checks and recovery |
|---|---|
| Port conflict | Inspect listeners on the Docker host for 8080/8081/8088/9090; change only loopback host bindings and matching CLI REST endpoint if necessary. Never publish Kafka/PG to resolve a BI connection problem |
| Broker unavailable | `docker compose logs kafka`; check free disk and healthcheck. Stop source load; restart broker; source delivery fails loudly after timeout. Start a new simulation run after a producer process failure |
| PostgreSQL sink unavailable | Inspect `docker compose logs postgres taskmanager`; JDBC retries and Flink restart policy are bounded. Restore DB service, watch checkpoints/lag, and reconcile. No additive counters need rolling back |
| Failed checkpoint | Inspect Flink UI, checkpoint failure reason and /checkpoints permissions/free space. Avoid deleting checkpoint files while a job relies on them. Resume only after successful new checkpoint |
| Full disk | `df -h`, `docker system df`; stop new producers, archive/preserve wanted data before removing project artifacts. Do not run global prune. Kafka/Prometheus retention is bounded; raw PostgreSQL/archive volume is not automatically purged |
| Superset unavailable | Check `docker compose logs superset`; wait for migrations and healthy status. Credentials are in ignored `.env`. Do not expose PostgreSQL to fix dashboard connectivity |
| Stale dashboard | Check selected run, source/processed times, authentication and 15-second refresh. Completed runs are static; start a new demo and select its run. Refresh is polling, not push delivery |
| JobManager restarted | No HA is configured. Inspect retained checkpoint path, submit `restore-checkpoint`; creating a fresh job from committed offsets without restoring state is a different recovery path (DB identities still protect counts) |
| Unexpected provisional match | Compare matched_ids and expected_gameplay_count; inspect DLQ and pending_late; restore missing retained source deliveries or reconcile late facts. Do not mark final manually |

## Destructive reset

`CONFIRM_RESET=DELETE_PROJECT_DEMO_DATA make reset-demo` removes only this Compose project's named volumes after stopping its services. It deletes database, Kafka, checkpoint, Superset metadata, archive and export named volumes. Run `make export-artifacts` first to preserve wanted outputs on the host. Source, `.env` and already copied host artifacts survive. Ordinary `make down` preserves volumes.

## Evidence commands

```sh
make test
make integration
python -m scripts.fault_acceptance
python -m scripts.benchmark
make dbt
# If memory is constrained, stop core workers before building ops:
docker compose stop simulator taskmanager jobmanager prometheus collector kafka
docker compose build airflow
docker compose run --rm --no-deps airflow bash -c 'airflow db migrate && airflow dags test telemetry_maintenance 2026-01-01'
```

Save command exit codes, dependency versions, runtime logs (without credentials), artifacts/fault-acceptance.json, benchmark.json, query-plan.txt, dbt results and Airflow batch status. A run with missing metrics is incomplete benchmark evidence. Never extrapolate a one-minute demo into production throughput.
