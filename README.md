# Battleground Telemetry

Synthetic battle-game analytics using **Python → Kafka (KRaft) → Flink Java → PostgreSQL → Apache Superset**, with dbt, Airflow, Prometheus and a bounded Parquet archive. No official PUBG affiliation or official scoring formula.

**0.1.0-dev.2 — development prerelease on `dev`.** See [STATUS](STATUS.md) and [acceptance evidence](docs/acceptance.md) for verified results and limits. Superset is the selected dashboard surface; Tableau specifications are retained only as an optional reference.

## Run the demo

Requires Docker Compose and Python 3, on an authorized Linux workspace or local Docker host. No paid services or trials are provisioned. Local testing was explicitly authorized; cloud operation remains subject to available quota.

```sh
make bootstrap       # Build, migrations, topics, Flink submission, actual four-source smoke match
make demo            # Default 100-player run, approximately four minutes
make bi-up           # Author four real Superset dashboards automatically
make status
make scenario SCENARIO=latency-spike
make smoke           # After completion
make reconcile
make dbt
make down            # Preserve data
```

Open [Superset](http://localhost:8088). Username `admin`; password is the `SUPERSET_ADMIN_PASSWORD` value in the ignored local `.env`. Select one run in the dashboard filter; pipeline metrics have installation scope. PostgreSQL and Kafka are not exposed to the host. Admin HTTP ports bind only to loopback; keep cloud forwards private.

For an Apple Silicon host without Docker, the local test setup uses an isolated Colima profile: `colima start --profile battleground --cpu 6 --memory 12 --disk 50 --vm-type vz --mount-type virtiofs`. Docker, Compose and Colima must already be installed. Bootstrap measures the Docker VM's capacity and generates local credentials without printing them.

## Project map

- `simulator/`, `contracts/`: four independent source schedules, shared match invariants, Pydantic/versioned JSON contracts.
- `streaming/`: Flink validation, Kafka watermarks/idleness, side outputs, keyed TTL deduplication, windows, persistent checkpoints and idempotent JDBC sinks.
- `warehouse/`: canonical facts, manifest reconciliation, revisioned sessions, server windows, live mart views and separate database roles.
- `superset/`: pinned application image, secure local initialization and reproducible authoring of four dashboards; no manual chart assembly.
- `dbt/`, `orchestration/`: documented transformations/tests, historical correction and manual archive/reconcile/dbt/export DAG.
- `observability/`: actual producer/broker/consumer/database/checkpoint observations and Prometheus.
- `scripts/`, `tests/`: operations, archive/replay, faults, bounded benchmark and browser evidence.

## Validate

```sh
make test
make integration
make dbt
python -m scripts.fault_acceptance
python -m scripts.benchmark
make ops-up
# Trigger telemetry_maintenance in private Airflow UI/CLI
make ops-down
```

Browser verification: install `requirements-browser.txt`, run `python -m playwright install chromium`, then `python -m scripts.verify_superset`. Images and measured browser responses are saved under ignored `artifacts/superset/`.

[Guarantees](docs/guarantees.md) · [Metric definitions](docs/metrics.md) · [Operations](docs/operations.md) · [Superset setup](superset/README.md) · [BI decision](docs/bi-decision.md) · [Five-minute interview walkthrough](docs/interview.md)

This is a single-broker portfolio deployment, not a production or high-availability service. Checkpoints do not imply end-to-end exactly-once delivery. Do not interpret a quiet completed run as a failed pipeline or a periodically queried dashboard as push streaming.
