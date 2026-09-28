# Status — 0.1.0-dev.2

Updated 2026-09-28. **Locally tested development prerelease on `dev`.** The selected analytics surface is Apache Superset; no Tableau authoring is required. Origin is `https://github.com/gv1shnu/pubg.git`.

The actual Python → Kafka → Flink → PostgreSQL pipeline runs in local Docker. Four authenticated Superset dashboards contain 24 charts. dbt models/tests, the six-task Airflow maintenance DAG, archives/replay and operational monitoring have been exercised.

Verified: 69 Python unit tests, four Java JUnit tests, four real-stack integration tests, 12 dbt models and 22 dbt tests. TaskManager fault testing increased committed lag from 108 to 553, recovered through a newer checkpoint and drained lag to zero. Browser verification opened all four dashboards successfully. Automatic refresh showed a newly observed SQL kill threshold after 12.9 seconds during a live run.

A clean 100-player, roughly one-minute benchmark persisted 1,524 records with 106 ms emitted-to-processed p95 delay. The sampled project-container memory peak was 1,725 MiB and the filtered leaderboard EXPLAIN ANALYZE execution took 215 ms. These are observations from one local run, not production capacity or latency guarantees.

See [acceptance details](docs/acceptance.md), [machine-readable evidence](docs/evidence/runtime-summary.json), [operations](docs/operations.md) and [boundaries](docs/guarantees.md). Single broker, no JobManager HA, no durable producer recovery and no sustained-load certification. No stable release tag is claimed.
