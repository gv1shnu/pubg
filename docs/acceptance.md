# Acceptance evidence — 2026-09-28

The owner selected Superset and authorized local Docker execution and development-branch pushes. Earlier cloud/Tableau blockers were superseded; `static-checks.json` is historical evidence from dev.1, not the current acceptance result.

## Runtime results

| Verification | Observed result |
|---|---|
| Domain/contracts | 69 Python tests passed; four integration tests skipped in the unit invocation |
| Java build | Maven compilation/shading and four JUnit tests passed |
| Four-source pipeline | Bootstrap's four-player real run persisted 98 records and reached final outcomes |
| Duplicate/repeated writes | Real integration kept canonical counts/outcomes stable |
| Finish-first/shuffled data | Provisional status became final after required gameplay arrived |
| Lateness/rejection | Bounded lateness accepted; excess lateness retained and corrected; malformed/version-invalid input rejected while valid input continued; two actual Kafka dead-letter messages independently read |
| Archive/replay | Checksum/count and isolated normalized outcome equivalence passed without changing original results |
| Integration suite | Four real Kafka/Flink/PostgreSQL tests passed in 12.05 seconds |
| Recovery | Checkpoint 6 preceded the fault; committed lag rose 108 → 553 during TaskManager outage, then drained to zero with a newer checkpoint and final outcomes |
| dbt | 12 models and 22 tests passed (34 successful nodes) |
| Airflow | All six `telemetry_maintenance` tasks succeeded: pin run, archive, reconcile, dbt, export, finish |
| Superset | Four authenticated dashboards and 24 saved charts; browser checks query responses and errors |
| Live refresh | A newly observed SQL kill threshold appeared in an automatic browser response after 12.936 seconds; incident rows were present and the incident table visible |

The live proof polls SQL once per second and compares its observed threshold with a later automatic chart response. It measures observation-to-browser-response delay, not exact database commit-to-pixel time. Configured refresh is 15 seconds. Independent chart queries can see different commit states.

## Bounded benchmark

Run `3c1881c3adbc49e3a7b52a68d420f51b`: 100 players, latency-spike scenario, 60-second source target, 64.46-second observed harness duration. No concurrent builds, maintenance or extra simulations. Core and Superset services were running; no active dashboard workload was imposed.

- 1,524 canonical records persisted across 62.306 seconds of processing timestamps.
- Emitted-to-processed p95 across this run: **0.1065 seconds**. This uses application wall timestamps; it does not include subsequent query/render latency.
- Nine Docker/Prometheus samples; maximum sampled installation-wide one-minute canonical rate: **22.22 records/second**. Window warm-up means this differs from total rows divided by run duration.
- Maximum sampled sum of container memory: **1,725 MiB**; maximum summed CPU: **84.01%** (Docker's one-core percentage convention, not percentage of all six CPUs).
- Filtered leaderboard `EXPLAIN (ANALYZE, BUFFERS)`: **214.739 ms** execution. The plan and original samples are included.
- Colima Docker VM: six CPUs, 12,513,529,856 bytes reported RAM, on an Apple Silicon macOS host. This is not cloud hardware or a saturation test. Sampling can miss peaks; VM overhead and build memory are excluded.

A prior benchmark overlapped an Airflow build and is retained only in ignored local artifacts. Reported results use the clean run above. Recovery delay during an intentional outage is not mixed into the run-specific latency figure.

## Reviewable artifacts

- [Summary](evidence/runtime-summary.json), [benchmark samples](evidence/benchmark.json), [query plan](evidence/query-plan.json).
- [Python result XML](evidence/python-unit.xml), [integration result XML](evidence/integration.xml), [browser responses](evidence/browser-evidence.json).
- [Actual DLQ topic inspection](evidence/dlq-topic.json), [CSV snapshot manifest](evidence/snapshot-manifest.json).
- [Superset native export](../superset/dashboards.zip) and reproducible [authoring code](../superset/bootstrap.py).
- Dashboard screenshots under `docs/images/`; raw local logs, archives, dbt output and exports remain in ignored `artifacts/` and `tableau/exports/`. Credentials are excluded.

## Limits

This is a tested development portfolio deployment. Single broker and non-HA JobManager; source process state is not durable; first accepted identity wins without a conflicting-payload audit. Gameplay manifests certify gameplay completeness, not independent server/session streams. Server windows remain provisional/corrected. Raw storage/archive retention requires operator management. Dashboard defaults select the newest run at authoring time; select a new run after starting another simulation. Cloud operation, production security and sustained capacity have not been certified. See [guarantees](guarantees.md).
