# Demo scripts

Open the authenticated Superset dashboards before presenting. The completed benchmark run is ready for an immediate static walkthrough. For live delivery start `make demo`, copy its new run ID from `make status`, then select that run in each analytical dashboard. Pipeline Reliability has installation scope.

## 90 seconds

0–15s: “This is synthetic battle telemetry: four source schedules, Kafka, Flink, PostgreSQL and Superset. It has no official game affiliation. The deployment is one broker, designed for a portfolio demo.”

15–40s: Start `make demo`. Show the selected run's provisional leaderboard. Explain that finish messages and gameplay may arrive in different orders; final means the immutable facts match the source manifest.

40–60s: `make scenario SCENARIO=latency-spike`. Show the labeled synthetic incident interval, p95 observation latency, packet-loss counts and tick budget.

60–80s: Show measured canonical throughput, checkpoint-committed lag and processing delay. Explain that backend freshness and a Superset refresh are different measurements.

80–90s: Show final/corrected results or the latest snapshot timestamp. State the measured timing and exact limitations, one broker and no production SLA.

## Three minutes

0–30s: Architecture and grains; select one run, match and region. Explain source event identity and delivery metadata.

30–65s: Show player ranks and configurable project score. Duplicate delivery leaves canonical results unchanged. Kill and placement counts come from facts, not retried increments.

65–100s: Show Match Operations. A finish-first scenario remains provisional until expected gameplay arrives; excessively late events remain retained, then appear as corrected after `make reconcile`.

100–130s: Inject latency spike. Show pooled synthetic observation p95 and packet loss from summed counts; current online sessions are distinct from alive players.

130–155s: Show the local fault-test evidence: checkpoint before TaskManager restart, lag while paused, drain after recovery, unchanged results. Do not run destructive resets live.

155–180s: Show archive checksum, isolated replay comparison, dbt checks and Airflow batch result. Show the measured 12.9-second automatic refresh proof and distinguish it from 106-ms processing p95 in the separate clean run.
