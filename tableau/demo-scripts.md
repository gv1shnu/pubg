# Demo scripts

Use only after the corresponding cloud and Tableau checks pass. If authoring remains blocked, say so and demonstrate verified SQL/export outputs instead; do not describe them as completed Tableau dashboards.

## 90 seconds

0–15s: “This is synthetic battle telemetry: four source schedules, Kafka, Flink, PostgreSQL and Tableau. It has no official game affiliation. The deployment is one broker, designed for a portfolio demo.”

15–40s: Start `make demo`. Show the selected run's provisional leaderboard. Explain that finish messages and gameplay may arrive in different orders; final means the immutable facts match the source manifest.

40–60s: `make scenario SCENARIO=latency-spike`. Show the labeled synthetic incident interval, p95 observation latency, packet-loss counts and tick budget.

60–80s: Show measured canonical throughput, checkpoint-committed lag and processing delay. Explain that backend freshness and a Tableau refresh are different measurements.

80–90s: Show final/corrected results or the latest snapshot timestamp. State the measured timing and exact limitations, including snapshot-only mode when applicable.

## Three minutes

0–30s: Architecture and grains; select one run, match and region. Explain source event identity and delivery metadata.

30–65s: Show player ranks and configurable project score. Duplicate delivery leaves canonical results unchanged. Kill and placement counts come from facts, not retried increments.

65–100s: Show Match Operations. A finish-first scenario remains provisional until expected gameplay arrives; excessively late events remain retained, then appear as corrected after `make reconcile`.

100–130s: Inject latency spike. Show pooled synthetic observation p95 and packet loss from summed counts; current online sessions are distinct from alive players.

130–155s: Show the cloud fault-test evidence: checkpoint before TaskManager restart, lag while paused, drain after recovery, unchanged results. Do not run destructive resets live.

155–180s: Show archive checksum, isolated replay comparison, dbt checks and Airflow batch result. Finish with actual Tableau freshness if measured, otherwise the precise authoring/networking blocker and honest snapshot timestamp.
