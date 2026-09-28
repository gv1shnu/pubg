# Five-minute interview walkthrough

## 0:00–0:40 — Problem and tool responsibilities

“This project models a synthetic battle game as four independently scheduled sources: match lifecycle, gameplay, sessions and server telemetry. Each has a real Kafka topic and a separate producer client. Shared simulation state prevents impossible health or alive-player actions. Kafka buffers delivery, Flink handles continuous validation/state/event time, PostgreSQL stores canonical facts, dbt documents and tests analytical models, Airflow runs bounded batch maintenance, and Superset provides four working dashboards. Gameplay is synthetic; pipeline metrics come from actual runtime instruments when the stack is running.”

Show the four Superset dashboards. Local evidence includes 69 Python unit tests, four JUnit tests, four real integration tests, 22 dbt tests, the six-task Airflow DAG and browser verification.

## 0:40–1:15 — Partitioning and event time

“I key match/gameplay records by namespace/run/match, and session/server records by namespace/run/server. That preserves order within a topic partition, but separate topics have no common ordering guarantee. The Flink Kafka source uses source event time with a ten-second out-of-order watermark tolerance and fifteen-second idle-partition detection. Emitted and processed wall timestamps are separate from accelerated simulation time, so fast simulated matches do not invent negative pipeline latency.”

Point to `Wire.Decoder` and `TelemetryJob`'s source watermark strategy. Kafka topic/partition/offset survive into the raw warehouse for investigation.

## 1:15–2:00 — State, checkpoints and idempotence

“Flink keys state by source identity and remembers it for twenty-four hours. That bounds memory, so it cannot promise deduplication forever. PostgreSQL's namespace/run/event primary key protects canonical facts beyond that horizon. JDBC retries use INSERT ON CONFLICT DO NOTHING; I never increment kills inside a retried sink write.

“Checkpoints persist source offsets and keyed state. A TaskManager restart can recover from the JobManager's completed checkpoint. The checkpoint directory is a Docker named volume, not highly available storage. JobManager loss requires explicit recovery and retained Kafka offsets. Checkpoints alone do not make this whole pipeline exactly-once; the JDBC and audit sinks are at-least-once, with idempotent canonical effects.”

## 2:00–2:45 — Completion and late data

“A match_finished record can arrive first. I retain its manifest and keep the display provisional. The match becomes final only when the exact declared gameplay IDs/count/sequence bound and per-player kills/damage/placements agree. This avoids freezing an incomplete leaderboard.

“Within-policy late events update the canonical views immediately. Excessively late events go to an audit topic and retained raw facts but stay excluded from curated results until reconciliation. The batch operation includes them idempotently and exposes corrected status. There is no lossy silent drop. Separate sink tables/topics become consistent eventually rather than atomically.”

## 2:45–3:35 — Analytical grain and metrics

“The player fact grain is one namespace/run/match/player. Server windows are namespace/run/server/ten-second window. Current sessions use session IDs and source sequence revisions. Counting reconnect events would overstate concurrency; I count current connected sessions and distinct online players separately from alive players.

“I compute p95 from underlying synthetic latency observations and packet loss from summed lost/sent counts. Averaging p95 values or unweighted loss percentages would be wrong. Superset datasets remain separate to avoid joining player rows to many server windows. Live SQL recomputes small-demo aggregates over indexed canonical facts; the benchmark captures query plans so I can explain the tradeoff instead of inventing scale claims.”

## 3:35–4:15 — Operations and measured reliability

“Prometheus scrapes producer deliveries, Flink metrics and a collector reading actual broker offsets, database facts and checkpoint status. Lag is checkpoint-committed offset lag, which differs from fetched or processed lag. Persisted throughput, event-time age and wall-clock processing delay are separate metrics. Missing data is not zero. A completed quiet simulation is not a failed service.

“Airflow archives a bounded run to Parquet, records count/checksum, reconciles, runs dbt build/tests and exports a consistent timestamped snapshot. Replay preserves source identity/time but uses a new namespace. Historical dbt models explicitly re-read eligible demo matches so late corrections are not missed by a naive timestamp cutoff.”

## 4:15–5:00 — Dashboard proof and limitations

“Superset queries PostgreSQL every fifteen seconds while the dashboard is open. During a live 100-player run, a newly observed database kill threshold appeared automatically in a browser chart after 12.9 seconds. That is an observed polling delay, not a latency SLA. A latency-spike scenario also populated the incident display.

“The four dashboards cover player standings, match operations, server health and actual pipeline reliability. Native filters select one run and optional match/region/map; pipeline measurements have installation scope. The real dashboard export and browser screenshots are included. Tableau was replaced because no usable authoring session/private route was available.

“This remains a development portfolio system: one broker, no JobManager HA, no persistent producer state, and small-demo SQL recomputation. I can explain the measured query plan and bounded load rather than claim production scale.”
