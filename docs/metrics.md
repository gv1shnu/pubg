# Metric definitions

Every gameplay/server measurement is synthetic. Every pipeline observation comes from actual runtime components. All source, delivery, processing and batch timestamps are UTC instants.

| Metric | Definition / grain |
|---|---|
| Kills | Count of canonical player_eliminated facts attributed to an attacker, namespace/run/match/player |
| Damage | Sum of actual health reduction from canonical damage_dealt facts, same grain |
| Placement | Victim's unique elimination placement; winner placement from manifest, still provisional until complete |
| Wins | 1 for placement 1 only when match status final/corrected, otherwise 0 |
| Completed eligible matches | 1 per final/corrected player-match; count matches separately at match grain |
| Win rate | Sum wins / sum completed eligible player-matches; null for no eligible matches |
| Average placement | Mean placement over completed eligible player-matches only |
| Project score | kill_points × kills + placement points. Defaults: kill=1; places 1=10, 2=6, 3=5, 4–10=2, others=0; editable by database admin in ops.scoring/settings |
| Rank | Score DESC, wins DESC, kills DESC, player_id ASC. SQL rank is per match; select one match in Superset to interpret rank; a cross-match ranking requires a separately defined aggregation |
| Alive players | Minimum alive count observed in each ten-second match window (latest elimination monotonically lowers it), not connected sessions |
| Current connections | Count of latest session revisions whose connected=true |
| Current online players | Distinct player IDs among connected current sessions per server; reconnecting the same session never increments a permanent event count |
| Server latency p95 | percentile_cont(.95) over pooled synthetic latency observations within one server/ten-second window, in milliseconds. Not an average of sample p95s and not measured real PUBG request latency |
| Packet loss | 100 × summed lost packets / summed sent packets, at selected sample/window grain |
| CPU, memory | Synthetic sample percentages, averaged per window |
| Tick duration | Synthetic sample milliseconds, averaged per window; compare to configured target 16.667ms (approximately 60 ticks/s) |
| Incident | At least one sample in a window belongs to the configured synthetic degradation interval |
| Committed lag | Broker next end offset minus Flink checkpoint-committed next offset, per topic/partition, clamped >=0. Before first commit, retained low offset is baseline. Includes fetched/processed but not-yet-checkpointed records; not processing delay |
| Persisted events/sec | Prometheus one-minute rate of canonical row-count gauge, clamped >=0. Demo count is monotonic unless reset; chart measures all namespaces. May be absent during scrape warm-up |
| Duplicate events | Flink keyed deduplication counter, current operator lifetime; may reset on recovery. Does not count repeats suppressed solely by database PK after TTL expiry |
| Rejected events | Count of persisted ops.rejections, keyed by source topic/partition/offset; repeated DLQ topic deliveries can exceed this count |
| Pending late | Raw rows flagged excessively_late with corrected_at still null |
| Checkpoint completed/failed | Counts from current job's Flink REST checkpoint endpoint |
| Checkpoint duration | Last completed checkpoint end-to-end milliseconds, not operator service time |
| Checkpoint age | Current wall time minus last completed checkpoint ack timestamp |
| Watermark | Minimum exported Flink operator input watermark, epoch milliseconds; idle/no-data operators may emit sentinel values; inspect operator metrics when diagnosing |
| Emitted-to-processed delay | processed_at minus source emitted_at, p95 over recent canonical persisted rows; wall clock, including Kafka/Flink handling up to sink enqueue, excluding later database commit visibility |
| Event-time age | processed_at minus event_time, separately reported; delayed/replayed source events can be old without slow new delivery |
| Producer deliveries | Broker-acknowledged event deliveries including deliberate duplicates; not logical action count |
| dependency_up / scrape_up | Last direct dependency collection success and Prometheus scrape health; absence is unknown, not zero |

`ingested_at` is the Flink decoder's wall clock. `processed_at` is the validator/deduplicator's processing timestamp, before JDBC commit. Simulation elapsed seconds are accelerated domain time; neither latency uses that field. Replay keeps event_time, run_id and event_id, adds an isolated namespace, and replaces emitted_at for the new delivery.

`source_last_event_at` is max contributing source event time. Live `mart_updated_at` is PostgreSQL statement evaluation time, not a materialized refresh or browser render time. Historical `built_at` is dbt build time. `snapshot_generated_at` is a consistent export timestamp shared by all CSVs. Latest operational sample time is `sampled_at`; an unchanged exporter gauge can still be scraped, so always inspect dependency health. Superset refresh is configured at 15 seconds. One live proof observed a new database kill threshold in an automatic browser chart response after 12.9 seconds; that is a polling observation, not a guaranteed SLA.

Freshness indicators should not call a completed quiet run failed. During an active run, investigate metric collection age >20 seconds or checkpoint age >60 seconds. These are diagnostic defaults, not an SLA. The target 5–15s backend / 10–30s dashboard freshness has not been measured or achieved by evidence in this session.
