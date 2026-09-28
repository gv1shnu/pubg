# Reproducible Tableau dashboard specification — v0.1

**Authoring outstanding. This file is a build specification, not a Tableau workbook.** No unvalidated .twb/.twbx XML is supplied. All measurements use UTC. All gameplay and server telemetry are synthetic; the project has no official PUBG affiliation.

## Shared configuration

Dashboard fixed size 1366 × 768, tiled layout, 24 px outer padding, 16 px gaps. Background #11151B; panels #1C232C; main text #E6EAF0; secondary #A9B3C2; restrained amber #E8B45B; incident red #E36B67. Use Tableau's installed sans-serif font, 24 pt titles, 11–12 pt axes/table, 10 pt metadata. No proprietary logos, images or assets. Tab order: Live Leaderboard, Match Operations, Server Health, Pipeline Reliability.

Top 64 px: title at left, synthetic/snapshot badge and UTC freshness at right. Next 48 px: filters. Content below. Footer 28 px: source/processed/query freshness and metric explanation. Live mode footer says `Live SQL — refresh queries Tableau; updates are not pushed`. Public mode says `Snapshot — <snapshot_generated_at UTC>; not live`.

Use a **Run ID string parameter**, set to exactly one run from `mart.freshness`, and a namespace parameter default `live`. Each run-scoped source gets boolean `[run_id]=[Run ID] AND [namespace]=[Namespace]` filter true. Never default to all runs. Match ID defaults to one match on Match Operations; leaderboard permits multiple matches. Region, map, match and started_at range filters apply to relevant leaderboard/overview sheets only. Hide irrelevant filters on server/pipeline sheets.

Sources remain separate logical data sources. Do not physically join leaderboard rows to windows, sessions or metrics. If cross-source filtering is configured, explicitly map namespace/run/match keys. Server health uses its own server/region filters. Pipeline metrics are installation-wide and intentionally have no run join.

## Source grains

| Source / CSV | Grain | Primary fields |
|---|---|---|
| mart.leaderboard / leaderboard.csv | namespace, run, match, player | kills, damage, placement, score, wins, completed_eligible_matches, status, rank, region, map, started_at, processed_at |
| mart.match_overview / match_overview.csv | namespace, run, match | status, actual_gameplay_count, matched_ids, expected_gameplay_count, started_at, finished_at, processed_at |
| mart.match_timeline / match_timeline.csv | namespace, run, match, ten-second event-time window | alive_players, eliminations, window_start |
| mart.server_health / server_health.csv | namespace, run, server, ten-second event-time window | latency_p95_ms, packet_loss_pct, cpu_pct, memory_pct, tick_ms, target_tick_ms, incident, current_connections, current_online_players |
| mart.pipeline_health / pipeline_health.csv | sampled_at, metric, labels | value; labels contains topic/partition or scrape/dependency target when relevant |
| mart.freshness / freshness.csv | namespace, run | source_last_event_at, processed_at, pending_late, mart_updated_at |

## Common calculations

- `Eligible placement`: `IF [completed_eligible_matches]=1 THEN [placement] END`.
- `Win rate`: `IF SUM([completed_eligible_matches])>0 THEN SUM([wins])/SUM([completed_eligible_matches]) END`. Format percent, 1 decimal; missing denominator displays an em dash.
- `Average placement`: `AVG([Eligible placement])`, 1 decimal.
- `Result badge`: for each player, show `PROVISIONAL` if any contributing match is running/provisional; otherwise `CORRECTED` if any is corrected; otherwise `FINAL`. Implement counts using `SUM(IIF([status]='running' OR [status]='provisional',1,0))` and corrected count. Do not use alphabetical MAX(status).
- `Completeness`: `IF [expected_gameplay_count]>0 THEN [matched_ids]/[expected_gameplay_count] END`; percent, capped only for display, retain actual counts in tooltip.
- `Snapshot label`: ATTR(snapshot_generated_at), present only in CSV sources.
- `Data age`: `DATEDIFF('second',MAX([processed_at]),NOW())`. Title says “seconds since data processing”; quiet completed matches are not disconnected.
- `Tick budget ratio`: `AVG([tick_ms])/AVG([target_tick_ms])` at a single server/window mark. Above 1 indicates mean tick exceeded target.

## 1. Live Leaderboard

Panel layout: four KPI sheets across x=24,y=136,w=1318,h=88; table x=24,y=240,w=1318,h=444; footer y=704.

| Sheet | Fields and aggregation | Presentation |
|---|---|---|
| Eligible matches | COUNTD(match_id) after filtering status final/corrected | Large integer; title `Completed eligible matches` |
| Kills | SUM(kills) | Large integer; status mix in tooltip |
| Damage | SUM(damage) | Large integer; label health points |
| Win rate | calculation above | Percent; selected players/filters determine denominator |
| Player rankings | Rows player_id; text columns rank, SUM(kills), SUM(wins), SUM(score), SUM(damage), AVG(eligible placement), result badge | 12 pt, subtle horizontal dividers, amber rank/score |
| Data freshness | MAX(processed_at) and source timestamp from freshness source | UTC timestamp; source grain is one run |

For a single match use SQL `rank` (ATTR). Across selected matches recompute ranking: nest discrete SUM(score), SUM(wins), SUM(kills), player_id on Rows; sort first three descending and player_id ascending; hide the three sorting headers. `INDEX()` computes Table Down across all player rows, without partition resets, to display rank. Verify tie fixtures before publishing. The score is SUM of the configurable per-match project scores, not an official ranking formula. Tooltip includes match count, region, map, score definition and provisional meaning.

## 2. Match Operations

Top 88 px KPI strip: running matches COUNTD(IF status='running' THEN match_id END); completed matches COUNTD(IF status final/corrected THEN match_id END); completeness ATTR for selected match; result badge. Remaining content: alive trend x=24,y=240,w=646,h=220; eliminations x=686,y=240,w=656,h=220; placements table x=24,y=476,w=860,h=220; completeness details x=900,y=476,w=442,h=220.

| Sheet | Source and shelves | Aggregation/sort |
|---|---|---|
| Alive players | timeline; Columns continuous window_start exact date; Rows MIN(alive_players); match_id Detail | One selected match; line, integer y-axis 0–100; show gaps, do not interpolate unknown data |
| Eliminations | timeline; Columns window_start; Rows SUM(eliminations) | Ten-second bars; match filter identical to alive trend |
| Placements | leaderboard; Rows player_id; columns placement, kills, damage, badge | Sort placement ascending, nulls last; ATTR placement at player-match grain |
| Completeness detail | overview; text actual_gameplay_count, matched_ids, expected_gameplay_count, status | Show missing counts and UTC latest processed timestamp; final never inferred solely from match_finished |

## 3. Server Health

Filters: server_id, region, event-time window range, selected run/namespace. Top 88 px: current online players, current connections, latest window p95, latest window packet loss. Two 646×220 time plots below; CPU/memory and tick budget plot at bottom. Red incident shading behind each timeline: separate area mark for IIF(incident,1,NULL) on synchronized hidden 0–1 secondary axis, 15% opacity; label `Injected synthetic degradation`.

- Online KPI: at one selected server use MAX(current_online_players), MAX(current_connections). These are **current session state**, repeated on window rows; never SUM over time. For multiple servers, FIXED namespace/run/server MAX then use one mark per server, or show the per-server table. Do not sum an LOD repeated on windows.
- Latency: Columns exact window_start; Rows ATTR(latency_p95_ms), Detail server_id. Unit ms. Each mark is already a p95 over underlying synthetic observation values. Never AVG p95 across servers/time buckets. Retain ten-second grain or re-query the raw observations for a different grain.
- Packet loss: at each server/window plot SUM(packets_lost)/SUM(packets_sent)*100; unit %. For rollups sum counts before dividing.
- CPU/memory: window_start with AVG(cpu_pct) and AVG(memory_pct) at exact server/window grain, fixed 0–100% axes; each window sample count in tooltip.
- Tick: window_start, AVG(tick_ms) and AVG(target_tick_ms); ms, amber actual and muted target. Call out values exceeding the target.
- Window status in tooltip: provisional means source completeness is not proven; corrected means late events were included by reconciliation. Do not label elapsed wall time as proof of finality.

## 4. Pipeline Reliability

Source pipeline_health only. Metrics have installation scope, not match scope. Filter to last 15 minutes; retain exact sampled_at to avoid accidental sums across samples. Left 860 px: processed rate, lag and latency as three aligned time charts. Right 442 px: counters/checkpoint card and dependency status table.

| Sheet | Metric filter | Mark/aggregation and units |
|---|---|---|
| Canonical throughput | persisted_events_per_second | sampled_at line, MAX(value), events/sec; warm-up can be missing |
| Kafka committed lag | committed_lag | sampled_at line SUM(value) across partition labels at that same instant; integer records; tooltip identifies checkpoint semantics |
| Processing delay | delay_p95_seconds and event_age_p95_seconds | Two lines, metric Color, MAX(value) at instant; seconds; do not average p95 across time |
| Duplicates | duplicate_events | Latest sampled_at per metric, MAX(value); label “current operator lifetime”; resets on recovery |
| Rejections | rejected_events | Latest instant MAX(value); canonical rejected Kafka records |
| Late backlog | pending_late | Latest instant MAX(value); rows awaiting reconciliation |
| Checkpoint health | checkpoint_completed, checkpoint_failed, checkpoint_duration_ms, checkpoint_age_seconds | Separate latest-value cells; units in titles; absent values say `Unavailable`, never 0 |
| Dependency health | dependency_up, scrape_up | Latest sampled_at per metric+labels; green 1/red 0, labels shown; producer scrape down after normal completion is expected |

Current run source/processed freshness can be shown in a **separate** freshness sheet without joining metrics. For scrape freshness use age of latest sampled_at. A recent sample of an unchanged gauge is not proof the underlying component progressed; use dependency_up and checkpoint age together. Thresholds: collection stale >20 seconds during active simulation; checkpoint stale >60 seconds during active processing. Treat finished runs as quiet, not failed. Query/render freshness remains unknown until measured in Tableau.

## Validation checklist before calling dashboards complete

Select one known completed run; compare each player's score/kills/damage/placement to PostgreSQL. Confirm one winner and stable ties. Test a missing gameplay event keeps provisional, then verify correction after delivery/reconciliation. Confirm current sessions reach zero after run completion. Verify an injected latency interval with millisecond labels. Cross-check pipeline lag per partition and checkpoint age. Capture Tableau query/render evidence with timestamps. For Public, confirm every sheet shows its shared snapshot timestamp and no live claim. Save a workbook only through successful authoring in a compatible Tableau application.
