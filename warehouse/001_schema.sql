CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS raw.events (
 namespace text NOT NULL, run_id text NOT NULL, event_id text NOT NULL,
 envelope jsonb NOT NULL, topic text NOT NULL, partition_id int NOT NULL, kafka_offset bigint NOT NULL,
 ingested_at timestamptz NOT NULL, processed_at timestamptz NOT NULL,
 excessively_late boolean NOT NULL DEFAULT false, corrected_at timestamptz,
 PRIMARY KEY(namespace,run_id,event_id),
 CHECK (envelope->>'simulated'='true'), CHECK ((envelope->>'schema_version')::int=1)
);
CREATE INDEX IF NOT EXISTS events_match ON raw.events(namespace,run_id,(envelope->>'match_id'),(envelope->>'event_type'));
CREATE INDEX IF NOT EXISTS events_time ON raw.events(processed_at);
CREATE TABLE IF NOT EXISTS ops.rejections (
 namespace text NOT NULL, topic text NOT NULL, partition_id int NOT NULL, kafka_offset bigint NOT NULL,
 reason text NOT NULL, body text NOT NULL, processed_at timestamptz NOT NULL,
 PRIMARY KEY(namespace,topic,partition_id,kafka_offset)
);
CREATE TABLE IF NOT EXISTS ops.metrics (
 sampled_at timestamptz NOT NULL, metric text NOT NULL, labels jsonb NOT NULL DEFAULT '{}', value double precision NOT NULL,
 PRIMARY KEY(sampled_at,metric,labels)
);
CREATE INDEX IF NOT EXISTS metrics_recent ON ops.metrics(sampled_at DESC);
CREATE TABLE IF NOT EXISTS ops.batch_runs (
 batch_id text PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 status text NOT NULL, details jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS ops.window_observations (
 namespace text,run_id text,server_id text,window_end timestamptz,sample_count bigint,
 PRIMARY KEY(namespace,run_id,server_id,window_end)
);
CREATE TABLE IF NOT EXISTS ops.scoring (placement int PRIMARY KEY, points int NOT NULL);
INSERT INTO ops.scoring SELECT n,CASE WHEN n=1 THEN 10 WHEN n=2 THEN 6 WHEN n=3 THEN 5 WHEN n<=10 THEN 2 ELSE 0 END FROM generate_series(1,100) n ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS ops.settings (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), kill_points int NOT NULL DEFAULT 1);
INSERT INTO ops.settings DEFAULT VALUES ON CONFLICT DO NOTHING;
CREATE OR REPLACE VIEW curated.events AS
SELECT namespace,run_id,event_id,envelope->>'event_type' event_type,
 envelope->>'match_id' match_id,envelope->>'server_id' server_id,envelope->>'player_id' player_id,
 (envelope->>'sequence')::bigint sequence,(envelope->>'event_time')::timestamptz event_time,
 (envelope->>'emitted_at')::timestamptz emitted_at,envelope->'payload' payload,
 topic,partition_id,kafka_offset,ingested_at,processed_at,corrected_at
FROM raw.events WHERE NOT excessively_late OR corrected_at IS NOT NULL;
CREATE OR REPLACE VIEW curated.match_definitions AS
SELECT DISTINCT ON(namespace,run_id,match_id) namespace,run_id,match_id,server_id,
 payload->>'region' region,payload->>'map' map,payload->'players' players,event_time started_at
FROM curated.events WHERE event_type='match_created' ORDER BY namespace,run_id,match_id,sequence DESC;
CREATE OR REPLACE VIEW curated.manifests AS
SELECT DISTINCT ON(namespace,run_id,match_id) namespace,run_id,match_id,payload, event_time finished_at
FROM curated.events WHERE event_type='match_finished' ORDER BY namespace,run_id,match_id,sequence DESC;
CREATE OR REPLACE VIEW curated.dim_players AS
SELECT DISTINCT d.namespace,d.run_id,d.match_id,p.player_id
FROM curated.match_definitions d CROSS JOIN LATERAL jsonb_array_elements_text(d.players) p(player_id);
CREATE OR REPLACE VIEW curated.dim_servers AS
SELECT DISTINCT namespace,run_id,server_id,region FROM curated.match_definitions;
CREATE OR REPLACE VIEW curated.player_match AS
WITH actions AS (
 SELECT namespace,run_id,match_id,player_id,
 count(*) FILTER(WHERE event_type='player_eliminated')::int kills,
 coalesce(sum((payload->>'damage')::int) FILTER(WHERE event_type='damage_dealt'),0)::int damage,
 max(event_time) source_last_event_at,max(processed_at) processed_at
 FROM curated.events WHERE event_type IN ('damage_dealt','player_eliminated','player_position_sample')
 GROUP BY 1,2,3,4
), places AS (
 SELECT namespace,run_id,match_id,payload->>'victim_id' player_id,min((payload->>'placement')::int) placement
 FROM curated.events WHERE event_type='player_eliminated' GROUP BY 1,2,3,4
), outcomes AS (
 SELECT m.namespace,m.run_id,m.match_id,o->>'player_id' player_id,(o->>'placement')::int placement,
 (o->>'kills')::int kills,(o->>'damage')::int damage
 FROM curated.manifests m CROSS JOIN LATERAL jsonb_array_elements(m.payload->'outcomes') o
)
SELECT p.*,d.server_id,d.region,d.map,d.started_at,coalesce(a.kills,0) kills,coalesce(a.damage,0) damage,
 coalesce(pl.placement,CASE WHEN o.placement=1 THEN 1 END) placement,
 o.kills expected_kills,o.damage expected_damage,o.placement expected_placement,
 a.source_last_event_at,a.processed_at
FROM curated.dim_players p JOIN curated.match_definitions d USING(namespace,run_id,match_id)
LEFT JOIN actions a USING(namespace,run_id,match_id,player_id)
LEFT JOIN places pl USING(namespace,run_id,match_id,player_id)
LEFT JOIN outcomes o USING(namespace,run_id,match_id,player_id);
CREATE OR REPLACE VIEW curated.match_status AS
WITH checks AS (
 SELECT d.*,m.finished_at,m.payload manifest,
 (SELECT count(*) FROM curated.events e WHERE e.namespace=d.namespace AND e.run_id=d.run_id AND e.match_id=d.match_id AND e.topic='gameplay.v1') actual_gameplay_count,
 (SELECT count(*) FROM jsonb_array_elements_text(m.payload->'expected_event_ids') ids(event_id)
    WHERE EXISTS(SELECT 1 FROM curated.events e WHERE e.namespace=d.namespace AND e.run_id=d.run_id AND e.match_id=d.match_id AND e.event_id=ids.event_id AND e.topic='gameplay.v1')) matched_ids,
 (SELECT count(*) FROM curated.player_match p WHERE p.namespace=d.namespace AND p.run_id=d.run_id AND p.match_id=d.match_id
    AND p.kills=p.expected_kills AND p.damage=p.expected_damage AND p.placement=p.expected_placement) matched_players,
 (SELECT max(sequence) FROM curated.events e WHERE e.namespace=d.namespace AND e.run_id=d.run_id AND e.match_id=d.match_id AND e.topic='gameplay.v1') sequence_max,
 (SELECT bool_or(corrected_at IS NOT NULL) FROM curated.events e WHERE e.namespace=d.namespace AND e.run_id=d.run_id AND e.match_id=d.match_id) corrected,
 (SELECT max(processed_at) FROM curated.events e WHERE e.namespace=d.namespace AND e.run_id=d.run_id AND e.match_id=d.match_id) processed_at
 FROM curated.match_definitions d LEFT JOIN curated.manifests m USING(namespace,run_id,match_id)
)
SELECT *, CASE WHEN manifest IS NULL THEN 'running'
 WHEN actual_gameplay_count=(manifest->>'expected_gameplay_count')::int
 AND matched_ids=actual_gameplay_count AND sequence_max=(manifest->>'gameplay_sequence_max')::int
 AND matched_players=jsonb_array_length(players)
 THEN CASE WHEN corrected THEN 'corrected' ELSE 'final' END ELSE 'provisional' END status
FROM checks;
CREATE OR REPLACE VIEW curated.sessions AS
SELECT DISTINCT ON(namespace,run_id,server_id,player_id,payload->>'session_id')
 namespace,run_id,server_id,player_id,payload->>'session_id' session_id,sequence,
 (payload->>'connected')::boolean connected,event_time source_last_event_at,processed_at
FROM curated.events WHERE event_type IN ('player_connected','player_disconnected','player_reconnected')
ORDER BY namespace,run_id,server_id,player_id,payload->>'session_id',sequence DESC;
CREATE OR REPLACE VIEW curated.server_windows AS
WITH samples AS (
 SELECT *,date_bin('10 seconds',event_time,'2000-01-01'::timestamptz) window_start
 FROM curated.events WHERE event_type='server_sample'
), latency AS (
 SELECT namespace,run_id,server_id,window_start,percentile_cont(.95) WITHIN GROUP(ORDER BY l.value::double precision) latency_p95_ms
 FROM samples CROSS JOIN LATERAL jsonb_array_elements_text(payload->'latency_ms') l(value) GROUP BY 1,2,3,4
), stats AS (
 SELECT namespace,run_id,server_id,window_start,count(*) sample_count,
 avg((payload->>'cpu_pct')::float) cpu_pct,avg((payload->>'memory_pct')::float) memory_pct,
 avg((payload->>'tick_ms')::float) tick_ms,avg((payload->>'target_tick_ms')::float) target_tick_ms,
 sum((payload->>'packets_lost')::bigint) packets_lost,sum((payload->>'packets_sent')::bigint) packets_sent,
 bool_or((payload->>'incident')::boolean) incident,max(event_time) source_last_event_at,max(processed_at) processed_at,
 bool_or(corrected_at IS NOT NULL) corrected
 FROM samples GROUP BY 1,2,3,4
)
SELECT s.*,l.latency_p95_ms,100.0*packets_lost/nullif(packets_sent,0) packet_loss_pct,
 CASE WHEN corrected THEN 'corrected' ELSE 'provisional' END status
FROM stats s JOIN latency l USING(namespace,run_id,server_id,window_start);
CREATE OR REPLACE VIEW mart.leaderboard AS
SELECT p.*,s.status,CASE WHEN s.status IN ('final','corrected') AND p.placement=1 THEN 1 ELSE 0 END wins,
 CASE WHEN s.status IN ('final','corrected') THEN 1 ELSE 0 END completed_eligible_matches,
 p.kills*(SELECT kill_points FROM ops.settings)+coalesce(sc.points,0) score,
 row_number() OVER(PARTITION BY p.namespace,p.run_id,p.match_id ORDER BY
 p.kills*(SELECT kill_points FROM ops.settings)+coalesce(sc.points,0) DESC,
 CASE WHEN s.status IN ('final','corrected') AND p.placement=1 THEN 1 ELSE 0 END DESC,p.kills DESC,p.player_id) rank,
 statement_timestamp() mart_updated_at
FROM curated.player_match p JOIN curated.match_status s USING(namespace,run_id,match_id)
LEFT JOIN ops.scoring sc ON sc.placement=p.placement;
CREATE OR REPLACE VIEW mart.match_overview AS
SELECT namespace,run_id,match_id,region,map,server_id,started_at,finished_at,status,
 actual_gameplay_count,matched_ids,(manifest->>'expected_gameplay_count')::int expected_gameplay_count,
 processed_at,statement_timestamp() mart_updated_at FROM curated.match_status;
CREATE OR REPLACE VIEW mart.match_timeline AS
SELECT namespace,run_id,match_id,date_bin('10 seconds',event_time,'2000-01-01'::timestamptz) window_start,
 min((payload->>'alive')::int) alive_players,count(*) FILTER(WHERE event_type='player_eliminated') eliminations,
 max(processed_at) processed_at
FROM curated.events WHERE event_type IN ('player_position_sample','player_eliminated','match_started') GROUP BY 1,2,3,4;
CREATE OR REPLACE VIEW mart.server_health AS
SELECT w.*,d.region,coalesce(s.connections,0) current_connections,coalesce(s.online_players,0) current_online_players,
 statement_timestamp() mart_updated_at
FROM curated.server_windows w JOIN curated.dim_servers d USING(namespace,run_id,server_id)
LEFT JOIN (SELECT namespace,run_id,server_id,count(*) FILTER(WHERE connected) connections,
 count(DISTINCT player_id) FILTER(WHERE connected) online_players FROM curated.sessions GROUP BY 1,2,3) s USING(namespace,run_id,server_id);
CREATE OR REPLACE VIEW mart.pipeline_health AS
SELECT *,statement_timestamp() mart_updated_at FROM ops.metrics WHERE sampled_at > now()-interval '1 hour';
CREATE OR REPLACE VIEW mart.freshness AS
SELECT namespace,run_id,max((envelope->>'event_time')::timestamptz) source_last_event_at,
 max(processed_at) processed_at,count(*) FILTER(WHERE excessively_late AND corrected_at IS NULL) pending_late,
 statement_timestamp() mart_updated_at FROM raw.events GROUP BY 1,2;
