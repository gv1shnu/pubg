"""Read-only Kafka/PG/Flink exporter; persist selected actual Prometheus samples."""
import json
import time
from datetime import datetime, timezone
import requests
import psycopg
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge, start_http_server
from simulator.contracts import TOPICS

LAG=Gauge('pipeline_committed_lag','Kafka end offset minus checkpoint-committed consumer offset',['topic','partition'])
END=Gauge('pipeline_broker_end_offset','Broker log end offset',['topic','partition'])
GAUGES={name:Gauge('pipeline_'+name,description) for name,description in {
    'canonical_events':'Persisted canonical event rows across namespaces',
    'pending_late':'Pending late canonical rows',
    'rejected_events':'Persisted invalid Kafka records',
    'delay_p95_seconds':'p95 emitted-to-processed wall delay of recent canonical rows',
    'event_age_p95_seconds':'p95 event-time age at processing of recent canonical rows',
    'checkpoint_completed':'Completed checkpoints for the current Flink job',
    'checkpoint_failed':'Failed checkpoints for the current Flink job',
    'checkpoint_duration_ms':'Last completed checkpoint duration',
    'checkpoint_age_seconds':'Wall time since last completed checkpoint',
}.items()}
UP=Gauge('pipeline_dependency_up','Most recent collection succeeded',['dependency'])
QUERIES={
    'persisted_events_per_second':'clamp_min(rate(pipeline_canonical_events[1m]), 0)',
    'committed_lag':'pipeline_committed_lag',
    'broker_end_offset':'pipeline_broker_end_offset',
    'pending_late':'pipeline_pending_late',
    'rejected_events':'pipeline_rejected_events',
    'delay_p95_seconds':'pipeline_delay_p95_seconds',
    'event_age_p95_seconds':'pipeline_event_age_p95_seconds',
    'checkpoint_completed':'pipeline_checkpoint_completed',
    'checkpoint_failed':'pipeline_checkpoint_failed',
    'checkpoint_duration_ms':'pipeline_checkpoint_duration_ms',
    'checkpoint_age_seconds':'pipeline_checkpoint_age_seconds',
    'duplicate_events':'sum(flink_taskmanager_job_task_operator_duplicate_events)',
    'watermark_ms':'min(flink_taskmanager_job_task_operator_currentInputWatermark)',
    'producer_deliveries':'sum(producer_delivered_total)',
    'dependency_up':'pipeline_dependency_up',
    'scrape_up':'up',
}

def kafka(client):
    metadata=client.list_topics(timeout=5)
    partitions=[TopicPartition(t,p) for t in TOPICS.values() for p in metadata.topics[t].partitions]
    committed={(p.topic,p.partition):p.offset for p in client.committed(partitions,timeout=5)}
    for p in partitions:
        low,high=client.get_watermark_offsets(p,timeout=5)
        offset=committed[(p.topic,p.partition)]
        END.labels(p.topic,str(p.partition)).set(high)
        LAG.labels(p.topic,str(p.partition)).set(max(0,high-(low if offset<0 else offset)))

def database():
    with psycopg.connect() as con:
        row=con.execute("SELECT count(*),count(*) FILTER(WHERE excessively_late AND corrected_at IS NULL) FROM raw.events").fetchone()
        GAUGES['canonical_events'].set(row[0]); GAUGES['pending_late'].set(row[1])
        GAUGES['rejected_events'].set(con.execute('SELECT count(*) FROM ops.rejections').fetchone()[0])
        row=con.execute("SELECT percentile_cont(.95) WITHIN GROUP(ORDER BY extract(epoch FROM processed_at-(envelope->>'emitted_at')::timestamptz)),percentile_cont(.95) WITHIN GROUP(ORDER BY extract(epoch FROM processed_at-(envelope->>'event_time')::timestamptz)) FROM raw.events WHERE processed_at>now()-interval '1 minute'").fetchone()
        for name,value in zip(('delay_p95_seconds','event_age_p95_seconds'),row): GAUGES[name].set(float(value) if value is not None else float('nan'))

def flink():
    response=requests.get('http://jobmanager:8081/jobs/overview',timeout=5);response.raise_for_status()
    jobs=[j for j in response.json()['jobs'] if j['state']=='RUNNING']
    if len(jobs)!=1: raise RuntimeError('Expected exactly one RUNNING job')
    response=requests.get(f'http://jobmanager:8081/jobs/{jobs[0]["jid"]}/checkpoints',timeout=5);response.raise_for_status()
    data=response.json()
    for name in ('completed','failed'): GAUGES['checkpoint_'+name].set(data['counts'][name])
    latest=data.get('latest',{}).get('completed')
    GAUGES['checkpoint_duration_ms'].set(latest['end_to_end_duration'] if latest else float('nan'))
    GAUGES['checkpoint_age_seconds'].set(time.time()-latest['latest_ack_timestamp']/1000 if latest else float('nan'))

def snapshot():
    sampled=datetime.now(timezone.utc)
    with psycopg.connect() as con:
        for name,query in QUERIES.items():
            response=requests.get('http://prometheus:9090/api/v1/query',params={'query':query},timeout=5);response.raise_for_status()
            data=response.json()
            if data['status']!='success': raise RuntimeError('Prometheus query failed')
            for item in data['data']['result']:
                labels={k:v for k,v in item['metric'].items() if k!='__name__'}
                # Missing measurements stay absent; do not substitute zero.
                value=float(item['value'][1])
                if not __import__('math').isfinite(value): continue
                con.execute('INSERT INTO ops.metrics VALUES (%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING',(sampled,name,json.dumps(labels),value))
        con.execute("DELETE FROM ops.metrics WHERE sampled_at<now()-interval '6 hours'")

def main():
    start_http_server(8001)
    client=Consumer({'bootstrap.servers':'kafka:9092','group.id':'battleground-v1','enable.auto.commit':False})
    try:
        while True:
            for name,fn in [('kafka',lambda:kafka(client)),('postgres',database),('flink',flink),('prometheus',snapshot)]:
                try: fn(); UP.labels(name).set(1)
                except Exception as error:
                    UP.labels(name).set(0)
                    print(json.dumps({'dependency':name,'error_type':type(error).__name__}),flush=True)
            time.sleep(5)
    finally: client.close()

if __name__=='__main__': main()
