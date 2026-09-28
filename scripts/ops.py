"""Bounded batch operations. Never changes source event identity on replay."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Producer

os.umask(0o002)  # Share demo artifact files with the ops container's root group.

VIEWS=('leaderboard','match_overview','match_timeline','server_health','pipeline_health','freshness')
def connect(): return psycopg.connect(row_factory=dict_row)
def stamp(): return datetime.now(timezone.utc).isoformat()
def run_scope(con, namespace, run):
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',namespace): raise ValueError('invalid namespace')
    if run: return run
    row=con.execute('SELECT run_id FROM raw.events WHERE namespace=%s GROUP BY run_id ORDER BY max(processed_at) DESC LIMIT 1',(namespace,)).fetchone()
    if not row: raise RuntimeError('No accepted run; start simulation and inspect Flink/DLQ')
    return row['run_id']

def status(namespace='live',run=None):
    with connect() as con:
        run=run_scope(con,namespace,run)
        return {'namespace':namespace,'run_id':run,'matches':con.execute('SELECT * FROM mart.match_overview WHERE namespace=%s AND run_id=%s',(namespace,run)).fetchall(),
                'freshness':con.execute('SELECT * FROM mart.freshness WHERE namespace=%s AND run_id=%s',(namespace,run)).fetchall()}

def reconcile(namespace='live',run=None):
    with connect() as con:
        run=run_scope(con,namespace,run)
        # All presentation windows derive from canonical facts; only late inclusion needs mutation.
        changed=con.execute('UPDATE raw.events SET corrected_at=now() WHERE namespace=%s AND run_id=%s AND excessively_late AND corrected_at IS NULL',(namespace,run)).rowcount
        result=con.execute('SELECT match_id,status,matched_ids,expected_gameplay_count FROM mart.match_overview WHERE namespace=%s AND run_id=%s',(namespace,run)).fetchall()
        return {'namespace':namespace,'run_id':run,'late_rows_corrected':changed,'matches':result}

def archive(namespace='live',run=None,limit=100000):
    if not 1<=limit<=100000: raise ValueError('archive row limit outside bounds')
    with connect() as con:
        con.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        run=run_scope(con,namespace,run)
        rows=con.execute('SELECT * FROM raw.events WHERE namespace=%s AND run_id=%s ORDER BY topic,partition_id,kafka_offset LIMIT %s',(namespace,run,limit+1)).fetchall()
    if len(rows)>limit: raise RuntimeError('Slice exceeds archive bound; select a smaller run')
    if not rows: raise RuntimeError('No data to archive')
    token=hashlib.sha256((namespace+run).encode()).hexdigest()[:16]
    output=Path('/artifacts/archive')/f'date={datetime.now(timezone.utc):%Y-%m-%d}'/f'run={token}'/str(uuid.uuid4())
    output.mkdir(parents=True,exist_ok=False)
    for row in rows:
        row['envelope']=json.dumps(row['envelope'],sort_keys=True,separators=(',',':'))
    file=output/'events.parquet'
    pq.write_table(pa.Table.from_pylist(rows),file,compression='zstd')
    manifest={'format_version':1,'namespace':namespace,'run_id':run,'rows':len(rows),'file':'events.parquet','sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'archived_at':stamp(),'source':'canonical accepted envelopes, including pending late; rejected records remain in ops.rejections'}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return str(output/'manifest.json')

def replay(manifest_path,namespace):
    if not namespace.startswith('replay-') or not re.fullmatch(r'replay-[a-zA-Z0-9_-]{1,57}',namespace): raise ValueError('Use an isolated replay-* namespace')
    path=Path(manifest_path).resolve()
    if not path.is_relative_to(Path('/artifacts/archive')): raise ValueError('Archive must be inside /artifacts/archive')
    manifest=json.loads(path.read_text());file=path.parent/manifest['file']
    if file.resolve().parent != path.parent or hashlib.sha256(file.read_bytes()).hexdigest()!=manifest['sha256']: raise ValueError('Archive checksum/path mismatch')
    rows=pq.ParquetFile(file).read().to_pylist()
    if len(rows)!=manifest['rows']: raise ValueError('Archive count mismatch')
    with connect() as con:
        if con.execute('SELECT 1 FROM raw.events WHERE namespace=%s LIMIT 1',(namespace,)).fetchone(): raise ValueError('Replay namespace already contains events; choose a fresh namespace')
    producer=Producer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),'enable.idempotence':True})
    errors=[]
    def delivered(error,msg):
        if error: errors.append(str(error))
    for row in rows:
        event=json.loads(row['envelope']);event['emitted_at']=stamp()
        entity=event['match_id'] if row['topic'] in ('match.v1','gameplay.v1') else event['server_id']
        producer.produce(row['topic'],key=f'{namespace}:{event["run_id"]}:{entity}',value=json.dumps(event),headers={'namespace':namespace},on_delivery=delivered)
        producer.poll(0)
    if producer.flush(30) or errors: raise RuntimeError('Replay delivery failed')
    return {'namespace':namespace,'run_id':manifest['run_id'],'delivered':len(rows),'next':'wait for canonical rows, then reconcile and compare normalized results'}

def export(namespace='live',run=None):
    generated=stamp()
    with connect() as con:
        con.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        run=run_scope(con,namespace,run)
        output=Path('/exports')/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ');output.mkdir(parents=True)
        counts={}
        for view in VIEWS:
            where=sql.SQL('sampled_at > now()-interval \'15 minutes\'') if view=='pipeline_health' else sql.SQL('namespace=%s AND run_id=%s')
            query=sql.SQL('SELECT *,%s::timestamptz snapshot_generated_at FROM mart.{} WHERE {} LIMIT 100001').format(sql.Identifier(view),where)
            rows=con.execute(query,(generated,) if view=='pipeline_health' else (generated,namespace,run))
            columns=[c.name for c in rows.description]; data=rows.fetchall()
            if len(data)>100000: raise RuntimeError('Export exceeds 100000 rows per source')
            with (output/f'{view}.csv').open('w',newline='') as file:
                writer=csv.DictWriter(file,fieldnames=columns);writer.writeheader();writer.writerows(data)
            counts[view]=len(data)
        (output/'manifest.json').write_text(json.dumps({'mode':'snapshot, not live','simulated_gameplay':True,'namespace':namespace,'run_id':run,'snapshot_generated_at':generated,'row_counts':counts,'pipeline_scope':'installation-wide actual metrics, not a run-level join'},indent=2)+'\n')
    return str(output)

def smoke(namespace='live',run=None,timeout=120):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        with connect() as con:
            run=run_scope(con,namespace,run)
            topics={r['topic'] for r in con.execute('SELECT DISTINCT topic FROM raw.events WHERE namespace=%s AND run_id=%s',(namespace,run))}
            matches=con.execute('SELECT status FROM mart.match_overview WHERE namespace=%s AND run_id=%s',(namespace,run)).fetchall()
            if topics=={'match.v1','gameplay.v1','session.v1','server.v1'} and matches and all(m['status'] in ('final','corrected') for m in matches):
                bad=con.execute('SELECT match_id FROM curated.player_match WHERE namespace=%s AND run_id=%s GROUP BY match_id HAVING count(*)<>count(DISTINCT placement) OR count(*) FILTER(WHERE placement=1)<>1 OR sum(kills)<>count(*)-1 OR sum(damage)<>(count(*)-1)*100',(namespace,run)).fetchall()
                online=con.execute('SELECT count(*) n FROM curated.sessions WHERE namespace=%s AND run_id=%s AND connected',(namespace,run)).fetchone()['n']
                if bad: raise AssertionError('Outcome invariants failed')
                if online==0: return {'passed':True,'run_id':run,'sources':sorted(topics),'matches':len(matches)}
        time.sleep(2)
    raise TimeoutError('Smoke did not reach reconciled complete results and closed sessions')

def batch_step(batch_id,action):
    with connect() as con:
        if action=='start':
            run=run_scope(con,'live',None)
            con.execute("INSERT INTO ops.batch_runs(batch_id,status,details) VALUES (%s,'running',%s::jsonb) ON CONFLICT DO NOTHING",(batch_id,json.dumps({'run_id':run})))
            return run
        row=con.execute('SELECT details FROM ops.batch_runs WHERE batch_id=%s',(batch_id,)).fetchone()
        if not row: raise RuntimeError('Batch prerequisites were not checked')
        run=row['details']['run_id']
    if action=='archive': result=archive(run=run)
    elif action=='reconcile': result=reconcile(run=run)
    elif action=='dbt':
        process=subprocess.run([os.getenv('DBT_EXECUTABLE','dbt'),'build','--project-dir','/app/dbt','--profiles-dir','/app/dbt'],text=True,capture_output=True)
        Path('/artifacts').mkdir(exist_ok=True)
        Path('/artifacts/dbt-last.log').write_text(process.stdout+process.stderr)
        result={'returncode':process.returncode}
        with connect() as con:
            con.execute('UPDATE ops.batch_runs SET details=details||%s::jsonb WHERE batch_id=%s',(json.dumps({'dbt':result}),batch_id))
        if process.returncode: raise RuntimeError('dbt failed; see /artifacts/dbt-last.log')
    elif action=='export': result=export(run=run) if os.getenv('EXPORT_SNAPSHOT','true')=='true' else 'disabled'
    elif action=='finish':
        with connect() as con: con.execute("UPDATE ops.batch_runs SET status='success',finished_at=now() WHERE batch_id=%s",(batch_id,))
        return 'success'
    else: raise ValueError(action)
    with connect() as con:
        con.execute('UPDATE ops.batch_runs SET details=details||%s::jsonb WHERE batch_id=%s',(json.dumps({action:result},default=str),batch_id))
    return result

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['status','reconcile','archive','replay','export','smoke','batch'])
    parser.add_argument('--namespace',default='live');parser.add_argument('--run');parser.add_argument('--manifest');parser.add_argument('--batch-id');parser.add_argument('--step');args=parser.parse_args()
    if args.action=='replay': result=replay(args.manifest,args.namespace)
    elif args.action=='batch': result=batch_step(args.batch_id,args.step)
    else: result=globals()[args.action](args.namespace,args.run)
    print(json.dumps(result,default=str,indent=2))
if __name__=='__main__': main()
