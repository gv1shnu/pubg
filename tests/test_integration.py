"""Only runs against the real core stack. No Kafka/Flink/Postgres substitutes."""
import json
import os
import random
import time
import uuid
from datetime import datetime,timezone,timedelta
import pytest
import psycopg
from confluent_kafka import Producer
from tests.fixtures import fixture
from scripts.ops import archive,replay,reconcile
pytestmark=pytest.mark.skipif(os.getenv('RUN_INTEGRATION')!='1',reason='requires real cloud core stack')

def query(sql,args=()):
    with psycopg.connect() as con:return con.execute(sql,args).fetchall()
def wait(fn,timeout=90):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        value=fn()
        if value:return value
        time.sleep(.5)
    raise AssertionError('Integration readiness timeout')
def send(events,namespace):
    producer=Producer({'bootstrap.servers':'kafka:9092','enable.idempotence':True});errors=[]
    for topic,event in events:
        producer.produce(topic,key=(namespace+event.get('match_id','bad')).encode(),value=json.dumps(event),headers={'namespace':namespace},on_delivery=lambda err,msg:errors.append(err) if err else None)
        producer.poll(0)
    assert producer.flush(20)==0 and not errors

def rows(namespace,run):
    return query('SELECT player_id,kills,damage,placement,score,wins,status FROM mart.leaderboard WHERE namespace=%s AND run_id=%s ORDER BY player_id',(namespace,run))
def count(namespace,run):return query('SELECT count(*) FROM raw.events WHERE namespace=%s AND run_id=%s',(namespace,run))[0][0]

def test_finish_first_duplicates_sessions_and_archive_replay():
    namespace='it-'+uuid.uuid4().hex[:8];run=str(uuid.uuid4());events=fixture(run)
    finish=[e for e in events if e[1]['event_type']=='match_finished'];others=[e for e in events if e not in finish]
    send(finish,namespace);wait(lambda:count(namespace,run)==1)
    created=[e for e in others if e[1]['event_type']=='match_created'];send(created,namespace)
    wait(lambda:query("SELECT status FROM mart.match_overview WHERE namespace=%s AND run_id=%s",(namespace,run))==[('provisional',)])
    random.Random(42).shuffle(others);send(others,namespace)
    wait(lambda:len(rows(namespace,run))==4 and all(r[-1]=='final' for r in rows(namespace,run)))
    baseline=rows(namespace,run);expected=count(namespace,run)
    send(events*2,namespace);time.sleep(3)
    # Deliberately repeat the canonical sink statement, including beyond Flink's state guard.
    with psycopg.connect(user='stream_writer',password=os.environ['WRITER_PASSWORD']) as writer:
        for _ in range(2):
            writer.execute('INSERT INTO raw.events SELECT * FROM raw.events WHERE namespace=%s AND run_id=%s ON CONFLICT(namespace,run_id,event_id) DO NOTHING',(namespace,run))
    assert rows(namespace,run)==baseline and count(namespace,run)==expected==len(events)
    assert query('SELECT connected FROM curated.sessions WHERE namespace=%s AND run_id=%s',(namespace,run))==[(False,)]
    manifest=archive(namespace,run);replay_namespace='replay-'+uuid.uuid4().hex[:8]
    replay(manifest,replay_namespace);wait(lambda:count(replay_namespace,run)==expected)
    reconcile(replay_namespace,run)
    assert [r[:-1] for r in rows(replay_namespace,run)]==[r[:-1] for r in baseline]
    assert rows(namespace,run)==baseline

def test_malformed_and_unsupported_do_not_stop_valid_traffic():
    namespace='it-'+uuid.uuid4().hex[:8];run=str(uuid.uuid4());events=fixture(run)
    send([(events[0][0],dict(events[0][1],schema_version=999))],namespace)
    producer=Producer({'bootstrap.servers':'kafka:9092'})
    producer.produce('gameplay.v1',value='{malformed',headers={'namespace':namespace});assert producer.flush(10)==0
    send(events,namespace)
    wait(lambda:query('SELECT count(*) FROM ops.rejections WHERE namespace=%s',(namespace,))[0][0]==2)
    wait(lambda:len(rows(namespace,run))==4 and all(r[-1]=='final' for r in rows(namespace,run)))

def test_excessively_late_is_preserved_then_corrected():
    namespace='it-'+uuid.uuid4().hex[:8];run=str(uuid.uuid4());events=fixture(run)
    delayed=next(e for e in events if e[1]['event_type']=='damage_dealt');events.remove(delayed)
    delayed[1]['event_time']=(datetime.now(timezone.utc)-timedelta(minutes=3)).isoformat()
    send(events,namespace);wait(lambda:count(namespace,run)==len(events));time.sleep(2)
    send([delayed],namespace);wait(lambda:count(namespace,run)==len(events)+1)
    assert query('SELECT excessively_late,corrected_at IS NULL FROM raw.events WHERE namespace=%s AND run_id=%s AND event_id=%s',(namespace,run,delayed[1]['event_id']))==[(True,True)]
    assert all(r[-1]=='provisional' for r in rows(namespace,run))
    reconcile(namespace,run);assert all(r[-1]=='corrected' for r in rows(namespace,run))
    baseline=rows(namespace,run);reconcile(namespace,run);assert rows(namespace,run)==baseline

def test_within_policy_late_updates_canonical_results():
    namespace='it-'+uuid.uuid4().hex[:8];run=str(uuid.uuid4());events=fixture(run)
    delayed=next(e for e in events if e[1]['event_type']=='damage_dealt');events.remove(delayed)
    delayed[1]['event_time']=(datetime.now(timezone.utc)-timedelta(seconds=20)).isoformat()
    send(events,namespace);wait(lambda:count(namespace,run)==len(events));time.sleep(2);send([delayed],namespace)
    wait(lambda:len(rows(namespace,run))==4 and all(r[-1]=='final' for r in rows(namespace,run)))
    assert query('SELECT excessively_late FROM raw.events WHERE namespace=%s AND run_id=%s AND event_id=%s',(namespace,run,delayed[1]['event_id']))==[(False,)]
