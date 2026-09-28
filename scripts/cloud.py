"""Developer CLI for authorized local Docker or Linux workspaces."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
def call(*args,**kwargs): return subprocess.run(args,cwd=ROOT,check=True,**kwargs)
def dc(*args,**kwargs): return call('docker','compose',*args,**kwargs)
def cloud():
    # Local Docker testing is explicitly supported; cloud use still requires available quota.
    if sys.platform not in ('linux','darwin'):
        raise SystemExit('Use Docker on Linux or macOS for this development release.')
def configure():
    path=ROOT/'.env'
    body=path.read_text() if path.exists() else (ROOT/'.env.example').read_text()
    for name in ('POSTGRES_PASSWORD','WRITER_PASSWORD','TRANSFORM_PASSWORD','TABLEAU_PASSWORD','AIRFLOW_DB_PASSWORD','SUPERSET_DB_PASSWORD','SUPERSET_SECRET_KEY','SUPERSET_ADMIN_PASSWORD'):
        if not any(line.startswith(name+'=') for line in body.splitlines()): body+=name+'=\n'
        body=body.replace(name+'=\n',name+'='+secrets.token_hex(24)+'\n')
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as file: file.write(body)
    for p in ('artifacts','tableau/exports'): (ROOT/p).mkdir(parents=True,exist_ok=True)
def wait(fn,timeout=120):
    end=time.monotonic()+timeout;last=None
    while time.monotonic()<end:
        try:
            result=fn()
            if result: return result
        except Exception as error: last=type(error).__name__
        time.sleep(2)
    raise TimeoutError(f'Readiness timeout ({last}); inspect docker compose logs')
def api(path):
    with urllib.request.urlopen('http://127.0.0.1:8081'+path,timeout=5) as response:return json.load(response)
def running():
    jobs=[j for j in api('/jobs/overview')['jobs'] if j['state']=='RUNNING']
    if len(jobs)>1: raise RuntimeError('Multiple streaming jobs: cancel the extra job before proceeding')
    return jobs[0] if jobs else None

def bootstrap():
    cloud();configure()
    call('docker','info',stdout=subprocess.DEVNULL)
    info=json.loads(subprocess.check_output(['docker','info','--format','{{json .}}'],text=True))
    memory=info['MemTotal']
    if memory<6*1024**3: raise SystemExit('Less than 6 GiB RAM detected; core runtime/build budget not established. Use sufficient existing quota or stop.')
    (ROOT/'artifacts/environment.json').write_text(json.dumps({'cpu_count':info['NCPU'],'ram_bytes':memory,'platform':sys.platform,'recorded_at':time.time()},indent=2))
    dc('config','--quiet')
    dc('build','toolbox')
    dc('run','--rm','--no-deps','toolbox','sh','-c','mkdir -p /artifacts /exports && chmod -R g+rwX /artifacts /exports && chmod 1777 /artifacts /exports')
    dc('build','jobmanager','postgres','prometheus')
    dc('run','--rm','--no-deps','--user','root','--entrypoint','/bin/sh','kafka','-c','chown -R 1000:1000 /var/lib/kafka/data')
    dc('up','-d','--wait','--wait-timeout','180','postgres','kafka')
    dc('exec','-T','postgres','sh','-c','psql -v ON_ERROR_STOP=1 -U telemetry_admin -d telemetry -f /warehouse/001_schema.sql -f /warehouse/002_roles.sql')
    for topic in ('match.v1','gameplay.v1','session.v1','server.v1','dead-letter.v1','excessively-late.v1'):
        dc('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:9092','--create','--if-not-exists','--topic',topic,'--partitions','2','--replication-factor','1','--config','retention.ms=21600000')
    dc('run','--rm','--no-deps','--user','root','--entrypoint','/bin/sh','jobmanager','-c','chown -R flink:flink /checkpoints')
    up()
    # Assert actual consumption with a tiny real four-source match, not just a running job.
    dc('run','--rm','--no-deps','toolbox','python','-c',"from pathlib import Path; Path('/control/settings.json').write_text('{}')")
    dc('run','--rm','--no-deps','-e','PLAYERS=4','-e','DURATION_SECONDS=10','-e','EVENT_RATE=10','-e','CONCURRENT_MATCHES=1','-e','SCENARIO=normal','simulator',timeout=120)
    dc('run','--rm','--no-deps','toolbox','python','-m','scripts.ops','smoke')

def up():
    dc('up','-d','jobmanager','taskmanager','prometheus','collector')
    wait(lambda:api('/overview').get('taskmanagers',0)>0)
    if not running(): dc('exec','-T','jobmanager','flink','run','-d','/opt/flink/usrlib/telemetry.jar')
    wait(running)

def checkpoint():
    job=running()
    if not job: raise RuntimeError('No running Flink job')
    return api(f'/jobs/{job["jid"]}/checkpoints').get('latest',{}).get('completed')

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action');parser.add_argument('extra',nargs='*');args=parser.parse_args()
    if args.action=='configure': configure();return
    cloud()
    if args.action=='bootstrap': bootstrap()
    elif args.action=='up':
        dc('up','-d','--wait','--wait-timeout','180','postgres','kafka');up()
    elif args.action=='demo':
        dc('run','--rm','--no-deps','toolbox','python','-c',"from pathlib import Path; Path('/control/settings.json').write_text('{}')")
        dc('up','-d','--force-recreate','simulator')
    elif args.action in ('pause','resume','scenario'):
        scenario=args.extra[0] if args.extra else 'normal'
        if scenario not in ('normal','latency-spike','disconnect-burst','duplicate','out-of-order','excessively-late','finish-first','invalid'): raise ValueError('Unknown scenario')
        value={'scenario':scenario} if args.action=='scenario' else {'paused':args.action=='pause'}
        code="import json; from pathlib import Path; p=Path('/control/settings.json'); d=json.loads(p.read_text()) if p.exists() else {}; d.update("+repr(value)+"); t=p.with_suffix('.tmp'); t.write_text(json.dumps(d)); t.replace(p)"
        dc('run','--rm','--no-deps','toolbox','python','-c',code)
    elif args.action=='restart-processing':
        saved=wait(checkpoint);dc('restart','taskmanager');wait(running)
        wait(lambda:checkpoint() and checkpoint()['id']>saved['id'],180)
    elif args.action=='restore-checkpoint':
        if running(): raise SystemExit('Cancel current job first using Flink UI; avoid concurrent writers')
        if len(args.extra)!=1 or not args.extra[0].startswith('file:///checkpoints/'): raise ValueError('Pass a retained checkpoint metadata URI inside /checkpoints')
        dc('exec','-T','jobmanager','flink','run','-d','-s',args.extra[0],'/opt/flink/usrlib/telemetry.jar');wait(running)
    elif args.action=='export-artifacts':
        import uuid
        name='battleground-artifact-copy-'+uuid.uuid4().hex[:8]
        dc('run','-d','--no-deps','--name',name,'toolbox','sleep','60',stdout=subprocess.DEVNULL)
        try:
            call('docker','cp',name+':/artifacts/.',str(ROOT/'artifacts'))
            call('docker','cp',name+':/exports/.',str(ROOT/'tableau/exports'))
        finally: call('docker','rm','-f',name,stdout=subprocess.DEVNULL)
    elif args.action=='bi-up': dc('up','-d','--build','superset')
    elif args.action=='ops-up': dc('up','-d','--build','airflow')
    elif args.action=='ops-down': dc('stop','airflow')
    elif args.action=='test': dc('run','--rm','--no-deps','toolbox','pytest','-q','tests')
    elif args.action=='integration': dc('run','--rm','--no-deps','-e','RUN_INTEGRATION=1','toolbox','pytest','-q','tests/test_integration.py')
    elif args.action=='dbt': dc('run','--rm','--no-deps','toolbox','dbt','build','--project-dir','/app/dbt')
    elif args.action=='drain':
        wait(lambda:api('/jobs/overview'),30)
        dc('run','--rm','--no-deps','toolbox','python','-m','scripts.drain')
    elif args.action=='down': dc('--profile','core','--profile','ops','--profile','bi','down')
    elif args.action=='reset-demo':
        if os.getenv('CONFIRM_RESET')!='DELETE_PROJECT_DEMO_DATA': raise SystemExit('Destructive project-volume reset requires CONFIRM_RESET=DELETE_PROJECT_DEMO_DATA')
        dc('--profile','core','--profile','ops','--profile','bi','down','--volumes','--remove-orphans')
    elif args.action in ('status','smoke','reconcile','archive','export'):
        dc('run','--rm','--no-deps','toolbox','python','-m','scripts.ops',args.action,*args.extra)
    else: raise ValueError('Unknown action')
if __name__=='__main__': main()
