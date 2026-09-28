"""Bounded Docker benchmark; stores actual observations without extrapolation."""
import json
import os
from pathlib import Path
import subprocess
import time
from scripts.cloud import cloud,dc,ROOT
from scripts.fault_acceptance import metric

def main():
    cloud();run=__import__('uuid').uuid4().hex
    started=time.time();samples=[]
    proc=subprocess.Popen(['docker','compose','run','--rm','--no-deps','-e',f'RUN_ID={run}','-e','DURATION_SECONDS=60','-e','SCENARIO=latency-spike','simulator'],cwd=ROOT)
    try:
        while proc.poll() is None and time.time()-started<150:
            stats=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}'],capture_output=True,text=True,check=True)
            samples.append({'at':time.time(),'rate':metric('clamp_min(rate(pipeline_canonical_events[1m]),0)'),
                'lag':metric('sum(pipeline_committed_lag)'),'delay_p95_seconds':metric('pipeline_delay_p95_seconds'),
                'containers':[json.loads(line) for line in stats.stdout.splitlines() if 'battleground-telemetry' in line]})
            time.sleep(5)
        if proc.poll() is None:raise TimeoutError('Bounded benchmark exceeded 150 seconds')
        if proc.returncode:raise RuntimeError('Simulation failed')
        dc('run','--rm','--no-deps','toolbox','python','-m','scripts.ops','smoke','--run',run)
        plan=dc('exec','-T','postgres','psql','-U','telemetry_admin','-d','telemetry','-At','-c',f"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM mart.leaderboard WHERE namespace='live' AND run_id='{run}' ORDER BY rank LIMIT 100",capture_output=True,text=True)
        (ROOT/'artifacts/query-plan.txt').write_text(plan.stdout)
    finally:
        if proc.poll() is None:proc.terminate()
        evidence={'run_id':run,'duration_seconds':time.time()-started,'samples':samples,'sample_count':len(samples),'hardware':json.loads((ROOT/'artifacts/environment.json').read_text())}
        (ROOT/'artifacts/benchmark.json').write_text(json.dumps(evidence,indent=2)+'\n')
if __name__=='__main__':main()
