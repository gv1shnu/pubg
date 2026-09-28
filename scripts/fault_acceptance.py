"""Cloud-side recovery and lag test, operating only on this Compose project."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request
from scripts.cloud import cloud,dc,wait,running,checkpoint,ROOT

def metric(query):
    url='http://127.0.0.1:9090/api/v1/query?'+urllib.parse.urlencode({'query':query})
    with urllib.request.urlopen(url,timeout=5) as response:data=json.load(response)['data']['result']
    return float(data[0]['value'][1]) if data else None

def main():
    cloud();saved=wait(checkpoint)
    run=__import__('uuid').uuid4().hex
    simulation=subprocess.Popen(['docker','compose','run','--rm','--no-deps','-e',f'RUN_ID={run}','-e','PLAYERS=20','-e','DURATION_SECONDS=60','-e','SCENARIO=duplicate','simulator'],cwd=ROOT)
    evidence={'run_id':run,'checkpoint_before':saved['id']}
    try:
        time.sleep(10)
        before=metric('sum(pipeline_committed_lag)')
        dc('stop','taskmanager')
        time.sleep(20)
        during=metric('sum(pipeline_committed_lag)')
        if before is None or during is None or during<=before:raise AssertionError('Checkpoint-committed lag did not grow')
        evidence.update(lag_before=before,lag_paused=during)
        dc('start','taskmanager');wait(running);wait(lambda:checkpoint() and checkpoint()['id']>saved['id'],180)
        if simulation.wait(timeout=180)!=0:raise AssertionError('Simulation failed')
        dc('run','--rm','--no-deps','toolbox','python','-m','scripts.drain')
        dc('run','--rm','--no-deps','toolbox','python','-m','scripts.ops','smoke','--run',run)
        evidence['status']='passed'
    finally:
        dc('start','taskmanager')
        if simulation.poll() is None:simulation.terminate()
        (ROOT/'artifacts/fault-acceptance.json').write_text(json.dumps(evidence,indent=2)+'\n')
if __name__=='__main__':main()
