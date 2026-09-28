"""Compare an observed database change with automatic dashboard refresh."""
import json
import subprocess
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
ROOT=Path(__file__).resolve().parents[1]
def sql(query):
    return subprocess.check_output(['docker','compose','exec','-T','postgres','psql','-U','telemetry_admin','-d','telemetry','-At','-c',query],cwd=ROOT,text=True).strip()
def main():
    cfg=dict(l.split('=',1) for l in (ROOT/'.env').read_text().splitlines() if '=' in l and not l.startswith('#'))
    run=sql("select run_id from mart.freshness where namespace='live' order by processed_at desc limit 1")
    assert all(c.isalnum() or c=='-' for c in run)
    events=[];evidence={'run_id':run,'sql_poll_interval_seconds':1}
    out=ROOT/'artifacts/superset';out.mkdir(exist_ok=True,parents=True)
    with sync_playwright() as p:
        browser=p.chromium.launch();page=browser.new_page(viewport={'width':1440,'height':1000})
        page.goto('http://localhost:8088/login/');page.locator('#username').fill('admin');page.locator('#password').fill(cfg['SUPERSET_ADMIN_PASSWORD']);page.locator('input[type=submit],button[type=submit]').first.click();page.wait_for_url('**/superset/welcome/**')
        def capture(response):
            if '/api/v1/chart/data' in response.url and response.status==200:
                for result in response.json().get('result',[]):
                    for row in result.get('data',[]):
                        if 'Eliminations' in row and row['Eliminations'] is not None:events.append((time.monotonic(),row['Eliminations']))
        page.on('response',capture)
        page.goto('http://localhost:8088/superset/dashboard/live-leaderboard/',wait_until='networkidle')
        deadline=time.monotonic()+50;threshold=None
        while time.monotonic()<deadline:
            page.wait_for_timeout(1000)
            if not events:continue
            count=int(sql(f"select coalesce(sum(kills),0) from mart.leaderboard where namespace='live' and run_id='{run}'"))
            if threshold is None and count>events[0][1]:threshold=count;changed=time.monotonic();evidence['observed_db_kills']=count
            if threshold is not None and any(t>=changed and v>=threshold for t,v in events):
                rendered=next(t for t,v in events if t>=changed and v>=threshold)
                evidence.update(automatic_refresh_verified=True,observed_db_to_browser_seconds=rendered-changed,initial_browser_kills=events[0][1],latest_browser_kills=events[-1][1]);break
        else:evidence['automatic_refresh_verified']=False
        page.screenshot(path=str(out/'live-refresh-proof.png'),full_page=True)
        page.goto('http://localhost:8088/superset/dashboard/server-health/',wait_until='networkidle');page.wait_for_timeout(5000)
        page.screenshot(path=str(out/'incident-proof.png'),full_page=True)
        evidence['incident_rows_in_database']=int(sql(f"select count(*) from mart.server_health where namespace='live' and run_id='{run}' and incident"))
        evidence['incident_table_visible']=page.get_by_text('Injected incident intervals — synthetic',exact=True).count()>0
        (out/'live-proof.json').write_text(json.dumps(evidence,indent=2)+'\n');browser.close()
    print(json.dumps(evidence));assert evidence['automatic_refresh_verified']
if __name__=='__main__':main()
