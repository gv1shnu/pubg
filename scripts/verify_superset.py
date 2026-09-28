"""Authenticated browser evidence, run locally with requirements-browser.txt."""
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1]
def main():
    secrets=dict(line.split('=',1) for line in (ROOT/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
    output=ROOT/'artifacts/superset';output.mkdir(parents=True,exist_ok=True)
    evidence=[]
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        page=browser.new_page(viewport={'width':1440,'height':1000})
        page.goto('http://localhost:8088/login/',wait_until='networkidle')
        page.locator('#username').fill('admin')
        page.locator('#password').fill(secrets['SUPERSET_ADMIN_PASSWORD'])
        page.locator('input[type=submit],button[type=submit]').first.click()
        page.wait_for_url('**/superset/welcome/**',timeout=30000)
        for slug in ('live-leaderboard','match-operations','server-health','pipeline-reliability'):
            replies=[]
            def capture(response):
                if '/api/v1/chart/data' in response.url:
                    try:
                        payload=response.json()
                        replies.append({'http_status':response.status,'query_statuses':[v.get('status') for v in payload.get('result',[])],
                            'row_counts':[v.get('rowcount') for v in payload.get('result',[])],
                            'sample_rows':[v.get('data',[])[:2] for v in payload.get('result',[])],
                            'error':payload.get('message') or payload.get('error')})
                    except Exception:replies.append({'http_status':response.status,'error':'non-JSON chart response'})
            page.on('response',capture)
            started=time.monotonic()
            page.goto(f'http://localhost:8088/superset/dashboard/{slug}/',wait_until='networkidle',timeout=60000)
            page.wait_for_timeout(10000)
            page.screenshot(path=str(output/f'{slug}.png'),full_page=True)
            body=page.locator('body').inner_text()
            record={'dashboard':slug,'browser_wait_seconds':time.monotonic()-started,'chart_responses':replies,
                    'has_error_text':any(s in body for s in ('Unexpected error','Unable to load','Data error'))}
            evidence.append(record)
            (output/'browser-evidence.json').write_text(json.dumps(evidence,indent=2)+'\n')
            page.remove_listener('response',capture)
            assert replies and all(r['http_status']==200 and not r['error'] for r in replies),record
            assert not record['has_error_text'],record
        browser.close()
    print(json.dumps({'dashboards_checked':len(evidence),'chart_responses':sum(len(e['chart_responses']) for e in evidence),'evidence':'artifacts/superset/browser-evidence.json'}))
if __name__=='__main__':main()
