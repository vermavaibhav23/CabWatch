"""Optional local UI smoke test: pip install playwright; playwright install chromium.
Use CABWATCH_BROWSER_EXECUTABLE to test an installed browser instead.
"""
import json
import os
from pathlib import Path
import time
from uuid import uuid4
import httpx
from playwright.sync_api import sync_playwright
from cab_watch import city
from cab_watch.live import stamp

base=os.environ.get('CABWATCH_KAFKA_TEST_URL','http://127.0.0.1:18082/cab-watch')
assert base.startswith(('http://127.0.0.1:', 'http://localhost:')), 'Local smoke tests only'
with httpx.Client(timeout=20) as client:
    account=client.post(base+'/auth/register',json=dict(name='Dashboard QA',email=f'qa-{uuid4().hex}@example.invalid',password=uuid4().hex)).json()
    headers={'Authorization':'Bearer '+account['access_token']}
    trip=client.post(base+'/me/trips',headers={**headers,'Idempotency-Key':str(uuid4())},json=dict(pickup='G',dropoff='K')).json()['id']
    with sync_playwright() as p:
        kwargs={'headless':True}
        if os.getenv('CABWATCH_BROWSER_EXECUTABLE'):kwargs['executable_path']=os.environ['CABWATCH_BROWSER_EXECUTABLE']
        browser=p.chromium.launch(**kwargs)
        page=browser.new_page(viewport={'width':1440,'height':1000})
        errors=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.add_init_script('sessionStorage.setItem("cabwatch.token",'+json.dumps(account['access_token'])+'); sessionStorage.setItem("cabwatch.name","Dashboard QA");')
        requests=[]
        page.on('request',lambda r:requests.append((r.method,r.url)))
        page.goto(base+'/',wait_until='networkidle')
        page.locator('.cabrow').wait_for()
        def reading(seq,node='D'):
            lat,lon=city.gps(city.NODES[node])
            r=client.post(base+'/me/gps',headers=headers,json={'readings':[dict(trip_id=trip,sequence=seq,captured_at=f'2020-01-01T00:00:0{seq}Z',lat=lat,lon=lon)]})
            assert r.status_code in (200,202),r.text
        for seq in (1,2,3):reading(seq)
        page.get_by_text('OFF ROUTE',exact=True).wait_for()
        page.locator('.cabpin.offroute').wait_for()
        page.locator('.cabrow.alarm').wait_for()
        assert 'connected' in page.locator('#alertConnection').inner_text()
        page.screenshot(path=str(Path(os.environ.get('CABWATCH_QA_OUTPUT','browser-smoke.png')).with_name('offroute-dashboard.png')),full_page=True)
        assert page.locator('#open').inner_text()=='1'
        page.reload(wait_until='networkidle')
        page.locator('.cabpin.offroute').wait_for()
        reading(4,'H')
        page.wait_for_function('document.querySelector("#open").textContent === "0"')
        assert page.locator('.cabpin.offroute').count()==0
        assert page.locator('.cabrow.alarm').count()==0
        page.wait_for_function('document.querySelector("#alerts").textContent.includes("Resolved")')
        assert page.locator('#historyRows').count()==0
        assert page.locator('#play').is_disabled()  # API-created trip has no local simulator.
        assert 'Display only' in page.locator('#tripline').inner_text()
        reading(5,'K')
        page.wait_for_function('document.querySelector("#tripline").textContent.includes("Completed")')
        page.reload(wait_until='networkidle')
        page.wait_for_function('document.querySelector("#alerts").textContent.includes("Resolved")')
        before=len(requests)
        page.wait_for_timeout(6500)
        assert not [url for method,url in requests[before:] if method=='GET' and '/me/trips' in url],requests[before:]
        out=Path(os.environ.get('CABWATCH_QA_OUTPUT','browser-smoke.png'))
        page.screenshot(path=str(out),full_page=True)
        assert not errors,errors
        browser.close()
        print('Dashboard smoke passed: capture times, incident resolution, display-only trips and reload; no JS errors')
