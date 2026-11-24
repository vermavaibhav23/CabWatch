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
        page.goto(base+'/',wait_until='networkidle')
        page.locator('.cabrow').wait_for()
        def reading(seq,node='D'):
            lat,lon=city.gps(city.NODES[node])
            r=client.post(base+'/me/gps',headers=headers,json={'readings':[dict(trip_id=trip,sequence=seq,captured_at=f'2020-01-01T00:00:0{seq}Z',lat=lat,lon=lon,motion={'mode':'route','index':0})]})
            assert r.status_code in (200,202),r.text
            page.wait_for_function('(seq)=>document.querySelector("#distance").textContent.includes("GPS #"+seq)',arg=seq)
        for seq in (1,2,3):reading(seq)
        page.get_by_text('Deviation in last record',exact=True).wait_for()
        assert page.locator('#open').inner_text()=='1'
        reading(4,'H')
        page.wait_for_function('document.querySelector("#open").textContent === "0"')
        page.locator('#historyRefresh').click()
        page.wait_for_function('document.querySelectorAll("#historyRows tr").length === 4')
        assert '2020' in page.locator('#historyRows').inner_text()
        page.reload(wait_until='networkidle')
        page.wait_for_function('document.querySelectorAll("#historyRows tr").length === 4')
        out=Path(os.environ.get('CABWATCH_QA_OUTPUT','browser-smoke.png'))
        page.screenshot(path=str(out),full_page=True)
        assert not errors,errors
        browser.close()
        print('Dashboard smoke passed: historical capture times, deviations, GPS table and reload; no JS errors')
