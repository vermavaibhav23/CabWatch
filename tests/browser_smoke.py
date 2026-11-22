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
        def reading(seq,node='D',age=0):
            lat,lon=city.gps(city.NODES[node])
            r=client.post(base+'/me/gps',headers=headers,json={'readings':[dict(trip_id=trip,sequence=seq,captured_at=stamp(int(time.time()*1000)-age),lat=lat,lon=lon,motion={'mode':'route','index':0})]})
            assert r.status_code==202,r.text
            page.wait_for_function('(seq)=>document.querySelector("#distance").textContent.includes("GPS #"+seq)',arg=seq)
        for seq in (1,2,3):reading(seq)
        page.get_by_text('Off-route alert',exact=True).wait_for()
        assert page.locator('#open').inner_text()=='1'
        reading(4,'Z',300000)
        page.get_by_text('Tracking delayed',exact=True).first.wait_for()
        assert page.locator('#open').inner_text()=='0'
        reading(5); reading(6)
        page.get_by_text('Confirming deviation',exact=True).wait_for()
        reading(7)
        page.get_by_text('Off-route alert',exact=True).wait_for()
        reading(8,'H')
        assert page.locator('#open').inner_text()=='0'
        out=Path(os.environ.get('CABWATCH_QA_OUTPUT','browser-smoke.png'))
        page.screenshot(path=str(out),full_page=True)
        assert not errors,errors
        browser.close()
        print('Dashboard smoke passed: confirmed polling, three samples, delayed marker, reset and recovery; no JS errors')
