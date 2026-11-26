"""Browser state recovery; CI additionally stops the real Kafka worker."""
import base64,json,os,time
from uuid import uuid4
from pathlib import Path
import httpx
from playwright.sync_api import sync_playwright

base=os.environ.get('CABWATCH_KAFKA_TEST_URL','http://127.0.0.1:18082/cab-watch')
assert base.startswith(('http://127.0.0.1:','http://localhost:'))
with httpx.Client(timeout=20) as client:
    account=client.post(base+'/auth/register',json=dict(name='Recovery QA',email=f'{uuid4()}@example.invalid',password=uuid4().hex)).json()
    part=account['access_token'].split('.')[1]
    subject=json.loads(base64.urlsafe_b64decode(part+'='*(-len(part)%4)))['sub']
    key='cabwatch.telemetry.'+subject
    with sync_playwright() as p:
        options={'headless':True}
        if os.getenv('CABWATCH_BROWSER_EXECUTABLE'):options['executable_path']=os.environ['CABWATCH_BROWSER_EXECUTABLE']
        browser=p.chromium.launch(**options);page=browser.new_page();errors=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.add_init_script('sessionStorage.setItem("cabwatch.token",'+json.dumps(account['access_token'])+');')
        page.add_init_script('window.testSockets=[];const NativeWS=window.WebSocket;window.WebSocket=class extends NativeWS{constructor(...args){super(...args);window.testSockets.push(this);}};')
        trip_reads=[]
        page.on('request',lambda r:trip_reads.append(r.url) if r.method=='GET' and r.url==base+'/me/trips' else None)
        page.goto(base+'/',wait_until='networkidle')
        page.locator('#create').click();page.locator('#pickup').select_option('G');page.locator('#dropoff').select_option('K');page.locator('#launch').click()
        def saved():return page.evaluate('(key)=>JSON.parse(sessionStorage.getItem(key))',key)
        def wait_state(expression):page.wait_for_function('(key)=>{const s=JSON.parse(sessionStorage.getItem(key));const c=s&&Object.values(s.simulations)[0];return c&&('+expression+');}',arg=key,timeout=45000)
        def pause():
            if 'Pause' in page.locator('#play').inner_text():page.locator('#play').click()
        wait_state('c.sequence>=2');pause();before=saved()['simulations']
        def assert_local_marker():
            page.wait_for_function('(key)=>{const c=Object.values(JSON.parse(sessionStorage.getItem(key)).simulations)[0];const m=document.querySelector(".cabpin").transform.baseVal.consolidate().matrix;return Math.hypot(m.e-c.point[0],m.f-c.point[1])<0.1;}',arg=key)
        assert_local_marker()
        count=len(trip_reads);page.wait_for_timeout(6500)
        assert len(trip_reads)==count,trip_reads
        # A real socket disconnect performs one HTTP recovery, not a polling loop.
        page.evaluate('window.testSockets.at(-1).close()')
        page.wait_for_function('document.querySelector("#alertConnection").textContent.includes("connected")')
        end=time.monotonic()+15
        while len(trip_reads)==count and time.monotonic()<end:page.wait_for_timeout(100)
        assert len(trip_reads)==count+1,trip_reads
        assert_local_marker();assert saved()['simulations']==before
        page.wait_for_timeout(6500);assert len(trip_reads)==count+1,trip_reads
        page.reload(wait_until='networkidle');page.wait_for_function('document.querySelector("#tripline").textContent.includes("Locally resumable")')
        assert saved()['simulations']==before
        page.locator('#wrong').click();page.locator('#play').click();wait_state('c.motion.mode==="detour"');pause()
        before=saved()['simulations'];page.reload(wait_until='networkidle');page.locator('#return').wait_for()
        assert saved()['simulations']==before
        page.locator('#return').click();before=saved()['simulations'];page.reload(wait_until='networkidle')
        assert saved()['simulations']==before
        page.wait_for_function('!document.querySelector("#play").disabled')
        # Lose an ACK/upload response: original coordinates and sequence must retry.
        failed=[]
        def fail(route):
            failed.extend(route.request.post_data_json['readings'])
            route.fulfill(status=503,json={'detail':'test unavailable'})
        page.route('**/me/gps',fail);page.locator('#play').click()
        page.wait_for_function('(key)=>JSON.parse(sessionStorage.getItem(key)).batches.length>0',arg=key)
        pause();pending=saved();page.unroute('**/me/gps',fail)
        retried=[]
        def observe(request):
            if request.url.endswith('/me/gps'):retried.extend(request.post_data_json['readings'])
        page.on('request',observe);page.reload(wait_until='networkidle')
        page.wait_for_function('(key)=>JSON.parse(sessionStorage.getItem(key)).batches.length===0',arg=key)
        for batch in pending['batches']:
            for reading in batch:assert reading in retried,(reading,retried)
        assert saved()['simulations']==pending['simulations']
        if os.getenv('CABWATCH_FAULT_TESTS')=='1':
            from test_kafka_flow import compose
            assert base=='http://127.0.0.1:18082/cab-watch'
            compose('stop','history-worker')
            try:
                page.locator('#play').click();start=next(iter(saved()['simulations'].values()))['sequence']
                wait_state('c.sequence>='+str(start+2));pause()
                page.wait_for_function('(key)=>JSON.parse(sessionStorage.getItem(key)).batches.length===0',arg=key)
                assert_local_marker()
                checkpoint=saved()['simulations'];tid=next(iter(checkpoint));seq=checkpoint[tid]['sequence']
                headers={'Authorization':'Bearer '+account['access_token']}
                trip=next(t for t in client.get(base+'/me/trips',headers=headers).json()['trips'] if t['id']==tid)
                assert trip['last_sequence']<seq
                page.reload(wait_until='networkidle');assert saved()['simulations']==checkpoint
                assert_local_marker()
                page.locator('#play').click();wait_state('c.sequence>'+str(seq));pause()
            finally:compose('start','history-worker')
        # Losing session storage must never resume from the DB sequence.
        page.evaluate('(key)=>sessionStorage.removeItem(key)',key);page.reload(wait_until='networkidle')
        page.wait_for_function('document.querySelector("#tripline").textContent.includes("Display only")')
        assert page.locator('#play').is_disabled();assert page.locator('#wrong').is_disabled()
        assert not errors,errors
        browser.close()
        print('Browser recovery passed: local marker animation, no HTTP polling, socket reconnect, detour, return path, original pending retries and lost session'+(' plus real worker backlog' if os.getenv('CABWATCH_FAULT_TESTS')=='1' else ''))
