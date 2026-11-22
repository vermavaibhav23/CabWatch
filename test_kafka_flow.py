"""Real broker tests. Never point these tests at production.

Recovery faults require CABWATCH_FAULT_TESTS=1 and the isolated Compose project
cabwatch-fastapi-test. They stop/restart only that project's containers.
"""
import os
from pathlib import Path
import secrets
import subprocess
import time
import unittest
from uuid import uuid4
import httpx
from cab_watch import city
from cab_watch.live import stamp

URL=os.getenv("CABWATCH_KAFKA_TEST_URL")
ROOT=Path(__file__).parent

def compose(*args):
    subprocess.run(["docker","compose","-p","cabwatch-fastapi-test","-f","compose.yaml","-f","compose.test.yaml",*args],
                   cwd=ROOT,check=True,timeout=120,stdout=subprocess.DEVNULL)

@unittest.skipUnless(URL, "Requires isolated Kafka test instance")
class KafkaFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        end=time.monotonic()+90
        while time.monotonic()<end:
            try:
                r=httpx.get(URL.split('/cab-watch')[0]+'/healthz',timeout=2)
                if r.status_code==200 and r.json().get('telemetry_mode')=='kafka':return
            except httpx.HTTPError:pass
            time.sleep(.5)
        raise RuntimeError('Kafka test API did not become ready')

    def setUp(self):
        self.client=httpx.Client(base_url=URL.rstrip("/")+"/",timeout=20)
        r=self.client.post("auth/register",json=dict(name="Kafka recovery",email=f"kafka-{uuid4().hex}@example.invalid",password=secrets.token_urlsafe(20)))
        r.raise_for_status()
        self.client.headers["Authorization"]="Bearer "+r.json()["access_token"]
        r=self.client.post("me/trips",headers={"Idempotency-Key":str(uuid4())},json=dict(pickup="G",dropoff="K"))
        r.raise_for_status(); self.trip=r.json()["id"]
        self.client.get("me/trips").raise_for_status()

    def tearDown(self): self.client.close()

    def point(self,seq,node="D",age=0,trip=None):
        lat,lon=city.gps(city.NODES[node])
        return dict(trip_id=trip or self.trip,sequence=seq,lat=lat,lon=lon,captured_at=stamp(int(time.time()*1000)-age))

    def send(self,seq,node="D",age=0):
        r=self.client.post("me/gps",json={"readings":[self.point(seq,node,age)]})
        self.assertEqual(r.status_code,202,r.text)
        self.assertTrue(r.json()["queued"])
        self.assertFalse(r.json()["results"][0]["applied"])

    def wait(self,predicate,timeout=45):
        end=time.monotonic()+timeout; last=None
        while time.monotonic()<end:
            try:
                last=predicate()
                if last:return last
            except (httpx.HTTPError,KeyError,ValueError):pass
            time.sleep(.15)
        self.fail(f"Timed out waiting for recovery; last result {last}")

    def state(self):
        r=self.client.get("me/live"); r.raise_for_status()
        return next((s for s in r.json()["states"] if s["trip_id"]==self.trip),{})

    def at(self,seq):
        return self.wait(lambda:(s if (s:=self.state()).get("sequence",0)>=seq else None))

    def history(self):
        r=self.client.get(f"me/trips/{self.trip}/history"); r.raise_for_status()
        return r.json()["readings"]

    def test_ownership_recovery_history_and_completion(self):
        r=self.client.post("me/gps",json={"readings":[self.point(1),self.point(1,trip=str(uuid4()))]})
        self.assertEqual(r.status_code,422)
        time.sleep(.3); self.assertEqual(self.history(),[])
        for seq in range(1,4): self.send(seq); self.at(seq)
        s=self.at(3); self.assertTrue(s["off_route"])
        alert_id=s["alert"]["id"]
        # A duplicate cannot advance confirmation or create a second history row.
        self.send(3); self.send(4,"Z",age=300000)
        s=self.at(4); self.assertEqual(s["tracking"],"DELAYED")
        self.assertFalse(s["off_route"]); self.assertEqual(s["consecutive"],0)
        for seq in (5,6):self.send(seq); self.at(seq)
        self.assertFalse(self.state()["off_route"])
        self.send(7); self.assertTrue(self.at(7)["off_route"])
        self.send(8,"H"); self.assertFalse(self.at(8)["off_route"])
        self.send(9,"K"); self.assertTrue(self.at(9)["completed"])
        rows=self.wait(lambda:(r if len(r:=self.history())==9 else None))
        self.assertEqual([r["sequence"] for r in rows],list(range(1,10)))
        self.assertTrue(any(r["sequence"]==4 for r in rows))
        self.wait(lambda:any(t["id"]==self.trip and t["completed"] for t in self.client.get("me/trips").json()["trips"]))
        self.wait(lambda:any(str(a["id"])==alert_id and a["closed_sequence"]==4 for a in self.client.get("me/trips").json()["alerts"]))
        # Another account cannot read history or live state.
        with httpx.Client(base_url=URL.rstrip("/")+"/",timeout=20) as other:
            r=other.post("auth/register",json=dict(name="Other",email=f"other-{uuid4().hex}@example.invalid",password=secrets.token_urlsafe(20)))
            other.headers["Authorization"]="Bearer "+r.json()["access_token"]
            self.assertEqual(other.get(f"me/trips/{self.trip}/history").status_code,404)
            self.assertEqual(other.get("me/live").json()["states"],[])

    @unittest.skipUnless(os.getenv("CABWATCH_FAULT_TESTS")=="1", "Local fault injection disabled")
    def test_database_and_redis_outage_then_restart(self):
        self.assertEqual(URL,"http://127.0.0.1:18082/cab-watch")
        self.send(1); self.at(1)
        compose("stop","db")
        try:
            for seq in (2,3):self.send(seq); self.at(seq)
            self.assertTrue(self.state()["off_route"],"Live alerts must not wait for SQL")
        finally:compose("start","db")
        self.wait(lambda:len(self.history())==3,timeout=60)
        self.wait(lambda:any(a["trip_id"]==self.trip for a in self.client.get("me/trips").json()["alerts"]))
        # Force an acknowledged Kafka send followed by a failed Redis write.
        reading=self.point(4,"H")
        compose("stop","redis")
        try:
            r=self.client.post("me/gps",json={"readings":[reading]})
            self.assertEqual(r.status_code,503)
        finally:compose("start","redis")
        r=self.client.post("me/gps",json={"readings":[reading]})
        self.assertEqual(r.status_code,202,r.text)
        self.at(4)
        self.wait(lambda:len(self.history())==4)
        compose("restart","dashboard")
        self.wait(lambda:self.state().get("sequence")==4,timeout=90)
        self.assertEqual(len(self.history()),4)
        # Old confirmation does not survive a long pause or the process restart.
        self.send(5); self.at(5)
        self.assertEqual(self.state()["consecutive"],1)
        self.send(6); self.at(6)
        self.assertFalse(self.state()["off_route"])
        self.send(7); self.assertTrue(self.at(7)["off_route"])
        self.wait(lambda:len(self.history())==7)

if __name__=="__main__":unittest.main()
