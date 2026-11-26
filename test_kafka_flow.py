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

    def history(self):
        r=self.client.get(f"me/trips/{self.trip}/history");r.raise_for_status();return r.json()

    @unittest.skipUnless(os.getenv('CABWATCH_FAULT_TESTS')=='1', 'Requires isolated Docker test stack')
    def test_three_partitions_have_three_active_consumers(self):
        def command(*args):
            return subprocess.check_output(['docker','compose','-p','cabwatch-fastapi-test',
                '-f','compose.yaml','-f','compose.test.yaml','exec','-T','kafka',*args],
                cwd=ROOT,text=True,timeout=30)
        description=command('/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092',
                            '--describe','--topic','cabwatch.gps.v2')
        self.assertIn('PartitionCount: 3',description)
        def assigned():
            output=command('/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','localhost:9092',
                           '--describe','--group','cabwatch.gps.v2.journey-history-v1')
            rows=[line.split() for line in output.splitlines()
                  if line.startswith('cabwatch.gps.v2.journey-history-v1')]
            return len(rows)==3 and len({row[6] for row in rows if row[6]!='-'})==3
        self.wait(assigned,timeout=60)

    def test_late_backlog_replay_and_completion(self):
        # Old capture times are intentional: history must not depend on wall-clock freshness.
        def send(seq,node='D'):
            p=self.point(seq,node);p['captured_at']=f'2020-01-01T00:00:0{seq}Z'
            r=self.client.post('me/gps',json={'readings':[p]})
            self.assertEqual(r.status_code,202,r.text)
        send(1);send(3)
        self.wait(lambda:self.history()['total']==2)
        self.assertEqual(self.history()['alerts'],[])
        send(2)
        self.wait(lambda:len(self.history()['alerts'])==1)
        send(2);send(4,'H');send(5,'K')
        self.wait(lambda:self.history()['total']==5)
        h=self.history()
        self.assertEqual([p['sequence'] for p in h['readings']],[1,2,3,4,5])
        self.assertEqual(len(h['alerts']),1)
        self.assertEqual((h['alerts'][0]['opened_sequence'],h['alerts'][0]['closed_sequence']),(3,4))
        self.assertTrue(h['alerts'][0]['opened_at'].startswith('2020-01-01'))
        self.assertTrue(next(t for t in self.client.get('me/trips').json()['trips'] if t['id']==self.trip)['completed'])
        self.assertEqual(self.client.get('me/live').status_code,404)

    @unittest.skipUnless(os.getenv('CABWATCH_FAULT_TESTS')=='1','Local fault injection disabled')
    def test_restart_replays_without_duplicate_history(self):
        self.assertEqual(URL,'http://127.0.0.1:18082/cab-watch')
        for seq in range(1,4):self.send(seq)
        self.wait(lambda:self.history()['total']==3)
        compose('restart','history-worker')
        self.wait(lambda:self.history()['total']==3,timeout=90)
        self.send(3)
        self.send(4,'H')
        self.wait(lambda:self.history()['total']==4)
        self.assertEqual(len(self.history()['alerts']),1)

    @unittest.skipUnless(os.getenv('CABWATCH_FAULT_TESTS')=='1','Local fault injection disabled')
    def test_worker_outage_accepts_then_catches_up(self):
        self.assertEqual(URL,'http://127.0.0.1:18082/cab-watch')
        compose('stop','history-worker')
        try:
            for seq in (1,2,3):
                p=self.point(seq)
                p['captured_at']=f'2020-01-01T00:00:0{seq}Z'
                r=self.client.post('me/gps',json={'readings':[p]})
                self.assertEqual(r.status_code,202,r.text)
            time.sleep(1)
            self.assertEqual(self.history()['total'],0)
            self.assertEqual(self.history()['accepted_sequence'],3)
            self.assertEqual(httpx.get(URL.split('/cab-watch')[0]+'/healthz').status_code,200)
        finally:
            compose('start','history-worker')
        self.wait(lambda:self.history()['total']==3,timeout=90)
        h=self.history()
        self.assertEqual(len(h['alerts']),1)
        self.assertEqual(h['alerts'][0]['opened_sequence'],3)

if __name__=='__main__':unittest.main()
