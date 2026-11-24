"""Run with CABWATCH_TEST_URL pointing at a test instance of /cab-watch."""
from concurrent.futures import ThreadPoolExecutor
import os
import secrets
import unittest
from uuid import uuid4

import httpx
from cab_watch import city


@unittest.skipUnless(os.getenv("CABWATCH_TEST_URL"), "Requires running test instance")
class AccountFlow(unittest.TestCase):
    def test_historical_late_readings_ownership_and_trip_idempotency(self):
        with httpx.Client(base_url=os.environ['CABWATCH_TEST_URL'].rstrip('/')+'/',timeout=60) as c:
            r=c.post('auth/register',json=dict(name='History',email=f'{uuid4()}@example.invalid',password=secrets.token_urlsafe(20)))
            r.raise_for_status();c.headers['Authorization']='Bearer '+r.json()['access_token']
            headers={'Idempotency-Key':str(uuid4())}
            r=c.post('me/trips',headers=headers,json=dict(pickup='G',dropoff='K'))
            r.raise_for_status();trip=r.json()['id']
            self.assertEqual(c.post('me/trips',headers=headers,json=dict(pickup='G',dropoff='K')).json()['id'],trip)
            self.assertEqual(c.post('me/trips',headers=headers,json=dict(pickup='A',dropoff='K')).status_code,409)
            def send(seq,node='D'):
                lat,lon=city.gps(city.NODES[node])
                r=c.post('me/gps',json={'readings':[dict(trip_id=trip,sequence=seq,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:0{seq}Z')]})
                r.raise_for_status();return r.json()
            send(1);send(3)
            self.assertEqual(c.get(f'me/trips/{trip}/history').json()['alerts'],[])
            send(2);send(2);send(4,'H');send(5,'K')
            h=c.get(f'me/trips/{trip}/history').json()
            self.assertEqual([p['sequence'] for p in h['readings']],[1,2,3,4,5])
            self.assertEqual(len(h['alerts']),1)
            self.assertEqual((h['alerts'][0]['opened_sequence'],h['alerts'][0]['closed_sequence']),(3,4))
            self.assertTrue(h['alerts'][0]['opened_at'].startswith('2020-01-01'))
            self.assertTrue(c.get('me/trips').json()['trips'][0]['completed'])
            c.headers.pop('Authorization')
            self.assertEqual(c.get(f'me/trips/{trip}/history').status_code,401)
            r=c.post('auth/register',json=dict(name='Other',email=f'{uuid4()}@example.invalid',password=secrets.token_urlsafe(20)))
            c.headers['Authorization']='Bearer '+r.json()['access_token']
            self.assertEqual(c.get(f'me/trips/{trip}/history').status_code,404)
            self.assertEqual(c.get('me/trips').json()['trips'],[])
