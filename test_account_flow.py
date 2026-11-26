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
    def test_trip_idempotency_concurrency_and_incremental_gps(self):
        with httpx.Client(base_url=os.environ['CABWATCH_TEST_URL'].rstrip('/')+'/',timeout=60) as c:
            r=c.post('auth/register',json=dict(name='Fleet',email=f'{uuid4()}@example.invalid',password=secrets.token_urlsafe(20)))
            r.raise_for_status();c.headers['Authorization']='Bearer '+r.json()['access_token']
            headers={'Idempotency-Key':str(uuid4())}
            def create(_):return c.post('me/trips',headers=headers,json=dict(pickup='G',dropoff='K'))
            with ThreadPoolExecutor(3) as pool:responses=list(pool.map(create,range(3)))
            self.assertEqual(sorted(r.status_code for r in responses),[200,200,201])
            self.assertEqual(len({r.json()['id'] for r in responses}),1)
            trip=responses[0].json()['id']
            self.assertNotIn('motion',responses[0].json())
            self.assertEqual(c.post('me/trips',headers=headers,json=dict(pickup='A',dropoff='K')).status_code,409)
            def new(_):return c.post('me/trips',headers={'Idempotency-Key':str(uuid4())},json=dict(pickup='G',dropoff='K')).json()
            with ThreadPoolExecutor(3) as pool:trips=list(pool.map(new,range(3)))
            self.assertEqual(len({t['vehicle_id'] for t in trips}),3)
            def send(seq,node='D',**extra):
                lat,lon=city.gps(city.NODES[node])
                return c.post('me/gps',json={'readings':[dict(trip_id=trip,sequence=seq,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:0{seq}Z',**extra)]})
            self.assertEqual(send(1,motion={'mode':'route','index':0}).status_code,422)
            send(1).raise_for_status();send(3).raise_for_status();send(2).raise_for_status()
            self.assertEqual(c.get(f'me/trips/{trip}/alerts').json()['alerts'],[])
            send(4).raise_for_status();send(5).raise_for_status()
            incident=c.get(f'me/trips/{trip}/alerts').json()['alerts'][0]
            send(5).raise_for_status();send(6,'H').raise_for_status();send(7,'K').raise_for_status();send(8).raise_for_status()
            alerts=c.get(f'me/trips/{trip}/alerts').json()['alerts']
            self.assertEqual(len(alerts),1);self.assertEqual(alerts[0]['id'],incident['id'])
            self.assertEqual((alerts[0]['opened_sequence'],alerts[0]['closed_sequence']),(5,6))
            final=next(t for t in c.get('me/trips').json()['trips'] if t['id']==trip)
            self.assertTrue(final['completed']);self.assertEqual(final['last_sequence'],7)
            self.assertEqual(c.get(f'me/trips/{trip}/history').status_code,404)
            c.headers.pop('Authorization');self.assertEqual(c.get('me/trips').status_code,401)
            r=c.post('auth/register',json=dict(name='Other',email=f'{uuid4()}@example.invalid',password=secrets.token_urlsafe(20)))
            c.headers['Authorization']='Bearer '+r.json()['access_token']
            self.assertEqual(c.get('me/trips').json()['trips'],[])
            self.assertEqual(send(9).status_code,422)
            self.assertEqual(c.get(f'me/trips/{trip}/alerts').json()['alerts'],[])
