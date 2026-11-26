"""WebSocket isolation, incident clearing and reconnects against a test API."""
import json
import os
import time
import unittest
from uuid import uuid4

import httpx
from websockets.sync.client import connect
from websockets.exceptions import ConnectionClosed

from cab_watch import city

URL = os.getenv('CABWATCH_KAFKA_TEST_URL') or os.getenv('CABWATCH_TEST_URL')


@unittest.skipUnless(URL, 'Requires isolated test API')
class AlertSockets(unittest.TestCase):
    def setUp(self):
        self.client = httpx.Client(base_url=URL.rstrip('/')+'/', timeout=20)
        self.accounts = []
        for _ in range(2):
            r = self.client.post('auth/register', json=dict(name='Socket test',
                email=f'{uuid4()}@example.invalid', password='test-password-12345'))
            r.raise_for_status()
            token = r.json()['access_token']
            headers = {'Authorization': 'Bearer '+token, 'Idempotency-Key': str(uuid4())}
            r = self.client.post('me/trips', headers=headers, json=dict(pickup='G', dropoff='K'))
            r.raise_for_status()
            self.accounts.append((token, headers, r.json()['id']))
        self.ws_url = URL.replace('http://', 'ws://').replace('https://', 'wss://').rstrip('/')+'/me/alerts/ws'

    def tearDown(self):
        self.client.close()

    def authenticate(self, ws, index=0):
        ws.send(json.dumps({'token': self.accounts[index][0]}))
        return json.loads(ws.recv(timeout=10))

    def send(self, sequence, node='D', index=0):
        _, headers, trip = self.accounts[index]
        lat, lon = city.gps(city.NODES[node])
        r = self.client.post('me/gps', headers=headers, json={'readings': [dict(
            trip_id=trip, sequence=sequence, lat=lat, lon=lon,
            captured_at=f'2020-01-01T00:00:0{sequence}Z')]})
        r.raise_for_status()

    def until(self, ws, predicate):
        end = time.monotonic()+45
        while time.monotonic()<end:
            payload = json.loads(ws.recv(timeout=10))
            self.assertEqual(payload['type'], 'off_route_snapshot')
            if predicate(payload):
                return payload
        self.fail('WebSocket state did not converge')

    def test_open_clear_isolation_and_reconnect(self):
        with connect(self.ws_url) as alice, connect(self.ws_url) as bob:
            first = self.authenticate(alice)
            other = self.authenticate(bob, 1)
            self.assertEqual([v['trip_id'] for v in first['vehicles']], [self.accounts[0][2]])
            self.assertEqual([v['trip_id'] for v in other['vehicles']], [self.accounts[1][2]])
            self.send(1); self.send(2); self.send(3)
            opened = self.until(alice, lambda p: p['vehicles'][0]['alert'] is not None)
            self.assertEqual(opened['vehicles'][0]['alert']['opened_sequence'], 3)
            self.assertTrue(opened['vehicles'][0]['captured_at'].startswith('2020-01-01'))
            self.assertGreater(opened['vehicles'][0]['distance_m'], 200)
            with connect(self.ws_url) as reconnect:
                self.assertIsNotNone(self.authenticate(reconnect)['vehicles'][0]['alert'])
            bob_view = json.loads(bob.recv(timeout=10))
            self.assertEqual([v['trip_id'] for v in bob_view['vehicles']], [self.accounts[1][2]])
            self.assertIsNone(bob_view['vehicles'][0]['alert'])
            self.send(4, 'H')
            cleared = self.until(alice, lambda p: p['vehicles'][0]['sequence']==4)
            self.assertIsNone(cleared['vehicles'][0]['alert'])
            self.send(5, 'K')
            self.until(alice, lambda p: not p['vehicles'])

    def test_invalid_token_receives_no_fleet_data(self):
        with connect(self.ws_url) as ws:
            ws.send(json.dumps({'token': 'invalid'}))
            with self.assertRaises(ConnectionClosed) as closed:
                ws.recv(timeout=10)
            self.assertEqual(closed.exception.rcvd.code, 4401)
