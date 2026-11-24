import asyncio
import json
import unittest
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock
from cab_watch import city
from cab_watch.journey import analyse
from cab_watch.stream import Stream

class JourneyAnalysis(unittest.TestCase):
    def setUp(self):
        self.trip=dict(id=uuid4(),user_id=uuid4(),vehicle_id='CB#001',planned_route=['G','H','I','J','K'])

    def point(self,seq,node='D',second=None):
        lat,lon=city.gps(city.NODES[node])
        return dict(sequence=seq,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:{second if second is not None else seq:02d}+00:00')

    def test_old_backlog_uses_capture_time_and_records_every_deviation(self):
        pts={str(i):self.point(i) for i in range(1,4)}
        pts['4']=self.point(4,'H')
        rows,state,alerts,_=analyse(self.trip,pts)
        self.assertEqual(len(rows),4)
        self.assertTrue(all(p['off_route_sample'] for p in rows[:3]))
        self.assertEqual(alerts[0]['opened_sequence'],3)
        self.assertEqual(alerts[0]['closed_sequence'],4)
        self.assertEqual(alerts[0]['resolved_at'],'2020-01-01T00:00:04+00:00')
        self.assertFalse(state['off_route'])

    def test_late_missing_sample_rebuilds_confirmation(self):
        pts={'3':self.point(3),'1':self.point(1)}
        self.assertEqual(analyse(self.trip,pts)[2],[])
        pts['2']=self.point(2)
        rows,_,alerts,_=analyse(self.trip,pts)
        self.assertEqual([p['sequence'] for p in rows],[1,2,3])
        self.assertEqual(len(alerts),1)
        self.assertEqual(analyse(self.trip,pts)[2],alerts)

    def test_capture_gap_and_reversed_time_do_not_confirm(self):
        pts={str(i):self.point(i) for i in (1,2)}
        pts['3']=self.point(3,second=20)
        self.assertEqual(analyse(self.trip,pts)[2],[])
        pts['3']=self.point(3,second=1)
        rows,_,alerts,_=analyse(self.trip,pts)
        self.assertTrue(rows[-1]['timestamp_reversed'])
        self.assertEqual(alerts,[])

    def test_historical_arrival_uses_capture_time(self):
        _,state,_,completed=analyse(self.trip,{'1':self.point(1,'K')})
        self.assertTrue(state['completed'])
        self.assertEqual(completed,'2020-01-01T00:00:01+00:00')

class OffsetRecovery(unittest.IsolatedAsyncioTestCase):
    async def test_failed_database_write_never_commits_then_retries(self):
        stream=object.__new__(Stream)
        calls=[]
        async def save(records):
            calls.append('save')
            if calls==['save']:raise RuntimeError('temporary database outage')
        stream.history=save
        class Consumer:
            async def getmany(self,**kwargs):
                if 'commit' in calls:raise asyncio.CancelledError
                return {'partition':[SimpleNamespace(offset=7)]}
            def assignment(self):return {'partition'}
            async def commit(self,offsets):
                self.offsets=offsets;calls.append('commit')
            def highwater(self,p):return None
        consumer=Consumer()
        with self.assertRaises(asyncio.CancelledError):await stream.consume(consumer)
        self.assertEqual(calls,['save','save','commit'])
        self.assertEqual(consumer.offsets,{'partition':8})

class KafkaAuthentication(unittest.IsolatedAsyncioTestCase):
    async def test_owner_does_not_depend_on_old_stream_key_cache(self):
        from cab_watch.auth import owner,issue
        from fastapi.security import HTTPAuthorizationCredentials
        account=dict(id=uuid4(),name='Test',jwt_key=b'x'*32)
        token=issue(account)['access_token']
        db=SimpleNamespace(fetchval=AsyncMock(return_value=account['jwt_key']))
        request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db=db,pipeline=object())))
        credentials=HTTPAuthorizationCredentials(scheme='Bearer',credentials=token)
        self.assertEqual(await owner(request,credentials),account['id'])
