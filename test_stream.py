"""Kafka wire-format compatibility without an external broker or database."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

from cab_watch.api import Batch
from cab_watch.stream import Stream, reading_from_event


class FlatTelemetry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.trip = UUID('11111111-1111-4111-8111-111111111111')
        self.user = UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
        self.point = dict(trip_id=str(self.trip), sequence=1,
                          captured_at='2020-01-01T00:00:01Z',
                          lat=13.097197269134028, lon=80.23597022616741,
                          )
        self.metadata = dict(trip_id=str(self.trip), user_id=str(self.user),
                             vehicle_id='CB#001')
        self.flat = {**self.point, **self.metadata}
        self.legacy = {**self.metadata, 'point': self.point.copy()}
        self.conn = MagicMock()
        self.conn.execute = AsyncMock()
        self.conn.fetchrow = AsyncMock(return_value=dict(user_id=self.user, vehicle_id='CB#001'))
        self.conn.transaction.return_value.__aenter__ = AsyncMock(return_value=self.conn)
        self.conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
        self.db = MagicMock()
        self.db.acquire.return_value.__aenter__ = AsyncMock(return_value=self.conn)
        self.db.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        self.db.fetch = AsyncMock(return_value=[dict(id=self.trip, vehicle_id='CB#001',
                                                   )])
        # Avoid constructing a real producer: exercise the real ingest/history methods.
        self.stream = object.__new__(Stream)
        self.stream.db = self.db
        self.stream.topic = 'test.gps'
        self.stream.producer = SimpleNamespace(send_and_wait=AsyncMock())

    def record(self, event, key=b'CB#001'):
        return SimpleNamespace(value=json.dumps(event).encode(), key=key)

    async def test_producer_emits_flat_record_without_database_writes(self):
        receipt = await self.stream.ingest(Batch(readings=[self.point]), self.user)
        args = self.stream.producer.send_and_wait.call_args
        self.assertEqual(args.args[0], 'test.gps')
        self.assertEqual(json.loads(args.args[1]), self.flat)
        self.assertEqual(args.kwargs['key'], b'CB#001')
        self.conn.execute.assert_not_awaited()
        self.db.acquire.assert_not_called()
        self.db.fetch.assert_awaited_once()
        self.assertEqual(receipt['results'][0]['sequence'], 1)
        self.assertTrue(receipt['queued'])

    async def test_http_ingestion_uses_exactly_two_application_reads(self):
        import httpx
        from fastapi import FastAPI
        from cab_watch.api import router
        from cab_watch.auth import issue
        self.db.fetchval=AsyncMock(return_value=b'x'*32)
        app=FastAPI();app.include_router(router)
        app.state.db=self.db;app.state.pipeline=self.stream
        token=issue(dict(id=self.user,name='Test',jwt_key=b'x'*32))['access_token']
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            response=await client.post('/cab-watch/me/gps',headers={'Authorization':'Bearer '+token},json={'readings':[self.point]})
        self.assertEqual(response.status_code,202,response.text)
        self.db.fetchval.assert_awaited_once();self.db.fetch.assert_awaited_once()
        self.assertIn('SELECT jwt_key',self.db.fetchval.call_args.args[0])
        self.assertIn('SELECT id,vehicle_id ',self.db.fetch.call_args.args[0])
        self.db.acquire.assert_not_called();self.conn.execute.assert_not_awaited()

    async def test_mixed_old_and_new_records_keep_reading_shape(self):
        second = {**self.flat, 'sequence': 2}
        with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock) as persist:
            await self.stream.history([self.record(self.legacy), self.record(second)])
            persist.assert_awaited_once_with(self.conn, self.trip,
                                            [self.point, {**self.point, 'sequence': 2}],expected_user=str(self.user),expected_vehicle='CB#001')

    async def test_legacy_and_flat_records_normalize_identically(self):
        self.assertEqual(reading_from_event(self.flat), reading_from_event(self.legacy))
        self.assertEqual(reading_from_event(self.flat), self.point)
        for event in (self.flat, self.legacy):
            with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock) as persist:
                await self.stream.history([self.record(event)])
                persist.assert_awaited_once_with(self.conn, self.trip, [self.point],expected_user=str(self.user),expected_vehicle='CB#001')

    async def test_key_check_rejects_invalid_record(self):
        with patch('cab_watch.stream.persist_readings',new_callable=AsyncMock) as persist:
            with self.assertRaises(ValueError):
                await self.stream.history([self.record(self.flat,b'wrong')])
            persist.assert_not_awaited()

    async def test_failed_sql_write_propagates_for_consumer_retry(self):
        with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock,
                   side_effect=RuntimeError('database unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'database unavailable'):
                await self.stream.history([self.record(self.flat)])
        self.assertIs(self.conn.transaction.return_value.__aexit__.call_args.args[0], RuntimeError)

    async def test_broker_failure_does_not_write_accepted_checkpoint(self):
        self.stream.producer.send_and_wait.side_effect = RuntimeError('broker unavailable')
        with self.assertRaises(RuntimeError):
            await self.stream.ingest(Batch(readings=[self.point]), self.user)
        self.conn.execute.assert_not_awaited()

    def test_optional_motion_and_mismatched_legacy_trip(self):
        old = {**self.flat, 'motion':{'mode':'route','index':0}, 'route':['G','H']}
        self.assertEqual(reading_from_event(old),self.point)
        bad = {**self.legacy, 'point': {**self.point, 'trip_id': str(self.user)}}
        with self.assertRaises(ValueError):
            reading_from_event(bad)


if __name__ == '__main__':
    unittest.main()


class SeparateProcesses(unittest.IsolatedAsyncioTestCase):
    async def test_api_starts_producer_without_a_consumer(self):
        producer=SimpleNamespace(start=AsyncMock(),stop=AsyncMock())
        with patch.dict('os.environ',{'KAFKA_CREATE_TOPIC':'false'}), \
             patch('cab_watch.stream.options',return_value={}), \
             patch('cab_watch.stream.AIOKafkaProducer',return_value=producer), \
             patch('cab_watch.stream.AIOKafkaConsumer') as consumer:
            stream=Stream(MagicMock())
            await stream.start()
            producer.start.assert_awaited_once()
            consumer.assert_not_called()
            self.assertEqual(stream.tasks,[])
            await stream.stop()
            producer.stop.assert_awaited_once()

    async def test_worker_starts_consumer_without_a_producer(self):
        import asyncio
        consumer=SimpleNamespace(start=AsyncMock(),stop=AsyncMock())
        async def wait_forever(_):
            await asyncio.Event().wait()
        with patch.dict('os.environ',{'KAFKA_CREATE_TOPIC':'false','HISTORY_CONSUMERS':'1'}), \
             patch('cab_watch.stream.options',return_value={}), \
             patch('cab_watch.stream.AIOKafkaProducer') as producer, \
             patch('cab_watch.stream.AIOKafkaConsumer',return_value=consumer) as factory:
            stream=Stream(MagicMock(),role='consumer')
            stream.consume=wait_forever
            await stream.start()
            await asyncio.sleep(0)
            producer.assert_not_called()
            consumer.start.assert_awaited_once()
            self.assertFalse(factory.call_args.kwargs['enable_auto_commit'])
            self.assertEqual(len(stream.tasks),1)
            await stream.stop()
            consumer.stop.assert_awaited_once()
            self.assertTrue(stream.tasks[0].cancelled())


class ThreeConsumers(unittest.IsolatedAsyncioTestCase):
    async def test_default_worker_has_three_independent_clients(self):
        import asyncio
        clients=[]
        def make(*args,**kwargs):
            client=SimpleNamespace(start=AsyncMock(),stop=AsyncMock())
            clients.append(client)
            return client
        async def wait_forever(client):
            await asyncio.Event().wait()
        with patch.dict('os.environ',{'KAFKA_CREATE_TOPIC':'false','HISTORY_CONSUMERS':'3','KAFKA_BOOTSTRAP_SERVERS':'unused:9092'}), \
             patch('cab_watch.stream.AIOKafkaConsumer',side_effect=make):
            stream=Stream(MagicMock(),role='consumer')
            stream.consume=wait_forever
            await stream.start()
            self.assertEqual(len(clients),3)
            self.assertEqual(len(stream.tasks),3)
            await stream.stop()
            for client in clients:
                client.start.assert_awaited_once()
                client.stop.assert_awaited_once()
