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
                          motion=dict(mode='detour', trail=['I'], target='D'))
        self.metadata = dict(trip_id=str(self.trip), user_id=str(self.user),
                             vehicle_id='CB#001', route=['G', 'H', 'I', 'J', 'K'])
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
                                                   planned_route=json.dumps(self.metadata['route']))])
        # Avoid constructing a real producer: exercise the real ingest/history methods.
        self.stream = object.__new__(Stream)
        self.stream.db = self.db
        self.stream.topic = 'test.gps'
        self.stream.producer = SimpleNamespace(send_and_wait=AsyncMock())

    def record(self, event, key=b'CB#001'):
        return SimpleNamespace(value=json.dumps(event).encode(), key=key)

    async def test_producer_emits_flat_record_and_preserves_checkpoint(self):
        receipt = await self.stream.ingest(Batch(readings=[self.point]), self.user)
        args = self.stream.producer.send_and_wait.call_args
        self.assertEqual(args.args[0], 'test.gps')
        self.assertEqual(json.loads(args.args[1]), self.flat)
        self.assertEqual(args.kwargs['key'], b'CB#001')
        saved = self.conn.execute.call_args.args
        self.assertEqual(json.loads(saved[2]), self.point)
        self.assertEqual(saved[3], 1)
        self.assertEqual(receipt['results'][0]['sequence'], 1)
        self.assertTrue(receipt['queued'])

    async def test_mixed_old_and_new_records_keep_reading_shape(self):
        second = {**self.flat, 'sequence': 2}
        with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock) as persist:
            await self.stream.history([self.record(self.legacy), self.record(second)])
            persist.assert_awaited_once_with(self.conn, self.trip,
                                            [self.point, {**self.point, 'sequence': 2}])

    async def test_legacy_and_flat_records_normalize_identically(self):
        self.assertEqual(reading_from_event(self.flat), reading_from_event(self.legacy))
        self.assertEqual(reading_from_event(self.flat), self.point)
        for event in (self.flat, self.legacy):
            with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock) as persist:
                await self.stream.history([self.record(event)])
                persist.assert_awaited_once_with(self.conn, self.trip, [self.point])

    async def test_key_and_ownership_checks_still_reject_invalid_records(self):
        invalid = [self.record(self.flat, b'CB#002'),
                   self.record({**self.flat, 'user_id': str(self.trip)})]
        for record in invalid:
            with patch('cab_watch.stream.persist_readings', new_callable=AsyncMock) as persist:
                with self.assertRaises(ValueError):
                    await self.stream.history([record])
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
        no_motion = {k: v for k, v in self.flat.items() if k != 'motion'}
        self.assertIsNone(reading_from_event(no_motion)['motion'])
        bad = {**self.legacy, 'point': {**self.point, 'trip_id': str(self.user)}}
        with self.assertRaises(ValueError):
            reading_from_event(bad)


if __name__ == '__main__':
    unittest.main()
