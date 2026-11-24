"""One Kafka consumer group for persisted journey and deviation history. No Redis."""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import logging
import os
from uuid import UUID
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import CommitFailedError, TopicAlreadyExistsError
from fastapi import HTTPException
from prometheus_client import Counter, Gauge
from .kafka_options import options
from .journey import persist_readings
from .live import milliseconds

log = logging.getLogger(__name__)
RETRIES = Counter('cabwatch_worker_retries', 'Journey worker retry attempts', ('worker',))
LAG = Gauge('cabwatch_consumer_lag', 'Offset lag after committed journey transaction', ('worker','partition'))


def reading_from_event(event):
    """Keep stored readings stable while accepting old and flat Kafka records."""
    point = event['point'] if 'point' in event else event
    if point.get('trip_id', event['trip_id']) != event['trip_id']:
        raise ValueError('Telemetry reading trip differs from envelope')
    return dict(trip_id=event['trip_id'], sequence=point['sequence'],
                captured_at=point['captured_at'], lat=point['lat'], lon=point['lon'],
                motion=point.get('motion'))


class Stream:
    def __init__(self, db, role="producer"):
        if role not in ("producer", "consumer"):
            raise ValueError("Stream role must be producer or consumer")
        self.role = role
        self.db = db
        self.topic = os.getenv('KAFKA_TOPIC', 'cabwatch.gps.recovery.v1')
        self.group = os.getenv('KAFKA_GROUP', self.topic + '.journey-history-v1')
        self.producer = (AIOKafkaProducer(**options(), enable_idempotence=True, acks='all', linger_ms=5)
                         if role == 'producer' else None)
        self.tasks, self.consumers = [], []

    async def ingest(self, batch, user):
        ids = [p.trip_id for p in batch.readings]
        if len(set(ids)) != len(ids):
            raise HTTPException(422, 'Send one reading per trip in each batch')
        rows = await self.db.fetch('SELECT id,vehicle_id,planned_route FROM cab_watch_accounts.trips WHERE user_id=$1 AND id=ANY($2::uuid[])', user, ids)
        trips = {r['id']:r for r in rows}
        if len(trips) != len(ids):
            raise HTTPException(422, 'Send readings only for trips in your fleet')
        now = datetime.now(timezone.utc)
        events = []
        for point in batch.readings:
            p = point.model_dump(mode='json')
            p['captured_at'] = p['captured_at'] or now.isoformat()
            if milliseconds(p['captured_at']) > int(now.timestamp()*1000)+2000:
                raise HTTPException(422, 'Capture time is too far in the future')
            row = trips[point.trip_id]
            events.append(dict(p, user_id=str(user), vehicle_id=row['vehicle_id'],
                               route=json.loads(row['planned_route'])))
        await asyncio.gather(*(self.producer.send_and_wait(self.topic,json.dumps(e).encode(),key=e['vehicle_id'].encode()) for e in events))
        # Accepted checkpoint survives reloads even while the consumer is behind.
        # Failure after Kafka ACK returns 503; retry the identical trip/sequence.
        async with self.db.acquire() as conn, conn.transaction():
            for e in sorted(events,key=lambda e:e['trip_id']):
                await conn.execute("""UPDATE cab_watch_accounts.trips SET accepted_point=$2::jsonb
                    WHERE id=$1 AND coalesce((accepted_point->>'sequence')::integer,0)<$3""",
                    UUID(e['trip_id']),json.dumps(reading_from_event(e)),e['sequence'])
        return dict(mode='kafka',queued=True,results=[dict(trip_id=e['trip_id'],sequence=e['sequence'],
                    queued=True,applied=False) for e in events])

    async def history(self, records):
        grouped = defaultdict(list)
        for record in records:
            e = json.loads(record.value)
            if record.key.decode() != e['vehicle_id']:
                raise ValueError('Kafka key differs from vehicle ID')
            grouped[UUID(e['trip_id'])].append(e)
        async with self.db.acquire() as conn, conn.transaction():
            for tid in sorted(grouped):
                events=grouped[tid]
                row=await conn.fetchrow('SELECT user_id,vehicle_id FROM cab_watch_accounts.trips WHERE id=$1',tid)
                if row is None:
                    continue
                if any(e['vehicle_id']!=row['vehicle_id'] or e['user_id']!=str(row['user_id']) for e in events):
                    raise ValueError('Telemetry ownership differs from stored trip')
                await persist_readings(conn,tid,[reading_from_event(e) for e in events])

    async def consume(self, consumer):
        while True:
            try:
                batches=await consumer.getmany(timeout_ms=500,max_records=500)
                for partition,records in batches.items():
                    if not records:
                        continue
                    while partition in consumer.assignment():
                        try:
                            await self.history(records)
                            await consumer.commit({partition:records[-1].offset+1})
                            high=consumer.highwater(partition)
                            if high is not None:
                                LAG.labels('history',str(partition.partition)).set(max(0,high-records[-1].offset-1))
                            break
                        except CommitFailedError:
                            break  # New owner replays; SQL transaction is idempotent.
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            RETRIES.labels('history').inc()
                            log.exception('History retry; offset not committed')
                            await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('History poll failed')
                await asyncio.sleep(1)

    async def start(self):
        if os.getenv('KAFKA_CREATE_TOPIC','true').lower() == 'true':
            admin=AIOKafkaAdminClient(**options())
            await admin.start()
            try:
                try:
                    await admin.create_topics([NewTopic(self.topic,int(os.getenv('KAFKA_PARTITIONS','12')),int(os.getenv('KAFKA_REPLICATION_FACTOR','1')))])
                except TopicAlreadyExistsError:
                    pass
            finally:
                await admin.close()
        if self.role == 'producer':
            await self.producer.start()
            return
        count=int(os.getenv('HISTORY_CONSUMERS','1'))
        if count<1:
            raise ValueError('HISTORY_CONSUMERS must be positive')
        for _ in range(count):
            c=AIOKafkaConsumer(self.topic,**options(),group_id=self.group,enable_auto_commit=False,auto_offset_reset='earliest',max_poll_records=500)
            self.consumers.append(c)
            await c.start()
            self.tasks.append(asyncio.create_task(self.consume(c)))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        await asyncio.gather(*(c.stop() for c in self.consumers),return_exceptions=True)
        if self.producer is not None:
            await asyncio.wait_for(self.producer.stop(),10)

@asynccontextmanager
async def runtime(app):
    stream=Stream(app.state.db)
    try:
        await stream.start()
        app.state.pipeline=stream
        yield stream
    finally:
        await stream.stop()
