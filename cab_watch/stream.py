"""Kafka ingestion, independent live/history groups, and a durable Redis event outbox."""
import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import logging
import os
import ssl
import time
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import CommitFailedError, TopicAlreadyExistsError
from fastapi import HTTPException
from prometheus_client import Counter, Gauge, Histogram
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .live import advance, milliseconds

log = logging.getLogger(__name__)
AGE = Histogram("cabwatch_capture_to_live_seconds", "Capture to published live state", buckets=(.1,.5,1,2,5,10,30))
ALERT_AGE = Histogram("cabwatch_capture_to_alert_seconds", "Confirming reading to published alert", buckets=(.1,.5,1,2,5,10,30))
STALE = Counter("cabwatch_stale_readings", "Readings excluded from live detection")
WARNINGS = Counter("cabwatch_latency_warnings", "Published states older than five seconds")
HISTORY = Counter("cabwatch_history_readings", "History input processed, including replay attempts")
RETRIES = Counter("cabwatch_worker_retries", "Worker retry attempts", ("worker",))
LAG = Gauge("cabwatch_consumer_lag", "Offset lag after successful persistence", ("worker","partition"))

SAVE = """
local old=redis.call('GET',KEYS[1])
if old and cjson.decode(old).sequence >= tonumber(ARGV[1]) then return 0 end
redis.call('SET',KEYS[1],ARGV[2],'EX',7776000)
for _,e in ipairs(cjson.decode(ARGV[3])) do
 redis.call('XADD',KEYS[2],'*','event',cjson.encode(e))
end
return 1
"""
ACCEPTED = """
local old=redis.call('GET',KEYS[1])
if not old or cjson.decode(old).sequence < tonumber(ARGV[1]) then
 redis.call('SET',KEYS[1],ARGV[2],'EX',7776000)
end
return 1
"""


def options():
    result = {"bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    result["security_protocol"] = protocol
    if protocol in ("SSL", "SASL_SSL"):
        result["ssl_context"] = ssl.create_default_context(cadata=os.getenv("KAFKA_SSL_CA_PEM"))
    if protocol.startswith("SASL"):
        result.update(sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.environ["KAFKA_USERNAME"], sasl_plain_password=os.environ["KAFKA_PASSWORD"])
    return result


class Stream:
    def __init__(self, db):
        self.db = db
        self.topic = os.getenv("KAFKA_TOPIC", "cabwatch.gps.recovery.v1")
        self.redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True,
                                   socket_connect_timeout=3, socket_timeout=5)
        self.prefix = os.getenv("REDIS_KEY_PREFIX", "cabwatch:")
        self.outbox = self.prefix + "events"
        self.consumer_group = "sql-events-v1"
        self.consumer_name = str(uuid4())
        self.cache = OrderedDict()
        self.key_cache = OrderedDict()
        self.tasks = []
        self.consumers = []
        self.stale_ms = int(os.getenv("LIVE_STALE_MS", "10000"))
        if self.stale_ms <= 0:
            raise ValueError("LIVE_STALE_MS must be positive")
        self.producer = AIOKafkaProducer(**options(), enable_idempotence=True, acks="all", linger_ms=5)

    async def metadata(self, ids, user):
        now = time.monotonic()
        missing = [i for i in ids if i not in self.cache or self.cache[i][0] < now]
        if missing:
            for i in missing:
                self.cache.pop(i, None)
            rows = await self.db.fetch("SELECT * FROM cab_watch_accounts.trips WHERE user_id=$1 AND id=ANY($2::uuid[])", user, missing)
            for row in rows:
                self.cache[row["id"]] = (now + 300, dict(row))
                self.cache.move_to_end(row["id"])
            while len(self.cache) > 10000:
                self.cache.popitem(last=False)
        result = {}
        for i in ids:
            entry = self.cache.get(i)
            if not entry or entry[1]["user_id"] != user:
                raise HTTPException(422, "Send readings only for trips in your fleet")
            result[i] = entry[1]
        return result

    async def seed(self, rows, user):
        if not rows:
            return
        p = self.redis.pipeline(transaction=False)
        for row in rows:
            self.cache[row["id"]] = (time.monotonic() + 300, dict(row))
            if row["completed_at"]:
                p.zrem(self.prefix + "fleet:" + str(user), str(row["id"]))
            else:
                p.zadd(self.prefix + "fleet:" + str(user), {str(row["id"]): time.time()})
        p.expire(self.prefix + "fleet:" + str(user), 7776000)
        await p.execute()
        while len(self.cache) > 10000:
            self.cache.popitem(last=False)

    async def ingest(self, batch, user):
        ids = [p.trip_id for p in batch.readings]
        if len(set(ids)) != len(ids):
            raise HTTPException(422, "Send one reading per trip in each batch")
        rows = await self.metadata(ids, user)
        now = datetime.now(timezone.utc)
        events = []
        for point in batch.readings:
            p = point.model_dump(mode="json")
            p["captured_at"] = p["captured_at"] or now.isoformat()
            if milliseconds(p["captured_at"]) > int(now.timestamp()*1000) + 2000:
                raise HTTPException(422, "Capture time is too far in the future")
            row = rows[point.trip_id]
            events.append(dict(trip_id=str(point.trip_id), user_id=str(user), vehicle_id=row["vehicle_id"],
                               route=json.loads(row["planned_route"]), point=p))
        # Ownership and every point are validated before any send. A partial broker
        # acknowledgment is possible on failure; replay uses stable trip/sequence IDs.
        await asyncio.gather(*(self.producer.send_and_wait(self.topic, json.dumps(e).encode(),
                               key=e["vehicle_id"].encode()) for e in events))
        p = self.redis.pipeline(transaction=False)
        for e in events:
            point = e["point"]
            p.eval(ACCEPTED, 1, self.prefix+"accepted:"+e["trip_id"], point["sequence"], json.dumps(point))
            p.zadd(self.prefix+"fleet:"+str(user), {e["trip_id"]: time.time()})
        p.expire(self.prefix+"fleet:"+str(user), 7776000)
        await p.execute()
        return dict(mode="kafka", queued=True, results=[dict(trip_id=e["trip_id"], sequence=e["point"]["sequence"],
                    queued=True, applied=False, distance_m=0, completed=False, alert=None) for e in events])

    async def snapshot(self, user):
        fleet_key = self.prefix+"fleet:"+str(user)
        await self.redis.zremrangebyscore(fleet_key, "-inf", time.time()-7776000)
        ids = await self.redis.zrange(fleet_key, 0, -1)
        if not ids:
            return dict(enabled=True, states=[], alerts=[])
        values = await self.redis.mget([self.prefix+"live:"+i for i in ids])
        states, alerts = [], {}
        now = int(time.time()*1000)
        for i, value in zip(ids, values):
            if not value:
                states.append(dict(trip_id=i, sequence=0, tracking="DELAYED", off_route=False))
                continue
            s = json.loads(value)
            if s["user_id"] != str(user):
                continue
            delayed = not s.get("completed") and (not s.get("captured_at") or now-milliseconds(s["captured_at"]) > self.stale_ms or s["tracking"]=="DELAYED")
            s["delayed"] = delayed
            if delayed:
                s.update(tracking="DELAYED", off_route=False)
            states.append(s)
            for a in s.get("recent", []) + ([s["alert"]] if s.get("alert") else []):
                alerts[a["id"]] = {**a, "tracking_delayed": delayed and a["closed_sequence"] is None}
        return dict(enabled=True, states=states, alerts=sorted(alerts.values(), key=lambda a:a["opened_at"], reverse=True)[:1000])

    async def live(self, records):
        events = [json.loads(r.value) for r in records]
        ids = list(dict.fromkeys(e["trip_id"] for e in events))
        values = await self.redis.mget([self.prefix+"live:"+i for i in ids])
        states = {i: json.loads(v) if v else None for i,v in zip(ids,values)}
        transitions = {i: [] for i in ids}
        for event, record in zip(events, records):
            if record.key.decode() != event["vehicle_id"]:
                raise ValueError("Kafka key differs from vehicle ID")
            before = states[event["trip_id"]]
            state, changes = advance(before, event, int(time.time()*1000), self.stale_ms)
            if state is not before and state["tracking"] == "DELAYED":
                STALE.inc()
            states[event["trip_id"]] = state
            transitions[event["trip_id"]].extend(changes)
        p = self.redis.pipeline(transaction=False)
        now = int(time.time()*1000)
        for i in ids:
            state = states[i]
            # A slow Redis recovery must not publish a once-fresh reading as live.
            if not state.get("completed") and state.get("captured_at") and now-milliseconds(state["captured_at"]) > self.stale_ms:
                state.update(tracking="DELAYED", off_route=False, consecutive=0)
            p.eval(SAVE, 2, self.prefix+"live:"+i, self.outbox, state["sequence"], json.dumps(state), json.dumps(transitions[i]))
        applied = await p.execute()
        published = int(time.time()*1000)
        for i, changed in zip(ids, applied):
            if not changed:
                continue
            s = states[i]
            if s["tracking"] != "DELAYED" and s.get("captured_at"):
                age = max(0, published-milliseconds(s["captured_at"])) / 1000
                AGE.observe(age)
                if age > 5:
                    WARNINGS.inc()
            for e in transitions[i]:
                if e.get("confirming_capture"):
                    ALERT_AGE.observe(max(0,published-milliseconds(e["confirming_capture"]))/1000)

    async def history(self, records):
        events = [json.loads(r.value) for r in records]
        async with self.db.acquire() as conn, conn.transaction():
            await conn.executemany("""INSERT INTO cab_watch_accounts.telemetry_history
                (trip_id,sequence,captured_at,lat,lon,motion) VALUES($1,$2,$3,$4,$5,$6::jsonb)
                ON CONFLICT(trip_id,sequence) DO NOTHING""", [
                (UUID(e["trip_id"]), e["point"]["sequence"], datetime.fromisoformat(e["point"]["captured_at"]),
                 e["point"]["lat"], e["point"]["lon"], json.dumps(e["point"].get("motion"))) for e in events])
            latest = {}
            for e in events:
                if e["point"]["sequence"] > latest.get(e["trip_id"], {}).get("point", {}).get("sequence", -1):
                    latest[e["trip_id"]] = e
            await conn.executemany("""UPDATE cab_watch_accounts.trips SET last_sequence=$2,last_lat=$3,last_lon=$4,
                last_captured_at=$5,motion=$6::jsonb WHERE id=$1 AND last_sequence<$2""", [
                (UUID(e["trip_id"]),e["point"]["sequence"],e["point"]["lat"],e["point"]["lon"],
                 datetime.fromisoformat(e["point"]["captured_at"]),json.dumps(e["point"].get("motion"))) for e in latest.values()])
        HISTORY.inc(len(events))

    async def consume(self, consumer, worker, handler):
        while True:
            try:
                batches = await consumer.getmany(timeout_ms=500, max_records=500)
                for partition, records in batches.items():
                    if not records:
                        continue
                    while partition in consumer.assignment():
                        try:
                            await handler(records)
                            await consumer.commit({partition:records[-1].offset+1})
                            high = consumer.highwater(partition)
                            if high is not None:
                                LAG.labels(worker,str(partition.partition)).set(max(0,high-records[-1].offset-1))
                            break
                        except CommitFailedError:
                            # Persistence is idempotent; the new partition owner replays.
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            RETRIES.labels(worker).inc()
                            log.exception("%s consumer retry; offsets remain uncommitted", worker)
                            await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                RETRIES.labels(worker).inc()
                log.exception("%s poll failed", worker)
                await asyncio.sleep(1)

    async def persist_event(self, conn, event):
        if event["kind"] == "reset":
            await conn.execute("""UPDATE cab_watch_accounts.alerts
                SET closed_sequence=greatest(opened_sequence,$2),resolved_at=now(),close_reason='tracking restarted'
                WHERE trip_id=$1 AND closed_sequence IS NULL AND opened_sequence<=$2""", UUID(event["trip_id"]),event["sequence"])
            return
        if event["kind"] == "complete":
            row = await conn.fetchrow("""UPDATE cab_watch_accounts.trips SET completed_at=coalesce(completed_at,now())
                WHERE id=$1 RETURNING vehicle_id""",UUID(event["trip_id"]))
            if row:
                await conn.execute("""UPDATE cab_watch_accounts.vehicles SET status='available' WHERE id=$1
                    AND NOT EXISTS(SELECT FROM cab_watch_accounts.trips WHERE vehicle_id=$1 AND completed_at IS NULL)""",row["vehicle_id"])
            return
        a = event["alert"]
        # Reconcile a pre-Kafka open incident or a replay before inserting a new one.
        await conn.execute("""UPDATE cab_watch_accounts.alerts SET closed_sequence=$2,resolved_at=now(),close_reason='confirmation reset'
            WHERE trip_id=$1 AND closed_sequence IS NULL AND event_key IS DISTINCT FROM $3
            AND opened_sequence<=$2""",UUID(a["trip_id"]),a["opened_sequence"],UUID(a["id"]))
        await conn.execute("""INSERT INTO cab_watch_accounts.alerts
            (trip_id,opened_sequence,closed_sequence,peak_metres,opened_at,resolved_at,event_key,close_reason)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(event_key) DO UPDATE SET
            peak_metres=greatest(cab_watch_accounts.alerts.peak_metres,excluded.peak_metres),
            closed_sequence=coalesce(excluded.closed_sequence,cab_watch_accounts.alerts.closed_sequence),
            resolved_at=coalesce(excluded.resolved_at,cab_watch_accounts.alerts.resolved_at),
            close_reason=coalesce(excluded.close_reason,cab_watch_accounts.alerts.close_reason)""",
            UUID(a["trip_id"]),a["opened_sequence"],a["closed_sequence"],a["peak_metres"],
            datetime.fromisoformat(a["opened_at"]),datetime.fromisoformat(a["resolved_at"]) if a["resolved_at"] else None,
            UUID(a["id"]),a.get("close_reason"))

    async def persist_events(self):
        while True:
            try:
                messages = []
                # PostgreSQL serializes event writers across app workers. Claim all
                # older pending records before new entries, preserving stream order.
                async with self.db.acquire() as conn, conn.transaction():
                    if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(73949205)"):
                        await asyncio.sleep(.2)
                        continue
                    claimed = await self.redis.xautoclaim(self.outbox, self.consumer_group, self.consumer_name, 0, "0-0", count=100)
                    messages = claimed[1]
                    if not messages:
                        rows = await self.redis.xreadgroup(self.consumer_group,self.consumer_name,{self.outbox:">"},count=100,block=200)
                        messages = rows[0][1] if rows else []
                    for mid, fields in messages:
                        await self.persist_event(conn, json.loads(fields["event"]))
                # A crash here replays committed SQL idempotently, never loses events.
                if messages:
                    p = self.redis.pipeline()
                    for mid, _ in messages:
                        p.xack(self.outbox,self.consumer_group,mid)
                        p.xdel(self.outbox,mid)
                    await p.execute()
            except asyncio.CancelledError:
                raise
            except Exception:
                RETRIES.labels("events").inc()
                log.exception("Alert/completion persistence will retry")
                await asyncio.sleep(1)

    async def start(self):
        await self.redis.ping()
        try:
            await self.redis.xgroup_create(self.outbox,self.consumer_group,id="0",mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        if os.getenv("KAFKA_CREATE_TOPIC", "true").lower() == "true":
            admin = AIOKafkaAdminClient(**options())
            await admin.start()
            try:
                try:
                    await admin.create_topics([NewTopic(self.topic, int(os.getenv("KAFKA_PARTITIONS","12")), int(os.getenv("KAFKA_REPLICATION_FACTOR","1")))])
                except TopicAlreadyExistsError:
                    pass
            finally:
                await admin.close()
        await self.producer.start()
        for worker, handler, env in (("live",self.live,"LIVE_CONSUMERS"),("history",self.history,"HISTORY_CONSUMERS")):
            count = int(os.getenv(env,"2"))
            if count < 1:
                raise ValueError(f"{env} must be positive")
            for _ in range(count):
                consumer = AIOKafkaConsumer(self.topic, **options(), group_id=f"{self.topic}.{worker}",
                    enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=500)
                self.consumers.append(consumer)
                await consumer.start()
                self.tasks.append(asyncio.create_task(self.consume(consumer,worker,handler)))
        self.tasks.append(asyncio.create_task(self.persist_events()))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        await asyncio.gather(*(c.stop() for c in self.consumers),return_exceptions=True)
        try:
            await asyncio.wait_for(self.producer.stop(), 10)
        finally:
            await self.redis.aclose()


@asynccontextmanager
async def runtime(app):
    stream = Stream(app.state.db)
    try:
        await stream.start()
        app.state.pipeline = stream
        yield stream
    finally:
        await stream.stop()
