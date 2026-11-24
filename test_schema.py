"""PostgreSQL migration checks, isolated in a random schema per test.

Set CABWATCH_TEST_DATABASE_URL to a disposable test database.
"""
import json
import os
from pathlib import Path
import unittest
from uuid import UUID, uuid4

import asyncpg
from cab_watch.api import apply_schema

ROOT = Path(__file__).parent
PLAN = json.dumps(dict(to_pickup=['A', 'G'], customer_ride=['G', 'K'], to_depot=['K', 'E']))


@unittest.skipUnless(os.getenv('CABWATCH_TEST_DATABASE_URL'), 'Requires a test PostgreSQL database')
class TripSchema(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.namespace = 'cw_schema_test_' + uuid4().hex
        self.db = await asyncpg.connect(os.environ['CABWATCH_TEST_DATABASE_URL'])

    async def asyncTearDown(self):
        try:
            await self.db.execute(f'DROP SCHEMA IF EXISTS {self.namespace} CASCADE')
        finally:
            await self.db.close()

    async def sql(self, query, *args):
        return await self.db.fetch(query.replace('cab_watch_accounts', self.namespace), *args)

    async def migrate(self, legacy=False):
        path = ROOT / ('tests/fixtures/pre_trip_idempotency.sql' if legacy else 'cab_watch/schema.sql')
        async with self.db.transaction():
            if legacy:
                await self.db.execute(path.read_text().replace('cab_watch_accounts', self.namespace))
            else:
                await apply_schema(self.db, self.namespace)

    async def seed(self, legacy=False):
        self.a, self.b = uuid4(), uuid4()
        for i, user in enumerate((self.a, self.b)):
            await self.sql("""INSERT INTO cab_watch_accounts.users(id,name,email,password_hash,jwt_key)
                VALUES($1,'Test',$2,'not-a-login',$3)""", user, f'{i}@test.invalid', b'x' * 32)
        self.cars = []
        for i, user in enumerate((self.a, self.a, self.b)):
            if legacy:
                rows = await self.sql("""INSERT INTO cab_watch_accounts.vehicles(user_id,cab_number,parked_depot_id)
                    VALUES($1,$2,(SELECT id FROM cab_watch_accounts.depots WHERE node='A')) RETURNING id""", user, i + 1)
            else:
                rows = await self.sql("INSERT INTO cab_watch_accounts.vehicles(user_id) VALUES($1) RETURNING id", user)
            self.cars.append(rows[0]['id'])

    async def insert_trip(self, user, key, vehicle):
        return await self.sql("""INSERT INTO cab_watch_accounts.trips
            (id,user_id,idempotency_key,vehicle_id,planned_route,completed_at)
            VALUES($1,$2,$3,$4,$5::jsonb,now()) RETURNING id""", uuid4(), user, key, vehicle, '["G","H"]')

    async def test_fresh_schema_enforces_ownership_and_per_user_keys(self):
        await self.migrate()
        await self.seed()
        self.assertEqual(self.cars, ['CB#001', 'CB#002', 'CB#003'])
        await self.sql("SELECT setval('cab_watch_accounts.cb_plate_serial', 999)")
        self.assertEqual((await self.sql('SELECT cab_watch_accounts.next_plate() AS id'))[0]['id'], 'CB#1000')
        key = uuid4()
        trip = (await self.insert_trip(self.a, key, self.cars[0]))[0]['id']
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.insert_trip(self.a, key, self.cars[1])
        await self.insert_trip(self.b, key, self.cars[2])
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.insert_trip(self.b, uuid4(), self.cars[0])
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.sql('UPDATE cab_watch_accounts.trips SET vehicle_id=$2 WHERE id=$1', trip, self.cars[2])
        with self.assertRaises(asyncpg.NotNullViolationError):
            await self.insert_trip(self.a, None, self.cars[0])
        with self.assertRaises(asyncpg.NotNullViolationError):
            await self.insert_trip(None, uuid4(), self.cars[0])

    async def test_upgrade_preserves_history_keys_and_repeat_startup(self):
        await self.migrate(legacy=True)
        await self.seed(legacy=True)
        trip, old_put, key = uuid4(), uuid4(), uuid4()
        for tid, car in ((trip, self.cars[0]), (old_put, self.cars[1])):
            await self.sql("""INSERT INTO cab_watch_accounts.trips
                (id,vehicle_id,planned_legs,last_sequence,last_lat,last_lon,phase,completed_at)
                VALUES($1,$2,$3::jsonb,17,12.9,77.5,3,now())""", tid, car, PLAN)
        await self.sql('INSERT INTO cab_watch_accounts.dispatch_requests VALUES($1,$2,$3)', self.a, key, trip)
        await self.sql("""INSERT INTO cab_watch_accounts.alerts(trip_id,opened_sequence,peak_metres)
            VALUES($1,15,270)""", trip)
        before = await self.sql('SELECT * FROM cab_watch_accounts.trips ORDER BY id')
        alerts = await self.sql('SELECT * FROM cab_watch_accounts.alerts')
        await self.migrate()
        after = await self.sql('SELECT * FROM cab_watch_accounts.trips ORDER BY id')
        tables = await self.sql("""SELECT table_name FROM information_schema.tables
            WHERE table_schema='cab_watch_accounts' AND table_type='BASE TABLE'""")
        self.assertEqual({r['table_name'] for r in tables}, {'users','vehicles','trips','alerts'})
        for original, updated in zip(before, after):
            for column in set(original.keys()) - {'motion','planned_legs','distance_m','phase'}:
                self.assertEqual(original[column], updated[column])
            self.assertEqual(json.loads(updated['planned_route']), ['G','K'])
            self.assertIsNone(updated['motion'])
            self.assertEqual(updated['user_id'], self.a)
            self.assertIsNotNone(updated['idempotency_key'])
        mapped = next(r for r in after if r['id'] == trip)
        self.assertEqual(mapped['idempotency_key'], key)
        for old, new in zip(alerts, await self.sql('SELECT * FROM cab_watch_accounts.alerts')):
            self.assertEqual(dict(old), {k:new[k] for k in old.keys()})
        self.assertIsNone((await self.sql("SELECT to_regclass('cab_watch_accounts.dispatch_requests') AS table_name"))[0]['table_name'])
        await self.migrate()
        self.assertEqual(after, await self.sql('SELECT * FROM cab_watch_accounts.trips ORDER BY id'))

    async def test_convert_active_depot_trips_by_their_saved_phase(self):
        await self.migrate()
        await self.seed()
        await self.sql("UPDATE cab_watch_accounts.vehicles SET status='busy'")
        for phase, (user, car) in enumerate(zip((self.a, self.a, self.b), self.cars)):
            motion = json.dumps({"index": 1, "mode": "route", "legacy": {"phase": phase}})
            tid = uuid4()
            await self.sql("""INSERT INTO cab_watch_accounts.trips
                (id,user_id,idempotency_key,vehicle_id,planned_route,last_sequence,last_lat,last_lon,motion)
                VALUES($1,$2,$3,$4,'["G","H","K"]',8,13,80,$5::jsonb)""",
                tid, user, uuid4(), car, motion)
            await self.sql("""INSERT INTO cab_watch_accounts.alerts(trip_id,opened_sequence,peak_metres)
                VALUES($1,6,250)""", tid)
        await self.migrate()
        query = """SELECT t.*, v.status, a.closed_sequence FROM cab_watch_accounts.trips t
            JOIN cab_watch_accounts.vehicles v ON v.id=t.vehicle_id
            JOIN cab_watch_accounts.alerts a ON a.trip_id=t.id ORDER BY v.id"""
        pickup, riding, finished = before = await self.sql(query)
        self.assertIsNone(pickup["last_lat"])
        self.assertEqual(json.loads(pickup["motion"]), {"mode":"route","index":0})
        self.assertEqual((pickup["status"], pickup["closed_sequence"]), ("busy", 8))
        self.assertEqual(json.loads(riding["motion"]), {"mode":"route","index":1})
        self.assertEqual((riding["last_lat"], riding["status"], riding["closed_sequence"]), (13, "busy", None))
        self.assertIsNotNone(finished["completed_at"])
        self.assertEqual((finished["status"], finished["closed_sequence"]), ("available", 8))
        await self.migrate()
        self.assertEqual(before, await self.sql(query))

    async def test_invalid_mapping_rolls_back_without_dropping_original(self):
        await self.migrate(legacy=True)
        await self.seed(legacy=True)
        tid, key = uuid4(), uuid4()
        await self.sql("""INSERT INTO cab_watch_accounts.trips(id,vehicle_id,planned_legs)
            VALUES($1,$2,$3::jsonb)""", tid, self.cars[0], PLAN)
        await self.sql('INSERT INTO cab_watch_accounts.dispatch_requests VALUES($1,$2,$3)', self.b, key, tid)
        with self.assertRaisesRegex(asyncpg.RaiseError, 'Dispatch mappings disagree'):
            await self.migrate()
        rows = await self.sql('SELECT * FROM cab_watch_accounts.dispatch_requests')
        self.assertEqual(rows[0]['request_key'], key)
        self.assertEqual(rows[0]['trip_id'], tid)
        self.assertNotIn('user_id', (await self.sql('SELECT * FROM cab_watch_accounts.trips'))[0].keys())

    async def test_five_table_migration_copies_all_readings_and_is_repeatable(self):
        await self.migrate()
        await self.seed()
        tid=(await self.insert_trip(self.a,uuid4(),self.cars[0]))[0]['id']
        await self.sql("""CREATE TABLE cab_watch_accounts.telemetry_history (
            trip_id uuid,sequence integer,captured_at timestamptz,lat float8,lon float8,motion jsonb,
            PRIMARY KEY(trip_id,sequence))""")
        await self.sql("""INSERT INTO cab_watch_accounts.telemetry_history VALUES
            ($1,1,'2020-01-01T00:00:01Z',13.0,80.0,null),
            ($1,2,'2020-01-01T00:00:02Z',13.0,80.0,null),
            ($1,3,'2020-01-01T00:00:03Z',13.0,80.0,null)""",tid)
        await self.migrate()
        before=await self.sql('SELECT * FROM cab_watch_accounts.trips WHERE id=$1',tid)
        self.assertEqual(len(json.loads(before[0]['readings'])),3)
        self.assertEqual((await self.sql('SELECT opened_at FROM cab_watch_accounts.alerts WHERE historical'))[0]['opened_at'].year,2020)
        await self.migrate()
        self.assertEqual(before,await self.sql('SELECT * FROM cab_watch_accounts.trips WHERE id=$1',tid))
        tables=await self.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='cab_watch_accounts' AND table_type='BASE TABLE'")
        self.assertEqual({r['table_name'] for r in tables},{'users','vehicles','trips','alerts'})

    async def test_transaction_rollback_and_duplicate_conflicts(self):
        from cab_watch.journey import persist_readings
        from cab_watch import city
        await self.migrate();await self.seed()
        tid=(await self.insert_trip(self.a,uuid4(),self.cars[0]))[0]['id']
        outer=self
        class Conn:
            async def fetchrow(self,q,*args):
                return await outer.db.fetchrow(q.replace('cab_watch_accounts',outer.namespace),*args)
            async def execute(self,q,*args):
                return await outer.db.execute(q.replace('cab_watch_accounts',outer.namespace),*args)
        conn=Conn()
        lat,lon=city.gps(city.NODES['D'])
        def point(i):return dict(sequence=i,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:0{i}Z')
        async with self.db.transaction():await persist_readings(conn,tid,[point(1),point(3)])
        self.assertEqual(await self.sql('SELECT * FROM cab_watch_accounts.alerts WHERE historical'),[])
        with self.assertRaises(RuntimeError):
            async with self.db.transaction():
                await persist_readings(conn,tid,[point(2)])
                raise RuntimeError('crash before transaction commit')
        self.assertEqual(len(json.loads((await self.sql('SELECT readings FROM cab_watch_accounts.trips WHERE id=$1',tid))[0]['readings'])),2)
        async with self.db.transaction():await persist_readings(conn,tid,[point(2)])
        before=await self.sql('SELECT * FROM cab_watch_accounts.alerts WHERE historical')
        async with self.db.transaction():await persist_readings(conn,tid,[dict(point(2),lat=0,lon=0)])
        self.assertEqual(before,await self.sql('SELECT * FROM cab_watch_accounts.alerts WHERE historical'))
        self.assertEqual(json.loads((await self.sql('SELECT readings FROM cab_watch_accounts.trips WHERE id=$1',tid))[0]['readings'])['2']['lat'],lat)
