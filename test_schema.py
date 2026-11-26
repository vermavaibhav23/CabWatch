"""Real PostgreSQL transactions and migration; random disposable schema per test."""
import json, os, unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
import asyncpg
from cab_watch.api import apply_schema
from cab_watch.journey import persist_readings
from cab_watch import city

@unittest.skipUnless(os.getenv('CABWATCH_TEST_DATABASE_URL'),'Requires disposable PostgreSQL')
class TripSchema(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ns='cw_test_'+uuid4().hex
        self.db=await asyncpg.connect(os.environ['CABWATCH_TEST_DATABASE_URL'])
        outer=self
        class Conn:
            async def fetchrow(self,q,*args):return await outer.db.fetchrow(q.replace('cab_watch_accounts',outer.ns),*args)
            async def execute(self,q,*args):return await outer.db.execute(q.replace('cab_watch_accounts',outer.ns),*args)
        self.conn=Conn()
    async def asyncTearDown(self):
        await self.db.execute(f'DROP SCHEMA IF EXISTS {self.ns} CASCADE')
        await self.db.close()
    async def sql(self,q,*args):return await self.db.fetch(q.replace('cab_watch_accounts',self.ns),*args)
    async def migrate(self):
        async with self.db.transaction():await apply_schema(self.db,self.ns)
    async def seed(self,old=False):
        if old:
            await self.db.execute(Path('tests/fixtures/history_schema.sql').read_text().replace('cab_watch_accounts',self.ns))
        else:await self.migrate()
        self.user,self.trip=uuid4(),uuid4()
        await self.sql("INSERT INTO cab_watch_accounts.users(id,name,email,password_hash,jwt_key) VALUES($1,'Test','test@example.invalid','unchanged',$2)",self.user,b'x'*32)
        self.vehicle=(await self.sql("INSERT INTO cab_watch_accounts.vehicles(user_id,status) VALUES($1,'busy') RETURNING id",self.user))[0]['id']
        await self.sql("INSERT INTO cab_watch_accounts.trips(id,user_id,idempotency_key,vehicle_id,planned_route) VALUES($1,$2,$3,$4,$5::jsonb)",self.trip,self.user,uuid4(),self.vehicle,json.dumps(['G','H','I','J','K']))
    def point(self,seq,node='D',second=None):
        lat,lon=city.gps(city.NODES[node])
        return dict(sequence=seq,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:{second if second is not None else seq:02d}Z')
    async def save(self,*points):
        async with self.db.transaction():await persist_readings(self.conn,self.trip,list(points),self.user,self.vehicle)
    async def triprow(self):return (await self.sql('SELECT * FROM cab_watch_accounts.trips WHERE id=$1',self.trip))[0]
    async def alerts(self):return await self.sql('SELECT * FROM cab_watch_accounts.alerts WHERE trip_id=$1 ORDER BY id',self.trip)
    async def test_exact_schema_constraints_and_repeat_start(self):
        await self.seed();await self.migrate()
        expected={'users':{'id','name','email','password_hash','jwt_key','created_at'},
            'vehicles':{'id','user_id','status','created_at'},
            'trips':{'id','user_id','idempotency_key','vehicle_id','planned_route','last_sequence','last_lat','last_lon','last_captured_at','off_route_count','created_at','completed_at'},
            'alerts':{'id','trip_id','opened_sequence','closed_sequence','peak_metres','opened_at','resolved_at'}}
        for table,cols in expected.items():
            rows=await self.db.fetch('SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2',self.ns,table)
            self.assertEqual({r['column_name'] for r in rows},cols)
        tables=await self.db.fetch('SELECT table_name FROM information_schema.tables WHERE table_schema=$1',self.ns)
        self.assertEqual({r['table_name'] for r in tables},set(expected))
        row=await self.triprow()
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.sql("INSERT INTO cab_watch_accounts.trips SELECT $1,user_id,idempotency_key,vehicle_id,planned_route,last_sequence,last_lat,last_lon,last_captured_at,off_route_count,created_at,completed_at FROM cab_watch_accounts.trips",uuid4())
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.sql('UPDATE cab_watch_accounts.trips SET user_id=$1',uuid4())
        self.assertTrue(self.vehicle.startswith('CB#'))
    async def test_incremental_replay_rollback_restart_and_completion(self):
        await self.seed()
        await self.save(self.point(2),self.point(1))
        self.assertEqual((await self.triprow())['off_route_count'],2)
        with self.assertRaises(RuntimeError):
            async with self.db.transaction():
                await persist_readings(self.conn,self.trip,[self.point(3)])
                raise RuntimeError('rollback')
        self.assertEqual((await self.triprow())['last_sequence'],2);self.assertEqual(await self.alerts(),[])
        # Fresh invocation reads durable count, exactly as a restarted worker does.
        await self.save(self.point(3));original=(await self.alerts())[0]
        await self.save(self.point(3,'H'),self.point(1,'H'))
        self.assertEqual((await self.alerts())[0],original)
        await self.save(self.point(5))  # gap resets count but does NOT resolve incident
        self.assertIsNone((await self.alerts())[0]['closed_sequence'])
        await self.save(self.point(6,'H',second=4))  # reversed on-route evidence is invalid
        self.assertIsNone((await self.alerts())[0]['closed_sequence'])
        self.assertEqual((await self.triprow())['off_route_count'],0)
        await self.save(self.point(7,'H'))
        resolved=(await self.alerts())[0]
        self.assertEqual(resolved['id'],original['id']);self.assertEqual(resolved['closed_sequence'],7)
        await self.save(self.point(8,'K'));done=await self.triprow()
        await self.save(self.point(9))
        self.assertEqual(await self.triprow(),done)
        self.assertIsNotNone(done['completed_at'])
        self.assertEqual((await self.sql('SELECT status FROM cab_watch_accounts.vehicles'))[0]['status'],'available')
    async def test_open_alert_peak_updates_without_replacement(self):
        await self.seed();await self.save(*(self.point(i) for i in (1,2,3)))
        old=(await self.alerts())[0]
        await self.save(self.point(4,'A'))
        new=(await self.alerts())[0]
        self.assertEqual(new['id'],old['id']);self.assertGreater(new['peak_metres'],old['peak_metres'])
        await self.save(self.point(5,'K'))
        self.assertEqual((await self.alerts())[0]['closed_sequence'],5)
    async def test_ownership_failure_rolls_back(self):
        await self.seed()
        with self.assertRaises(ValueError):
            async with self.db.transaction():await persist_readings(self.conn,self.trip,[self.point(1)],uuid4(),self.vehicle)
        self.assertEqual((await self.triprow())['last_sequence'],0)
    async def test_history_migration_preserves_ids_positions_and_count(self):
        await self.seed(old=True)
        state=json.dumps(dict(sequence=2,captured_at='2020-01-01T00:00:02Z',consecutive=2,tracking='CONFIRMING'))
        await self.sql("UPDATE cab_watch_accounts.trips SET last_sequence=2,last_lat=13,last_lon=80,last_captured_at='2020-01-01T00:00:02Z',history_state=$1::jsonb",state)
        await self.sql("INSERT INTO cab_watch_accounts.alerts(trip_id,opened_sequence,closed_sequence,peak_metres,opened_at,resolved_at) VALUES($1,1,2,300,'2019-01-01','2019-01-02')",self.trip)
        original=await self.alerts()
        with patch.dict(os.environ,{'CABWATCH_BACKUP_CONFIRMED':'0'}):
            with self.assertRaisesRegex(RuntimeError,'Back up'):await self.migrate()
        with patch.dict(os.environ,{'CABWATCH_BACKUP_CONFIRMED':'1'}):await self.migrate()
        row=await self.triprow();self.assertEqual(row['off_route_count'],2)
        self.assertEqual((row['last_lat'],row['last_lon'],row['last_sequence']),(13,80,2))
        self.assertNotIn('readings',row)
        kept=await self.alerts()
        self.assertEqual(kept[0]['id'],original[0]['id']);self.assertEqual(kept[0]['resolved_at'],original[0]['resolved_at'])
        await self.migrate();self.assertEqual(await self.triprow(),row)
    async def test_overlapping_open_alerts_stop_without_data_loss(self):
        await self.seed(old=True)
        await self.sql("INSERT INTO cab_watch_accounts.alerts(trip_id,opened_sequence,peak_metres,historical) VALUES($1,1,300,true),($1,2,400,true)",self.trip)
        original=await self.alerts()
        with patch.dict(os.environ,{'CABWATCH_BACKUP_CONFIRMED':'1'}):
            with self.assertRaisesRegex(RuntimeError,'Overlapping open alerts'):await self.migrate()
        self.assertEqual(await self.alerts(),original);self.assertIn('history_state',await self.triprow())
    async def test_migration_recovers_count_from_remaining_evidence(self):
        await self.seed(old=True)
        evidence={str(i):self.point(i) for i in (4,5)}
        await self.sql("UPDATE cab_watch_accounts.trips SET last_sequence=5,last_captured_at='2020-01-01T00:00:05Z',readings=$1::jsonb",json.dumps(evidence))
        with patch.dict(os.environ,{'CABWATCH_BACKUP_CONFIRMED':'1'}):await self.migrate()
        self.assertEqual((await self.triprow())['off_route_count'],2)

    async def test_old_route_schema_is_converted_once_without_recreating_motion(self):
        fixture=Path('tests/fixtures/pre_trip_idempotency.sql').read_text()
        await self.db.execute(fixture.replace('cab_watch_accounts',self.ns))
        user,tid=uuid4(),uuid4()
        await self.sql("INSERT INTO cab_watch_accounts.users(id,name,email,password_hash,jwt_key) VALUES($1,'Legacy','legacy@example.invalid','preserve',$2)",user,b'x'*32)
        vehicle=(await self.sql("INSERT INTO cab_watch_accounts.vehicles(user_id,cab_number,parked_depot_id) VALUES($1,1,(SELECT id FROM cab_watch_accounts.depots WHERE node='A')) RETURNING id",user))[0]['id']
        plan=json.dumps(dict(to_pickup=['A','G'],customer_ride=['G','H','I','J','K'],to_depot=['K','E']))
        await self.sql("INSERT INTO cab_watch_accounts.trips(id,vehicle_id,planned_legs,last_sequence) VALUES($1,$2,$3::jsonb,7)",tid,vehicle,plan)
        await self.migrate();await self.migrate()
        row=(await self.sql('SELECT * FROM cab_watch_accounts.trips WHERE id=$1',tid))[0]
        self.assertEqual(row['last_sequence'],7);self.assertEqual(row['vehicle_id'],vehicle)
        self.assertNotIn('motion',row);self.assertNotIn('planned_legs',row)
        self.assertEqual(json.loads(row['planned_route']),['G','H','I','J','K'])

    async def test_notify_is_delivered_only_after_commit(self):
        import asyncio
        await self.seed()
        listener=await asyncpg.connect(os.environ['CABWATCH_TEST_DATABASE_URL']);received=[]
        await listener.add_listener('cabwatch_journey',lambda c,p,ch,payload:received.append(payload))
        try:
            with self.assertRaises(RuntimeError):
                async with self.db.transaction():
                    await persist_readings(self.conn,self.trip,[self.point(1)])
                    raise RuntimeError('rollback')
            await asyncio.sleep(.05);self.assertNotIn(str(self.user),received)
            await self.save(self.point(1))
            for _ in range(20):
                if str(self.user) in received:break
                await asyncio.sleep(.05)
            self.assertIn(str(self.user),received)
        finally:await listener.close()
