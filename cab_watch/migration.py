"""One transactional migration from full history to latest position + incidents."""
import os
from .journey import decode, evaluate, capture_time


async def simplify(conn, namespace='cab_watch_accounts'):
    columns = {r['column_name'] for r in await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='trips'", namespace)}
    old_table = await conn.fetchval('SELECT to_regclass($1)', namespace+'.telemetry_history')
    if ('readings' in columns or old_table) and os.getenv('CABWATCH_BACKUP_CONFIRMED') != '1':
        raise RuntimeError('Back up the database before removing GPS history; then set CABWATCH_BACKUP_CONFIRMED=1 for this migration')
    duplicates = await conn.fetch(f"""SELECT trip_id,array_agg(id ORDER BY id) AS ids FROM {namespace}.alerts
        WHERE closed_sequence IS NULL GROUP BY trip_id HAVING count(*)>1""")
    if duplicates:
        raise RuntimeError('Overlapping open alerts require evidence-based review before migration: '+
                           '; '.join(str(r['trip_id'])+': '+str(r['ids']) for r in duplicates))
    if 'off_route_count' not in columns:
        await conn.execute(f'ALTER TABLE {namespace}.trips ADD COLUMN off_route_count integer NOT NULL DEFAULT 0 CHECK(off_route_count BETWEEN 0 AND 3)')
        rows = await conn.fetch(f'SELECT * FROM {namespace}.trips')
        for row in rows:
            count = 0
            state = decode(row.get('history_state'), {})
            if row['completed_at'] is None:
                # Only trust a state describing the exact retained checkpoint.
                captured = capture_time(state.get('captured_at'))
                if (state.get('sequence') == row['last_sequence'] and captured is not None
                    and captured == row['last_captured_at'] and state.get('tracking') != 'DELAYED'
                    and type(state.get('consecutive')) is int and 0 <= state['consecutive'] <= 3):
                    count = state['consecutive']
                else:
                    evidence = decode(row.get('readings'), {})
                    if not evidence and old_table:
                        records = await conn.fetch(f'SELECT * FROM {namespace}.telemetry_history WHERE trip_id=$1 AND sequence<=$2 ORDER BY sequence DESC LIMIT 3',row['id'],row['last_sequence'])
                        evidence = {str(r['sequence']):dict(r) for r in records}
                    tail = []
                    for i in range(row['last_sequence'], max(0,row['last_sequence']-3), -1):
                        point = evidence.get(str(i))
                        if point is None:
                            break
                        tail.insert(0,point)
                    if tail and all(tail) and capture_time(tail[-1].get('captured_at')) == row['last_captured_at']:
                        checkpoint = dict(row, last_sequence=0,last_captured_at=None,off_route_count=0)
                        for point in tail:
                            result = evaluate(checkpoint, point)
                            checkpoint.update(last_sequence=point['sequence'],off_route_count=result['count'])
                            if result['valid']:
                                checkpoint['last_captured_at'] = result['captured']
                        count = checkpoint['off_route_count']
            await conn.execute(f'UPDATE {namespace}.trips SET off_route_count=$2 WHERE id=$1',row['id'],count)
    # Never invent closure times, delete incidents, or replace their numeric IDs.
    await conn.execute(f"""DROP INDEX IF EXISTS {namespace}.one_open_alert;
        CREATE UNIQUE INDEX one_open_alert ON {namespace}.alerts(trip_id) WHERE closed_sequence IS NULL;
        ALTER TABLE {namespace}.trips DROP COLUMN IF EXISTS motion, DROP COLUMN IF EXISTS accepted_point,
            DROP COLUMN IF EXISTS readings, DROP COLUMN IF EXISTS history_state, DROP COLUMN IF EXISTS processed_at;
        ALTER TABLE {namespace}.alerts DROP COLUMN IF EXISTS event_key, DROP COLUMN IF EXISTS close_reason,
            DROP COLUMN IF EXISTS historical;
        DROP TABLE IF EXISTS {namespace}.telemetry_history;""")
