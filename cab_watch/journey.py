"""Incremental, replay-safe GPS processing. Call inside a SQL transaction."""
import json
import struct
from datetime import datetime, timezone, timedelta
from . import city


def decode(value, default):
    return json.loads(value) if isinstance(value, str) else (value if value is not None else default)


def capture_time(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if value is None or value.tzinfo is None or value.utcoffset() is None:
            return None
        if value > datetime.now(timezone.utc) + timedelta(seconds=2):
            return None
        return value
    except (ValueError, TypeError, AttributeError):
        return None


def evaluate(trip, point):
    """Pure one-reading transition; an invalid time consumes sequence, not position."""
    captured = capture_time(point.get('captured_at'))
    previous = trip['last_captured_at']
    valid = captured is not None and (previous is None or captured > previous)
    if not valid:
        return dict(valid=False, count=0, captured=None)
    line = [city.NODES[n] for n in decode(trip['planned_route'], [])]
    distance = city.distance_from_path(line, point['lat'], point['lon'])
    contiguous = (point['sequence'] == trip['last_sequence'] + 1 and previous is not None
                  and (captured - previous).total_seconds() <= 10)
    count = min(3, trip['off_route_count'] + 1) if contiguous else 1
    if distance < city.LIMIT:
        count = 0
    completed = city.distance_from_path([line[-1]], point['lat'], point['lon']) < 1
    return dict(valid=True, count=0 if completed else count, captured=captured,
                distance=distance, completed=completed)


async def persist_readings(conn, trip_id, incoming, expected_user=None, expected_vehicle=None):
    row = await conn.fetchrow('SELECT * FROM cab_watch_accounts.trips WHERE id=$1 FOR UPDATE', trip_id)
    if row is None:
        return  # Retained Kafka message for a trip that no longer exists.
    trip = dict(row)
    if ((expected_user is not None and str(trip['user_id']) != str(expected_user)) or
        (expected_vehicle is not None and trip['vehicle_id'] != expected_vehicle)):
        raise ValueError('Telemetry ownership differs from stored trip')
    if trip['completed_at'] is not None:
        return
    alert = await conn.fetchrow('SELECT * FROM cab_watch_accounts.alerts WHERE trip_id=$1 AND closed_sequence IS NULL', trip_id)
    status_changed = False
    for point in sorted(incoming, key=lambda p: p['sequence']):
        if point['sequence'] <= trip['last_sequence']:
            continue
        result = evaluate(trip, point)
        trip['last_sequence'] = point['sequence']
        trip['off_route_count'] = result['count']
        if not result['valid']:
            # Do not turn an invalid timestamp into evidence of returning to route.
            await conn.execute('UPDATE cab_watch_accounts.trips SET last_sequence=$2,off_route_count=0 WHERE id=$1', trip_id, point['sequence'])
            continue
        captured, distance = result['captured'], result['distance']
        # Compare at the precision of the REAL column to avoid no-op peak updates.
        peak = struct.unpack('!f', struct.pack('!f', distance))[0]
        if alert and (distance < city.LIMIT or result['completed']):
            await conn.execute('UPDATE cab_watch_accounts.alerts SET closed_sequence=$2,resolved_at=$3 WHERE id=$1',
                               alert['id'], point['sequence'], captured)
            alert = None
            status_changed = True
        elif alert and peak > alert['peak_metres']:
            await conn.execute('UPDATE cab_watch_accounts.alerts SET peak_metres=$2 WHERE id=$1', alert['id'], peak)
            alert = {**alert, 'peak_metres': peak}
            status_changed = True
        elif alert is None and result['count'] == 3 and not result['completed']:
            alert = await conn.fetchrow("""INSERT INTO cab_watch_accounts.alerts
                (trip_id,opened_sequence,peak_metres,opened_at) VALUES($1,$2,$3,$4) RETURNING *""",
                trip_id, point['sequence'], peak, captured)
            status_changed = True
        await conn.execute("""UPDATE cab_watch_accounts.trips SET last_sequence=$2,last_lat=$3,last_lon=$4,
            last_captured_at=$5,off_route_count=$6,completed_at=$7 WHERE id=$1""",
            trip_id, point['sequence'], point['lat'], point['lon'], captured, result['count'],
            captured if result['completed'] else None)
        trip.update(last_lat=point['lat'], last_lon=point['lon'], last_captured_at=captured)
        if result['completed']:
            status_changed = True
            await conn.execute("UPDATE cab_watch_accounts.vehicles SET status='available' WHERE id=$1", trip['vehicle_id'])
            break
    if status_changed:
        # Delivered only after commit; SQL remains the durable source for reconnects.
        await conn.execute("SELECT pg_notify('cabwatch_journey', $1)", str(trip['user_id']))
