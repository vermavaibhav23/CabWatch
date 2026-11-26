"""Capture-time journey analysis and transactional, replay-safe persistence."""
import json
from datetime import datetime, timezone
from uuid import UUID, uuid5
from . import city
from .live import advance, milliseconds


def decode(value, default):
    return json.loads(value) if isinstance(value, str) else (value if value is not None else default)


def analyse(trip, readings):
    """Rebuild in sequence order; wall-clock processing delay never rejects evidence.

    Missing sequences, reversed capture times and capture gaps over ten seconds
    break confirmation. Late arrivals rebuild affected incidents deterministically.
    """
    state, alerts, completed = None, {}, None
    route = decode(trip['planned_route'], [])
    points = []
    previous = None
    for point in sorted(readings.values(), key=lambda p: p['sequence']):
        point = dict(point)
        point['distance_m'] = city.distance_from_path([city.NODES[n] for n in route], point['lat'], point['lon'])
        point['off_route_sample'] = point['distance_m'] >= city.LIMIT
        point['sequence_gap'] = point['sequence'] != (previous['sequence'] + 1 if previous else 1)
        point['timestamp_reversed'] = bool(previous and milliseconds(point['captured_at']) <= milliseconds(previous['captured_at']))
        event = dict(trip_id=str(trip['id']), user_id=str(trip['user_id']), vehicle_id=trip['vehicle_id'], route=route, point=point)
        state, changes = advance(state, event, milliseconds(point['captured_at']))
        for change in changes:
            if change['kind'] == 'alert':
                a = dict(change['alert'])
                a['id'] = str(uuid5(UUID(str(trip['id'])), 'history:'+str(a['opened_sequence'])))
                alerts[a['id']] = a
            elif change['kind'] == 'complete':
                completed = point['captured_at']
        points.append(point)
        previous = point
    return points, state, list(alerts.values()), completed


async def persist_readings(conn, trip_id, incoming):
    # The lock also serializes duplicate/rebalance deliveries for this trip.
    trip = await conn.fetchrow('SELECT * FROM cab_watch_accounts.trips WHERE id=$1 FOR UPDATE', trip_id)
    if trip is None:
        # A retained Kafka record may refer to a trip removed by retention.
        return
    readings = decode(trip['readings'], {})
    changed = False
    for point in incoming:
        key = str(point['sequence'])
        if key not in readings:
            readings[key] = point
            changed = True
    if not changed and trip['history_state']:
        return
    points, state, alerts, completed = analyse(trip, readings)
    if not points:
        return
    # Preserve original legacy alerts; replace only derived historical incidents.
    await conn.execute('DELETE FROM cab_watch_accounts.alerts WHERE trip_id=$1 AND historical', trip_id)
    for a in alerts:
        await conn.execute("""INSERT INTO cab_watch_accounts.alerts
            (trip_id,opened_sequence,closed_sequence,peak_metres,opened_at,resolved_at,event_key,close_reason,historical)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,true)""",
            trip_id,a['opened_sequence'],a['closed_sequence'],a['peak_metres'],
            datetime.fromisoformat(a['opened_at']),datetime.fromisoformat(a['resolved_at']) if a['resolved_at'] else None,
            UUID(a['id']),a.get('close_reason'))
    latest = points[-1]
    await conn.execute("""UPDATE cab_watch_accounts.trips SET readings=$2::jsonb,history_state=$3::jsonb,
        last_sequence=$4,last_lat=$5,last_lon=$6,last_captured_at=$7,motion=$8::jsonb,
        completed_at=least(completed_at,$9),processed_at=now() WHERE id=$1""",
        trip_id,json.dumps({str(p['sequence']):p for p in points}),json.dumps(state),
        latest['sequence'],latest['lat'],latest['lon'],datetime.fromisoformat(latest['captured_at']),
        json.dumps(latest.get('motion')),datetime.fromisoformat(completed) if completed else None)
    if completed:
        await conn.execute("""UPDATE cab_watch_accounts.vehicles SET status='available' WHERE id=$1
            AND NOT EXISTS(SELECT FROM cab_watch_accounts.trips WHERE vehicle_id=$1 AND completed_at IS NULL)""",trip['vehicle_id'])

    # PostgreSQL delivers NOTIFY only after this transaction commits.
    # Payload is just a routing hint; WebSockets read authorized durable state.
    await conn.execute("SELECT pg_notify('cabwatch_journey', $1)", str(trip['user_id']))
