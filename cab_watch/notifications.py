"""Committed journey updates -> user-scoped WebSocket snapshots, without Redis."""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
import logging

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPAuthorizationCredentials

from .auth import owner
from .journey import decode

router = APIRouter()
CHANNEL = 'cabwatch_journey'
log = logging.getLogger(__name__)


class Notifications:
    def __init__(self, db):
        self.db = db
        self.watchers = defaultdict(set)

    def changed(self, connection, pid, channel, user):
        for event in self.watchers.get(user, ()):
            event.set()

    async def listen(self):
        while True:
            try:
                async with self.db.acquire() as conn:
                    await conn.add_listener(CHANNEL, self.changed)
                    try:
                        while not conn.is_closed():
                            await asyncio.sleep(1)
                    finally:
                        if not conn.is_closed():
                            await conn.remove_listener(CHANNEL, self.changed)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Journey notifications reconnecting; snapshots recover missed updates')
                await asyncio.sleep(1)


@asynccontextmanager
async def runtime(app):
    hub = app.state.notifications = Notifications(app.state.db)
    task = asyncio.create_task(hub.listen())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def snapshot(db, user):
    from .fleet import fleet_alerts
    async with db.acquire() as conn, conn.transaction(isolation='repeatable_read', readonly=True):
        rows = await conn.fetch("""SELECT t.id,t.vehicle_id,t.completed_at,row_to_json(a) AS alert
            FROM cab_watch_accounts.trips t LEFT JOIN cab_watch_accounts.alerts a
            ON a.trip_id=t.id AND a.closed_sequence IS NULL
            WHERE t.user_id=$1 ORDER BY t.id""", user)
        alerts = await fleet_alerts(conn, user)
    # Status only: no coordinates, route geometry, or per-reading GPS checkpoints.
    return dict(type='off_route_snapshot', vehicles=[dict(
        trip_id=str(row['id']), vehicle_id=row['vehicle_id'],
        completed=row['completed_at'] is not None, completed_at=row['completed_at'],
        alert=decode(row['alert'], None)
    ) for row in rows], alerts=[dict(a) for a in alerts])



async def disconnected(ws):
    # Observe disconnects even when unchanged status suppresses all outgoing frames.
    while True:
        if (await ws.receive())['type'] == 'websocket.disconnect':
            return


@router.websocket('/me/alerts/ws')
async def alerts_socket(ws: WebSocket):
    # Token travels in the first frame, never in URLs/access logs.
    await ws.accept()
    hub = ws.app.state.notifications
    event, user = asyncio.Event(), None
    closed = None
    try:
        message = await asyncio.wait_for(ws.receive_json(), 5)
        token = message.get('token', '')
        if not isinstance(token, str) or len(token) > 4096:
            raise HTTPException(401)
        credentials = HTTPAuthorizationCredentials(scheme='Bearer', credentials=token)
        user = await owner(ws, credentials)
        hub.watchers[str(user)].add(event)
        closed = asyncio.create_task(disconnected(ws))
        previous = None
        while True:
            event.clear()
            # Revalidate expiry/revocation even on an otherwise idle connection.
            await owner(ws, credentials)
            current = jsonable_encoder(await snapshot(hub.db, user))
            if current != previous:
                await asyncio.wait_for(ws.send_json(current), 5)
                previous = current
            notified = asyncio.create_task(event.wait())
            try:
                await asyncio.wait({notified, closed}, timeout=5, return_when=asyncio.FIRST_COMPLETED)
            finally:
                notified.cancel()
                await asyncio.gather(notified, return_exceptions=True)
            if closed.done():
                break
            # Five-second reconciliation recovers missed NOTIFY, without GPS requests.
    except (HTTPException, ValueError, AttributeError, TimeoutError):
        with suppress(RuntimeError, WebSocketDisconnect):
            await ws.close(code=4401)
    except WebSocketDisconnect:
        pass
    finally:
        if closed is not None:
            closed.cancel()
            await asyncio.gather(closed, return_exceptions=True)
        if user is not None:
            hub.watchers[str(user)].discard(event)
            if not hub.watchers[str(user)]:
                del hub.watchers[str(user)]
