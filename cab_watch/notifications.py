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
    rows = await db.fetch("""SELECT id,vehicle_id,last_sequence,last_captured_at,
        processed_at,history_state->>'distance_m' AS distance_m,
        history_state->'alert' AS alert FROM cab_watch_accounts.trips
        WHERE user_id=$1 AND completed_at IS NULL ORDER BY id""", user)
    return dict(type='off_route_snapshot', vehicles=[dict(
        trip_id=str(row['id']), vehicle_id=row['vehicle_id'], sequence=row['last_sequence'],
        captured_at=row['last_captured_at'], processed_at=row['processed_at'],
        distance_m=float(row['distance_m'] or 0), alert=decode(row['alert'], None)
    ) for row in rows])


@router.websocket('/me/alerts/ws')
async def alerts_socket(ws: WebSocket):
    # Token travels in the first frame, never in URLs/access logs.
    await ws.accept()
    hub = ws.app.state.notifications
    event, user = asyncio.Event(), None
    try:
        message = await asyncio.wait_for(ws.receive_json(), 5)
        token = message.get('token', '')
        if not isinstance(token, str) or len(token) > 4096:
            raise HTTPException(401)
        credentials = HTTPAuthorizationCredentials(scheme='Bearer', credentials=token)
        user = await owner(ws, credentials)
        hub.watchers[str(user)].add(event)
        while True:
            event.clear()
            # Revalidate expiry/revocation even on an otherwise idle connection.
            await owner(ws, credentials)
            await asyncio.wait_for(ws.send_json(jsonable_encoder(await snapshot(hub.db, user))), 5)
            try:
                await asyncio.wait_for(event.wait(), 5)
            except TimeoutError:
                pass  # Reconcile reconnects, missed NOTIFY, retention and completion.
    except (HTTPException, ValueError, AttributeError, TimeoutError):
        with suppress(RuntimeError, WebSocketDisconnect):
            await ws.close(code=4401)
    except WebSocketDisconnect:
        pass
    finally:
        if user is not None:
            hub.watchers[str(user)].discard(event)
            if not hub.watchers[str(user)]:
                del hub.watchers[str(user)]
