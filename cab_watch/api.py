import asyncio
import json
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

import httpx
from fastapi import Depends, APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import AwareDatetime, BaseModel, Field

from .agent import generate, tool
from .admission import TelemetryAdmission
from . import city

ROOT = Path(__file__).parent
router = APIRouter(prefix="/cab-watch")


from .auth import owner, router as auth_router
from .fleet import router as fleet_router

router.include_router(auth_router)
router.include_router(fleet_router)
from .notifications import router as notifications_router
router.include_router(notifications_router)

class Point(BaseModel, extra="forbid"):
    trip_id: UUID
    captured_at: AwareDatetime | None = None
    sequence: int = Field(ge=1, le=2**31-1)
    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)


class Batch(BaseModel):
    readings: list[Point] = Field(min_length=1, max_length=100)


async def latest_alert(conn, trip_id):
    incident = await conn.fetchrow(
        """SELECT * FROM cab_watch_accounts.alerts WHERE trip_id=$1 ORDER BY id DESC LIMIT 1""", trip_id)
    return dict(incident) if incident else None


async def record(conn, trip, reading):
    from datetime import datetime, timezone
    from .journey import persist_readings
    point = reading.model_dump(mode="json")
    point['captured_at'] = point['captured_at'] or datetime.now(timezone.utc).isoformat()
    from .live import milliseconds
    if milliseconds(point['captured_at']) > datetime.now(timezone.utc).timestamp()*1000+2000:
        raise HTTPException(422, 'Capture time is too far in the future')
    applied = reading.sequence > trip['last_sequence'] and trip['completed_at'] is None
    await persist_readings(conn,reading.trip_id,[point])
    updated = await conn.fetchrow('SELECT * FROM cab_watch_accounts.trips WHERE id=$1',reading.trip_id)
    from .fleet import trip_payload
    alert = await latest_alert(conn,reading.trip_id)
    return dict(trip_id=reading.trip_id,sequence=updated['last_sequence'],applied=applied,
                distance_m=trip_payload(updated)['distance_m'],completed=updated['completed_at'] is not None,alert=alert)


async def apply_schema(conn, namespace="cab_watch_accounts"):
    legacy = await conn.fetchval("""SELECT EXISTS(SELECT FROM information_schema.columns
        WHERE table_schema=$1 AND table_name='trips' AND column_name='planned_legs')""", namespace)
    for filename in (["upgrade_legacy.sql", "schema.sql"] if legacy else ["schema.sql"]):
        await conn.execute((ROOT / filename).read_text(encoding="utf-8").replace("cab_watch_accounts", namespace))
    if legacy:
        await conn.execute(f"""ALTER TABLE {namespace}.trips ADD CONSTRAINT valid_planned_route
            CHECK ({namespace}.valid_route(planned_route))""")

    from .migration import simplify
    await simplify(conn, namespace)



@asynccontextmanager
async def cab_runtime(db):
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(73949203)")
        await apply_schema(conn)
    yield



@router.get("")
@router.get("/")
async def page():
    return FileResponse(ROOT / "index.html")


@router.get("/network")
async def network():
    return dict(nodes=city.NODES, roads=city.ROADS,
                metres=city.METRES, origin=city.ORIGIN,
                lat_scale=city.LAT_SCALE, lon_scale=city.LON_SCALE)


@router.post("/me/gps")
async def ingest(batch: Batch, request: Request, user: UUID=Depends(owner)):
    if not hasattr(request.app.state, "telemetry_admission"):
        request.app.state.telemetry_admission = TelemetryAdmission()
    async with request.app.state.telemetry_admission.slot(user):
        try:
            async with asyncio.timeout(10):
                pipeline = getattr(request.app.state, "pipeline", None)
                if pipeline:
                    result = await pipeline.ingest(batch, user)
                    return JSONResponse(result, status_code=202)
                return await ingest_batch(batch, request.app.state.db, user)
        except TimeoutError:
            raise HTTPException(503, "Telemetry processing timed out. Retry this batch.",
                                headers={"Retry-After": "1"}) from None
        except HTTPException:
            raise
        except Exception:
            if getattr(request.app.state, "pipeline", None):
                raise HTTPException(503, "Telemetry not acknowledged. Retry the same trip/sequence batch.",
                                    headers={"Retry-After": "2"}) from None
            raise


@router.get("/metrics", include_in_schema=False)
async def metrics(request: Request):
    expected = os.getenv("METRICS_TOKEN")
    if not expected or not secrets.compare_digest(request.headers.get("Authorization",""), "Bearer "+expected):
        raise HTTPException(401,"Metrics credentials required")
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(generate_latest(),media_type=CONTENT_TYPE_LATEST)


async def ingest_batch(batch, db, user):
    readings = {point.trip_id: point for point in batch.readings}
    async with db.acquire(timeout=2) as conn, conn.transaction():
        # A second worker must also avoid processing this account simultaneously.
        admitted = await conn.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 73949204))", str(user))
        if not admitted:
            raise HTTPException(429, "Telemetry already processing. Retry this batch.",
                                headers={"Retry-After": "1"})
        trips = await conn.fetch("""
            SELECT t.* FROM cab_watch_accounts.trips t
            WHERE t.user_id=$1 AND t.id=ANY($2::uuid[])
            ORDER BY t.id FOR UPDATE OF t""", user, list(readings))
        if len(trips) != len(batch.readings):
            raise HTTPException(422, "Send one reading per trip in your fleet")
        results = [await record(conn, trip, readings[trip["id"]]) for trip in trips]
    return {"results": results}


@router.post("/me/alerts/{alert_id}/explain")
async def explain(alert_id: int, request: Request,user: UUID=Depends(owner)):
    db = request.app.state.db
    if not 0 < alert_id <= 9223372036854775807:
        raise HTTPException(404, 'Unknown alert')
    alert = await db.fetchrow("""SELECT a.*,t.vehicle_id FROM cab_watch_accounts.alerts a
        JOIN cab_watch_accounts.trips t ON t.id=a.trip_id
        WHERE t.user_id=$1 AND a.id=$2""",user,alert_id)
    if not alert:
        raise HTTPException(404, "Unknown alert for this simulation")
    facts = dict(vehicle_id=alert["vehicle_id"], threshold_m=city.LIMIT,
                 opened_sequence=alert["opened_sequence"], closed_sequence=alert["closed_sequence"],
                 status="resolved" if alert["closed_sequence"] is not None else "open",
                 peak_m=round(alert["peak_metres"]))
    baseline = (f"{facts['vehicle_id']} triggered the {city.LIMIT} m off-route alert at GPS "
                f"#{facts['opened_sequence']}. The maximum recorded offset was {facts['peak_m']} m.")
    if facts["closed_sequence"] is not None:
        baseline += f" The incident resolved at GPS #{facts['closed_sequence']}."
    declarations = [tool("read_alert_evidence", "Read the measured offsets and status for this simulated alert.")]
    contents = [{"role":"user","parts":[{"text":"Call read_alert_evidence then explain the result to a dispatcher "
                "in two short sentences. The peak is the maximum during the incident. Do not infer the opening distance. Never infer danger or a reason for the detour."}]}]
    try:
        turn = await generate(contents, declarations, "ANY")
        calls = [p["functionCall"] for p in turn["parts"] if "functionCall" in p]
        if len(calls)!=1 or calls[0]["name"]!="read_alert_evidence" or calls[0].get("args"):
            raise ValueError("Invalid tool call")
        reply = {"name":"read_alert_evidence","response":facts}
        if calls[0].get("id"):
            reply["id"] = calls[0]["id"]
        contents.extend([turn, {"role":"user","parts":[{"functionResponse":reply}]}])
        final = await generate(contents, declarations, "NONE")
        summary = " ".join(p["text"] for p in final["parts"] if p.get("text") and not p.get("thought")).strip()
        if summary:
            return dict(source="Gemini · read_alert_evidence",summary=summary[:800],facts=facts)
        reason = "Gemini returned an empty summary."
    except httpx.HTTPStatusError as exc:
        reason = f"Gemini returned HTTP {exc.response.status_code}."
    except httpx.RequestError:
        reason = "The model service could not be reached in time."
    except (KeyError, ValueError, IndexError, TypeError):
        reason = "The model or its evidence tool was unavailable."
    return dict(source="Rule-based explanation",summary=baseline,facts=facts,notice=reason)


