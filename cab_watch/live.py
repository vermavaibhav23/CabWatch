"""Pure route-confirmation state machine (no consumer or Redis).

Journey analysis evaluates this at capture time, retaining all historical samples.
"""
from copy import deepcopy
from datetime import datetime, timezone
from uuid import UUID, uuid5

from . import city


def milliseconds(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def stamp(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def advance(old, event, now, stale_ms=10000):
    """Return a new state and durable alert/completion events; duplicates do nothing."""
    p = event["point"]
    sequence, captured = p["sequence"], milliseconds(p["captured_at"])
    if old and (sequence <= old["sequence"] or old.get("completed")):
        return old, []
    s = deepcopy(old) if old else dict(sequence=0, captured_at=None, lat=None, lon=None,
        motion=None, consecutive=0, alert=None, recent=[], completed=False)
    events = []
    if old is None:
        events.append({"kind": "reset", "trip_id": event["trip_id"], "sequence": sequence})
    def close(reason):
        if s["alert"]:
            a = {**s["alert"], "closed_sequence": sequence, "resolved_at": stamp(now), "close_reason": reason}
            s["recent"] = [a, *s["recent"]][:30]
            events.append({"kind": "alert", "alert": a})
            s["alert"] = None

    age = now - captured
    previous_time = milliseconds(s["captured_at"]) if s["captured_at"] else 0
    # Allow small positive client clock skew, but never use reversed timestamps.
    fresh = -2000 <= age <= stale_ms and captured > previous_time
    contiguous = bool(old and sequence == old["sequence"] + 1 and
                      old.get("tracking") != "DELAYED" and captured - previous_time <= stale_ms and now-previous_time<=stale_ms)
    s.update(sequence=sequence, trip_id=event["trip_id"], user_id=event["user_id"],
             vehicle_id=event["vehicle_id"], processed_at=stamp(now), latency_ms=max(0, age))
    if not fresh:
        close("tracking delayed")
        s.update(consecutive=0, tracking="DELAYED", off_route=False)
        return s, events

    line = [city.NODES[n] for n in event["route"]]
    distance = city.distance_from_path(line, p["lat"], p["lon"])
    s.update(lat=p["lat"], lon=p["lon"], captured_at=p["captured_at"], motion=p.get("motion"), distance_m=distance)
    if not contiguous:
        close("confirmation reset")
    s["consecutive"] = min(3, s["consecutive"] + 1) if distance >= city.LIMIT and contiguous else (1 if distance >= city.LIMIT else 0)
    if distance < city.LIMIT:
        close("returned to route")
    elif s["consecutive"] >= 3:
        if s["alert"] is None:
            s["alert"] = dict(id=str(uuid5(UUID(event["trip_id"]), str(sequence))), trip_id=event["trip_id"],
                vehicle_id=event["vehicle_id"], opened_sequence=sequence, closed_sequence=None,
                peak_metres=distance, opened_at=stamp(now), resolved_at=None, close_reason=None)
            events.append({"kind": "alert", "alert": deepcopy(s["alert"]), "confirming_capture": p["captured_at"]})
        elif distance > s["alert"]["peak_metres"]:
            s["alert"]["peak_metres"] = distance
            events.append({"kind": "alert", "alert": deepcopy(s["alert"])})
    s["off_route"] = s["alert"] is not None
    s["tracking"] = "LAGGING" if age > 5000 else "CONFIRMING" if s["consecutive"] in (1, 2) else "LIVE"
    arrived = not p.get("motion") or p["motion"]["mode"] == "arrived"
    if arrived and city.distance_from_path([line[-1]], p["lat"], p["lon"]) < 1:
        close("trip completed")
        s.update(completed=True, off_route=False)
        events.append({"kind": "complete", "trip_id": event["trip_id"], "sequence": sequence})
    return s, events
