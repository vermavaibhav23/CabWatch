"""UTC timestamp helpers shared by Kafka validation and tests."""
from datetime import datetime, timezone

def milliseconds(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def stamp(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()
