# CabWatch

CabWatch is a FastAPI fleet simulation with Kafka, Redis and PostgreSQL. Browser JavaScript sends GPS readings on a fictional road network. No real vehicles or locations are tracked.

Existing deployment: https://ev-charger-guide.onrender.com/cab-watch/
The deployment URL is historical; check `/healthz` for the running version and telemetry mode.

## Run locally

```sh
docker compose up --build
```

Open http://localhost:18080/cab-watch/. Compose starts PostgreSQL, a single Kafka broker, persistent Redis and FastAPI. All volumes are retained when containers stop. Kafka in this local stack is not a highly available production cluster. `GEMINI_API_KEY` optionally enables explanations requested by the user; model calls are never part of live detection.

Sign in, add a trip, and choose pickup and drop-off. The server assigns a cab and plans a route with Dijkstra. Change route queues a detour; Return to route retraces it. Completed cabs can be reused. The simulator sends a reading every real second while advancing five simulated seconds. Closing its tab stops GPS generation.

## Kafka data flow

```mermaid
flowchart LR
  Browser[JavaScript GPS and retry outbox] --> API[FastAPI authentication and ownership]
  API -->|acks=all, key=vehicle ID| Kafka[Partitioned telemetry topic]
  Kafka -->|live consumer group| Live[Cached route geometry and freshness check]
  Live -->|atomic state and event write| Redis[Persistent Redis state and event outbox]
  Redis -->|authenticated polling| Dashboard[Confirmed map positions and alerts]
  Redis --> Writer[Retrying alert and completion writer]
  Writer --> SQL[(PostgreSQL)]
  Kafka -->|independent history group| History[Idempotent history and checkpoint writer]
  History --> SQL
```

`POST /cab-watch/me/gps` returns **202 queued**, not an alert result, after broker acknowledgment and the accepted checkpoint. A partial send or timeout may have reached Kafka: retry exactly the same trip/sequence IDs. The producer is idempotent; history has a `(trip_id, sequence)` primary key; live state advances only for a higher sequence. This is retry-safe at-least-once processing, not a claim of end-to-end exactly-once delivery.

The API validates the whole batch's ownership and timestamps before sending anything. Records are keyed by vehicle ID, preserving order within a vehicle's partition. Default topic: `cabwatch.gps.recovery.v1`, 12 partitions, two live consumers and two history consumers. Increase consumer counts up to the available partitions after measuring demand. Do not change partition counts while accepting traffic without a controlled cutover, because a vehicle can move to a different partition.

Live detection reads routes embedded by the trusted API and state from Redis. It makes no per-reading SQL or external API call. History writes run in a separate consumer group. Alert and completion events are atomically appended with live state to a Redis stream; an independent SQL writer commits them before acknowledging them. Consumers commit offsets only after successful persistence. Failed writes retry. Rebalances and restarts may replay records safely.

## Freshness and recovery

- A reading at least 200 m off route must be confirmed by **three consecutive fresh readings** before an alert opens.
- A reading older than `LIVE_STALE_MS` (default 10000 ms), a reversed timestamp, a missing sequence or a long time gap resets confirmation. An existing incident closes with an explicit reason, not a claim that the vehicle returned to its route.
- During backlog recovery, the live group drains stale records without using their geometry for detection. It retains the last fresh position, marks tracking delayed, and rebuilds confirmation from new readings. It does not seek past offsets or discard history.
- The history group stores every unique accepted reading, including stale and out-of-order ones. Repeated trip/sequence IDs represent the same reading; the first stored history row wins.
- Redis state survives a process restart. A state loss resets live confirmation; it cannot reconstruct already committed incidents automatically. Keep persistent Redis backups and do not delete its data independently of Kafka offsets.
- The dashboard polls confirmed state every second. It greys delayed positions and hides their active-alert indicator. A stopped GPS source also becomes delayed. A queued HTTP receipt is never treated as a confirmed alert or fresh position.

Target capture-to-alert processing below 2 seconds normally and investigate above 5 seconds. These are test targets, not guarantees. Kafka cannot prevent lag under unlimited load or infrastructure outages. Sustained throughput must exceed incoming readings with spare capacity. The FastAPI Kafka path has not been certified for 2000 readings/second; measure it on the intended infrastructure, including bursts and recovery.

## Persistence and limits

Use standalone Redis (not Redis Cluster) with `noeviction`, durable storage and AOF persistence. The local setup uses `appendfsync always`; weaker hosted durability can lose an acknowledged state/outbox write before Kafka replay is possible. Back up Redis and PostgreSQL. Do not apply stream trimming to pending SQL events. Monitor memory and outbox size during database outages.

Route/ownership and JWT verification-key caches last five minutes and are bounded to 10000 entries. Cached accounts/trips can continue live processing during a short SQL outage. Login, uncached ownership and metadata refresh still need SQL. Revoking a user's signing key can take up to five minutes to reach cached workers; signatures and expiration are checked on every request.

Live state and accepted checkpoints expire after 90 days without updates. Fleet indexes expire/prune after 90 days. Completed PostgreSQL trips, alerts and their new history rows expire after `TRIP_RETENTION_DAYS` (default 90), checked every `RETENTION_INTERVAL_HOURS` (default 6). Active trip history is retained, so its storage can grow. Kafka local retention is seven days; increase storage/retention to cover your maximum supported outage. If an outage exceeds broker retention, history cannot be recovered from Kafka alone.

The API bounds concurrent telemetry requests and returns retry guidance under pressure. The browser stores up to two batches in account-scoped session storage and sends chunks of 25 readings. Movement pauses when that buffer fills. One active GPS producer per trip is expected; competing browser tabs can reuse sequences and should not both drive the same trip.

## API and monitoring

- `POST /cab-watch/me/trips`: authenticated; UUID `Idempotency-Key`, `{"pickup":"G","dropoff":"K"}`. Repeated identical dispatch returns the same trip.
- `GET /cab-watch/me/trips`: saved trips, alert history, accepted producer checkpoints and `telemetry_mode`.
- `GET /cab-watch/me/live`: account-scoped confirmed live state, freshness and recent alerts.
- `GET /cab-watch/me/trips/{id}/history?after=0`: owned trip's history, paginated by sequence, maximum 1000 rows.
- `POST /cab-watch/me/alerts/{id}/explain`: legacy numeric ID or new UUID event ID; Gemini or factual fallback.
- `GET /healthz`: process liveness, version and active mode. It is not a full dependency readiness check.
- `GET /cab-watch/metrics`: bearer `METRICS_TOKEN` required. Includes capture-to-live and confirming-reading-to-alert histograms, stale readings, consumer offset lag and retry counters. Metrics are per process; scrape each replica. Alert on stale readings and retries as well as latency, since dropped stale samples are excluded from fresh latency histograms.

The live snapshot keeps the latest 30 closed incidents per trip. Older incidents remain in SQL. The additive migration preserves existing accounts, trips and alerts.

## Existing Render deployment

See [the deployment checklist](docs/KAFKA_DEPLOYMENT.md). Existing deployments remain in `TELEMETRY_MODE=direct` until explicitly configured for Kafka. Legacy Kafka environment variables alone do not enable the new path. In direct mode, GPS is processed synchronously in SQL and the existing one-reading alert behavior remains available for compatibility. Never run direct and Kafka writers concurrently for the same account data.

## Tests

```sh
pip install -r requirements.txt
python -m unittest -v test_cab_watch test_retention test_telemetry test_live
node --test test_motion.mjs test_telemetry.mjs
```

For isolated database and Kafka tests:

```sh
docker compose -p cabwatch-fastapi-test -f compose.yaml -f compose.test.yaml up -d --build
CABWATCH_TEST_DATABASE_URL=postgresql://cabwatch:cabwatch@127.0.0.1:15433/cabwatch python -m unittest -v test_schema
CABWATCH_KAFKA_TEST_URL=http://127.0.0.1:18082/cab-watch CABWATCH_FAULT_TESTS=1 python -m unittest -v test_kafka_flow
```

The fault test stops only this isolated project's PostgreSQL and Redis containers and restarts its API. Never run it against production. On PowerShell set the variables with `$env:NAME='value'` before each Python command. Set `TELEMETRY_MODE=direct`, recreate only the test dashboard, and use `CABWATCH_TEST_URL` with `test_account_flow` to verify the original direct path.
