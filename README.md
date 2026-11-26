# CabWatch journey history

CabWatch simulates cab/delivery journeys and records GPS deviations from planned routes. It records journey history and pushes confirmed off-route status to the dashboard over a WebSocket. The map shows the latest **processed** reading; processing can lag behind GPS uploads.

## Run

```sh
docker compose up --build
```

Open http://localhost:18080/cab-watch/. Register, add a trip, and start the simulator. Change route creates a detour; Return to route retraces it. Select a trip to see its captured GPS readings, measured deviations, and incidents. Refresh history loads the latest records; Load more paginates older-to-newer readings. The roster lists all trips, including multiple trips by the same vehicle.

## One Kafka group, four tables

```mermaid
flowchart LR
 Browser[Browser GPS] --> API[FastAPI validation]
 API -->|key = vehicle ID| Kafka[GPS topic: 3 partitions]
 Kafka --> Worker[Journey history consumer group]
 Worker -->|one SQL transaction| SQL[(PostgreSQL)]
 SQL --> Dashboard[Recorded journeys and deviations]
```

There is no live Kafka group and no Redis dependency. One group named `<topic>.journey-history-v1` runs three consumer clients in one worker process by default (one partition each after assignment settles); `HISTORY_CONSUMERS` changes the worker count within that group. More workers than partitions cannot add partition-reader parallelism. The local broker has one replica, persistent storage and seven-day retention; it is not highly available.

Exactly four application tables exist in `cab_watch_accounts`:

| Table | Contents |
| --- | --- |
| users | Accounts and signing keys |
| vehicles | Vehicle ownership and assignment |
| trips | Planned route, accepted checkpoint, GPS readings keyed by sequence in JSONB, processed checkpoint and analysis state |
| alerts | Historical off-route incidents and preserved legacy alerts |

`POST /cab-watch/me/gps` validates the entire batch before publishing separate records. `202 queued` follows broker acknowledgment and a PostgreSQL accepted-checkpoint write. It does not mean analysis has finished. Retry the identical trip/sequence after a timeout or 503; a partial send may have succeeded. One active GPS sender per trip is expected.

The consumer locks each trip, inserts previously unseen readings, rebuilds analysis in sequence order, and commits readings, derived alerts and trip checkpoints together. Only then does it commit Kafka offsets. A crash between SQL commit and offset commit safely replays. The first stored reading for a trip/sequence wins; conflicting duplicate content does not replace it. Historical alert IDs are deterministic, while internal numeric row IDs may change during recomputation. Use `event_key` as the durable incident identifier.

Kafka records use a flat envelope: `trip_id`, `user_id`, `vehicle_id`, `route`,
`sequence`, `captured_at`, `lat`, `lon`, and `motion`. There is no nested `point`
or duplicated trip ID. The Kafka key remains the vehicle ID. Consumers also read
older nested `point` records so retained messages can replay on the same topic.
Accepted checkpoints and stored readings retain their existing reading-only
shape; routing/ownership metadata is not copied into every stored GPS point.
Pause uploads and replace all older app replicas before resuming: older consumers
cannot read the new flat records. See the deployment checklist for rollout and rollback.

## Historical detection

Every reading includes its measured distance and whether it is at least 200 metres off route. An incident requires three consecutive off-route samples, increasing capture timestamps, and no more than ten seconds between capture times. This is an evidence rule, not a processing-time freshness requirement. A week-old backlog still produces events dated at capture time. Missing sequences, reversed timestamps and capture gaps break confirmation; missing data is not proof a vehicle returned to its route. A later missing sample triggers deterministic recomputation. Completed journeys remain terminal for incident analysis; subsequent samples remain in recorded history.

The dashboard labels unresolved incidents as open at the last recorded sample, not as current danger. Historical capture times and processing timestamps are exposed. No system can infer a deviation that happened between unrecorded positions.

## API

- `GET /healthz`: version `journey-history-v2`, active telemetry mode (liveness only).
- `POST /cab-watch/me/trips`: create a trip with UUID `Idempotency-Key`.
- `POST /cab-watch/me/gps`: upload one reading per trip in a batch, maximum 1000.
- `GET /cab-watch/me/trips`: owned trips, accepted checkpoints, processing status and recent alerts.
- `GET /cab-watch/me/trips/{id}/history?after=0`: owned GPS history (1000 per page), incident evidence, next cursor and processing progress.
- `POST /cab-watch/me/alerts/{event_key}/explain`: explain recorded evidence, optionally through Gemini.
- `GET /cab-watch/metrics`: bearer `METRICS_TOKEN`, consumer lag and retries.

`/me/live` has been removed. `TELEMETRY_MODE=direct` is a database-only development fallback using the same historical analysis; Kafka mode is the default. Do not mix modes against the same active workload.

## Migration and operating limits

See [the cutover checklist](docs/KAFKA_DEPLOYMENT.md) before replacing a running installation. Startup transactionally folds existing `telemetry_history` rows into `trips.readings`, rebuilds missing history state and then removes the fifth table. Existing legacy alerts are retained and marked `historical=false`; derived historical incidents are separate and may overlap old evidence. Older direct-mode GPS samples were never stored and cannot be reconstructed.

JSONB keeps the requested four-table model but has an important cost: each changed trip is read/rebuilt and its JSON rewritten. Very long trips increase CPU, row size and write amplification. This release has **not** been certified for 10,000 vehicles or unbounded trips. Benchmark representative journey lengths, batch sizes, downtime and retention before scaling. If SQL slows down, ingestion checkpoint writes can also fail and clients must retry. Keep Kafka retention longer than your supported outage/backlog. Completed trips and their readings/alerts expire after `TRIP_RETENTION_DAYS` (default 90); active-trip data can continue growing.

## Tests

```sh
python -m pip install -r requirements.txt
python -m unittest -v test_cab_watch test_retention test_telemetry test_live test_journey test_stream
node --test test_motion.mjs test_telemetry.mjs
docker compose -p cabwatch-fastapi-test -f compose.yaml -f compose.test.yaml up -d --build
```

With the isolated test stack, set `CABWATCH_TEST_DATABASE_URL=postgresql://cabwatch:cabwatch@127.0.0.1:15433/cabwatch` and `CABWATCH_KAFKA_TEST_URL=http://127.0.0.1:18082/cab-watch`, then run `python -m unittest -v test_schema test_kafka_flow`. Optional `CABWATCH_FAULT_TESTS=1` restarts only the test project's application. Never use a production database for tests. CI also checks direct-mode API ownership, late arrivals and deduplication.


## Final-year demo: independent producer and consumer

Compose runs FastAPI (`dashboard`) and a separate `history-worker` process. The API only produces Kafka messages; it never consumes them. The worker uses the same image with `python -m cab_watch.worker`, owns the history group, and writes PostgreSQL before committing offsets. Either process can initialize the schema, using the same migration lock. Both require PostgreSQL; this demo proves consumer-outage buffering, not database-independent ingestion.

1. Start the stack: `docker compose up -d --build`.
2. Open the dashboard, create a trip and start movement.
3. Run `docker compose stop history-worker`. Continue the simulation. Requests return 202, accepted checkpoints advance, but processed history stays behind.
4. Inspect the backlog:
   `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group cabwatch.gps.v2.journey-history-v1`
5. Run `docker compose start history-worker`. Refresh trip history: buffered readings and capture-time deviations appear as the worker catches up.
6. Run `docker compose restart history-worker` to demonstrate resumption without duplicate readings.

The Kafka offset command may show no group if the worker has never started. Stop/start retains Kafka and PostgreSQL volumes. In direct development mode, stop the history worker before running a direct-mode API. Do not run the previous combined API/consumer release alongside this release if you want the worker-stop demo to pause all consumption.

## Schema compared with the original four-table drawing

This process split requires **no new database migration**. The earlier historical version already added:

| Table | Additional column | Purpose |
| --- | --- | --- |
| trips | readings JSONB NOT NULL DEFAULT '{}' | GPS history keyed by sequence; includes calculated distance and evidence flags |
| trips | history_state JSONB, nullable | Derived confirmation and incident state |
| trips | accepted_point JSONB, nullable | Latest upload acknowledged to the simulator |
| trips | processed_at timestamptz, nullable | Actual history processing time |
| alerts | event_key UUID, nullable, unique index | Stable identity for a recalculated incident |
| alerts | close_reason TEXT, nullable | Why an incident ended |
| alerts | historical BOOLEAN NOT NULL DEFAULT false | Distinguishes derived history from preserved legacy alerts |

Users and vehicles are unchanged. Existing primary keys and foreign keys remain. The physical table is named `alerts` (plural). Kafka offsets are stored in Kafka, not in another SQL table. The one-open-alert index is now restricted to legacy (`historical=false`) rows; historical incidents are rebuilt per trip. Old installations with a fifth `telemetry_history` table still use the documented copy-then-drop migration.

## WebSocket off-route alerts and three partitions

The default application topic is now `cabwatch.gps.v2`, with **3 partitions** and
**3 consumers** in the existing history group. Run one `history-worker` service;
its three async consumer tasks share a process (they are not three CPU processes).
Kafka assigns one partition to each while all three are healthy; on a failure,
surviving consumers take over. Vehicle ID remains the record key.

The detector still requires three consecutive readings at least 200 m off route.
After saving each changed journey, it calls PostgreSQL `pg_notify` inside the SQL
transaction. PostgreSQL delivers that signal only on commit. FastAPI has one
LISTEN connection and wakes the signed-in user's WebSockets. It reads the latest
committed state and sends `off_route_snapshot` to `/cab-watch/me/alerts/ws`.
The first client frame is `{"token":"<access token>"}`; tokens never go in URLs.
The server verifies the token on connect and again before each snapshot.
No Redis, second Kafka group, extra table, or schema migration is introduced.

An active incident turns the vehicle marker and roster red with a glowing border,
an OFF ROUTE badge, measured distance, sequence and capture time. Returning to the
route or completing the trip clears it. A snapshot on reconnect and every five
seconds restores state after missed notifications; these are current processed
states, not a guaranteed delivery stream of every intermediate alert transition.
The history table retains those incidents. A disconnected socket falls back to
roster polling and reconnects. WebSocket delivery does not remove Kafka backlog:
old readings retain their capture time and are never represented as current GPS.

### Upgrade an existing 12-partition installation

Kafka cannot reduce an existing topic's partition count. Do not delete its data.
Before deploying this release, pause all browser simulators/uploads, let the old
worker drain `cabwatch.gps.recovery.v1` until group lag is zero, then stop the old
API and worker. Deploy this release with `docker compose up -d --build` and resume
uploads. The new topic/group start empty; existing PostgreSQL history is retained.
Keep the old topic for recovery until its records are no longer needed.

If already switched before draining: pause uploads, stop the new worker, and run
`docker compose run --rm -e KAFKA_TOPIC=cabwatch.gps.recovery.v1 -e KAFKA_PARTITIONS=12 history-worker`
until the old group has zero lag, stop that temporary worker, then start the normal
worker and resume. Never run old/new-topic workers concurrently during cutover.
On a fresh install only the new three-partition application topic is created.
Kafka's internal offset topic is separate and must not be resized.
