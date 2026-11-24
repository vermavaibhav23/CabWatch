# CabWatch journey history

CabWatch simulates cab/delivery journeys and records GPS deviations from planned routes. It is a historical audit tool, not a real-time alerting service. The map shows the latest **processed** reading; processing can lag behind GPS uploads.

## Run

```sh
docker compose up --build
```

Open http://localhost:18080/cab-watch/. Register, add a trip, and start the simulator. Change route creates a detour; Return to route retraces it. Select a trip to see its captured GPS readings, measured deviations, and incidents. Refresh history loads the latest records; Load more paginates older-to-newer readings. The roster lists all trips, including multiple trips by the same vehicle.

## One Kafka group, four tables

```mermaid
flowchart LR
 Browser[Browser GPS] --> API[FastAPI validation]
 API -->|key = vehicle ID| Kafka[GPS topic: 12 partitions]
 Kafka --> Worker[Journey history consumer group]
 Worker -->|one SQL transaction| SQL[(PostgreSQL)]
 SQL --> Dashboard[Recorded journeys and deviations]
```

There is no live Kafka group and no Redis dependency. One group named `<topic>.journey-history-v1` runs one consumer per app process by default; `HISTORY_CONSUMERS` changes the worker count within that group. More workers than partitions cannot add partition-reader parallelism. The local broker has one replica, persistent storage and seven-day retention; it is not highly available.

Exactly four application tables exist in `cab_watch_accounts`:

| Table | Contents |
| --- | --- |
| users | Accounts and signing keys |
| vehicles | Vehicle ownership and assignment |
| trips | Planned route, accepted checkpoint, GPS readings keyed by sequence in JSONB, processed checkpoint and analysis state |
| alerts | Historical off-route incidents and preserved legacy alerts |

`POST /cab-watch/me/gps` validates the entire batch before publishing separate records. `202 queued` follows broker acknowledgment and a PostgreSQL accepted-checkpoint write. It does not mean analysis has finished. Retry the identical trip/sequence after a timeout or 503; a partial send may have succeeded. One active GPS sender per trip is expected.

The consumer locks each trip, inserts previously unseen readings, rebuilds analysis in sequence order, and commits readings, derived alerts and trip checkpoints together. Only then does it commit Kafka offsets. A crash between SQL commit and offset commit safely replays. The first stored reading for a trip/sequence wins; conflicting duplicate content does not replace it. Historical alert IDs are deterministic, while internal numeric row IDs may change during recomputation. Use `event_key` as the durable incident identifier.

## Historical detection

Every reading includes its measured distance and whether it is at least 200 metres off route. An incident requires three consecutive off-route samples, increasing capture timestamps, and no more than ten seconds between capture times. This is an evidence rule, not a processing-time freshness requirement. A week-old backlog still produces events dated at capture time. Missing sequences, reversed timestamps and capture gaps break confirmation; missing data is not proof a vehicle returned to its route. A later missing sample triggers deterministic recomputation. Completed journeys remain terminal for incident analysis; subsequent samples remain in recorded history.

The dashboard labels unresolved incidents as open at the last recorded sample, not as current danger. Historical capture times and processing timestamps are exposed. No system can infer a deviation that happened between unrecorded positions.

## API

- `GET /healthz`: version `journey-history-v1`, active telemetry mode (liveness only).
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
python -m unittest -v test_cab_watch test_retention test_telemetry test_live test_journey
node --test test_motion.mjs test_telemetry.mjs
docker compose -p cabwatch-fastapi-test -f compose.yaml -f compose.test.yaml up -d --build
```

With the isolated test stack, set `CABWATCH_TEST_DATABASE_URL=postgresql://cabwatch:cabwatch@127.0.0.1:15433/cabwatch` and `CABWATCH_KAFKA_TEST_URL=http://127.0.0.1:18082/cab-watch`, then run `python -m unittest -v test_schema test_kafka_flow`. Optional `CABWATCH_FAULT_TESTS=1` restarts only the test project's application. Never use a production database for tests. CI also checks direct-mode API ownership, late arrivals and deduplication.
