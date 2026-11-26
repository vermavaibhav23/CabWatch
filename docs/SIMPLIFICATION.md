# Simplification delivery notes

## Changes

- Latest GPS checkpoints replace full journey history/recomputation.
- Kafka ingestion performs two SELECTs and no writes; events contain seven fields.
- Incremental off-route count and permanent numeric incident IDs survive restart/replay.
- Three partitions, three consumers, existing topic/group, manual offsets and WebSockets remain.
- Simulator snapshots and original pending readings persist atomically in sessionStorage.
- Lost local sessions display existing trips without generating new GPS.
- Backup-gated migration preserves records, initializes counters and rejects overlapping open incidents.
- Automatic trip/incident deletion is removed; Kafka retention remains seven days.

## Reference deliverables

- [Exhaustive four-table schema and ER diagram](SCHEMA.md)
- [Architecture, trip/GPS API examples and Kafka envelope](../README.md)
- [Browser refresh behaviour and incremental consumer flow](../README.md#consumer-processing-and-recovery)
- [Safe backup, migration, deployment and rollback](KAFKA_DEPLOYMENT.md)

## Executed local checks

- 36 Python unit/database tests passed, including exact schema, legacy migration,
  overlap rollback, backup guard, evidence-derived counters, SQL rollback, NOTIFY
  commit timing, incremental incident lifecycle and two-read/no-write HTTP ingestion.
- 3 live HTTP/WebSocket tests passed with a local PostgreSQL-backed direct-mode API:
  concurrent/idempotent dispatch, ownership, incident clearing and reconnects.
- 11 JavaScript tests passed for movement, detours, return paths and atomic outbox state.
- Two Playwright scripts passed against local PostgreSQL/direct-mode API: red markers,
  capture-time display, resolved incidents, display-only trips, normal/detour/return
  refresh and exact pending-upload retries. No browser JavaScript errors occurred.
- Real Kafka integration and browser refresh during a real worker outage are run by
  GitHub Actions on the pushed commit. Use its actual result, not local direct-mode
  results, as evidence for broker behaviour.

Production data has not been backed up or migrated by these repository changes.
Follow the deployment checklist on your running instance before starting new code.

## Changed files

Status: M modified, A added, D removed. Removed migration/state machinery is replaced
by the current incremental processor and transactional migration.

| Status | File |
|---|---|
| M | `.github/workflows/tests.yml` |
| M | `README.md` |
| M | `cab_watch/api.py` |
| M | `cab_watch/assets/board.mjs` |
| M | `cab_watch/assets/telemetry.mjs` |
| M | `cab_watch/fleet.py` |
| M | `cab_watch/index.html` |
| M | `cab_watch/journey.py` |
| D | `cab_watch/kafka_schema.sql` |
| M | `cab_watch/live.py` |
| D | `cab_watch/normalize_trips.sql` |
| M | `cab_watch/notifications.py` |
| D | `cab_watch/retention.py` |
| M | `cab_watch/schema.sql` |
| M | `cab_watch/stream.py` |
| M | `compose.yaml` |
| M | `docs/KAFKA_DEPLOYMENT.md` |
| M | `main.py` |
| M | `test_account_flow.py` |
| M | `test_journey.py` |
| M | `test_kafka_flow.py` |
| M | `test_live.py` |
| M | `test_retention.py` |
| M | `test_schema.py` |
| M | `test_stream.py` |
| M | `test_telemetry.mjs` |
| M | `tests/browser_smoke.py` |
| A | `cab_watch/migration.py` |
| A | `docs/SCHEMA.md` |
| A | `tests/browser_recovery.py` |
| A | `tests/fixtures/history_schema.sql` |
| A | `docs/SIMPLIFICATION.md` |
