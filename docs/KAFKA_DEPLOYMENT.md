# Journey-history cutover

This changes the application to one historical Kafka consumer group and four PostgreSQL tables. It does not provision hosting or automatically switch a running service. Existing Render auto-deploy settings may deploy a push to main; coordinate the following before enabling the new code against production.

1. Back up PostgreSQL, Redis and Kafka configuration. Pause GPS senders. On the OLD release, drain both Kafka groups and the Redis `cabwatch:events` SQL outbox. Verify history and pending alert/completion events have reached PostgreSQL. The new release does not read Redis, so deleting Redis first can lose unpersisted events.
2. Stop every old application replica. Never run old and new schema writers together. Keep old Redis data until the cutover has been verified; the new Compose file leaves its named volume untouched.
3. Deploy the new code. Startup transactionally copies the fifth table's GPS history into `trips.readings`, rebuilds history state, then drops `telemetry_history`. No `CASCADE` is used. A failed migration rolls back. Existing users, vehicles, trips and legacy alerts remain. On large databases, schedule enough migration time and disk headroom.
4. Supply `DATABASE_URL`, `TELEMETRY_MODE=kafka`, `KAFKA_BOOTSTRAP_SERVERS`, and `METRICS_TOKEN`. Keep existing SSL/SASL variables (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_CA_PEM`, `KAFKA_SASL_MECHANISM`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`). Redis settings are no longer used.
5. Keep `KAFKA_TOPIC=cabwatch.gps.recovery.v1`: this consumer accepts both legacy nested `point` records and new flat records. The NEW group defaults to `<topic>.journey-history-v1`; do not reuse old live/history group IDs. It reads retained data from earliest and deduplicates against stored trip readings. Existing SQL history older than Kafka retention is preserved by migration. Kafka records whose trips were already deleted by retention are skipped.
6. Defaults: `KAFKA_PARTITIONS=12`, `KAFKA_REPLICATION_FACTOR=1`, `HISTORY_CONSUMERS=1` per process. Existing topics are not resized. For a suitable production cluster use replicated brokers and configure minimum in-sync replicas appropriately. `KAFKA_CREATE_TOPIC=false` permits administrator-managed topics. Retention must cover maximum outage and catch-up time.
7. Verify `/healthz` reports `journey-history-v2`. Verify exactly users, vehicles, trips and alerts in `cab_watch_accounts`. Upload historical samples including a duplicate and a missing sample arriving late; check capture-time incidents and unique history. Restart a worker and verify unchanged results. Resume senders and reload dashboards.

The new UI exposes historical processing progress, not a real-time alert guarantee. Legacy alerts remain alongside derived historical incidents. Preserve backups until verified. Rollback to old code requires a coordinated database restore/migration plan: the old code expects a fifth table and Redis state, so do not simply restart it against this schema.

## Flat Kafka message rollout (existing journey-history installations)

Pause GPS uploads and stop all older application replicas before deploying the
flat-message producer. Deploy all replicas with the compatible reader, then resume
uploads. Keep the existing topic and journey-history group ID/offsets; do not reset
offsets or delete queued messages. No database migration is needed for this wire-format
change. Verify a new record has top-level `sequence`, `captured_at`, `lat`, `lon`
and `motion`, then check the accepted checkpoint and processed journey history.

Do not roll back to a nested-only reader after flat records have been published.
For rollback, retain the dual-format reader (even if restoring the old producer)
so queued or replayed flat records remain readable.


## Separate worker release

Deploy two process types from the same code/image:
- API: `uvicorn main:app --host 0.0.0.0 --port 8080` (or the host-provided port).
- Worker: `python -m cab_watch.worker`.

Both need DATABASE_URL, broker settings and Kafka mode. HISTORY_CONSUMERS applies only to the worker. The API no longer launches any consumers. Run at least one worker to process queued data. A web-only deployment accepts records but cannot advance history. This release adds no columns beyond the existing four-table history migration. Stop old combined-process replicas before the worker-outage demonstration. The API metrics endpoint no longer contains the separate worker's lag observations; use Kafka consumer-group lag commands and worker logs for this small demo. There is no live-monitoring guarantee.
