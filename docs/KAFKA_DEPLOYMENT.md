# Safe upgrade: incremental GPS and four tables

This upgrade deliberately removes complete GPS history from PostgreSQL. It keeps
accounts, vehicle IDs, trips, latest processed coordinates and existing numeric
alert records. Do not run old and new versions against the same database.

## Existing deployment

1. Pause all simulators/uploads. Wait for each browser's pending uploads to finish.
   Keep the OLD worker running until the existing group has zero lag on all three
   partitions. The old API's accepted/processed display can help verify the drain.

   ```sh
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group cabwatch.gps.v2.journey-history-v1
   docker compose stop dashboard history-worker
   ```

2. With all application writers stopped, back up PostgreSQL. These commands avoid
   binary shell redirection and work in PowerShell as well as POSIX shells:

   ```sh
   docker compose exec -T db pg_dump -U cabwatch -d cabwatch -Fc -f /tmp/cabwatch-before-simplify.dump
   docker compose cp db:/tmp/cabwatch-before-simplify.dump ./cabwatch-before-simplify.dump
   docker compose exec -T db pg_restore --list /tmp/cabwatch-before-simplify.dump
   ```

   Store the backup outside container volumes; confirm it exists and test restoring
   it in a disposable PostgreSQL database before discarding the only copy of history.
   Do not use `docker compose down -v`. Back up retained Kafka storage too if it is
   needed for your recovery plan; SQL backup is not a Kafka backup.

3. Check overlapping open alerts before upgrade:

   ```sql
   SELECT trip_id, array_agg(id ORDER BY id) AS open_alert_ids
   FROM cab_watch_accounts.alerts
   WHERE closed_sequence IS NULL
   GROUP BY trip_id HAVING count(*) > 1;
   ```

   Migration stops transactionally if any exist, naming the affected trip/alert IDs.
   It never deletes an incident or invents a resolution timestamp. Review the backed-up
   evidence under the old version; only close incidents if an actual valid return or
   completion reading proves the closing sequence and capture time. If no such evidence
   exists, keep the old version/schema until that data conflict is resolved. Do not
   disable the constraint, merge IDs or fabricate closure evidence to force deployment.

4. Deploy this source. After the verified backup, set `CABWATCH_BACKUP_CONFIRMED=1`
   in Compose's `.env` (or your deployment environment) for the first startup:

   ```sh
   docker compose up -d --build
   docker compose logs --tail 100 dashboard history-worker
   ```

   Both processes use the same migration advisory lock. Migration initializes counts
   from matching valid analysis state, or a contiguous suffix of retained evidence
   where possible, before dropping obsolete columns/history. Unknown counts start at
   zero; an existing open incident stays open regardless. Completed trips start at zero.
   The migration preserves current positions and all incident rows/IDs and restores
   the one-open-alert index. Any error rolls the transaction back, including schema changes.
   Restarting repeats safely and does not recreate removed fields. Once upgraded,
   remove the backup-confirmation setting; fresh installs do not require it.

5. Keep topic `cabwatch.gps.v2`, its three partitions, and existing group
   `cabwatch.gps.v2.journey-history-v1`. **Do not delete/recreate the topic or reset offsets.**
   The same consumer accepts old nested/flat records while ignoring route/motion fields.
   Historical records with sequences already processed are skipped. Kafka retention
   remains seven days, so an outage longer than retention can lose unprocessed evidence.

6. Reload browsers to get the new JavaScript before resuming uploads. Old pending
   sessionStorage arrays can still retry their GPS fields, but old arrays without
   complete simulator checkpoints cannot resume movement. Existing trips without
   this tab's local checkpoint are explicitly display only. Add a new trip if needed;
   unfinished trips may keep vehicles busy after local session loss.

## Rollback

Before accepting new uploads, rollback means stop both new processes, restore the
verified backup into a clean replacement database, and deploy the previous source.
The old binary cannot run against the simplified schema. After accepting new uploads,
coordinate SQL backup restoration with Kafka offsets and retained records; simply
rolling code back would lose new checkpoint/incident changes. Do not rewind offsets
blindly or run mixed versions. Keep the backup until the upgrade has been validated.

## Validation

Check sign-in, idempotent trip creation, latest positions, numeric-ID incident
explanation, red WebSocket markers and return-to-route clearing. Confirm exactly four
application tables and the column list in [SCHEMA.md](SCHEMA.md). The live database
migration is an operator action; changing repository source alone does not back up
or deploy a running installation.

For disposable integration tests:

```sh
docker compose -p cabwatch-fastapi-test -f compose.yaml -f compose.test.yaml up -d --build
```

Set these environment variables in your shell:
- `CABWATCH_TEST_DATABASE_URL=postgresql://cabwatch:cabwatch@127.0.0.1:15433/cabwatch`
- `CABWATCH_KAFKA_TEST_URL=http://127.0.0.1:18082/cab-watch`
- `CABWATCH_FAULT_TESTS=1` only for that isolated Docker project.

Then run `python -m unittest -v test_schema test_kafka_flow test_notifications`.
For UI tests install `playwright==1.65.2` and run `python -m playwright install chromium`,
then `python -c "import runpy; runpy.run_path('tests/browser_smoke.py')"` and the same
command with `tests/browser_recovery.py`. The latter stops/restarts the test worker
only when fault tests are enabled. All browser tests use local test URLs.

CI runs unit tests, JS state/outbox tests, real PostgreSQL and Kafka tests, browser
checks with worker-outage refresh, then direct-mode HTTP checks. Assertions include
three actual consumer assignments, zero ingestion writes, migration rollback,
no duplicate incidents, timestamp/sequence continuity, restart confirmation counts,
user isolation, WebSocket reconnects and original pending GPS retries.

## Operational limits

Three asynchronous clients share one worker process. A process crash stops all three;
Kafka buffers until it restarts. A member failure/rebalance resumes from committed
offsets, and SQL checkpoints deduplicate replay. A failed SQL batch retries without
advancing offsets. A permanently malformed retained event can block its partition;
inspect worker logs and correct the source rather than discarding records silently.
The project adds neither a dead-letter topic nor a second alert consumer group.

Snapshots are latest processed state, not an audit of every WebSocket notification.
Use the durable alerts table for incident history. Capture-time labels expose delay;
WebSockets cannot remove Kafka lag or guarantee instant detection.
