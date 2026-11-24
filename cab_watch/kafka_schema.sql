-- Four-table migration. Run inside the startup transaction with old writers stopped.
ALTER TABLE cab_watch_accounts.alerts ADD COLUMN IF NOT EXISTS event_key uuid;
ALTER TABLE cab_watch_accounts.alerts ADD COLUMN IF NOT EXISTS close_reason text;
ALTER TABLE cab_watch_accounts.alerts ADD COLUMN IF NOT EXISTS historical boolean NOT NULL DEFAULT false;
CREATE UNIQUE INDEX IF NOT EXISTS kafka_alert_event ON cab_watch_accounts.alerts(event_key);
ALTER TABLE cab_watch_accounts.trips ADD COLUMN IF NOT EXISTS readings jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE cab_watch_accounts.trips ADD COLUMN IF NOT EXISTS history_state jsonb;
ALTER TABLE cab_watch_accounts.trips ADD COLUMN IF NOT EXISTS accepted_point jsonb;
ALTER TABLE cab_watch_accounts.trips ADD COLUMN IF NOT EXISTS processed_at timestamptz;
-- Historical recomputation must not overwrite pre-cutover alert evidence.
DROP INDEX IF EXISTS cab_watch_accounts.one_open_alert;
CREATE UNIQUE INDEX one_open_alert ON cab_watch_accounts.alerts(trip_id) WHERE closed_sequence IS NULL AND NOT historical;
DO $$
BEGIN
    IF to_regclass('cab_watch_accounts.telemetry_history') IS NOT NULL THEN
        UPDATE cab_watch_accounts.trips t SET readings=h.points || t.readings
        FROM (SELECT trip_id,jsonb_object_agg(sequence::text,
              jsonb_build_object('trip_id',trip_id,'sequence',sequence,'captured_at',captured_at,
                                 'lat',lat,'lon',lon,'motion',motion)) AS points
              FROM cab_watch_accounts.telemetry_history GROUP BY trip_id) h
        WHERE t.id=h.trip_id;
        -- Data was copied above; transactional rollback preserves the source on failure.
        DROP TABLE cab_watch_accounts.telemetry_history;
    END IF;
END $$;
