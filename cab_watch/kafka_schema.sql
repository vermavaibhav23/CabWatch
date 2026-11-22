-- Additive migration. Existing users, trips and alerts are retained.
ALTER TABLE cab_watch_accounts.alerts ADD COLUMN IF NOT EXISTS event_key uuid;
ALTER TABLE cab_watch_accounts.alerts ADD COLUMN IF NOT EXISTS close_reason text;
CREATE UNIQUE INDEX IF NOT EXISTS kafka_alert_event ON cab_watch_accounts.alerts(event_key);
CREATE TABLE IF NOT EXISTS cab_watch_accounts.telemetry_history (
    trip_id uuid NOT NULL REFERENCES cab_watch_accounts.trips ON DELETE CASCADE,
    sequence integer NOT NULL, captured_at timestamptz NOT NULL,
    lat double precision NOT NULL, lon double precision NOT NULL, motion jsonb,
    received_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(trip_id,sequence)
);
