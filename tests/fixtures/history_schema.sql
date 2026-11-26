CREATE SCHEMA IF NOT EXISTS cab_watch_accounts;
CREATE TABLE IF NOT EXISTS cab_watch_accounts.users (
    id uuid PRIMARY KEY, name text NOT NULL, email text NOT NULL UNIQUE,
    password_hash text NOT NULL, jwt_key bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS cab_watch_accounts.cb_plate_serial START WITH 1 NO CYCLE;
CREATE OR REPLACE FUNCTION cab_watch_accounts.next_plate() RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT 'CB#' || lpad(digits, greatest(3, length(digits)), '0')
    FROM (SELECT nextval('cab_watch_accounts.cb_plate_serial')::text AS digits) serial
$$;
CREATE TABLE IF NOT EXISTS cab_watch_accounts.vehicles (
    id text PRIMARY KEY DEFAULT cab_watch_accounts.next_plate()
        CHECK(id ~ '^(CB#[0-9]{3,}|[A-Z]{2}[0-9]{2}[A-Z]{2}[0-9]{4})$'),
    user_id uuid NOT NULL REFERENCES cab_watch_accounts.users,
    status text NOT NULL DEFAULT 'available' CHECK(status IN ('available','busy')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(id,user_id)
);

CREATE OR REPLACE FUNCTION cab_watch_accounts.valid_route(route jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN jsonb_typeof(route) = 'array' THEN
        jsonb_array_length(route) >= 2 AND NOT EXISTS (
            SELECT FROM jsonb_array_elements_text(route) AS n(node)
            WHERE node IS NULL OR node !~ '^[A-Z]$'
        ) ELSE false END
$$;
CREATE TABLE IF NOT EXISTS cab_watch_accounts.trips (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES cab_watch_accounts.users,
    idempotency_key uuid NOT NULL,
    vehicle_id text NOT NULL,
    planned_route jsonb NOT NULL CONSTRAINT valid_planned_route CHECK(cab_watch_accounts.valid_route(planned_route)),
    last_sequence integer NOT NULL DEFAULT 0,
    last_lat double precision, last_lon double precision, last_captured_at timestamptz,
    motion jsonb,
    created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
    UNIQUE(user_id,idempotency_key),
    FOREIGN KEY(vehicle_id,user_id) REFERENCES cab_watch_accounts.vehicles(id,user_id) ON UPDATE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_trip ON cab_watch_accounts.trips(vehicle_id) WHERE completed_at IS NULL;
CREATE INDEX IF NOT EXISTS vehicle_history ON cab_watch_accounts.trips(vehicle_id,created_at);

CREATE TABLE IF NOT EXISTS cab_watch_accounts.alerts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trip_id uuid NOT NULL REFERENCES cab_watch_accounts.trips ON DELETE CASCADE,
    opened_sequence integer NOT NULL, closed_sequence integer, peak_metres real NOT NULL,
    opened_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_alert ON cab_watch_accounts.alerts(trip_id) WHERE closed_sequence IS NULL;
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
