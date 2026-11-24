CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS market_events (
    received_at timestamptz NOT NULL,
    event_id uuid NOT NULL,
    source text NOT NULL,
    event_type text NOT NULL,
    source_instrument_id text,
    market_segment text NOT NULL,
    feed_time timestamptz,
    payload jsonb NOT NULL,
    PRIMARY KEY (received_at, event_id)
);
SELECT create_hypertable('market_events', 'received_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS market_events_instrument_time
    ON market_events (source_instrument_id, received_at DESC);
