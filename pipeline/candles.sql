CREATE TABLE IF NOT EXISTS candle_offsets (
    partition_id integer PRIMARY KEY,
    next_offset bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS candle_state_5m (
    source text NOT NULL,
    source_instrument_id text NOT NULL,
    candle_start timestamptz NOT NULL,
    partition_id integer NOT NULL,
    open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL, close numeric NOT NULL,
    first_time timestamptz NOT NULL, first_offset bigint NOT NULL,
    last_time timestamptz NOT NULL, last_offset bigint NOT NULL,
    complete boolean NOT NULL DEFAULT false,
    PRIMARY KEY (source, source_instrument_id, candle_start)
);
SELECT create_hypertable('candle_state_5m', 'candle_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS candle_state_pending
    ON candle_state_5m (partition_id, candle_start) WHERE NOT complete;

CREATE OR REPLACE VIEW candles_5m AS
SELECT source, source_instrument_id, candle_start, open, high, low, close
FROM candle_state_5m WHERE complete;
