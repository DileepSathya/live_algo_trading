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

CREATE TABLE IF NOT EXISTS volume_baseline (
    source text, source_instrument_id text, trade_time timestamptz NOT NULL,
    price double precision NOT NULL, vtt bigint NOT NULL,
    PRIMARY KEY (source, source_instrument_id)
);
CREATE TABLE IF NOT EXISTS candle_volume (
    source text, source_instrument_id text, candle_start timestamptz,
    uptick_volume bigint NOT NULL, downtick_volume bigint NOT NULL,
    unchanged_price_volume bigint NOT NULL, volume_partial boolean NOT NULL,
    PRIMARY KEY (source, source_instrument_id, candle_start)
);
CREATE OR REPLACE VIEW candles_5m AS
WITH candles AS (
SELECT source, source_instrument_id, candle_start, open, high, low, close,
       uptick_volume + downtick_volume + unchanged_price_volume AS volume,
       uptick_volume, downtick_volume, unchanged_price_volume,
       coalesce(volume_partial, true) AS volume_partial
FROM candle_state_5m LEFT JOIN candle_volume USING (source, source_instrument_id, candle_start)
WHERE complete
)
SELECT candles.*,
       round(100.0 * uptick_volume / nullif(volume, 0), 2) AS uptick_pct,
       round(100.0 * downtick_volume / nullif(volume, 0), 2) AS downtick_pct,
       round(100.0 * unchanged_price_volume / nullif(volume, 0), 2) AS unchanged_vol_pct
FROM candles;

-- Persist the completed projection and keep it in sync with late corrections.
CREATE TABLE IF NOT EXISTS "5mins_candle" (
    source text NOT NULL, source_instrument_id text NOT NULL, candle_start timestamptz NOT NULL,
    open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL, close numeric NOT NULL,
    volume bigint, uptick_volume bigint, downtick_volume bigint, unchanged_price_volume bigint,
    volume_partial boolean NOT NULL, uptick_pct numeric, downtick_pct numeric, unchanged_vol_pct numeric,
    PRIMARY KEY (source, source_instrument_id, candle_start)
);

CREATE OR REPLACE FUNCTION store_completed_candle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO "5mins_candle"
    SELECT * FROM candles_5m
    WHERE source = NEW.source AND source_instrument_id = NEW.source_instrument_id
      AND candle_start = NEW.candle_start
    ON CONFLICT (source, source_instrument_id, candle_start) DO UPDATE SET
        (open, high, low, close, volume, uptick_volume, downtick_volume, unchanged_price_volume,
         volume_partial, uptick_pct, downtick_pct, unchanged_vol_pct) =
        (excluded.open, excluded.high, excluded.low, excluded.close, excluded.volume,
         excluded.uptick_volume, excluded.downtick_volume, excluded.unchanged_price_volume,
         excluded.volume_partial, excluded.uptick_pct, excluded.downtick_pct, excluded.unchanged_vol_pct);
    RETURN NULL;
END;
$$;

-- Timescale propagates hypertable triggers to its chunks.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='candle_state_5m'::regclass
                   AND tgname='store_completed_price') THEN
        CREATE TRIGGER store_completed_price AFTER INSERT OR UPDATE ON candle_state_5m
        FOR EACH ROW WHEN (NEW.complete) EXECUTE FUNCTION store_completed_candle();
    END IF;
END;
$$;
CREATE OR REPLACE TRIGGER store_completed_volume AFTER INSERT OR UPDATE ON candle_volume
FOR EACH ROW EXECUTE FUNCTION store_completed_candle();

INSERT INTO "5mins_candle" SELECT * FROM candles_5m
ON CONFLICT (source, source_instrument_id, candle_start) DO NOTHING;
