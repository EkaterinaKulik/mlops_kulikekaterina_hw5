BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS raw.raw_transactions;

CREATE TABLE raw.raw_transactions (
    transaction_time   timestamp without time zone,
    merch              text,
    cat_id             text,
    amount             numeric(18, 2),

    name_1             text,
    name_2             text,
    gender             text,

    street             text,
    one_city           text,
    us_state           text,
    post_code          text,

    lat                double precision,
    lon                double precision,
    population_city    integer,

    jobs               text,

    merchant_lat       double precision,
    merchant_lon       double precision,

    target             integer
);

CREATE INDEX IF NOT EXISTS ix_raw_trx_time ON raw.raw_transactions (transaction_time);
CREATE INDEX IF NOT EXISTS ix_raw_state_time ON raw.raw_transactions (us_state, transaction_time);
CREATE INDEX IF NOT EXISTS ix_raw_cat ON raw.raw_transactions (cat_id);
CREATE INDEX IF NOT EXISTS ix_raw_merch ON raw.raw_transactions (merch);
CREATE INDEX IF NOT EXISTS ix_raw_target ON raw.raw_transactions (target);

COMMIT;
