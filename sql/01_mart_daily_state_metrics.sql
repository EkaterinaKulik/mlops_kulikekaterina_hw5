BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.mart_daily_state_metrics;

CREATE TABLE mart.mart_daily_state_metrics AS
WITH base AS (
    SELECT
        DATE(transaction_time) AS trx_date,
        us_state,
        amount
    FROM raw.raw_transactions
),
agg AS (
    SELECT
        trx_date,
        us_state,
        COUNT(*) AS tx_count,
        SUM(amount) AS tx_sum,
        AVG(amount) AS avg_check,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount,
        AVG(CASE WHEN amount >= 1000 THEN 1.0 ELSE 0.0 END) AS large_tx_share
    FROM base
    GROUP BY trx_date, us_state
)
SELECT
    trx_date,
    us_state,
    tx_count,
    ROUND(tx_sum::numeric, 2) AS tx_sum,
    ROUND(avg_check::numeric, 2) AS avg_check,
    ROUND(p95_amount::numeric, 2) AS p95_amount,
    ROUND((large_tx_share * 100)::numeric, 4) AS large_tx_share_pct
FROM agg
ORDER BY trx_date, us_state;

CREATE INDEX IF NOT EXISTS ix_mart_daily_state_metrics_date_state
    ON mart.mart_daily_state_metrics (trx_date, us_state);

COMMIT;
