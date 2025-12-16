BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.mart_fraud_by_state;

CREATE TABLE mart.mart_fraud_by_state AS
WITH base AS (
    SELECT
        us_state,
        merch,
        amount,
        target,
        md5(
            COALESCE(name_1, '') || '|' ||
            COALESCE(name_2, '') || '|' ||
            COALESCE(gender, '') || '|' ||
            COALESCE(street, '') || '|' ||
            COALESCE(one_city, '') || '|' ||
            COALESCE(us_state, '') || '|' ||
            COALESCE(post_code, '')
        ) AS customer_sk
    FROM raw.raw_transactions
),
agg AS (
    SELECT
        us_state,
        COUNT(*) AS tx_total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS fraud_cnt,
        ROUND((SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)) * 100, 4) AS fraud_rate_pct,

        COUNT(DISTINCT customer_sk) AS unique_customers,
        COUNT(DISTINCT merch) AS unique_merchants,

        SUM(amount) AS amount_total,
        SUM(CASE WHEN target = 1 THEN amount ELSE 0 END) AS amount_fraud
    FROM base
    GROUP BY us_state
)
SELECT
    us_state,
    tx_total,
    fraud_cnt,
    fraud_rate_pct,
    unique_customers,
    unique_merchants,
    ROUND(amount_total::numeric, 2) AS amount_total,
    ROUND(amount_fraud::numeric, 2) AS amount_fraud,
    ROUND((amount_fraud::numeric / NULLIF(amount_total, 0)) * 100, 4) AS fraud_amount_share_pct
FROM agg
ORDER BY fraud_rate_pct DESC, tx_total DESC;

CREATE INDEX IF NOT EXISTS ix_mart_fraud_by_state_state ON mart.mart_fraud_by_state (us_state);

COMMIT;
