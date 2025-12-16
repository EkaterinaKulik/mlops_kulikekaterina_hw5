BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.mart_merchant_analytics;

CREATE TABLE mart.mart_merchant_analytics AS
WITH base AS (
    SELECT
        merch,
        amount,
        target,
        us_state,
        cat_id,
        transaction_time
    FROM raw.raw_transactions
),
agg AS (
    SELECT
        merch,
        COUNT(*) AS tx_total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS fraud_cnt,
        (SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)) AS fraud_rate,
        SUM(amount) AS turnover,
        AVG(amount) AS avg_check,
        SUM(CASE WHEN target = 1 THEN amount ELSE 0 END) AS amount_fraud,

        COUNT(DISTINCT us_state) AS states_cnt,
        COUNT(DISTINCT cat_id) AS categories_cnt,

        MIN(transaction_time) AS first_tx_time,
        MAX(transaction_time) AS last_tx_time
    FROM base
    GROUP BY merch
)
SELECT
    merch,
    tx_total,
    fraud_cnt,
    ROUND((fraud_rate * 100)::numeric, 4) AS fraud_rate_pct,
    ROUND(turnover::numeric, 2) AS turnover,
    ROUND(avg_check::numeric, 2) AS avg_check,
    ROUND(amount_fraud::numeric, 2) AS amount_fraud,
    ROUND((amount_fraud::numeric / NULLIF(turnover, 0)) * 100, 4) AS fraud_amount_share_pct,
    states_cnt,
    categories_cnt,
    first_tx_time,
    last_tx_time,
    CASE
        WHEN (fraud_rate >= 0.03) AND (tx_total >= 50) THEN 1
        ELSE 0
    END AS is_suspicious
FROM agg
ORDER BY is_suspicious DESC, fraud_rate_pct DESC, tx_total DESC;

CREATE INDEX IF NOT EXISTS ix_mart_merchant_analytics_merch
    ON mart.mart_merchant_analytics (merch);

CREATE INDEX IF NOT EXISTS ix_mart_merchant_analytics_suspicious
    ON mart.mart_merchant_analytics (is_suspicious);

COMMIT;
