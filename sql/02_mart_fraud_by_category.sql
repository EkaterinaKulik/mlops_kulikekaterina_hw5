BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.mart_fraud_by_category;

CREATE TABLE mart.mart_fraud_by_category AS
WITH agg AS (
    SELECT
        cat_id,
        COUNT(*) AS tx_total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS fraud_cnt,
        SUM(amount) AS amount_total,
        SUM(CASE WHEN target = 1 THEN amount ELSE 0 END) AS amount_fraud
    FROM raw.raw_transactions
    GROUP BY cat_id
)
SELECT
    cat_id,
    tx_total,
    fraud_cnt,
    ROUND((fraud_cnt::numeric / NULLIF(tx_total, 0)) * 100, 4) AS fraud_rate_pct,
    ROUND(amount_total::numeric, 2) AS amount_total,
    ROUND(amount_fraud::numeric, 2) AS amount_fraud,
    ROUND((amount_fraud::numeric / NULLIF(amount_total, 0)) * 100, 4) AS fraud_amount_share_pct
FROM agg
ORDER BY fraud_rate_pct DESC, tx_total DESC;

CREATE INDEX IF NOT EXISTS ix_mart_fraud_by_category_cat ON mart.mart_fraud_by_category (cat_id);

COMMIT;
