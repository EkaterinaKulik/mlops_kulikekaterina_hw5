BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.mart_customer_risk_profile;

CREATE TABLE mart.mart_customer_risk_profile AS
WITH base AS (
    SELECT
        md5(
            COALESCE(name_1, '') || '|' ||
            COALESCE(name_2, '') || '|' ||
            COALESCE(gender, '') || '|' ||
            COALESCE(street, '') || '|' ||
            COALESCE(one_city, '') || '|' ||
            COALESCE(us_state, '') || '|' ||
            COALESCE(post_code, '')
        ) AS customer_sk,
        name_1,
        name_2,
        gender,
        street,
        one_city,
        us_state,
        post_code,
        amount,
        target,
        transaction_time
    FROM raw.raw_transactions
),
agg AS (
    SELECT
        customer_sk,
        MIN(transaction_time) AS first_tx_time,
        MAX(transaction_time) AS last_tx_time,

        COUNT(*) AS tx_total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS fraud_cnt,
        (SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)) AS fraud_rate,

        AVG(amount) AS avg_check,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount,
        SUM(amount) AS amount_total,
        SUM(CASE WHEN target = 1 THEN amount ELSE 0 END) AS amount_fraud
    FROM base
    GROUP BY customer_sk
),
labeled AS (
    SELECT
        customer_sk,
        first_tx_time,
        last_tx_time,
        tx_total,
        fraud_cnt,
        ROUND((fraud_rate * 100)::numeric, 4) AS fraud_rate_pct,
        ROUND(avg_check::numeric, 2) AS avg_check,
        ROUND(p95_amount::numeric, 2) AS p95_amount,
        ROUND(amount_total::numeric, 2) AS amount_total,
        ROUND(amount_fraud::numeric, 2) AS amount_fraud,
        ROUND((amount_fraud::numeric / NULLIF(amount_total, 0)) * 100, 4) AS fraud_amount_share_pct,
        CASE
            WHEN (fraud_rate >= 0.05) OR (fraud_cnt >= 3) THEN 'HIGH'
            WHEN fraud_rate >= 0.01 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_segment
    FROM agg
)
SELECT
    customer_sk,
    risk_segment,
    tx_total,
    fraud_cnt,
    fraud_rate_pct,
    avg_check,
    p95_amount,
    amount_total,
    amount_fraud,
    fraud_amount_share_pct,
    first_tx_time,
    last_tx_time
FROM labeled
ORDER BY
    CASE risk_segment WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    fraud_rate_pct DESC,
    tx_total DESC;

CREATE INDEX IF NOT EXISTS ix_mart_customer_risk_profile_customer
    ON mart.mart_customer_risk_profile (customer_sk);

CREATE INDEX IF NOT EXISTS ix_mart_customer_risk_profile_segment
    ON mart.mart_customer_risk_profile (risk_segment);

COMMIT;
