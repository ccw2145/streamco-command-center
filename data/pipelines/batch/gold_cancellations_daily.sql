-- Owner: Stream A (Data + Pipelines). cc_batch MV: effective cancels per day (ET) x tier x state x cancel type x reason.
CREATE OR REFRESH MATERIALIZED VIEW gold_cancellations_daily
COMMENT 'Effective cancellations (net of reversals within 24 hours) by ET day, tier, state, cancel type (voluntary / involuntary_billing) and cancel reason.'
AS SELECT day, tier, state, cancel_type, cancel_reason_code, CAST(count(*) AS INT) AS cancels
FROM gold_cancel_events
GROUP BY ALL;
