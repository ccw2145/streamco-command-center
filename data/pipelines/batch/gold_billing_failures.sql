-- Owner: Stream A (Data + Pipelines). cc_batch MV: hourly (UTC hour) payment outcomes by acquirer and attempt type.
CREATE OR REFRESH MATERIALIZED VIEW gold_billing_failures
COMMENT 'Hourly payment attempts, declines, decline rate, most common decline code and share of declines with a retry scheduled, by acquirer and attempt type.'
AS WITH a AS (
  SELECT date_trunc('HOUR', attempt_ts) AS hour_ts, acquirer, attempt_type, result, decline_code, retry_scheduled
  FROM silver_billing_attempts
), codes AS (
  SELECT hour_ts, acquirer, attempt_type, decline_code,
         row_number() OVER (PARTITION BY hour_ts, acquirer, attempt_type ORDER BY count(*) DESC, decline_code) AS rk
  FROM a WHERE result = 'declined' GROUP BY hour_ts, acquirer, attempt_type, decline_code
)
SELECT a.hour_ts, a.acquirer, a.attempt_type,
       CAST(count(*) AS INT) AS attempts,
       CAST(count_if(a.result = 'declined') AS INT) AS declines,
       count_if(a.result = 'declined') / count(*) AS decline_rate,
       max(c.decline_code) AS top_decline_code,
       count_if(a.result = 'declined' AND a.retry_scheduled) / nullif(count_if(a.result = 'declined'), 0)
         AS retry_scheduled_rate
FROM a LEFT JOIN codes c ON c.hour_ts = a.hour_ts AND c.acquirer = a.acquirer AND c.attempt_type = a.attempt_type
                         AND c.rk = 1
GROUP BY a.hour_ts, a.acquirer, a.attempt_type;
