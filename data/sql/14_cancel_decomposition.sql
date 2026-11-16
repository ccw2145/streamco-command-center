-- Owner: Stream A (Data + Pipelines). Deterministic decomposition of the day-after Sports cancels
-- (contracts/tables.md section 3). Evidence tables only, never eval_gt_*. Run by notebooks/00_run_sql.py
-- after cc_batch, 10_anomaly_engine_sim and 13_train_cancel_forecast.
--
-- Day = the day after the high-severity alert (ET). Rules, applied in this order (first match wins):
--   payment     involuntary cancel of a subscriber whose processor_timeout decline was never retried and
--               whose grace window had expired; observed, high. Wins any overlap with playback.
--   playback    voluntary cancel of a subscriber with >= 5 min of rebuffering in the alert cell that night;
--               inferred, high.
--   event_only  voluntary cancel of an event_promo sign-up within 24 h of signing up; expected, high
--               (the rule text shows the same rate on the prior big-game nights).
--   baseline    average of the last six ordinary same-weekday days (fact_daily_cancels); pct is null.
--   unexplained everything else above the baseline; unexplained, low.
-- Evidence types and confidences equal planted.CANCEL_DRIVERS (asserted by 27).

CREATE OR REPLACE TABLE {catalog}.{schema}.gold_cancel_decomposition
COMMENT 'Day-after Sports cancellations split into drivers by deterministic evidence rules: payment (timed-out sign-up payment never retried), playback (5+ minutes of buffering in the failing cell), event-only (event sign-ups cancelling within 24 hours, same rate as prior big games), baseline and unexplained. pct is the share of cancels above the baseline.'
AS WITH alert AS (
  SELECT cdn, metro, device_family, app_version, detected_ts,
         to_date(from_utc_timestamp(detected_ts, 'America/New_York')) AS night,
         date_add(to_date(from_utc_timestamp(detected_ts, 'America/New_York')), 1) AS day
  FROM {catalog}.{schema}.gold_anomaly_alerts
  WHERE severity = 'high' AND metric = 'rebuffer_ratio'
  ORDER BY detected_ts LIMIT 1
), cancels AS (
  SELECT e.subscriber_id, e.event_ts, e.cancel_type, a.day
  FROM {catalog}.{schema}.fact_subscription_events e CROSS JOIN alert a
  WHERE e.event_type = 'cancel' AND e.tier = 'Sports'
    AND to_date(from_utc_timestamp(e.event_ts, 'America/New_York')) = a.day
), timeouts AS (
  SELECT subscriber_id, min(grace_expires_ts) AS grace_expires_ts
  FROM {catalog}.{schema}.fact_billing_attempts
  WHERE decline_code = 'processor_timeout' AND NOT retry_scheduled
  GROUP BY subscriber_id
), exposed AS (
  SELECT s.subscriber_id
  FROM {catalog}.{schema}.silver_sessions s JOIN alert a
    ON s.cdn = a.cdn AND s.metro = a.metro AND s.device_family = a.device_family AND s.app_version = a.app_version
   AND to_date(from_utc_timestamp(s.start_ts, 'America/New_York')) = a.night
  WHERE s.sample_weight = 1
  GROUP BY s.subscriber_id
  HAVING sum(s.rebuffer_seconds) >= 5 * 60
), labelled AS (
  SELECT c.subscriber_id,
         CASE WHEN c.cancel_type = 'involuntary_billing' AND t.grace_expires_ts <= c.event_ts THEN 'payment'
              WHEN c.cancel_type = 'voluntary' AND x.subscriber_id IS NOT NULL THEN 'playback'
              WHEN c.cancel_type = 'voluntary' AND d.signup_source = 'event_promo'
                   AND c.event_ts <= d.signup_ts + INTERVAL 24 HOURS THEN 'event_only'
              ELSE 'other' END AS driver
  FROM cancels c
  LEFT JOIN timeouts t ON t.subscriber_id = c.subscriber_id
  LEFT JOIN exposed x ON x.subscriber_id = c.subscriber_id
  LEFT JOIN {catalog}.{schema}.dim_subscribers d ON d.subscriber_id = c.subscriber_id
), baseline AS (
  SELECT round(avg(f.cancels)) AS n
  FROM (SELECT f.cancels, row_number() OVER (ORDER BY f.day DESC) AS rk
        FROM {catalog}.{schema}.fact_daily_cancels f CROSS JOIN alert a
        WHERE f.tier = 'Sports' AND f.day < a.day AND NOT f.is_big_event_next_day
          AND dayofweek(f.day) = dayofweek(a.day)) f
  WHERE rk <= 6
), event_rates AS (
  -- event-only rate per big-game night: voluntary cancels within 24 h / event_promo sign-ups that evening
  SELECT to_date(from_utc_timestamp(d.signup_ts, 'America/New_York')) AS night,
         count_if(e.event_ts IS NOT NULL) / count(*) AS rate
  FROM {catalog}.{schema}.dim_subscribers d
  LEFT JOIN {catalog}.{schema}.fact_subscription_events e
    ON e.subscriber_id = d.subscriber_id AND e.event_type = 'cancel' AND e.cancel_type = 'voluntary'
   AND e.event_ts <= d.signup_ts + INTERVAL 24 HOURS
  WHERE d.signup_source = 'event_promo' AND d.tier = 'Sports'
    AND hour(from_utc_timestamp(d.signup_ts, 'America/New_York')) >= 18
    AND to_date(from_utc_timestamp(d.signup_ts, 'America/New_York')) IN (
      SELECT date_sub(f.day, 1) FROM {catalog}.{schema}.fact_daily_cancels f CROSS JOIN alert a
      WHERE f.tier = 'Sports' AND f.is_big_event_next_day AND f.day <= a.day
        AND f.day >= date_sub(a.day, 15))
  GROUP BY 1
), rate_text AS (
  SELECT format_string('event_promo sign-ups cancelling within 24 h: %.1f%% on this night vs %s on the prior big-game nights',
                       100 * max_by(e.rate, e.night),
                       array_join(transform(sort_array(collect_list(CASE WHEN e.night < a.night
                                                                      THEN round(100 * e.rate, 1) END)),
                                            r -> concat(CAST(r AS STRING), '%')), ' and ')) AS txt
  FROM event_rates e CROSS JOIN alert a
), totals AS (
  SELECT (SELECT count(*) FROM cancels) AS total, (SELECT n FROM baseline) AS base
), drivers AS (
  SELECT 'payment' AS driver, count_if(driver = 'payment') AS n FROM labelled
  UNION ALL SELECT 'playback', count_if(driver = 'playback') FROM labelled
  UNION ALL SELECT 'event_only', count_if(driver = 'event_only') FROM labelled
)
SELECT a.day, 'Sports' AS tier, r.driver, CAST(r.n AS INT) AS n_cancels,
       CASE WHEN r.driver = 'baseline' THEN NULL ELSE r.n / (t.total - t.base) END AS pct,
       r.evidence_type, r.confidence, r.rule
FROM (
  SELECT 'baseline' AS driver, (SELECT base FROM totals) AS n, 'expected' AS evidence_type, 'high' AS confidence,
         'Average Sports cancels on the last six ordinary days with the same weekday (fact_daily_cancels)' AS rule
  UNION ALL
  SELECT d.driver, d.n,
         CASE d.driver WHEN 'payment' THEN 'observed' WHEN 'playback' THEN 'inferred' ELSE 'expected' END,
         'high',
         CASE d.driver
           WHEN 'payment' THEN 'Involuntary cancels after a processor_timeout sign-up decline with no retry and an expired 24 h grace window (fact_billing_attempts); wins any overlap with playback'
           WHEN 'playback' THEN 'Voluntary cancels by viewers with 5+ minutes of buffering in the alerted cell that night (silver_sessions, gold_anomaly_alerts)'
           ELSE (SELECT txt FROM rate_text) END
  FROM drivers d
  UNION ALL
  SELECT 'unexplained', (SELECT total - base FROM totals) - (SELECT sum(n) FROM drivers), 'unexplained', 'low',
         'Cancels above the baseline that match none of the rules above'
) r CROSS JOIN alert a CROSS JOIN totals t;
