-- Owner: Stream A (Data + Pipelines). cc_batch MV: effective cancellations, one row per cancel, with the
-- evidence each source system holds about it. This is where the five sources meet:
--   subscriptions (cancel event, net of reversals within 24 h; account state), billing (processor_timeout
--   decline with no retry), payments (processor's failure code), support (a case 24 h before to 1 h
--   after the cancel), telemetry (minutes of buffering that night).
-- Governed definition (planted.GOVERNED_DEFINITION): subscription ended, net of reversals within 24 hours.
CREATE OR REFRESH MATERIALIZED VIEW gold_cancel_events
COMMENT 'Effective cancellations (net of reversals within 24 hours), one row per cancel, with billing, payment-processor, support and playback evidence. Source of gold_cancellations_daily, gold_subscriber_daily and the governed cancel count.'
AS WITH cancels AS (
  SELECT event_id, subscriber_id, tier, event_ts AS cancel_ts, cancel_type, cancel_reason_code
  FROM silver_subscription_events
  WHERE event_type = 'cancel' AND NOT (is_reversed AND reversed_ts <= event_ts + INTERVAL 24 HOURS)
), timeouts AS (
  SELECT subscriber_id, max(grace_expires_ts) AS grace_expires_ts
  FROM silver_billing_attempts
  WHERE decline_code = 'processor_timeout' AND NOT retry_scheduled AND result = 'declined'
  GROUP BY subscriber_id
), processor AS (
  SELECT subscriber_id, max_by(failure_code, created_ts) AS failure_code
  FROM silver_processor_events WHERE event_type = 'charge.failed' GROUP BY subscriber_id
), care AS (
  SELECT DISTINCT c.event_id
  FROM cancels c JOIN silver_support_cases s ON s.subscriber_id = c.subscriber_id
   AND s.created_ts BETWEEN c.cancel_ts - INTERVAL 24 HOURS AND c.cancel_ts + INTERVAL 1 HOUR
), buffering AS (
  SELECT subscriber_id, sum(rebuffer_seconds) / 60.0 AS rebuffer_min
  FROM silver_sessions WHERE sample_weight = 1 GROUP BY subscriber_id
)
SELECT c.event_id, c.subscriber_id,
       to_date(from_utc_timestamp(c.cancel_ts, 'America/New_York')) AS day,
       c.cancel_ts, c.tier, a.state, c.cancel_type, c.cancel_reason_code,
       coalesce(c.cancel_type = 'involuntary_billing' AND t.grace_expires_ts <= c.cancel_ts, false) AS payment_timeout_no_retry,
       p.failure_code AS processor_failure_code,
       cr.event_id IS NOT NULL AS care_touched,
       coalesce(b.rebuffer_min, 0) AS rebuffer_min_game_night
FROM cancels c
LEFT JOIN silver_accounts a ON a.subscriber_id = c.subscriber_id
LEFT JOIN timeouts t ON t.subscriber_id = c.subscriber_id
LEFT JOIN processor p ON p.subscriber_id = c.subscriber_id
LEFT JOIN care cr ON cr.event_id = c.event_id
LEFT JOIN buffering b ON b.subscriber_id = c.subscriber_id;
