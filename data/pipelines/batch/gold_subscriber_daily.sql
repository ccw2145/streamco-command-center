-- Owner: Stream A (Data + Pipelines). cc_batch MV: day (ET) x tier x state subscribers. Source of mv_command_center_metrics.
-- active_subs = subscribers on the books at the end of the day, excluding paused accounts: sign-ups so far
-- minus effective cancels so far plus reactivations. starts / cancels = events that day.
-- cancel_rate = cancels / active_subs.
CREATE OR REFRESH MATERIALIZED VIEW gold_subscriber_daily
COMMENT 'Daily active (not paused) subscribers, starts, effective cancels and cancel rate by tier and state (ET days of the event history).'
AS WITH acct AS (SELECT subscriber_id, tier, state, signup_ts, status FROM silver_accounts),
ev AS (
  SELECT to_date(from_utc_timestamp(e.event_ts, 'America/New_York')) AS day, a.tier, a.state, e.event_type
  FROM silver_subscription_events e JOIN acct a USING (subscriber_id)
  WHERE e.event_type IN ('start', 'resume') AND a.status <> 'paused'
  UNION ALL
  SELECT day, tier, state, 'cancel' AS event_type FROM gold_cancel_events
), days AS (
  SELECT explode(sequence(min(day), max(day))) AS day FROM ev
), signed AS (
  SELECT tier, state, to_date(from_utc_timestamp(signup_ts, 'America/New_York')) AS sday, count(*) AS n
  FROM acct WHERE status <> 'paused' GROUP BY ALL
), grid AS (
  SELECT d.day, t.tier, t.state FROM days d CROSS JOIN (SELECT DISTINCT tier, state FROM acct) t
), daily AS (
  SELECT day, tier, state,
         count_if(event_type = 'start') AS starts,
         count_if(event_type = 'cancel') AS cancels,
         count_if(event_type = 'resume') AS resumes
  FROM ev GROUP BY ALL
), cum AS (
  SELECT g.day, g.tier, g.state, coalesce(d.starts, 0) AS starts, coalesce(d.cancels, 0) AS cancels,
         sum(coalesce(d.resumes, 0) - coalesce(d.cancels, 0))
           OVER (PARTITION BY g.tier, g.state ORDER BY g.day) AS net_change
  FROM grid g LEFT JOIN daily d USING (day, tier, state)
)
SELECT c.day, c.tier, c.state,
       CAST(coalesce(sum(s.n), 0) + c.net_change AS BIGINT) AS active_subs,
       CAST(c.starts AS INT) AS starts, CAST(c.cancels AS INT) AS cancels,
       c.cancels / nullif(coalesce(sum(s.n), 0) + c.net_change, 0) AS cancel_rate
FROM cum c LEFT JOIN signed s ON s.tier = c.tier AND s.state = c.state AND s.sday <= c.day
GROUP BY c.day, c.tier, c.state, c.starts, c.cancels, c.net_change;
