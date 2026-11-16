-- Owner: Stream A (Data + Pipelines). cc_batch MV: "three teams, three numbers" for yesterday's Sports cancels
-- (the latest day with cancels), each derived from the source its team uses, next to the governed count.
--   Product analytics: cancel clicks in the app (subscriptions source), including saves and reversals
--   Finance: the billing ledger
--   Customer care: effective cancels with a support case from 24 h before to 1 h after the cancel
--   governed: effective cancels, net of reversals within 24 hours (planted.GOVERNED_DEFINITION)
CREATE OR REFRESH MATERIALIZED VIEW gold_team_cancel_counts
COMMENT 'Yesterday''s Sports cancellations as Product analytics, Finance and Customer care each count them, with the one governed count they reconcile to.'
AS WITH d AS (SELECT max(day) AS day FROM gold_cancel_events WHERE tier = 'Sports'),
gov AS (SELECT count(*) AS n, count_if(care_touched) AS care
        FROM gold_cancel_events g JOIN d USING (day) WHERE g.tier = 'Sports'),
clicks AS (SELECT count(*) AS n FROM silver_subscription_events e CROSS JOIN d
           WHERE e.event_type = 'cancel_request' AND e.tier = 'Sports'
             AND to_date(from_utc_timestamp(e.event_ts, 'America/New_York')) = d.day),
led AS (SELECT sum(ledger_cancels) AS n FROM billing_ledger_cancels_daily l JOIN d USING (day) WHERE l.tier = 'Sports')
SELECT t.team, CAST(t.n AS INT) AS count, t.definition, CAST(gov.n AS INT) AS governed_count,
       'Effective cancellations: subscription ended, net of reversals within 24 hours, reconciled to the billing ledger.'
         AS governed_definition
FROM (
  SELECT 'Product analytics' AS team, (SELECT n FROM clicks) AS n,
         'cancel button clicks in the app, including people who were saved or changed their mind' AS definition
  UNION ALL SELECT 'Finance', (SELECT n FROM led), 'billing ledger, counted when the final charge is reversed'
  UNION ALL SELECT 'Customer care', (SELECT care FROM gov), 'cancellations that came through a support contact'
) t CROSS JOIN gov;
