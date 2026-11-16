-- Owner: Stream A (Data + Pipelines). cc_batch MV: the expected cancel band (replaces the cut forecast model).
-- For each tier and day: yhat = mean of the last six ordinary same-weekday days (no big event the night
-- before), band = yhat +/- 1.5 x their standard deviation. actual comes from the governed cancel table.
CREATE OR REFRESH MATERIALIZED VIEW gold_cancel_expected
COMMENT 'Expected cancellations per tier and day: the average of the last six ordinary same-weekday days, with a band of plus or minus 1.5 standard deviations, next to the actual effective cancels.'
AS WITH hist AS (
  SELECT day, tier, cancels, is_big_event_next_day FROM fact_daily_cancels
), actual AS (
  SELECT day, tier, count(*) AS n FROM gold_cancel_events GROUP BY ALL
), six AS (
  SELECT a.day, a.tier, h.cancels,
         row_number() OVER (PARTITION BY a.day, a.tier ORDER BY h.day DESC) AS rk
  FROM actual a JOIN hist h ON h.tier = a.tier AND h.day < a.day AND NOT h.is_big_event_next_day
   AND dayofweek(h.day) = dayofweek(a.day)
), band AS (
  SELECT day, tier, avg(cancels) AS yhat, stddev_samp(cancels) AS sd FROM six WHERE rk <= 6 GROUP BY day, tier
)
SELECT b.day, b.tier, b.yhat, b.yhat - 1.5 * b.sd AS lo, b.yhat + 1.5 * b.sd AS hi,
       CAST(a.n AS INT) AS actual, a.n NOT BETWEEN b.yhat - 1.5 * b.sd AND b.yhat + 1.5 * b.sd AS outside_band,
       'mean +/- 1.5 sd of the last six ordinary same-weekday days' AS method
FROM band b JOIN actual a USING (day, tier);
