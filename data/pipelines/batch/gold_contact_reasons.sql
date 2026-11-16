-- Owner: Stream A (Data + Pipelines). cc_batch MV: hourly (ET) contacts per classified reason vs the baseline
-- expectation = average count for the same ET hour and reason over the previous days (up to 7) in the data.
CREATE OR REFRESH MATERIALIZED VIEW gold_contact_reasons
COMMENT 'Support contacts per ET hour and reason, the expected count from prior days at the same hour, and the excess.'
AS WITH c AS (
  SELECT from_utc_timestamp(f.contact_ts, 'America/New_York') AS et_ts, r.reason_code
  FROM fact_support_contacts f JOIN silver_contact_reasons r USING (contact_id)
), days AS (SELECT DISTINCT to_date(et_ts) AS day FROM c),
reasons AS (SELECT DISTINCT reason_code FROM c),
grid AS (
  SELECT d.day, h.hr, r.reason_code
  FROM days d CROSS JOIN (SELECT explode(sequence(0, 23)) AS hr) h CROSS JOIN reasons r
), counts AS (
  SELECT g.day, g.hr, g.reason_code, count(c.et_ts) AS n
  FROM grid g LEFT JOIN c ON to_date(c.et_ts) = g.day AND hour(c.et_ts) = g.hr AND c.reason_code = g.reason_code
  GROUP BY g.day, g.hr, g.reason_code
)
SELECT cur.day,
       to_utc_timestamp(cur.day + make_interval(0, 0, 0, 0, cur.hr, 0, 0), 'America/New_York') AS hour_bucket,
       cur.reason_code,
       CAST(cur.n AS INT) AS n_contacts,
       avg(prev.n) AS n_baseline_expected,
       cur.n - avg(prev.n) AS n_excess
FROM counts cur
LEFT JOIN counts prev ON prev.reason_code = cur.reason_code AND prev.hr = cur.hr
                     AND prev.day BETWEEN date_sub(cur.day, 7) AND date_sub(cur.day, 1)
GROUP BY cur.day, cur.hr, cur.reason_code, cur.n;
