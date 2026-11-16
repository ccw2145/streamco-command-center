-- Owner: Stream A (Data + Pipelines). cc_batch MV: daily (ET) playback quality per device x version x CDN x metro.
-- Weighted by sample_weight (cohort viewers are 1, sampled sessions 100). rebuffer_ratio = stalled time / watch time.
CREATE OR REFRESH MATERIALIZED VIEW gold_playback_failures_by_device
COMMENT 'Daily playback quality by device, app version, CDN and metro (ET day), from silver_sessions.'
AS SELECT
  to_date(from_utc_timestamp(start_ts, 'America/New_York')) AS day,
  device_family, app_version, cdn, metro,
  CAST(round(sum(sample_weight)) AS BIGINT) AS sessions,
  CAST(round(sum(sample_weight * CAST(failed_start AS INT))) AS BIGINT) AS failed_starts,
  sum(sample_weight * rebuffer_seconds)
    / nullif(sum(sample_weight * greatest(unix_timestamp(end_ts) - unix_timestamp(start_ts), 1)), 0) AS rebuffer_ratio,
  sum(sample_weight * CAST(dropped AS INT)) / nullif(sum(sample_weight), 0) AS session_drop_rate
FROM silver_sessions
WHERE cdn IS NOT NULL AND metro IS NOT NULL
GROUP BY 1, device_family, app_version, cdn, metro;
