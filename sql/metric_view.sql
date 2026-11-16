-- The governed metric view. Run by notebooks/01_setup.py after sql/views.sql. Tokens as in views.sql.
-- One source with two grains: subscriber rows (day x tier x state) and viewing rows (minute x metro x CDN x device). Subscriber measures ignore viewing rows and vice versa, because each measure sums only its own columns.

CREATE OR REPLACE VIEW {catalog}.{schema}.mv_streamco_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "StreamCo governed metrics. Subscriber measures (cancels, cancel_rate, active_subs) by day, tier and state; buffering_ratio by minute, metro and CDN. Days are US Eastern calendar days. Before quoting one tier's cancel count, check gold_reconciliation_daily for that day and tier."
source: |
  SELECT day, tier, state, CAST(NULL AS TIMESTAMP) AS minute_ts,
         CAST(NULL AS STRING) AS metro, CAST(NULL AS STRING) AS cdn,
         cancels, active_subs, CAST(NULL AS DOUBLE) AS rebuffer_viewers, CAST(NULL AS BIGINT) AS viewers
  FROM {catalog}.{src}.gold_subscriber_daily
  UNION ALL
  SELECT to_date(from_utc_timestamp(minute_ts, 'America/New_York')) AS day, CAST(NULL AS STRING), CAST(NULL AS STRING),
         minute_ts, metro, cdn, CAST(NULL AS INT), CAST(NULL AS BIGINT),
         rebuffer_ratio * concurrent_sessions, concurrent_sessions
  FROM {catalog}.{src}.gold_cdn_region_minute
dimensions:
  - name: day
    expr: day
    comment: "Calendar day in US Eastern time. The Big Game was on {game_date}; the cancellation spike lands on {day_after}."
  - name: tier
    expr: tier
    comment: "Subscription tier: Sports, Family, Originals or Bundle. Null on viewing rows."
  - name: state
    expr: state
    comment: "Subscriber's US state (full name). Null on viewing rows."
  - name: minute_ts
    expr: minute_ts
    comment: "Minute (UTC) for viewing rows. Convert to US Eastern for clock labels such as 8:17 PM."
  - name: metro
    expr: metro
    comment: "Metro code for viewing rows, e.g. DFW = Dallas-Fort Worth."
  - name: cdn
    expr: cdn
    comment: "Content delivery network for viewing rows (CDN-A to CDN-G)."
measures:
  - name: cancels
    expr: SUM(cancels)
    comment: "Governed cancellations: effective cancellations, net of reversals within 24 hours, reconciled to the Finance ledger."
  - name: active_subs
    expr: SUM(active_subs)
    comment: "Active subscribers at the end of the day. Use for one day at a time; do not sum across days."
  - name: cancel_rate
    expr: SUM(cancels) / NULLIF(SUM(active_subs), 0)
    comment: "Cancellations divided by active subscribers for the same slice (a fraction)."
  - name: buffering_ratio
    expr: SUM(rebuffer_viewers) / NULLIF(SUM(viewers), 0)
    comment: "Share of watch time spent buffering, weighted by concurrent viewers (a fraction; about 1% is normal)."
$$;
