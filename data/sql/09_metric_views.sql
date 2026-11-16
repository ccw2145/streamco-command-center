-- StreamCo: copied from the generator project. The two metric views it used to create are replaced by
-- notebooks/01_setup.py + sql/metric_view.sql (mv_streamco_metrics); this file keeps the source-system counts and
-- the source-chain consistency check. Run by data/notebooks/00_run_sql.py after the batch pipeline.

-- Source-system freshness for the One Truth source map: row count and latest arrival per source.
MERGE INTO {catalog}.{schema}.ref_source_systems r
USING (
  SELECT source_system, count(*) AS n, max(_ingested_at) AS t FROM (
    SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_mysql_accounts
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_mysql_subscriptions
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_pg_billing_attempts
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_sfdc_support_cases
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_processor_events
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_session_events
    UNION ALL SELECT source_system, _ingested_at FROM {catalog}.{schema}.bronze_cdn_telemetry
  ) GROUP BY source_system
) b ON r.source_id = b.source_system
WHEN MATCHED THEN UPDATE SET r.row_count = b.n, r.last_ingested_at = b.t;

-- The governed chain (sources -> bronze -> silver -> gold) must agree with the generator's tables.
-- A CTAS so the check is evaluated (a bare SELECT is lazy under spark.sql).
CREATE OR REPLACE TABLE {catalog}.{schema}.gen_source_chain_check AS
SELECT current_timestamp() AS checked_ts, CASE
  WHEN (SELECT count(*) FROM {catalog}.{schema}.ref_source_systems WHERE row_count IS NULL OR row_count = 0) > 0
    THEN raise_error('ref_source_systems: a source has no bronze rows')
  WHEN (SELECT count(*) FROM {catalog}.{schema}.gold_cancel_events)
       <> (SELECT count(*) FROM {catalog}.{schema}.fact_subscription_events WHERE event_type = 'cancel')
    THEN raise_error('gold_cancel_events disagrees with fact_subscription_events cancels')
  WHEN (SELECT count(*) FROM {catalog}.{schema}.silver_billing_attempts)
       <> (SELECT count(*) FROM {catalog}.{schema}.fact_billing_attempts)
    THEN raise_error('silver_billing_attempts disagrees with fact_billing_attempts')
  WHEN (SELECT count(*) FROM {catalog}.{schema}.silver_accounts)
       <> (SELECT count(*) FROM {catalog}.{schema}.dim_subscribers)
    THEN raise_error('silver_accounts disagrees with dim_subscribers')
  ELSE 'source chain consistent' END AS source_chain_check;
