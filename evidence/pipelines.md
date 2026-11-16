# Lakeflow pipelines: ingest and quality

## [dev cindy_wu] streamco-data-batch (`c49305b5-2a9b-47ad-b0fb-bd7aafe3a488`)

| update_id | state | full_refresh | created |
|---|---|---|---|
| 7960cd8e-59ef-407d-ac35-ab55406e5819 | COMPLETED | True | 2026-11-09 04:28 UTC |

## [dev cindy_wu] streamco-data-stream (`c7eafee0-a490-45eb-9b5f-85a8821de5a6`)

| update_id | state | full_refresh | created |
|---|---|---|---|
| 9688bd78-0529-4b6f-8460-bd4d2392d67d | COMPLETED | False | 2026-11-09 20:15 UTC |
| 37a5bd33-f723-4a24-b0af-c5701a8674c9 | COMPLETED | True | 2026-11-09 04:25 UTC |

### Quality rules (expectations) on the latest full refresh

```sql
WITH ev AS (SELECT * FROM event_log(TABLE(cindy_wu_catalog.streamco.silver_accounts))),
upd AS (SELECT max_by(origin.update_id, timestamp) AS update_id FROM ev
        WHERE event_type = 'create_update' AND details:create_update.full_refresh::boolean)
SELECT e.name AS rule, regexp_replace(e.dataset, '^.*[.]', '') AS dataset,
       sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
        'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')) AS e
      FROM ev JOIN upd ON ev.origin.update_id = upd.update_id
      WHERE event_type = 'flow_progress' AND details:flow_progress.status = 'COMPLETED')
GROUP BY 1, 2 ORDER BY failed DESC, rule
```

| rule | dataset | passed | failed |
|---|---|---|---|
| app_version_present | cdn_rows_checked | 1896962 | 19090 |
| amount_non_negative | silver_billing_attempts | 482831 | 0 |
| cdn_and_metro_not_null | cdn_rows_checked | 1916052 | 0 |
| event_type_known | silver_subscription_events | 788865 | 0 |
| ratios_between_0_and_1 | cdn_rows_checked | 1916052 | 0 |
| subscriber_present | silver_accounts | 4000000 | 0 |

### Rows landed per source system

```sql
SELECT source_id, display_name, system_type, prod_connector, row_count FROM cindy_wu_catalog.streamco.ref_source_systems ORDER BY source_id
```

| source_id | display_name | system_type | prod_connector | row_count |
|---|---|---|---|---|
| billing | Billing & payment retries | PostgreSQL | Lakeflow Connect (CDC) | 502264 |
| payments | Payment processor events | Payment processor API (SaaS) | Webhook to Zerobus Ingest | 482831 |
| subscriptions | Subscriptions & accounts | MySQL | Lakeflow Connect (CDC) | 5047247 |
| support | Support cases & transcripts | Salesforce Service Cloud (SaaS) | Lakeflow Connect (SaaS connector) | 37036 |
| telemetry | Playback & CDN telemetry | Event stream | Zerobus Ingest / Kafka | 4350064 |
