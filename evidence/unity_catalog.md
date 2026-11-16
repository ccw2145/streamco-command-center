# Unity Catalog: the governed layer

### Tables and views in the schema

```sql
SELECT table_name, table_type FROM cindy_wu_catalog.information_schema.tables WHERE table_schema = 'streamco' ORDER BY table_type, table_name
```

| table_name | table_type |
|---|---|
| churn_scores_serving | FOREIGN |
| knowledge_idx | FOREIGN |
| personalized_messages_serving | FOREIGN |
| __7504e4e4_60db_41db_9a34_059c4f0b0c70___online_index_view | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_billing_failures_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_cancel_events_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_cancel_expected_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_cancellations_daily_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_contact_reasons_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_playback_failures_by_device_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_reconciliation_daily_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_subscriber_daily_1 | MANAGED |
| __materialization_mat_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488_gold_team_cancel_counts_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_cdn_telemetry_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_mysql_accounts_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_mysql_subscriptions_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_pg_billing_attempts_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_processor_events_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_session_events_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_bronze_sfdc_support_cases_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_gold_cdn_region_minute_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_gold_session_drops_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_accounts_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_billing_attempts_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_cdn_minute_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_processor_events_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_sessions_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_subscription_events_1 | MANAGED |
| __materialization_mat_c7eafee0_a490_45eb_9b5f_85a8821de5a6_silver_support_cases_1 | MANAGED |
| approvals | MANAGED |
| ask_recorded_run | MANAGED |
| billing_ledger_cancels_daily | MANAGED |
| churn_model_card | MANAGED |
| churn_monitoring | MANAGED |
| churn_monitoring_features | MANAGED |
| churn_monitoring_outcomes | MANAGED |
| churn_scores | MANAGED |
| dim_profiles | MANAGED |
| dim_subscriber_viewing | MANAGED |
| dim_subscribers | MANAGED |
| eval_gt_cancel_cause | MANAGED |
| eval_gt_churn_future | MANAGED |
| eval_gt_contact_reason | MANAGED |
| eval_gt_incident | MANAGED |
| eval_gt_kids_accounts | MANAGED |
| eval_gt_pii_spans | MANAGED |
| event_log_7504e4e4_60db_41db_9a34_059c4f0b0c70 | MANAGED |
| event_log_7609adb7_416b_4f1e_8ce1_2c3c01132c93 | MANAGED |
| event_log_c49305b5_2a9b_47ad_b0fb_bd7aafe3a488 | MANAGED |
| event_log_c7eafee0_a490_45eb_9b5f_85a8821de5a6 | MANAGED |
| event_log_f0d3b0bb_0fbc_47e9_8ac6_70dc33347282 | MANAGED |
| fact_billing_attempts | MANAGED |
| fact_concurrency_event_5min | MANAGED |
| fact_concurrency_hourly | MANAGED |
| fact_daily_cancels | MANAGED |
| fact_subscription_events | MANAGED |
| fact_support_contacts | MANAGED |
| forecast_flags | MANAGED |
| forecast_viewers | MANAGED |
| gen_calibration | MANAGED |
| gen_cancel_funnel_tmp | MANAGED |
| gen_cell_mix | MANAGED |
| gen_event_curve | MANAGED |
| gen_events | MANAGED |
| gen_pregame_features | MANAGED |
| gen_source_chain_check | MANAGED |
| gen_subscriber_features | MANAGED |
| gold_anomaly_alerts | MANAGED |
| gold_cancel_decomposition | MANAGED |
| gold_shift_recommendation | MANAGED |
| knowledge_chunks | MANAGED |
| knowledge_citations | MANAGED |
| labels_churn_30d | MANAGED |
| offer_holdout_results | MANAGED |
| personalization_run | MANAGED |
| personalized_messages | MANAGED |
| personalized_messages_preview | MANAGED |
| ref_app_releases | MANAGED |
| ref_assumptions | MANAGED |
| ref_cdn_capacity | MANAGED |
| ref_incidents_history | MANAGED |
| ref_metric_provenance | MANAGED |
| ref_model_prices | MANAGED |
| ref_price_changes | MANAGED |
| ref_sample_subscribers | MANAGED |
| ref_source_systems | MANAGED |
| ref_team_budgets | MANAGED |
| ref_thresholds | MANAGED |
| save_plan | MANAGED |
| silver_contact_reasons | MANAGED |
| gold_billing_failures | MATERIALIZED_VIEW |
| gold_cancel_events | MATERIALIZED_VIEW |
| gold_cancel_expected | MATERIALIZED_VIEW |
| gold_cancellations_daily | MATERIALIZED_VIEW |
| gold_cdn_region_minute | MATERIALIZED_VIEW |
| gold_contact_reasons | MATERIALIZED_VIEW |
| gold_playback_failures_by_device | MATERIALIZED_VIEW |
| gold_reconciliation_daily | MATERIALIZED_VIEW |
| gold_session_drops | MATERIALIZED_VIEW |
| gold_subscriber_daily | MATERIALIZED_VIEW |
| gold_team_cancel_counts | MATERIALIZED_VIEW |
| silver_accounts | MATERIALIZED_VIEW |
| silver_billing_attempts | MATERIALIZED_VIEW |
| silver_cdn_minute | MATERIALIZED_VIEW |
| silver_processor_events | MATERIALIZED_VIEW |
| silver_sessions | MATERIALIZED_VIEW |
| silver_subscription_events | MATERIALIZED_VIEW |
| silver_support_cases | MATERIALIZED_VIEW |
| mv_streamco_metrics | METRIC_VIEW |
| bronze_cdn_telemetry | STREAMING_TABLE |
| bronze_mysql_accounts | STREAMING_TABLE |
| bronze_mysql_subscriptions | STREAMING_TABLE |
| bronze_pg_billing_attempts | STREAMING_TABLE |
| bronze_processor_events | STREAMING_TABLE |
| bronze_session_events | STREAMING_TABLE |
| bronze_sfdc_support_cases | STREAMING_TABLE |
| v_scoring_features | VIEW |
| v_subscribers | VIEW |
| v_support_showcase | VIEW |
| v_training_features | VIEW |

### Three teams, three numbers, one governed count

```sql
SELECT team, count, definition, governed_count FROM cindy_wu_catalog.streamco.gold_team_cancel_counts
```

| team | count | definition | governed_count |
|---|---|---|---|
| Product analytics | 17108 | cancel button clicks in the app, including people who were saved or changed their mind | 15800 |
| Finance | 15760 | billing ledger, counted when the final charge is reversed | 15800 |
| Customer care | 12851 | cancellations that came through a support contact | 15800 |

### The governed number from the Metric View

```sql
SELECT MEASURE(cancels) AS cancels FROM cindy_wu_catalog.streamco.mv_streamco_metrics WHERE day = DATE'2027-02-15' AND tier = 'Sports'
```

| cancels |
|---|
| 15800 |

### Reconciliation with the billing ledger

```sql
SELECT * FROM cindy_wu_catalog.streamco.gold_reconciliation_daily WHERE day = DATE'2027-02-15' ORDER BY tier
```

| day | tier | subscriber_live_cancels | ledger_cancels | pct_diff | status | likely_cause |
|---|---|---|---|---|---|---|
| 2027-02-15 | Bundle | 302 | 302 | 0.0 | ok |  |
| 2027-02-15 | Family | 1782 | 1781 | 5.611672278338945E-4 | ok |  |
| 2027-02-15 | Originals | 4017 | 4268 | 0.062484441125217824 | sources_disagree |  |
| 2027-02-15 | Sports | 15800 | 15760 | 0.002531645569620253 | within_tolerance | late-arriving events |

### Governed view: personal data masked, kids-only accounts excluded

```sql
SELECT subscriber_id, email, phone, tier, metro FROM cindy_wu_catalog.streamco.v_subscribers LIMIT 5
```

| subscriber_id | email | phone | tier | metro |
|---|---|---|---|---|
| 2187597 | d***@yahoo.com | ***-***-1214 | Family |  |
| 2187599 | d***@icloud.com | ***-***-5582 | Family |  |
| 2187742 | j***@outlook.com | ***-***-2199 | Sports |  |
| 2187836 | t***@icloud.com | ***-***-9559 | Sports |  |
| 2187848 | m***@hotmail.com | ***-***-7499 | Bundle | MIA |

### Kids-only accounts: in the source, not in the governed view

```sql
SELECT (SELECT count(*) FROM cindy_wu_catalog.streamco.silver_accounts) AS accounts_in_source, (SELECT count(*) FROM cindy_wu_catalog.streamco.v_subscribers) AS subscribers_in_governed_view
```

| accounts_in_source | subscribers_in_governed_view |
|---|---|
| 4000000 | 3859513 |

### Lineage into the governed number (system.access.table_lineage)

```sql
SELECT DISTINCT source_table_name, target_table_name FROM system.access.table_lineage WHERE target_table_catalog = 'cindy_wu_catalog' AND target_table_schema = 'streamco' AND source_table_schema = 'streamco' AND event_time >= current_timestamp() - INTERVAL 30 DAYS ORDER BY 2, 1
```

| source_table_name | target_table_name |
|---|---|
| churn_scores | churn_scores_serving |
| dim_subscribers | dim_profiles |
| gen_scored_tmp | dim_subscriber_viewing |
| dim_subscribers | dim_subscribers |
| fact_subscription_events | dim_subscribers |
| gen_sports_cancels_tmp | dim_subscribers |
| gen_viewers_tmp | dim_subscribers |
| gen_sports_cancels_tmp | eval_gt_cancel_cause |
| gen_scored_tmp | eval_gt_churn_future |
| gen_contacts_tmp | eval_gt_contact_reason |
| dim_subscribers | eval_gt_kids_accounts |
| gen_contacts_tmp | eval_gt_pii_spans |
| dim_subscribers | fact_billing_attempts |
| gen_gn_signups_tmp | fact_billing_attempts |
| dim_subscribers | fact_subscription_events |
| gen_gn_signups_tmp | fact_subscription_events |
| gen_sports_cancels_tmp | fact_subscription_events |
| gen_viewers_tmp | fact_subscription_events |
| gen_contacts_tmp | fact_support_contacts |
| forecast_viewers | forecast_flags |
| dim_subscribers | gen_cancel_funnel_tmp |
| fact_subscription_events | gen_cancel_funnel_tmp |
| gen_sports_cancels_tmp | gen_cancel_funnel_tmp |
| gen_viewers_tmp | gen_cancel_funnel_tmp |
| dim_subscribers | gen_contacts_tmp |
| fact_billing_attempts | gen_contacts_tmp |
| fact_subscription_events | gen_contacts_tmp |
| gen_cell_mix | gen_game_telemetry_tmp |
| dim_subscribers | gen_gn_signups_tmp |
| dim_subscribers | gen_pregame_features |
| gen_gn_signups_tmp | gen_scored_pre_tmp |
| gen_pregame_features | gen_scored_pre_tmp |
| gen_sessions_tmp | gen_scored_pre_tmp |
| gen_sports_cancels_tmp | gen_scored_pre_tmp |
| gen_viewers_tmp | gen_scored_pre_tmp |
| gen_scored_pre_tmp | gen_scored_tmp |
| gen_sessions_tmp | gen_session_events_tmp |
| dim_subscribers | gen_sessions_tmp |
| gen_cell_mix | gen_sessions_tmp |
| gen_viewers_tmp | gen_sessions_tmp |
| dim_subscribers | gen_source_chain_check |
| fact_billing_attempts | gen_source_chain_check |
| fact_subscription_events | gen_source_chain_check |
| gold_cancel_events | gen_source_chain_check |
| ref_source_systems | gen_source_chain_check |
| silver_accounts | gen_source_chain_check |
| silver_billing_attempts | gen_source_chain_check |
| dim_subscribers | gen_sports_cancels_tmp |
| gen_gn_signups_tmp | gen_sports_cancels_tmp |
| gen_viewers_tmp | gen_sports_cancels_tmp |
| gen_scored_tmp | gen_subscriber_features |
| gen_training_panel_tmp | gen_subscriber_features |
| dim_subscribers | gen_viewers_tmp |
| gen_gn_signups_tmp | gen_viewers_tmp |
| dim_subscribers | gold_cancel_decomposition |
| fact_billing_attempts | gold_cancel_decomposition |
| fact_daily_cancels | gold_cancel_decomposition |
| fact_subscription_events | gold_cancel_decomposition |
| gold_anomaly_alerts | gold_cancel_decomposition |
| silver_sessions | gold_cancel_decomposition |
| silver_accounts | gold_cancel_events |
| silver_billing_attempts | gold_cancel_events |
| silver_processor_events | gold_cancel_events |
| silver_sessions | gold_cancel_events |
| silver_subscription_events | gold_cancel_events |
| silver_support_cases | gold_cancel_events |
| fact_daily_cancels | gold_cancel_expected |
| gold_cancel_events | gold_cancel_expected |
| gold_cancel_events | gold_cancellations_daily |
| gold_session_drops | gold_cdn_region_minute |
| silver_cdn_minute | gold_cdn_region_minute |
| fact_support_contacts | gold_contact_reasons |
| silver_contact_reasons | gold_contact_reasons |
| silver_sessions | gold_playback_failures_by_device |
| billing_ledger_cancels_daily | gold_reconciliation_daily |
| gold_cancel_events | gold_reconciliation_daily |
| ref_thresholds | gold_reconciliation_daily |
| silver_sessions | gold_session_drops |
| gold_cancel_events | gold_subscriber_daily |
| silver_accounts | gold_subscriber_daily |
| silver_subscription_events | gold_subscriber_daily |
| billing_ledger_cancels_daily | gold_team_cancel_counts |
| gold_cancel_events | gold_team_cancel_counts |
| silver_subscription_events | gold_team_cancel_counts |
| dim_subscribers | knowledge_chunks |
| fact_support_contacts | knowledge_chunks |
| silver_contact_reasons | knowledge_chunks |
| tmp_knowledge_stage | knowledge_chunks |
| v_subscribers | knowledge_chunks |
| knowledge_chunks | knowledge_idx |
| gen_training_panel_tmp | labels_churn_30d |
| gold_cdn_region_minute | mv_streamco_metrics |
| gold_subscriber_daily | mv_streamco_metrics |
| personalized_messages | personalized_messages_serving |
| bronze_cdn_telemetry | ref_source_systems |
| bronze_mysql_accounts | ref_source_systems |
| bronze_mysql_subscriptions | ref_source_systems |
| bronze_pg_billing_attempts | ref_source_systems |
| bronze_processor_events | ref_source_systems |
| bronze_session_events | ref_source_systems |

(24 more rows)
