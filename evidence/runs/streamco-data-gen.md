# streamco-data-gen

- Run id: 893383645665125
- Result: SUCCESS
- Started: 2026-11-09 01:51 UTC
- Duration: 1195 s

## Task `gen_transcripts` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/05_gen_transcripts`  
Task run id: 901583557429014

#### Cell 1: `# Owner: Stream A (Data + Pipelines). Template-generated support transcripts (design deviation 5).`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import hashlib`

_(no output)_

#### Cell 5: `# Contact plan per (day, ET hour, reason). Daily volume varies (weekday pattern), but the six days`

_(no output)_

#### Cell 6: `# Rows and subscriber links. Excess contacts link (~90%) to the people the night affected: cohort`

```
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
```

#### Cell 7: `# Transcript text. Faker is seeded per contact_id, so every row is reproducible on any partitioning.`

```
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
```

#### Cell 8: `# Simulated Salesforce Service Cloud source (planted.SOURCE_SYSTEMS "support"): every transcript is a`

_(no output)_

#### Cell 9: `# Self-asserts`

```
excess by reason: {'account_update': 1.0, 'cancel_request_other': 652.0, 'charged_declined_cancelled': 1085.0, 'device_setup': 1.0, 'login_issue': 1.0, 'playback_general': 1.0, 'stream_froze_halftime': 1364.0}
PII slot rates: {'email': 0.5, 'phone': 0.399, 'card_last4': 0.298, 'address': 0.197}
Customer care cancels: 12,851 (target 12,851)
05_gen_transcripts: 25,000 contacts, 34,851 PII spans; all self-asserts passed
```

## Task `classify_transcripts` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/07_classify_transcripts`  
Task run id: 415664983972253

#### Cell 1: `# OWNER: Stream C (GenAI+Governance). Task classify_transcripts (after gen_transcripts and probe_gateway).`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import os`

_(no output)_

#### Cell 5: `# Reason codes and phrasings come from the phrase bank Stream A generates transcripts from. Accepted shapes:`

```
10 reason codes: ['account_update', 'billing_question', 'cancel_request_other', 'charged_declined_cancelled', 'content_question', 'device_setup', 'login_issue', 'playback_general', 'promo_question', 'stream_froze_halftime']
```

#### Cell 6: `# Rules classifier: token-overlap nearest phrase (transcripts are generated from these phrases, design deviation 5).`

_(no output)_

#### Cell 7: `# StreamCo: no Lakebase flags. The deterministic rules classifier is used (the transcripts are generated`

```
llm flags: {'llm_available_structured': False} -> classifier: rules
/home/spark-74f0fb56-94b3-4a9a-870e-81/.ipykernel/9481/command-4664120361242405-2798119742:53: DeprecationWarning: In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace instead of always setting a new array. To retain the old behavior, use either `df[df.columns[i]] = newvals` or, if columns are non-unique, `df.isetitem(i, newvals)`
  pdf.loc[fallback, "confidence"] = ruled.map(lambda x: x[1])
classified 25,000 transcripts: 0 by the model, 25,000 by rules
```

#### Cell 8: `pdf["n_redactions"] = pdf["redacted"].str.count(r"\[(?:EMAIL|PHONE|CARD|ADDRESS)\]").astype("int32")`

```
DataFrame[]
```

#### Cell 9: `# Validation: row counts, source split, reason mix, and accuracy against the generator's ground truth.`

```
| source | n | avg_conf | redactions |
|---|---|---|---|
| rules | 25000 | 1.0 | 34851 |
| reason_code | n |
|---|---|
| login_issue | 3943 |
| billing_question | 3723 |
| content_question | 3066 |
| cancel_request_other | 2839 |
| device_setup | 2628 |
| playback_general | 2628 |
| account_update | 2410 |
| stream_froze_halftime | 1364 |
| promo_question | 1314 |
| charged_declined_cancelled | 1085 |
accuracy rules: 1.000 on 25,000
```

## Task `gen_game_night` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/04_gen_game_night`  
Task run id: 649013936233231

#### Cell 1: `# Owner: Stream A (Data + Pipelines). The Big Game night and the day after (design section 6.3).`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import json`

```
baseline expected 8,977.2; drivers {'payment': 1999, 'playback': 2302, 'event_only': 2064}; unexplained 458
```

#### Cell 5: `# Game-night sign-ups and their billing. Exactly planted.CANCEL_DRIVERS["payment"] Acquirer-B`

_(no output)_

#### Cell 6: `# The three sample subscribers (planted.SAMPLE_SUBSCRIBERS): real rows chosen deterministically, then`

```
sample subscribers: {'playback': 643364, 'payment': 1611257, 'onboarding': 3877967}
```

#### Cell 7: `# Incident cohort viewers: existing Sports subscribers in DFW watching on Roku 7.2.0 via CDN-C at halftime`

```
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
```

#### Cell 8: `# Day-after Sports cancels.`

_(no output)_

#### Cell 9: `# Cancel funnel (source of Product analytics' number): every voluntary cancel starts with a cancel click;`

_(no output)_

#### Cell 10: `# Write subscription events, billing, daily cancels, ledger; update the dimension. Each write first`

```
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
```

#### Cell 11: `# CDN telemetry. Total viewers N(t) = pre_level * pre_shape(t) * (1 + (m - 1) h(t)) split by gen_cell_mix.`

_(no output)_

#### Cell 12: `# Session events. Cohort viewers are individual subscribers (sample_weight 1): one session in progress`

_(no output)_

#### Cell 13: `# Ground-truth tables (only 25 and 27 read these).`

_(no output)_

#### Cell 14: `# Post-game scoring snapshot (310k Sports subscribers who have not cancelled): cohort viewers, game-night`

```
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
calibrated share of non-cohort subscribers who streamed the game: 0.521
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
```

#### Cell 15: `# Viewing history for personalization (scoring cohort): favourite genre, up to three recent titles and a`

_(no output)_

#### Cell 16: `# Simulated source systems (planted.SOURCE_SYSTEMS). The tables above are the generator's state; here`

_(no output)_

#### Cell 17: `# Self-asserts: every planted number this notebook owns, recomputed from the generated rows.`

```
PASS peak concurrency: 9,666,822
PASS halftime multiplier: x2.43
PASS sign-up multiplier: x11.1
PASS cohort rebuffer plateau: 0.1041
PASS cohort start failures: 0.1502
PASS whole-cell rebuffer: 0.0307
PASS minutes to resolve: 19
PASS control CDN-C DFW Roku 7.1.x: max change 0.75%
PASS control CDN-A DFW Roku 7.2.0: max change 0.61%
PASS control CDN-C HOU * *: max change 0.04%
PASS control CDN-C ATL * *: max change 0.08%
PASS decoy max robust z: 1.74
PASS decoy window z: 0.97
PASS session drop lift: 0.0480 vs 0.0395 (+22%)
PASS affected sessions: 208,498
PASS Acquirer B window decline rate: 0.173
PASS other acquirers normal: {'B': 0.17254654906901862, 'A': 0.029146491977594228, 'D': 0.04126862820022927, 'C': 0.037656117945346984}
PASS processor_timeout never retried: 2038 declines (incl. 2 renewal timeouts)
PASS timeouts are not all cancels: 2038 timeouts vs 1999 payment cancels
PASS +76% vs baseline: baseline 8,977.2, +76.0%
PASS Sports day-after cancels: 15,800
PASS most cancels in by 07:00: 56% before 07:00 ET; payment all before 07:00
PASS driver playback: 2,302 (33.7%)
PASS driver payment: 1,999 (29.3%)
PASS driver event_only: 2,064 (30.3%)
PASS driver unexplained: 458 (6.7%)
PASS payment/playback overlap: 80
PASS playback survey reason: 60% vs 4.1%
PASS cancels by state: top4 ('Texas', 'California', 'Florida', 'New York')
PASS all Sports cancels on the day after: {datetime.date(2027, 2, 15): 15800}
PASS event-only rate matches prior big nights: {'2027-01-31': 0.0276, '2027-02-14': 0.0295, '2027-02-07': 0.0291}
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
PASS retro segment: gross 4,700, kids-only 160, reactivated 290
PASS Family inside its range: 1,782 vs 1,800
PASS Originals inside its range: 4,017 vs 4,000
PASS ledger Sports: 15,760
PASS ledger Originals: 4,268
/databricks/python/lib/python3.10/site-packages/pyspark/sql/connect/expressions.py:968: UserWarning: WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
  warnings.warn(
PASS scoring cohort size: 310,000
PASS top-decile projected cancels (oracle): 4,897 (mean p 0.158)
PASS telemetry rows: 1,916,052
PASS Product analytics clicks: 17,134 (saved 3,000, reversed 1,478)
PASS Finance ledger: 15,760
PASS samples active: {'status': {3877967: 'active', 1611257: 'active', 643364: 'active'}, 'canceled_ts': {3877967: NaT, 1611257: NaT, 643364: NaT}, 'is_kids_only_account': {3877967: False, 1611257: False, 643364: False}}
PASS samples at risk: {'risk_rank': {643364: 1193, 1611257: 1447, 3877967: 2994}, 'p_oracle': {643364: 0.396, 1611257: 0.368, 3877967: 0.278}}
PASS Marcus: 14 min buffering on Roku 7.2.0 in DFW: 14 min
PASS Priya: two processor timeouts, no retry: 2 attempts
PASS Jordan: event sign-up in CHI, nothing else watched: 0 sampled sessions
PASS viewing history covers the scoring cohort: 310,000
PASS session event rows: 2,434,012
04_gen_game_night: all self-asserts passed
```

## Task `gen_setup` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/01_setup_uc`  
Task run id: 342114213593840

#### Cell 1: `# Owner: Stream A (Data + Pipelines). Schema, volumes and the small reference tables every later`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import os`

_(no output)_

#### Cell 5: `spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema} "  # noqa: F821`

_(no output)_

#### Cell 6: `# ref_assumptions: keys are exactly planted.ASSUMPTIONS. ARPU and retention honour the job parameters.`

_(no output)_

#### Cell 7: `# ref_metric_provenance from seed/metric_provenance.yaml. The YAML uses two value tokens, filled here`

_(no output)_

#### Cell 8: `# Validation`

```
ref_assumptions 5
ref_team_budgets 3
ref_model_prices 1
ref_thresholds 1
ref_source_systems 5
ref_metric_provenance 22
| metric_id | display_name | sql_text | source_tables | owner_system |
|---|---|---|---|---|
| peak_viewers | Peak concurrent viewers | SELECT max(concurrent_sessions) AS peak_viewers FROM (SELECT minute_ts, sum(concurrent_sessions) AS concurrent_sessions FROM gold_cdn_region_minute GROUP BY min | ['gold_cdn_region_minute'] | Session Backbone |
| forecast_peak | Forecast peak (p50) | SELECT max(p50_total) AS forecast_peak FROM (SELECT forecast_ts, sum(p50) AS p50_total FROM gold_demand_forecast GROUP BY forecast_ts) | ['gold_demand_forecast'] | Demand forecast |
| incidents_caught | Incidents caught | SELECT count(*) AS incidents, max(persistence_min) AS persistence_min FROM gold_anomaly_alerts WHERE severity = 'high' | ['gold_anomaly_alerts'] | Anomaly Engine |
| affected_sessions | Sessions affected | SELECT affected_sessions FROM gold_anomaly_alerts WHERE alert_id = 'ALR-20270214-DFW-C-ROKU720' | ['gold_anomaly_alerts'] | Anomaly Engine |
| hot_cell_utilization | Forecast utilization, CDN-C Dallas | SELECT cdn, metro, max(utilization_p50) AS utilization FROM gold_demand_forecast WHERE metro = 'DFW' GROUP BY cdn, metro ORDER BY utilization DESC | ['gold_demand_forecast', 'ref_cdn_capacity'] | Demand forecast |
| cancels_actual | Sports cancellations, day after | SELECT sum(cancels) AS cancels FROM gold_cancellations_daily WHERE day = DATE'2027-02-15' AND tier = 'Sports' | ['gold_cancellations_daily'] | Subscriber Live |
| cancels_expected | Expected Sports cancellations | SELECT round(yhat) AS expected, round(lo) AS lo, round(hi) AS hi FROM gold_cancel_expected WHERE day = DATE'2027-02-15' AND tier = 'Sports' | ['gold_cancel_expected', 'fact_daily_cancels'] | Subscriber Live |
| cancels_pct_over | Cancellations vs expected | SELECT actual / yhat - 1 AS pct_over FROM gold_cancel_expected WHERE day = DATE'2027-02-15' AND tier = 'Sports' | ['gold_cancel_expected'] | Subscriber Live |
| subs_at_risk | Subscribers at high risk | SELECT count(*) AS at_risk, round(sum(churn_prob)) AS projected_cancels_30d FROM gold_churn_scores WHERE is_high_risk | ['gold_churn_scores'] | Churn model |
| revenue_at_risk | Revenue at risk if no action (projected cancels, 12 months, gross) | SELECT round(sum(churn_prob)) * a.arpu * a.months AS revenue_at_risk FROM gold_churn_scores, (SELECT max(CASE WHEN key='arpu_monthly' THEN value END) AS arpu, m | ['gold_churn_scores', 'ref_assumptions'] | Churn model |
| subs_saved_retro | Already-cancelled subscribers won back | SELECT saved_point FROM gold_save_plan WHERE card_id = 'retro_total' | ['gold_save_plan', 'gold_uplift_estimates'] | Save plan |
| plan_saved_total | Subscribers kept by the plan | SELECT sum(saved_point) AS saved, sum(saved_lo) AS lo, sum(saved_hi) AS hi FROM gold_save_plan WHERE card_id IN ('payment','playback','onboarding') | ['gold_save_plan', 'gold_uplift_estimates'] | Save plan |
| plan_revenue_protected | Revenue protected (12 months) | SELECT sum(revenue_protected_usd) AS revenue_protected FROM gold_save_plan WHERE card_id IN ('payment','playback','onboarding') | ['gold_save_plan', 'ref_assumptions'] | Save plan |
| recon_sports | Subscriber Live vs Finance ledger | SELECT subscriber_live_cancels, ledger_cancels, pct_diff, status FROM gold_reconciliation_daily WHERE day = DATE'2027-02-15' AND tier = 'Sports' | ['gold_reconciliation_daily'] | Subscriber Live + Billing Ledger |
| cancel_driver_playback | Cancel driver: Couldn't watch halftime properly | SELECT driver, n_cancels, pct, evidence_type, confidence, rule FROM gold_cancel_decomposition WHERE day = DATE'2027-02-15' AND tier = 'Sports' AND driver = 'pla | ['gold_cancel_decomposition'] | Subscriber Live |
| cancel_driver_payment | Cancel driver: Sign-up payment timed out, never retried | SELECT driver, n_cancels, pct, evidence_type, confidence, rule FROM gold_cancel_decomposition WHERE day = DATE'2027-02-15' AND tier = 'Sports' AND driver = 'pay | ['gold_cancel_decomposition'] | Subscriber Live |
| cancel_driver_event_only | Cancel driver: Expected event-only cancels | SELECT driver, n_cancels, pct, evidence_type, confidence, rule FROM gold_cancel_decomposition WHERE day = DATE'2027-02-15' AND tier = 'Sports' AND driver = 'eve | ['gold_cancel_decomposition'] | Subscriber Live |
| cancel_driver_unexplained | Cancel driver: Unexplained | SELECT driver, n_cancels, pct, evidence_type, confidence, rule FROM gold_cancel_decomposition WHERE day = DATE'2027-02-15' AND tier = 'Sports' AND driver = 'une | ['gold_cancel_decomposition'] | Subscriber Live |
| cancels_governed | Sports cancellations, governed definition | SELECT DISTINCT governed_count, governed_definition FROM gold_team_cancel_counts | ['gold_team_cancel_counts', 'gold_cancel_events'] | Unity Catalog metric view |
| team_cancels_product | Sports cancellations, as Product analytics counts them | SELECT team, count, definition, governed_count FROM gold_team_cancel_counts WHERE team = 'Product analytics' | ['gold_team_cancel_counts', 'silver_subscription_events'] | Subscriptions (MySQL) |
| team_cancels_finance | Sports cancellations, as Finance counts them | SELECT team, count, definition, governed_count FROM gold_team_cancel_counts WHERE team = 'Finance' | ['gold_team_cancel_counts', 'billing_ledger_cancels_daily'] | Billing ledger |
| team_cancels_care | Sports cancellations, as Customer care counts them | SELECT team, count, definition, governed_count FROM gold_team_cancel_counts WHERE team = 'Customer care' | ['gold_team_cancel_counts', 'silver_support_cases'] | Support cases (Salesforce) |
```

## Task `gen_history` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/03_gen_history_facts`  
Task run id: 600650822403712

#### Cell 1: `# Owner: Stream A (Data + Pipelines). History before the Big Game, plus the churn training labels.`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import json`

_(no output)_

#### Cell 5: `# fact_concurrency_hourly: 90 days through game day 17:00 ET, metro x CDN x device. Regular hours follow`

_(no output)_

#### Cell 6: `# Reference tables for the ruled-out hypotheses.`

_(no output)_

#### Cell 7: `# fact_daily_cancels: 18 months per tier, negative binomial with weekday seasonality and a mild`

_(no output)_

#### Cell 8: `# History slice of fact_subscription_events (15 days, game day before 18:00 ET).`

```
DataFrame[num_affected_rows: bigint, num_updated_rows: bigint, num_deleted_rows: bigint, num_inserted_rows: bigint]
```

#### Cell 9: `# fact_billing_attempts for history-window sign-ups: declines at 3-4% by acquirer (never processor_timeout`

_(no output)_

#### Cell 10: `# Churn feature generator. One function maps per-row uniforms to the fs_subscriber_features columns plus`

_(no output)_

#### Cell 11: `# Training panel: 200k subscribers x 6 monthly snapshots. Historical accounts (ids 9e9+), many of which`

_(no output)_

#### Cell 12: `# Calibration loop on a 200k sample: bisect beta_z for full AUC, outer bisection on the QoE dose slope`

```
qoe_slope=2.500 beta_z=0.678 auc_full=0.7998 auc_noqoe=0.7195
qoe_slope=1.250 beta_z=0.917 auc_full=0.7997 auc_noqoe=0.7707
qoe_slope=1.875 beta_z=0.831 auc_full=0.8001 auc_noqoe=0.7530
qoe_slope=1.562 beta_z=0.876 auc_full=0.8001 auc_noqoe=0.7627
qoe_slope=1.406 beta_z=0.899 auc_full=0.8000 auc_noqoe=0.7677
qoe_slope=1.328 beta_z=0.908 auc_full=0.8001 auc_noqoe=0.7693
qoe_slope=1.289 beta_z=0.912 auc_full=0.7998 auc_noqoe=0.7698
```

#### Cell 13: `# Write the panel features (fs_subscriber_features columns only) and the labels.`

```
calibrated beta_z=0.912 qoe_slope=1.289; verify AUC full=0.7973 no-QoE=0.7682 prevalence=4.943% QoE exposure=19.0%
```

#### Cell 14: `# Pre-game state for every Sports and Bundle subscriber still on the books (as of the day after), from the same recipe.`

_(no output)_

#### Cell 15: `# Self-asserts`

```
fact_daily_cancels days=548; Sports same-weekday baseline=8,977; event-only rate big game=0.0293, prior nights={'EVT-20270207': 0.029096751160299892, 'EVT-20270131': 0.02762662201758058}
fact_concurrency_hourly=627,264  fact_concurrency_event_5min=20,736  fact_subscription_events=375,056
training prevalence by snapshot: {'2026-08-18': 0.0467, '2026-09-17': 0.0527, '2026-10-17': 0.0473, '2026-11-16': 0.0547, '2026-12-16': 0.0472, '2027-01-15': 0.05}
03_gen_history_facts: all self-asserts passed
```

## Task `gen_dimensions` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/02_gen_dimensions`  
Task run id: 1012971354401311

#### Cell 1: `# Owner: Stream A (Data + Pipelines). The synthetic "world": who the subscribers are and how traffic`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import os`

_(no output)_

#### Cell 5: `# Traffic mix. DFW's share is fixed by the planted cell loads: CDN-C DFW peak 232k = 45% of DFW.`

```
cells: 672  metro x CDN pairs: 48
```

#### Cell 6: `# Event-night curve (10 s grain, 18:00-22:30 ET). Total concurrent viewers on an event night:`

_(no output)_

#### Cell 7: `# Sign-up segments with exact row counts. Ids: base 0..N_base-1, then window / event sign-ups.`

```
Big Game sign-ups 107,487; peak 2727/min vs 245/min -> x11.1
```

#### Cell 8: `# Base attributes for every id with dbldatagen (deterministic: fixed seed, hash_fieldname, 64 partitions).`

_(no output)_

#### Cell 9: `# State follows planted.STATE_SHARE; metro is the home metro when the state has one (METRO_STATE), else null.`

_(no output)_

#### Cell 10: `seg_sdf = spark.createDataFrame(seg[["id", "kind", "tier", "signup_ts", "event_id", "acquirer_seg",  # noqa: F821`

_(no output)_

#### Cell 11: `# dim_profiles: kids-only accounts have only kids profiles; other accounts have 1-3 adults and`

_(no output)_

#### Cell 12: `# ref_cdn_capacity: capacity = forecast peak / target utilization (forecast peak = planted p50 x traffic share).`

_(no output)_

#### Cell 13: `# ref_incidents_history (~40 past incidents). CDN-F Seattle is chronically noisy and self-recovers,`

_(no output)_

#### Cell 14: `# offer_holdout_results: 6 randomized tests per (offer_type, phase). save rate = treated save rate`

_(no output)_

#### Cell 15: `# Self-asserts on the planted numbers this notebook owns.`

```
dim_subscribers 4,000,000  Sports 2,501,871  kids-only 3.51%
dim_profiles 6,995,664  kids 15.01%
worst state share: California 0.1187 vs 0.1190
Acquirer-B window sign-ups 14,286 (target 14,286)
CDN-C DFW util 0.940; CDN-A DFW capacity 226,000, headroom 71,333; cohort load 51,040
                                  mean       std
offer_type       phase                          
credit_replay    post_cancel  0.240067  0.062396
                 pre_cancel   0.249953  0.077046
onboarding_nudge pre_cancel   0.349979  0.095663
reprocess        post_cancel  0.719969  0.040001
                 pre_cancel   0.499989  0.124836
emails 3,920,171 distinct 3,920,171; null phone 2.01%, null device 2.01%
02_gen_dimensions: all self-asserts passed
```
