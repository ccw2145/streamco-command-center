# streamco-pipeline-lakebase

- Run id: 1041774858371460
- Result: SUCCESS
- Started: 2026-11-13 23:00 UTC
- Duration: 48 s

## Task `personalize` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/05_personalize`  
Task run id: 745736931184884

## Task `classify_transcripts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/07_classify_transcripts`  
Task run id: 1018335067919722

## Task `churn` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/03_churn`  
Task run id: 1007980805595355

## Task `lakebase` (SUCCESS)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/09_lakebase`  
Task run id: 288928752681825

#### Cell 1: `%md`

_(no output)_

#### Cell 2: `%pip install -q "psycopg[binary]>=3.2" "databricks-sdk>=0.81"`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `import re`

_(no output)_

#### Cell 4: `# 1. Create each synced table if it's missing; otherwise refresh its snapshot.`

```
cindy_wu_catalog.streamco.churn_scores_serving: exists, snapshot refresh started (pipeline 7609adb7-416b-4f1e-8ce1-2c3c01132c93)
cindy_wu_catalog.streamco.personalized_messages_serving: exists, snapshot refresh started (pipeline f0d3b0bb-0fbc-47e9-8ac6-70dc33347282)
```

#### Cell 5: `# 2. Wait until every snapshot is online.`

```
cindy_wu_catalog.streamco.churn_scores_serving: SyncedTableState.SYNCED_TABLE_ONLINE_UPDATING_PIPELINE_RESOURCES
cindy_wu_catalog.streamco.personalized_messages_serving: SyncedTableState.SYNCED_TABLE_ONLINE_UPDATING_PIPELINE_RESOURCES
```

#### Cell 6: `# 3. Check the Postgres copy against the Delta source, then time one subscriber lookup.`

```
churn_scores: Delta 299,624 rows, Lakebase 299,624 rows, match=True
personalized_messages: Delta 28,269 rows, Lakebase 28,269 rows, match=True
Marcus: churn 0.42, decile 10, card playback, message 'We're sorry about the halftime buffering' (3.3 ms)
Priya: churn 0.37, decile 10, card payment, message 'We’re sorry we didn’t retry your renewal' (2.6 ms)
Jordan: churn 0.61, decile 10, card onboarding, message 'Welcome after The Big Game, Jordan' (2.9 ms)
read access granted to the app role <app-service-principal-id>
```

## Task `pipeline_batch` (DISABLED)

Notebook: ``  
Task run id: 446513729597621

## Task `source_counts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/00_run_sql`  
Task run id: 251351590446653

## Task `pipeline_stream` (DISABLED)

Notebook: ``  
Task run id: 718428119076888

## Task `gen_setup` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/01_setup_uc`  
Task run id: 1042935845750365

## Task `ask_agent` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/06_ask_agent`  
Task run id: 635293513778768

## Task `decomposition` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/00_run_sql`  
Task run id: 541140482042217

## Task `genie` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/04_genie`  
Task run id: 972425702948113

## Task `forecast` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/02_forecast`  
Task run id: 698390315084395

## Task `setup` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/01_setup`  
Task run id: 572324737533645

## Task `gen_game_night` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/04_gen_game_night`  
Task run id: 95081550576330

## Task `gen_dimensions` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/02_gen_dimensions`  
Task run id: 859947461199017

## Task `gen_history` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/03_gen_history_facts`  
Task run id: 540961911506802

## Task `monitor` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/08_monitor`  
Task run id: 646589022180560

## Task `anomaly` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/10_anomaly_engine_sim`  
Task run id: 338994853968610

## Task `gen_transcripts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/05_gen_transcripts`  
Task run id: 1112319777693353
