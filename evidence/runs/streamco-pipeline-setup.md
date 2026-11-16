# streamco-pipeline-setup

- Run id: 266957143556597
- Result: SUCCESS
- Started: 2026-11-16 22:43 UTC
- Duration: 104 s

## Task `personalize` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/05_personalize`  
Task run id: 1061787236321192

## Task `classify_transcripts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/07_classify_transcripts`  
Task run id: 343909197610916

## Task `churn` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/03_churn`  
Task run id: 486322051054107

## Task `lakebase` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/09_lakebase`  
Task run id: 1122731797826072

## Task `pipeline_batch` (DISABLED)

Notebook: ``  
Task run id: 1076291249190490

## Task `source_counts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/00_run_sql`  
Task run id: 654976291144432

## Task `pipeline_stream` (DISABLED)

Notebook: ``  
Task run id: 647385559446174

## Task `gen_setup` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/01_setup_uc`  
Task run id: 428809280563358

## Task `ask_agent` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/06_ask_agent`  
Task run id: 461815812013058

## Task `decomposition` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/00_run_sql`  
Task run id: 810737818754209

## Task `genie` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/04_genie`  
Task run id: 261643240582525

## Task `forecast` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/02_forecast`  
Task run id: 349435840135329

## Task `setup` (SUCCESS)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/01_setup`  
Task run id: 1027015080242796

#### Cell 1: `# StreamCo Command Center 01: demo-layer views, the Ask tool function, the governed metric view, and checks.`

_(no output)_

#### Cell 2: `import datetime as dt`

```
DataFrame[]
```

#### Cell 3: `def run_sql_file(path: str) -> int:`

```
views.sql: 5 statements
metric_view.sql: 1 statements
```

#### Cell 4: `# Structural checks.`

```
OK  v_subscribers             3,859,513
OK  v_support_showcase                1
OK  v_training_features       1,200,000
OK  v_scoring_features          299,624
PASS  every view returns rows  
PASS  no blocked terms in screen text  
PASS  tiers are StreamCo names  
PASS  kids-only accounts filtered from v_subscribers  
PASS  scoring population is eligible only  
PASS  emails masked for this caller  
PASS  showcase transcript has redactions
```

#### Cell 5: `# Numbers (reported, not asserted: the app numbers are illustrative).`

```
ok governed=15800
```

## Task `gen_game_night` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/04_gen_game_night`  
Task run id: 433794374360050

## Task `gen_dimensions` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/02_gen_dimensions`  
Task run id: 305847572754005

## Task `gen_history` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/03_gen_history_facts`  
Task run id: 835417423477907

## Task `monitor` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/notebooks/08_monitor`  
Task run id: 838279153286133

## Task `anomaly` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/10_anomaly_engine_sim`  
Task run id: 859590089880898

## Task `gen_transcripts` (DISABLED)

Notebook: `/Workspace/Users/<user>/.bundle/streamco-command-center/dev/files/data/notebooks/05_gen_transcripts`  
Task run id: 398268306542975
