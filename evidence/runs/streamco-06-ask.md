# streamco-06-ask

- Run id: 44317544455871
- Result: SUCCESS
- Started: 2026-11-09 04:52 UTC
- Duration: 168 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/06_ask_agent`  
Task run id: 944901626458559

#### Cell 1: `# StreamCo Command Center 06: record the Ask investigation for the app's fixture.`

_(no output)_

#### Cell 2: `%pip install -q "mlflow>=3.4" "openai>=1.60" "databricks-sdk>=0.124"`

```
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
jupyter-server 1.23.4 requires anyio<4,>=3.1.0, but you have anyio 4.16.0 which is incompatible.
googleapis-common-protos 1.62.0 requires protobuf!=3.20.0,!=3.20.1,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0.dev0,>=3.19.5, but you have protobuf 7.36.2 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import asyncio`

```
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```

#### Cell 5: `async def record(question: str) -> tuple[list, dict, str]:`

```
tool_start {"step": 1, "tool": "cancel_drivers", "label": "Cancellation drivers", "args": {"day": "2027-02-15", "tier": "Sports"}}
tool_done {"step": 1, "tool": "cancel_drivers", "status": "ok", "duration_ms": 1429, "sql": "SELECT * FROM cindy_wu_catalog.streamco.fn_cancel_drivers(DATE'2027-02-15', 'Sports')", "columns": ["driver", "n_cancels", "pct", "evidence_type", "confidenc
tool_start {"step": 2, "tool": "ask_genie", "label": "Genie Agent: StreamCo Subscriber Insights", "args": {"question": "What context explains the jump in Sports-tier cancellations on 2027-02-15, the day after the Big Game?"}}
tool_done {"step": 2, "tool": "ask_genie", "status": "ok", "duration_ms": 32808, "sql": "WITH `recon` AS (\n  SELECT\n    'reconciliation' AS `section`,\n    `gold_reconciliation_daily`.`tier` AS `tier`,\n    CAST(NULL AS STRING) AS `cancel_type`,\n 
answer {"question": "Why did Sports cancellations jump?", "day": "2027-02-15", "tier": "Sports", "total": {"observed": 15800, "expected": 8977, "excess": 6823, "pct_over": 0.7601}, "drivers": [{"driver": "playback", "label": "Couldn't watch the ha

Why did Sports cancellations jump?
  Sports cancellations rose after the Big Game, led by playback issues, event-only churn, and payment failures.
  declined=False  trace tr-c59f3589d2badc6a07c7910129201537

tool_start {"step": 1, "tool": "cancel_drivers", "label": "Cancellation drivers", "args": {"day": "2027-02-15", "tier": "Originals"}}
tool_done {"step": 1, "tool": "cancel_drivers", "status": "ok", "duration_ms": 929, "sql": "SELECT * FROM cindy_wu_catalog.streamco.fn_cancel_drivers(DATE'2027-02-15', 'Originals')", "columns": ["driver", "n_cancels", "pct", "evidence_type", "confide
tool_start {"step": 2, "tool": "ask_genie", "label": "Genie Agent: StreamCo Subscriber Insights", "args": {"question": "For the Originals tier on 2027-02-15, what context is relevant to cancellations versus expected after the Big Game?"}}
tool_done {"step": 2, "tool": "ask_genie", "status": "ok", "duration_ms": 20390, "sql": "SELECT\n  e.`day`,\n  e.`tier`,\n  e.`actual`,\n  e.`yhat` AS `expected`,\n  e.`lo` AS `normal_lo`,\n  e.`hi` AS `normal_hi`,\n  e.`outside_band`,\n  e.`method`,
answer {"question": "What about Originals?", "day": "2027-02-15", "tier": "Originals", "total": {"observed": 4017, "expected": 4000, "excess": 17, "pct_over": 0.0043}, "drivers": [], "reconciliation": {"status": "sources_disagree", "subscriber_liv

What about Originals?
  Originals cancellation sources disagree, so no confident number is available.
  declined=True  trace tr-a7e25ac3c1e507c04c9594d47cc074cf
DataFrame[]
[Trace(trace_id='tr-c59f3589d2badc6a07c7910129201537'), Trace(trace_id='tr-a7e25ac3c1e507c04c9594d47cc074cf')]
```

#### Cell 6: `main = json.loads(rows[0][2])`

```
{"genie_agent_id": "01f1dbf923701cb3b3ec3d2fcbee9e87", "trace_ids": ["tr-c59f3589d2badc6a07c7910129201537", "tr-a7e25ac3c1e507c04c9594d47cc074cf"], "drivers": [["playback", 2302, 0.337], ["payment", 1999, 0.293], ["event_only", 2064, 0.303], ["unexplained", 458, 0.067]], "total": {"observed": 15800, "expected": 8977, "excess": 6823, "pct_over": 0.7601}, "originals_declined": true}
```
