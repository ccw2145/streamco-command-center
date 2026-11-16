# Databricks notebook source
# StreamCo Command Center 06: record the Ask investigation for the app's fixture.
# The Ask agent lives in agent/ (Agent Bricks CLI, deployed to Agent Runtime as agent-bricks-streamco-ask). This
# notebook runs the same code (agent/streamco_ask/core.py: Genie Agent + fn_cancel_drivers + AI Search over
# knowledge_idx (from 07_knowledge) + one Unity Gateway model) in-process as the notebook user, traced to MLflow, and saves the streamed events and the final answer to
# ask_recorded_run (question, events JSON, answer JSON, trace_id). The Originals follow-up, where the agent
# declines because its sources disagree, is recorded too.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.4" "openai>=1.60" "databricks-sdk>=0.124"

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import asyncio
import concurrent.futures
import datetime as dt
import json
import os
import re
import sys

import mlflow
from databricks.sdk import WorkspaceClient

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "warehouse_id": "<warehouse-id>",
             "gateway_model": "system.ai.gpt-5-6-sol", "game_date": "2027-02-14", "genie_agent_id": ""}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
W = {k: dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "warehouse_id", "gateway_model", "game_date", "genie_agent_id")}  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", W[k]) for k in ("catalog", "schema", "warehouse_id")), "invalid identifier"
sys.path.insert(0, os.path.abspath("../agent"))
from streamco_ask import core  # noqa: E402

w = WorkspaceClient()
genie_id = W["genie_agent_id"] or next((s.space_id for s in (w.genie.list_spaces(page_size=100).spaces or [])
                                        if s.title == "StreamCo Subscriber Insights"), "")
if not genie_id:
    raise RuntimeError("Genie Agent 'StreamCo Subscriber Insights' not found: run 04_genie first.")
settings = core.Settings.from_env(catalog=W["catalog"], schema=W["schema"], warehouse_id=W["warehouse_id"],
                                  genie_agent_id=genie_id, gateway_model=W["gateway_model"], game_date=W["game_date"])
user = spark.sql("SELECT current_user() AS u").first().u  # noqa: F821
mlflow.set_experiment(f"/Users/{user}/streamco-command-center")
mlflow.openai.autolog()
QUESTIONS = ["Why did Sports cancellations jump?", "What about Originals?"]

# COMMAND ----------


async def record(question: str) -> tuple[list, dict, str]:
    events, answer = [], None
    with mlflow.start_span(name="streamco-ask", span_type="AGENT") as root:
        root.set_inputs({"question": question})
        async for event in core.run(question, w, settings):
            events.append(event)
            print(event["type"], json.dumps(event["data"], default=str)[:240])
            if event["type"] == "answer":
                answer = event["data"]
        root.set_outputs(answer)
        trace_id = root.trace_id
    return events, answer, trace_id


rows = []
for q in QUESTIONS:
    events, answer, trace_id = concurrent.futures.ThreadPoolExecutor(1).submit(asyncio.run, record(q)).result()
    errors = [e["data"] for e in events if e["type"] == "tool_done" and e["data"]["status"] == "error"]
    if errors:
        raise RuntimeError(f"tool errors while recording {q!r}: {errors}")
    rows.append((q, json.dumps(events, default=str), json.dumps(answer, default=str), trace_id,
                 dt.datetime.now(dt.timezone.utc), "in_process"))
    print(f"\n{q}\n  {answer['headline']}\n  declined={answer['declined']}  trace {trace_id}\n")

table = f"{W['catalog']}.{W['schema']}.ask_recorded_run"
spark.createDataFrame(rows, "question STRING, events STRING, answer STRING, trace_id STRING, recorded_at TIMESTAMP, "  # noqa: F821
                      "runtime STRING").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
spark.sql(f"COMMENT ON TABLE {table} IS 'Recorded Ask investigations for the app fixture: streamed events (tool_start / "  # noqa: F821
          "tool_done / answer) and the final answer as JSON, with the MLflow trace id.'")

# COMMAND ----------

main = json.loads(rows[0][2])
summary = {"genie_agent_id": genie_id, "trace_ids": [r[3] for r in rows],
           "drivers": [(d["driver"], d["n_cancels"], round(d["pct"], 3)) for d in main["drivers"]],
           "total": main["total"], "originals_declined": json.loads(rows[1][2])["declined"]}
print(json.dumps(summary, indent=1, default=str))
assert main["drivers"] and not main["declined"], "the Sports investigation produced no drivers"
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
