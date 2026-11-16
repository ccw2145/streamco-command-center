"""Log MLflow traces for the Ask investigation, run locally as the presenter.

Runs streamco_ask.core.run() for variants of the Sports question and records each run as one trace (root span
"streamco-ask", tool spans, and the model calls via mlflow.openai.autolog) in the project experiment
/Users/<you>/streamco-command-center. Read-only: the tools only query.

  cd agent && DATABRICKS_CONFIG_PROFILE=my-workspace .venv/bin/python ../scripts/log_ask_traces.py [N]
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import mlflow
from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agent"))
from streamco_ask import core  # noqa: E402

QUESTIONS = [
    "Why did Sports cancellations jump?",
    "Why did Sports cancellations jump after the game?",
    "Why did Sports cancellations spike after the Big Game?",
    "What caused the Sports cancel spike?",
    "Why are Sports cancellations up so much this morning?",
    "What drove yesterday's Sports cancellations?",
    "Why did so many Sports subscribers cancel after the Big Game?",
    "Break down the Sports cancellation spike by cause.",
]
GENIE_AGENT_ID = "01f1dbf923701cb3b3ec3d2fcbee9e87"


async def one(question: str, w: WorkspaceClient, settings: core.Settings) -> tuple[str, dict | None, list]:
    answer, errors = None, []
    with mlflow.start_span(name="streamco-ask", span_type="AGENT") as root:
        root.set_inputs({"question": question})
        async for event in core.run(question, w, settings):
            if event["type"] == "tool_done" and event["data"].get("status") == "error":
                errors.append(event["data"].get("tool") or event["data"].get("lane"))
            if event["type"] == "answer":
                answer = event["data"]
        root.set_outputs(answer)
        trace_id = root.trace_id
    return trace_id, answer, errors


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else len(QUESTIONS)
    w = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "my-workspace"))
    user = w.current_user.me().user_name
    mlflow.set_tracking_uri(f"databricks://{os.getenv('DATABRICKS_CONFIG_PROFILE', 'my-workspace')}")
    exp = mlflow.set_experiment(f"/Users/{user}/streamco-command-center")
    mlflow.openai.autolog()
    settings = core.Settings.from_env(warehouse_id="<warehouse-id>", genie_agent_id=GENIE_AGENT_ID)
    print(f"experiment {exp.experiment_id}")
    for i in range(n):
        q = QUESTIONS[i % len(QUESTIONS)]
        start = time.monotonic()
        try:
            trace_id, answer, errors = asyncio.run(one(q, w, settings))
        except Exception as exc:  # noqa: BLE001 - keep going, report the failure
            print(f"{i + 1:>2}. FAILED {q!r}: {str(exc)[:200]}")
            continue
        head = (answer or {}).get("headline", "no answer")
        print(f"{i + 1:>2}. {time.monotonic() - start:5.1f}s  {trace_id}  {q}\n      {head}"
              + (f"\n      tool errors: {errors}" if errors else ""))


if __name__ == "__main__":
    main()
