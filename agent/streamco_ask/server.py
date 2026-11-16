"""Agent Runtime entrypoint (DurableAgentServer), started by `uv run start-server` (app.yaml).

POST /api/invocations {"input": [{"role": "user", "content": "Why did Sports cancellations jump?"}], "stream": true}
(`input` may also be {"question": "..."}). Each core.run() event is emitted as it happens; the final output
carries the answer as {"type": "answer", "data": ...}. No memory or session store.
"""
from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from databricks_agentkit import DurableAgentServer, InvocationContext, start_trace, workspace_client
from databricks_agentkit.openai import configure_tracing  # binds mlflow.openai.autolog: model calls become spans
from dotenv import load_dotenv

from . import core

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)


def question_of(value) -> str:
    if isinstance(value, dict):
        if isinstance(value.get("question"), str):
            return value["question"].strip()
        value = value.get("messages") or value.get("input") or []
    if isinstance(value, str):
        return value.strip()
    for msg in reversed(value if isinstance(value, list) else []):
        if isinstance(msg, dict) and msg.get("role", "user") == "user":
            c = msg.get("content")
            return (c if isinstance(c, str) else " ".join(p.get("text", "") for p in c or [] if isinstance(p, dict))).strip()
    raise ValueError("No question: send input=[{'role': 'user', 'content': '...'}] or {'question': '...'}")


async def invoke(value, context: InvocationContext) -> dict:
    question = question_of(value)
    answer = None
    with start_trace(name="streamco-ask", inputs={"question": question}, session_id=context.session_id) as span:
        async for event in core.run(question, workspace_client(), core.Settings.from_env()):
            await context.emit(event)
            if event["type"] == "answer":
                answer = event["data"]
        if span is not None and answer is not None:
            span.set_outputs(answer)
    text = f"{answer['headline']}\n\n{answer['narrative']}" if answer else "No answer was produced."
    return {"output": [{"type": "message", "role": "assistant", "content": text}, {"type": "answer", "data": answer}],
            "status": "completed"}


configure_tracing()
app = DurableAgentServer()
app.invoke(invoke)
app.recover(invoke)  # read-only tools: re-running a whole investigation on recovery is safe


def main() -> None:
    uvicorn.run("streamco_ask.server:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
