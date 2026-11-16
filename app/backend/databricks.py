"""The three live clients: SQL warehouse, Genie Agent, and the LLM through Unity Gateway.

Dual-mode auth: inside a Databricks App (DATABRICKS_APP_NAME is set) the SDK picks up the app's service
principal; locally it uses DATABRICKS_CONFIG_PROFILE (default my-workspace). Every function here is
blocking; callers run them with asyncio.to_thread and their own timeout.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
import uuid
from decimal import Decimal

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementState,
)

from .config import get_settings

_lock = threading.Lock()
_client: WorkspaceClient | None = None


def workspace() -> WorkspaceClient:
    global _client
    with _lock:
        if _client is None:
            if os.getenv("DATABRICKS_APP_NAME"):
                _client = WorkspaceClient()
            else:
                _client = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "my-workspace"))
    return _client


# ---------------------------------------------------------------- SQL
def _param(name: str, value) -> StatementParameterListItem:
    if isinstance(value, bool):
        return StatementParameterListItem(name=name, value=str(value).lower(), type="BOOLEAN")
    if isinstance(value, int):
        return StatementParameterListItem(name=name, value=str(value), type="BIGINT")
    if isinstance(value, float):
        return StatementParameterListItem(name=name, value=repr(value), type="DOUBLE")
    if isinstance(value, dt.datetime):
        return StatementParameterListItem(name=name, value=value.isoformat(), type="TIMESTAMP")
    if isinstance(value, dt.date):
        return StatementParameterListItem(name=name, value=value.isoformat(), type="DATE")
    return StatementParameterListItem(name=name, value=None if value is None else str(value), type="STRING")


_INT = {"INT", "BIGINT", "LONG", "SHORT", "BYTE", "TINYINT", "SMALLINT", "INTEGER"}
_FLOAT = {"DOUBLE", "FLOAT", "DECIMAL"}


def _coerce(value: str | None, type_name: str):
    if value is None:
        return None
    try:
        if type_name in _INT:
            return int(value)
        if type_name in _FLOAT:
            return float(value)
        if type_name == "BOOLEAN":
            return value.lower() == "true"
        if type_name in ("ARRAY", "MAP", "STRUCT"):
            return json.loads(value)
    except ValueError:
        return value
    return value


def run_sql(statement: str, params: dict | None = None, *, timeout_s: float | None = None,
            row_limit: int = 2000) -> list[dict]:
    """Run one parameterized statement on the warehouse; returns rows as dicts. Raises on any failure."""
    s = get_settings()
    if not s.warehouse_id:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set")
    timeout_s = timeout_s or s.sql_timeout_s
    w = workspace()
    deadline = time.monotonic() + timeout_s
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=s.warehouse_id, catalog=s.catalog, schema=s.schema,
        parameters=[_param(k, v) for k, v in (params or {}).items()] or None,
        wait_timeout=f"{max(5, min(50, int(timeout_s)))}s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE, format=Format.JSON_ARRAY, row_limit=row_limit)
    done = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}
    while resp.status and resp.status.state not in done:
        if time.monotonic() > deadline:
            try:
                w.statement_execution.cancel_execution(resp.statement_id)
            except Exception:  # noqa: BLE001 - best effort; the timeout is what we report
                pass
            raise TimeoutError(f"query did not finish within {timeout_s:.0f}s")
        time.sleep(0.4)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error.message if resp.status and resp.status.error else "statement failed"
        raise RuntimeError(err)
    cols = resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else []
    names = [c.name for c in cols or []]
    types = [c.type_name.value if c.type_name else "" for c in cols or []]
    raw = (resp.result.data_array if resp.result else None) or []
    return [{n: _coerce(v, types[i]) for i, (n, v) in enumerate(zip(names, r))} for r in raw]


def jsonable(v):
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


# ---------------------------------------------------------------- Genie Agent
def genie_ask(question: str, conversation_id: str | None = None, *, timeout_s: float | None = None) -> dict:
    """Ask the Genie Agent; returns {conversation_id, message_id, text, sql, columns, rows, duration_ms}."""
    s = get_settings()
    if not s.genie_agent_id:
        raise RuntimeError("GENIE_AGENT_ID is not set")
    timeout = dt.timedelta(seconds=timeout_s or s.genie_timeout_s)
    w = workspace()
    t0 = time.monotonic()
    if conversation_id:
        msg = w.genie.create_message_and_wait(s.genie_agent_id, conversation_id, question, timeout=timeout)
    else:
        msg = w.genie.start_conversation_and_wait(s.genie_agent_id, question, timeout=timeout)
    if msg.error:
        raise RuntimeError(f"Genie error: {msg.error.error}")
    text, sql, columns, rows = [], None, [], []
    for att in msg.attachments or []:
        if att.text and att.text.content:
            text.append(att.text.content)
        if att.query:
            sql = att.query.query
            if att.query.description:
                text.append(att.query.description)
            res = w.genie.get_message_attachment_query_result(
                s.genie_agent_id, msg.conversation_id, msg.message_id, att.attachment_id)
            st = res.statement_response
            if st and st.manifest and st.manifest.schema:
                cols = st.manifest.schema.columns or []
                columns = [c.name for c in cols]
                types = [c.type_name.value if c.type_name else "" for c in cols]
                data = (st.result.data_array if st.result else None) or []
                rows = [[_coerce(v, types[i]) for i, v in enumerate(r)] for r in data]
    return {"conversation_id": msg.conversation_id, "message_id": msg.message_id,
            "text": "\n\n".join(text).strip(), "sql": sql, "columns": columns, "rows": rows,
            "duration_ms": int((time.monotonic() - t0) * 1000)}


# ---------------------------------------------------------------- LLM through Unity Gateway
def llm_json(messages: list[dict], *, timeout_s: float | None = None) -> dict:
    """One chat call to GATEWAY_MODEL through Unity Gateway's OpenAI-compatible route; returns parsed JSON."""
    s = get_settings()
    w = workspace()
    body = {"model": s.gateway_model, "messages": messages, "response_format": {"type": "json_object"},
            "reasoning_effort": "none"}
    r = requests.post(w.config.host.rstrip("/") + "/ai-gateway/mlflow/v1/chat/completions",
                      headers={**w.config.authenticate(), "Content-Type": "application/json"},
                      json=body, timeout=timeout_s or s.llm_timeout_s)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"] or ""
    content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(content)


# ---------------------------------------------------------------- the Ask agent on Agent Runtime
_agent_url: dict[str, str | None] = {}


def agent_answer(question: str, *, timeout_s: float = 90) -> dict:
    """Ask the deployed Ask agent (app AGENT_NAME); returns its answer dict (headline, narrative, declined, ...)."""
    s = get_settings()
    w = workspace()
    if s.agent_name not in _agent_url:
        app = w.apps.get(s.agent_name)
        _agent_url[s.agent_name] = app.url.rstrip("/") if app.url else None
    url = _agent_url[s.agent_name]
    if not url:
        raise RuntimeError(f"agent app {s.agent_name} has no URL")
    r = requests.post(f"{url}/api/invocations", headers={**w.config.authenticate(), "Content-Type": "application/json"},
                      json={"id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                            "input": [{"role": "user", "content": question}]}, timeout=timeout_s)
    r.raise_for_status()
    out = r.json().get("output") or []
    if isinstance(out, dict):  # Agent Runtime wraps the handler's return value: {"output": {"output": [...]}}
        out = out.get("output") or []
    answer = next((o["data"] for o in out if isinstance(o, dict) and o.get("type") == "answer" and o.get("data")), None)
    if not answer:
        raise RuntimeError("agent returned no answer")
    return answer
