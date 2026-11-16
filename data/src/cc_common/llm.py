"""Notebook-side LLM helpers: ai_query SQL on the validated gateway_model, responseFormat, llm_available flags.
Owner: Stream C.

- `ai_query_expr()` builds an `ai_query('<model>', <expr>, ...)` SQL fragment. The model name is validated by
  config.validate_identifier and written unquoted-by-backticks inside a plain string literal (Gateway Amendment).
  Verified on the dev warehouse: plain ai_query and responseFormat json_schema both work on system.ai.gpt-5-6-sol;
  with failOnError => false the result is STRUCT<result STRING, errorMessage STRING>.
- `gateway_openai_client()` is the OpenAI-compatible Unity Gateway route used by probes and notebooks 24/25.
  Any call that passes function tools must add config.GATEWAY_TOOL_CALL_PARAMS (spike S1).
- Flags live in Lakebase ops.app_config (keys llm_available_sql / _structured / _agent), written by 01c.
"""
from __future__ import annotations

import json

from .config import GATEWAY_TOOL_CALL_PARAMS, Config, validate_identifier

FLAG_KEYS = ("llm_available_sql", "llm_available_structured", "llm_available_agent")
GATEWAY_ROUTE = "/ai-gateway/mlflow/v1"

__all__ = ["FLAG_KEYS", "GATEWAY_TOOL_CALL_PARAMS", "ai_query_expr", "gateway_openai_client", "read_flags",
           "response_format", "sql_string", "write_flags"]


def sql_string(value: str) -> str:
    """A Spark SQL string literal. Spark treats backslash as an escape and '' as two adjacent literals, so
    escape backslashes and quotes with a backslash."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def response_format(name: str, schema: dict) -> str:
    """JSON string for ai_query(responseFormat => ...): strict json_schema structured output."""
    validate_identifier(name, "response format name")
    return json.dumps({"type": "json_schema", "json_schema": {"name": name, "schema": schema, "strict": True}},
                      separators=(",", ":"))


def ai_query_expr(cfg: Config, prompt_sql: str, *, response_format_json: str | None = None,
                  fail_on_error: bool = False) -> str:
    """`ai_query('<gateway_model>', <prompt_sql>[, responseFormat => '...'][, failOnError => false])`.

    `prompt_sql` is a SQL expression built by the caller from column references and literals made with
    `sql_string()`; never format user input into it.
    """
    model = validate_identifier(cfg.gateway_model, "gateway_model")
    parts = [f"'{model}'", prompt_sql]
    if response_format_json is not None:
        parts.append(f"responseFormat => {sql_string(response_format_json)}")
    if not fail_on_error:
        parts.append("failOnError => false")
    return f"ai_query({', '.join(parts)})"


def gateway_openai_client(w=None, timeout_s: float = 60.0):
    """OpenAI client on the Unity Gateway route for the identity running the notebook."""
    from databricks.sdk import WorkspaceClient
    from openai import OpenAI

    w = w or WorkspaceClient()
    token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
    return OpenAI(api_key=token, base_url=w.config.host.rstrip("/") + GATEWAY_ROUTE, timeout=timeout_s, max_retries=1)


def write_flags(cfg: Config, flags: dict[str, bool], *, details: dict[str, str] | None = None) -> None:
    """Upsert llm_available_* (and the gateway_model used) into Lakebase ops.app_config."""
    import datetime as dt

    from . import lakebase

    now = dt.datetime.now(dt.UTC)
    rows = [{"key": k, "value": "true" if v else "false", "updated_at": now} for k, v in flags.items()]
    rows.append({"key": "gateway_model", "value": cfg.gateway_model, "updated_at": now})
    for k, v in (details or {}).items():
        rows.append({"key": f"{k}_detail", "value": v[:500], "updated_at": now})
    with lakebase.connect(cfg) as conn:
        lakebase.upsert_rows(conn, "app_config", rows, key=["key"])


def read_flags(cfg: Config) -> dict[str, bool]:
    """llm_available_* from ops.app_config; a missing key or an unreachable Lakebase reads as False."""
    from . import lakebase

    try:
        with lakebase.connect(cfg) as conn:
            query = lakebase.sql.SQL("SELECT key, value FROM {} WHERE key = ANY(%s)").format(
                lakebase.ident("app_config"))
            rows = lakebase.fetch_all(conn, query, (list(FLAG_KEYS),))
    except Exception as exc:  # noqa: BLE001 - flags are advisory; callers fall back to rules/authored text
        print(f"read_flags: Lakebase unavailable ({type(exc).__name__}); treating LLM flags as false")
        return dict.fromkeys(FLAG_KEYS, False)
    found = {r["key"]: r["value"] == "true" for r in rows}
    return {k: found.get(k, False) for k in FLAG_KEYS}
