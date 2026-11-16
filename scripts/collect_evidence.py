"""Collect execution evidence as text into evidence/: what ran, and what it produced.

Every file is generated from the live workspace (job runs, pipeline event logs, Unity Catalog queries, the model
registry, the Genie Agent, the Ask agent, MLflow traces, Lakebase and the deployed app). Re-run to refresh:

  DATABRICKS_CONFIG_PROFILE=my-workspace app/.venv/bin/python scripts/collect_evidence.py
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.parse
import uuid
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "evidence"
sys.path.insert(0, str(ROOT / "app"))
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "<warehouse-id>")

PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "my-workspace")
CATALOG, SCHEMA = "cindy_wu_catalog", "streamco"
FQ = f"{CATALOG}.{SCHEMA}"
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
GENIE_ID = "01f1dbf923701cb3b3ec3d2fcbee9e87"
AGENT_APP, APP = "agent-bricks-streamco-ask", "streamco-command-center"
EXPERIMENT = "/Users/<user>/streamco-command-center"
PIPELINES = ("streamco-data-stream", "streamco-data-batch")
# The run behind each stage (the latest successful one; failed retries are listed in runs/README.md).
RUN_NAMES = ("streamco-data-gen", "streamco-02-forecast", "streamco-09", "streamco-10", "streamco-14",
             "streamco-01-setup", "streamco-03-churn", "streamco-04-genie", "streamco-05-personalize",
             "streamco-06-ask", "streamco-07-knowledge", "streamco-08-monitor", "streamco-pipeline")
MAX_OUT = 6000          # characters kept per cell output
ANSI = re.compile(r"\x1b\[[0-9;]*m")

w = WorkspaceClient(profile=PROFILE)


def write(name: str, text: str) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")
    print(f"wrote {path.relative_to(ROOT)}")


def when(ms: int | None) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%Y-%m-%d %H:%M UTC") if ms else ""


def sql(stmt: str) -> tuple[list[str], list[list]]:
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=WAREHOUSE, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"{stmt[:80]}: {r.status.error.message if r.status.error else r.status.state}")
    cols = [c.name for c in r.manifest.schema.columns]
    return cols, (r.result.data_array or []) if r.result else []


def table(cols: list[str], rows: list[list], limit: int = 60) -> str:
    cell = lambda v: "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")[:160]  # noqa: E731
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows[:limit]]
    if len(rows) > limit:
        lines.append(f"\n({len(rows) - limit} more rows)")
    return "\n".join(lines)


def query_block(title: str, stmt: str, limit: int = 60) -> str:
    cols, rows = sql(stmt)
    return f"### {title}\n\n```sql\n{stmt.strip()}\n```\n\n{table(cols, rows, limit)}\n"


# ------------------------------------------------------------------------------------------------- job runs
def all_runs() -> list[dict]:
    runs, tok = [], None
    for _ in range(20):
        d = w.api_client.do("GET", "/api/2.2/jobs/runs/list", query={"limit": 25, **({"page_token": tok} if tok else {})})
        runs += d.get("runs", [])
        tok = d.get("next_page_token")
        if not tok:
            break
    return [r for r in runs if "streamco" in (r.get("run_name") or "")]


def cell_outputs(results: dict | None) -> str:
    if not results:
        return ""
    data, kind = results.get("data"), results.get("type")
    parts = []
    if kind == "listResults":
        for item in data or []:
            parts.append(cell_outputs(item))
    elif kind == "table":
        cols = [c.get("name") for c in results.get("schema") or []]
        parts.append(table(cols, data or [], 30))
    elif kind == "mimeBundle":
        parts.append((data or {}).get("text/plain", ""))
    elif kind in ("ansi", "raw", "text", "html"):
        parts.append(data if isinstance(data, str) else json.dumps(data))
    elif kind == "error":
        parts.append(f"ERROR: {results.get('summary') or results.get('cause') or data}")
    elif data:
        parts.append(data if isinstance(data, str) else json.dumps(data))
    text = ANSI.sub("", "\n".join(p for p in parts if p)).strip()
    return text if len(text) <= MAX_OUT else text[:MAX_OUT] + f"\n... ({len(text) - MAX_OUT} more characters)"


def notebook_markdown(task_run_id: int) -> str:
    view = w.jobs.export_run(task_run_id).views[0].content
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", view)
    if not m:
        return "_No notebook output in this run's export._\n"
    model = json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode()))
    out = []
    for i, c in enumerate(model.get("commands", []), 1):
        code = (c.get("command") or "").strip()
        if not code:
            continue
        first = code.splitlines()[0][:120]
        res = cell_outputs(c.get("results"))
        out.append(f"#### Cell {i}: `{first}`\n")
        out.append(f"```\n{res}\n```\n" if res else "_(no output)_\n")
    return "\n".join(out)


def collect_runs() -> list[str]:
    runs = all_runs()
    index = ["# Job runs\n", "Every run of this project's notebooks, oldest first. The latest successful run of each "
             "stage has its notebook output exported below (cell by cell, as text).\n",
             "| Run | Name | Result | Started | Duration (s) | Output |", "|---|---|---|---|---|---|"]
    def key(r: dict) -> str:
        """The run name; a partial run of the bundle job is named after the task it ran."""
        name = r["run_name"].replace("[dev cindy_wu] ", "")
        if name == "streamco-pipeline":
            ran = [t.task_key for t in w.jobs.get_run(r["run_id"]).tasks or []
                   if t.state and t.state.result_state and t.state.result_state.value == "SUCCESS"]
            name = f"{name}-{'-'.join(ran) if len(ran) <= 2 else 'all'}"
        return name

    keys = {r["run_id"]: key(r) for r in runs}
    chosen = {}
    for r in sorted(runs, key=lambda r: r["start_time"]):
        name = keys[r["run_id"]]
        if r["state"].get("result_state") == "SUCCESS" and name.startswith(RUN_NAMES):
            chosen[name] = r
    files = []
    for r in sorted(runs, key=lambda r: r["start_time"]):
        name = keys[r["run_id"]]
        link = f"[{name}.md]({name}.md)" if chosen.get(name) is r else ""
        index.append(f"| {r['run_id']} | {name} | {r['state'].get('result_state')} | {when(r['start_time'])} | "
                     f"{round(r.get('run_duration', 0) / 1000)} | {link} |")
    for name, r in chosen.items():
        full = w.jobs.get_run(r["run_id"])
        body = [f"# {name}\n", f"- Run id: {r['run_id']}", f"- Result: {r['state'].get('result_state')}",
                f"- Started: {when(r['start_time'])}", f"- Duration: {round(r.get('run_duration', 0) / 1000)} s\n"]
        for t in full.tasks or []:
            state = t.state.result_state.value if t.state and t.state.result_state else ""
            path = t.notebook_task.notebook_path if t.notebook_task else ""
            body.append(f"## Task `{t.task_key}` ({state})\n\nNotebook: `{path}`  \nTask run id: {t.run_id}\n")
            if state == "SUCCESS" and t.notebook_task:
                body.append(notebook_markdown(t.run_id))
        write(f"runs/{name}.md", "\n".join(body))
        files.append(name)
    write("runs/README.md", "\n".join(index))
    return files


# ------------------------------------------------------------------------------------------------ pipelines
def collect_pipelines() -> None:
    body = ["# Lakeflow pipelines: ingest and quality\n"]
    for p in w.pipelines.list_pipelines(filter="name LIKE '%streamco-data%'"):
        body.append(f"## {p.name} (`{p.pipeline_id}`)\n")
        rows = [[u.update_id, u.state.value if u.state else "", u.full_refresh, when(u.creation_time)]
                for u in (w.pipelines.list_updates(p.pipeline_id, max_results=5).updates or [])]
        body.append(table(["update_id", "state", "full_refresh", "created"], rows) + "\n")
    body.append(query_block(
        "Quality rules (expectations) on the latest full refresh",
        f"""WITH ev AS (SELECT * FROM event_log(TABLE({FQ}.silver_accounts))),
upd AS (SELECT max_by(origin.update_id, timestamp) AS update_id FROM ev
        WHERE event_type = 'create_update' AND details:create_update.full_refresh::boolean)
SELECT e.name AS rule, regexp_replace(e.dataset, '^.*[.]', '') AS dataset,
       sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
        'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')) AS e
      FROM ev JOIN upd ON ev.origin.update_id = upd.update_id
      WHERE event_type = 'flow_progress' AND details:flow_progress.status = 'COMPLETED')
GROUP BY 1, 2 ORDER BY failed DESC, rule"""))
    body.append(query_block("Rows landed per source system", f"SELECT source_id, display_name, system_type, "
                            f"prod_connector, row_count FROM {FQ}.ref_source_systems ORDER BY source_id"))
    write("pipelines.md", "\n".join(body))


# -------------------------------------------------------------------------------------------- Unity Catalog
def collect_uc() -> None:
    body = ["# Unity Catalog: the governed layer\n"]
    body.append(query_block("Tables and views in the schema", f"SELECT table_name, table_type FROM {CATALOG}."
                            f"information_schema.tables WHERE table_schema = '{SCHEMA}' ORDER BY table_type, "
                            "table_name", 200))
    body.append(query_block("Three teams, three numbers, one governed count", f"SELECT team, count, definition, "
                            f"governed_count FROM {FQ}.gold_team_cancel_counts"))
    body.append(query_block("The governed number from the Metric View", f"SELECT MEASURE(cancels) AS cancels FROM "
                            f"{FQ}.mv_streamco_metrics WHERE day = DATE'2027-02-15' AND tier = 'Sports'"))
    body.append(query_block("Reconciliation with the billing ledger", f"SELECT * FROM {FQ}.gold_reconciliation_daily "
                            "WHERE day = DATE'2027-02-15' ORDER BY tier"))
    body.append(query_block("Governed view: personal data masked, kids-only accounts excluded",
                            f"SELECT subscriber_id, email, phone, tier, metro FROM {FQ}.v_subscribers LIMIT 5"))
    body.append(query_block("Kids-only accounts: in the source, not in the governed view",
                            f"SELECT (SELECT count(*) FROM {FQ}.silver_accounts) AS accounts_in_source, "
                            f"(SELECT count(*) FROM {FQ}.v_subscribers) AS subscribers_in_governed_view"))
    body.append(query_block("Lineage into the governed number (system.access.table_lineage)",
                            "SELECT DISTINCT source_table_name, target_table_name FROM system.access.table_lineage "
                            f"WHERE target_table_catalog = '{CATALOG}' AND target_table_schema = '{SCHEMA}' AND "
                            f"source_table_schema = '{SCHEMA}' AND event_time >= current_timestamp() - INTERVAL 30 DAYS "
                            "ORDER BY 2, 1", 100))
    write("unity_catalog.md", "\n".join(body))


# ----------------------------------------------------------------------------------------------------- ML
def collect_ml() -> None:
    body = ["# Predictive ML: forecast and churn\n"]
    rows = []
    for name in ("viewer_forecast", "churn_propensity"):
        full = f"{FQ}.{name}"
        for v in w.model_versions.list(full):
            rows.append([full, v.version, ", ".join(a.alias_name for a in (w.registered_models.get(full).aliases or [])
                                                     if a.version_num == v.version), when(v.created_at), v.status])
    body.append("## Unity Catalog model registry\n\n" + table(["model", "version", "aliases", "created", "status"],
                                                              rows) + "\n")
    body.append(query_block("Forecast: capacity flags before kickoff", f"SELECT * FROM {FQ}.forecast_flags ORDER BY "
                            "util_at_peak DESC LIMIT 8"))
    body.append(query_block("Churn model card", f"SELECT * FROM {FQ}.churn_model_card"))
    body.append(query_block("Save plan", f"SELECT card_id, title, n_subscribers, expected_cancels, save_rate, "
                            f"saved_point, saved_lo, saved_hi, cost_usd, revenue_protected_usd FROM {FQ}.save_plan"))
    body.append(query_block("Monitoring: accuracy and drift status", f"SELECT champion_version, auc_train, auc_recent, "
                            f"recent_window, auc_drop, max_top_psi, max_top_psi_feature, status, status_reason, "
                            f"retrain_rule FROM {FQ}.churn_monitoring"))
    body.append(query_block("Monitoring: drift per feature (PSI)", f"SELECT label, importance_rank, is_top, psi, "
                            f"baseline_mean, current_mean, drift FROM {FQ}.churn_monitoring_features ORDER BY psi DESC"))
    body.append(query_block("Learning from outcomes: holdout tests behind each save rate", f"SELECT card_id, test_id, "
                            f"run_date, n_treated, n_holdout, treated_rate, holdout_rate, lift, plan_save_rate FROM "
                            f"{FQ}.churn_monitoring_outcomes ORDER BY card_id, run_date"))
    write("ml.md", "\n".join(body))


# -------------------------------------------------------------------------------------------------- Gen AI
def genie(question: str) -> str:
    msg = w.genie.start_conversation_and_wait(GENIE_ID, question)
    parts = [f"### Q: {question}\n", f"Status: {msg.status.value if msg.status else ''}\n"]
    for a in msg.attachments or []:
        if a.text and a.text.content:
            parts.append(a.text.content + "\n")
        if a.query:
            parts.append(f"```sql\n{a.query.query}\n```\n")
            res = w.genie.get_message_attachment_query_result(GENIE_ID, msg.conversation_id, msg.message_id,
                                                              a.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.result:
                parts.append(table([c.name for c in sr.manifest.schema.columns], sr.result.data_array or [], 20) + "\n")
    return "\n".join(parts)


def ask_agent(question: str) -> dict:
    url = w.apps.get(AGENT_APP).url.rstrip("/")
    r = requests.post(f"{url}/api/invocations", headers={**w.config.authenticate(), "Content-Type": "application/json"},
                      json={"id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                            "input": [{"role": "user", "content": question}]}, timeout=180)
    r.raise_for_status()
    return r.json()


def collect_genai() -> None:
    body = ["# Genie Agent: natural-language questions (live)\n",
            f"Genie Agent \"StreamCo Subscriber Insights\" (`{GENIE_ID}`), asked when this file was generated.\n"]
    for q in ("How many Sports cancellations were there on 2027-02-15, and how many were expected?",
              "Show Sports cancellations by state on 2027-02-15, top 10.", "Did Originals cancellations go up too?"):
        body.append(genie(q))
    write("genie.md", "\n".join(body))

    q = "Why did Sports cancellations jump?"
    resp = ask_agent(q)
    write("ask_agent_response.json", json.dumps({"question": q, "agent_app": AGENT_APP, "response": resp}, indent=1,
                                                default=str))
    import mlflow  # noqa: PLC0415 - only needed here
    mlflow.set_tracking_uri(f"databricks://{PROFILE}")
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    traces = mlflow.search_traces(experiment_ids=[exp.experiment_id], return_type="list", max_results=50)
    rows = []
    for t in traces:
        spans = t.data.spans
        root = next((s for s in spans if s.parent_id is None), None)
        q_in = (root.inputs or {}).get("question", "") if root else ""
        tools = sorted({s.name for s in spans if str(s.span_type) == "TOOL"})
        rows.append([t.info.trace_id, when(t.info.request_time), t.info.state, t.info.execution_duration,
                     len(spans), ", ".join(tools), q_in])
    body = ["# MLflow traces of the Ask agent\n", f"Experiment `{EXPERIMENT}` (`{exp.experiment_id}`): "
            f"{len(rows)} traces.\n", table(["trace", "time", "state", "ms", "spans", "tools", "question"], rows, 50)]
    if traces:
        t = traces[0]
        body.append(f"\n## Spans of {t.info.trace_id}\n")
        body.append(table(["span", "type", "status", "ms"], [[s.name, s.span_type, s.status.status_code,
                          round((s.end_time_ns - s.start_time_ns) / 1e6)] for s in t.data.spans]))
    write("mlflow_traces.md", "\n".join(body))

    body = ["# Personalized messages (ai_query batch)\n"]
    body.append(query_block("The batch run", f"SELECT * FROM {FQ}.personalization_run ORDER BY run_at DESC"))
    body.append(query_block("Messages for the three sample subscribers", f"SELECT s.first_name, m.card_id, m.subject, "
                            f"m.body, m.watch_next, m.agent_note FROM {FQ}.personalized_messages m JOIN "
                            f"{FQ}.ref_sample_subscribers s USING (subscriber_id)"))
    body.append(query_block("Offer-policy passage retrieved with AI Search for each plan card",
                            f"SELECT card_id, doc_title, score, passage FROM {FQ}.knowledge_citations"))
    body.append(query_block("Knowledge index source: policy sections and masked transcripts",
                            f"SELECT doc_type, count(*) AS chunks FROM {FQ}.knowledge_chunks GROUP BY 1"))
    write("personalization.md", "\n".join(body))


# ------------------------------------------------------------------------------------------------ Lakebase
def collect_lakebase() -> None:
    from backend import lakebase  # noqa: PLC0415 - the app's own client
    body = ["# Lakebase: operational serving\n", "Project `streamco`, branch `production`, database "
            "`databricks_postgres`. Synced tables (snapshot) in Postgres schema `streamco`; the app's approvals in "
            "`streamco_app`.\n", "## Synced tables\n"]
    rows = []
    for t in ("churn_scores_serving", "personalized_messages_serving"):
        st = w.postgres.get_synced_table(name=f"synced_tables/{FQ}.{t}")
        rows.append([f"{FQ}.{t}", st.spec.source_table_full_name if st.spec else "",
                     st.status.detailed_state if st.status else ""])
    body.append(table(["synced table", "source", "state"], rows) + "\n")
    checks = []
    for src, pg in (("churn_scores", "churn_scores_serving"), ("personalized_messages", "personalized_messages_serving")):
        _, delta = sql(f"SELECT count(*) FROM {FQ}.{src}")
        n = lakebase.query(f"SELECT count(*) AS n FROM streamco.{pg}")[0]["n"]
        checks.append([src, delta[0][0], n, str(int(delta[0][0]) == n)])
    body.append("## Row counts: Delta source vs Lakebase\n\n" + table(["table", "delta_rows", "lakebase_rows", "match"],
                                                                       checks) + "\n")
    _, samples = sql(f"SELECT first_name, subscriber_id FROM {FQ}.ref_sample_subscribers")
    stmt = ("SELECT s.churn_prob, s.risk_decile, s.card_id, m.subject FROM streamco.churn_scores_serving s "
            "LEFT JOIN streamco.personalized_messages_serving m USING (subscriber_id) WHERE s.subscriber_id = %(id)s")
    look = []
    for name, sid in samples:
        r, ms = lakebase.timed_query(stmt, {"id": int(sid)})
        look.append([name, sid, round(r[0]["churn_prob"], 3), r[0]["risk_decile"], r[0]["card_id"], r[0]["subject"],
                     round(ms, 1)])
    body.append("## One-subscriber lookups (the app's read)\n\n```sql\n" + stmt + "\n```\n\n" + table(
        ["subscriber", "id", "churn_prob", "decile", "card", "message subject", "query ms"], look) + "\n")
    grants = lakebase.query("SELECT grantee, table_schema, table_name, privilege_type FROM "
                            "information_schema.role_table_grants WHERE table_schema = 'streamco' AND grantee NOT IN "
                            "(SELECT current_user)")
    body.append("## Read access for the app's role\n\n" + table(list(grants[0].keys()) if grants else ["grantee"],
                                                                [list(g.values()) for g in grants]) + "\n")
    try:
        appr = lakebase.query("SELECT approval_id, approved_by, approved_at, jsonb_array_length(plan_snapshot->'cards') "
                              "AS cards FROM streamco_app.approvals ORDER BY approval_id DESC LIMIT 10")
        body.append("## Approvals written by the deployed app\n\n" + table(
            ["approval_id", "approved_by", "approved_at", "cards"], [list(a.values()) for a in appr]) + "\n")
    except Exception as exc:  # noqa: BLE001 - no approval yet
        body.append(f"## Approvals written by the deployed app\n\n_None yet ({type(exc).__name__})._\n")
    write("lakebase.md", "\n".join(body))


# ------------------------------------------------------------------------------------------------------ app
def collect_app() -> None:
    a = w.apps.get(APP)
    d = a.active_deployment
    body = ["# Databricks App: the Command Center\n", f"- URL: {a.url}",
            f"- App status: {a.app_status.state.value if a.app_status else ''} ({a.app_status.message if a.app_status else ''})",
            f"- Compute: {a.compute_status.state.value if a.compute_status else ''}",
            f"- Active deployment: {d.deployment_id if d else ''}, {d.status.state.value if d and d.status else ''}, "
            f"{d.create_time if d else ''}",
            "- Resources: " + ", ".join(f"{r.name}" for r in a.resources or []) + "\n",
            "The screens are served from [app/fixture.json](../app/fixture.json), exported from the tables above by "
            "[scripts/export_fixture.py](../scripts/export_fixture.py); live calls (Ask, Genie follow-ups, Regenerate, "
            "the Lakebase lookups and Approve) fall back to it per request.\n"]
    fx = json.loads((ROOT / "app" / "fixture.json").read_text())
    body.append(f"Fixture: generated {fx.get('generated_at')}, source `{fx.get('source')}`, illustrative="
                f"{fx.get('illustrative')}, Ask run `{fx.get('ask_run_source')}`.\n")
    write("app.md", "\n".join(body))


def main() -> None:
    OUT.mkdir(exist_ok=True)
    steps = {"runs": collect_runs, "pipelines": collect_pipelines, "uc": collect_uc, "ml": collect_ml,
             "genai": collect_genai, "lakebase": collect_lakebase, "app": collect_app}
    only = set(sys.argv[1:]) or set(steps)
    for name, fn in steps.items():
        if name in only:
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 - keep going; report what failed
                print(f"FAILED {name}: {type(exc).__name__}: {str(exc)[:300]}")


if __name__ == "__main__":
    main()
