"""The StreamCo Ask agent: one model, three tools, numbers copied from tool output.

Tools
  ask_genie(question)          the Genie Agent "StreamCo Subscriber Insights" (subscriber and cancel numbers)
  cancel_drivers(day, tier)    UC function {catalog}.{schema}.fn_cancel_drivers on the SQL warehouse
  search_knowledge(query)      AI Search over {catalog}.{schema}.knowledge_idx (policy docs and masked support
                               transcripts; the query is embedded with EMBEDDING_MODEL through Unity Gateway),
                               plus the governed count of transcripts per reason code

The model (Unity Gateway, tool calls need reasoning_effort "none") decides which tools to call and writes the
headline and narrative. Every number on the answer card is copied from the cancel_drivers result by code; the card's
"From your documents" citations are copied from the search_knowledge results by code. When
that result says the tier's sources disagree (recon_status sources_disagree), the answer is declined.

run() is an async generator of events, the same stream the app's "lanes" view shows:
  {"type": "tool_start", "data": {"step", "tool", "label", "args"}, "offset_ms"}
  {"type": "tool_done",  "data": {"step", "tool", "status", "duration_ms", "sql", "columns", "rows", "summary"}, "offset_ms"}
  {"type": "answer",     "data": <answer>, "offset_ms"}
Used by the Agent Runtime server (server.py) and in-process by notebooks/06_ask_agent.py.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta

TIERS = ("Sports", "Family", "Originals", "Bundle")
DRIVER_LABELS = {"playback": "Couldn't watch the halftime show properly",
                 "payment": "Sign-up payment timed out and was never retried",
                 "event_only": "Expected event-only cancels", "unexplained": "Not explained yet"}
SYSTEM = """You are StreamCo's investigation analyst. An executive asks about cancellations the morning after the Big Game.
Use the tools: cancel_drivers(day, tier) explains one tier's cancellations with evidence and the Finance-ledger check;
ask_genie(question) answers subscriber and cancellation questions from governed metrics; search_knowledge(query,
doc_type) searches StreamCo's policy documents and masked support transcripts and counts transcripts per reason.
Call cancel_drivers for the tier in the question. Call ask_genie at most once, for context. Call search_knowledge
twice: once with doc_type "policy" about the payment driver (sign-up payment declined, retries, grace window), and
once with doc_type "transcript" about customers who were charged, declined and cancelled.
Then reply with a JSON object {"headline": str, "narrative": str}:
- headline: one sentence, at most 20 words.
- narrative: four to six plain sentences: observed vs expected, each driver's share, and the ledger check. For the
  payment driver, cite the payment retry policy by its title: what it says about retries and the grace window, and
  what happened instead. Cite the support transcripts with their count from transcript_counts (e.g. "N Sports
  support conversations said they were charged, declined and cancelled").
Rules: use only numbers that appear in tool results; never estimate. If recon_status is sources_disagree, say you
can't give a confident number, give both sources' figures and the gap, and stop. Never repeat personal data or
quote a transcript's placeholders. No table, function, index or SQL names."""
TOOLS = [
    {"type": "function", "function": {
        "name": "cancel_drivers",
        "description": "Why cancellations moved for one tier on one day: drivers with counts, share of the excess, "
                       "evidence and confidence, plus observed/expected totals and the Finance-ledger reconciliation.",
        "parameters": {"type": "object", "additionalProperties": False, "required": ["day", "tier"], "properties": {
            "day": {"type": "string", "description": "ISO date, e.g. the day after the Big Game"},
            "tier": {"type": "string", "enum": list(TIERS)}}}}},
    {"type": "function", "function": {
        "name": "search_knowledge",
        "description": "Search StreamCo's policy documents and masked support transcripts (AI Search). Returns the "
                       "best passages with document title and section, and for transcript hits the number of support "
                       "conversations with the same reason code and tier.",
        "parameters": {"type": "object", "additionalProperties": False, "required": ["query"], "properties": {
            "query": {"type": "string"},
            "doc_type": {"type": "string", "enum": ["policy", "transcript"]},
            "tier": {"type": "string", "enum": list(TIERS), "description": "Filters transcripts only."}}}}},
    {"type": "function", "function": {
        "name": "ask_genie",
        "description": "Ask the StreamCo Subscriber Insights Genie Agent one question about subscribers or cancellations.",
        "parameters": {"type": "object", "additionalProperties": False, "required": ["question"], "properties": {
            "question": {"type": "string"}}}}},
]


@dataclass(frozen=True)
class Settings:
    catalog: str
    schema: str
    warehouse_id: str
    genie_agent_id: str
    gateway_model: str = "system.ai.gpt-5-6-sol"
    game_date: str = "2027-02-14"
    knowledge_index: str = ""
    embedding_model: str = "system.ai.gte-large-en"

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        values = {"catalog": os.getenv("CATALOG_NAME", "cindy_wu_catalog"), "schema": os.getenv("SCHEMA_NAME", "streamco"),
                  "warehouse_id": os.getenv("WAREHOUSE_ID", ""), "genie_agent_id": os.getenv("GENIE_AGENT_ID", ""),
                  "gateway_model": os.getenv("GATEWAY_MODEL", "system.ai.gpt-5-6-sol"),
                  "game_date": os.getenv("GAME_DATE", "2027-02-14"), "knowledge_index": os.getenv("KNOWLEDGE_INDEX", ""),
                  "embedding_model": os.getenv("EMBEDDING_MODEL", "system.ai.gte-large-en")}
        values.update({k: v for k, v in overrides.items() if v})
        values["knowledge_index"] = values["knowledge_index"] or f"{values['catalog']}.{values['schema']}.knowledge_idx"
        for k in ("catalog", "schema", "gateway_model", "knowledge_index", "embedding_model"):
            if not re.fullmatch(r"[A-Za-z0-9_.\-]+", values[k]):
                raise ValueError(f"invalid {k}: {values[k]!r}")
        return cls(**values)

    @property
    def day_after(self) -> str:
        return (date.fromisoformat(self.game_date) + timedelta(days=1)).isoformat()


@contextmanager
def _span(name: str, inputs=None):
    try:
        import mlflow
    except ImportError:
        yield None
        return
    with mlflow.start_span(name=name, span_type="TOOL") as span:
        if inputs is not None:
            span.set_inputs(inputs)
        yield span


def _coerce(v):
    if not isinstance(v, str):
        return v
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


# ---- tools ---------------------------------------------------------------------------------------------------

def cancel_drivers(w, s: Settings, day: str, tier: str) -> dict:
    from databricks.sdk.service.sql import StatementParameterListItem

    date.fromisoformat(day)
    if tier not in TIERS:
        raise ValueError(f"tier must be one of {TIERS}")
    fn = f"{s.catalog}.{s.schema}.fn_cancel_drivers"
    resp = w.statement_execution.execute_statement(
        statement=f"SELECT * FROM {fn}(:day, :tier)", warehouse_id=s.warehouse_id, wait_timeout="30s",
        parameters=[StatementParameterListItem(name="day", value=day, type="DATE"),
                    StatementParameterListItem(name="tier", value=tier, type="STRING")])
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        raise RuntimeError(f"fn_cancel_drivers {state}: {getattr(resp.status.error, 'message', '')}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = [[_coerce(v) for v in r] for r in (resp.result.data_array or [])] if resp.result else []
    return {"sql": f"SELECT * FROM {fn}(DATE'{day}', '{tier}')", "columns": cols, "rows": rows}


def ask_genie(w, s: Settings, question: str, timeout_s: float = 60.0) -> dict:
    if not s.genie_agent_id:
        raise RuntimeError("GENIE_AGENT_ID is not set")
    msg = w.genie.start_conversation_and_wait(s.genie_agent_id, question, timeout=timedelta(seconds=timeout_s))
    text, sql, att_id = "", None, None
    for a in msg.attachments or []:
        if a.text and a.text.content:
            text = text or a.text.content
        if a.query:
            sql, att_id = sql or a.query.query, att_id or a.attachment_id
            text = text or (a.query.description or "")
    cols, rows = [], []
    if att_id:
        res = w.genie.get_message_attachment_query_result(s.genie_agent_id, msg.conversation_id, msg.message_id, att_id)
        st = res.statement_response
        cols = [c.name for c in st.manifest.schema.columns]
        rows = [[_coerce(v) for v in r] for r in (st.result.data_array or [])][:50] if st.result else []
    return {"sql": sql, "columns": cols, "rows": rows, "text": text}


KNOWLEDGE_COLS = ["chunk_id", "doc_type", "doc_title", "section", "text", "reason_code", "tier"]
_PLACEHOLDER = re.compile(r"\[(EMAIL|PHONE|CARD|ADDRESS|NAME)\]")


def quote_of(hit: dict, max_chars: int = 220) -> str:
    """A short quote: a policy section as written; for a transcript, the customer's first line with no placeholders."""
    text = str(hit.get("text") or "")
    if hit.get("doc_type") == "transcript":
        lines = [ln.split(":", 1)[1].strip() for ln in text.splitlines() if ln.lower().startswith("customer:")]
        sentences = [x for ln in lines for x in re.split(r"(?<=[.!?])\s+", ln) if x and not _PLACEHOLDER.search(x)]
        text = sentences[0] if sentences else ""
    text = _PLACEHOLDER.sub("", text).strip()
    return text if len(text) <= max_chars else text[: max_chars - 3].rsplit(" ", 1)[0] + "..."


def search_knowledge(w, s: Settings, query: str, doc_type: str | None = None, tier: str | None = None,
                     k: int = 3) -> dict:
    from databricks.sdk.service.sql import StatementParameterListItem

    if doc_type not in (None, "policy", "transcript") or (tier and tier not in TIERS):
        raise ValueError("doc_type must be policy or transcript; tier one of " + ", ".join(TIERS))
    if doc_type != "transcript":
        tier = None  # policy sections have no tier; a tier filter would hide them
    filters = {key: v for key, v in (("doc_type", doc_type), ("tier", tier)) if v}
    # The index holds Unity Gateway embeddings (self-managed vectors), so the query is embedded the same way.
    vector = _client(w).embeddings.create(model=s.embedding_model, input=[query]).data[0].embedding
    norm = sum(x * x for x in vector) ** 0.5 or 1.0  # the chunks are stored at unit length (07_knowledge)
    vector = [x / norm for x in vector]
    r = w.vector_search_indexes.query_index(index_name=s.knowledge_index, columns=KNOWLEDGE_COLS, query_vector=vector,
                                            num_results=k, filters_json=json.dumps(filters) if filters else None)
    names = [c.name for c in r.manifest.columns]
    hits = [dict(zip(names, row)) for row in (r.result.data_array or [])]
    passages = [{"doc_type": h["doc_type"], "doc_title": h["doc_title"], "section": h["section"],
                 "quote": quote_of(h), "reason_code": h.get("reason_code"), "tier": h.get("tier"),
                 "score": round(float(h.get("score") or 0), 4)} for h in hits]
    counts = []
    reasons = sorted({h["reason_code"] for h in hits if h.get("reason_code")})
    if reasons:  # how many transcripts share each hit's reason code (and tier, when asked for one)
        chunks = f"{s.catalog}.{s.schema}.knowledge_chunks"
        params = [StatementParameterListItem(name=f"r{i}", value=v, type="STRING") for i, v in enumerate(reasons)]
        where = f"doc_type = 'transcript' AND reason_code IN ({', '.join(f':r{i}' for i in range(len(reasons)))})"
        if tier:
            where += " AND tier = :tier"
            params.append(StatementParameterListItem(name="tier", value=tier, type="STRING"))
        resp = w.statement_execution.execute_statement(
            statement=f"SELECT reason_code, tier, count(*) AS n_conversations FROM {chunks} WHERE {where} "
                      "GROUP BY reason_code, tier ORDER BY n_conversations DESC",
            warehouse_id=s.warehouse_id, wait_timeout="30s", parameters=params)
        if resp.status and resp.status.state and resp.status.state.value == "SUCCEEDED" and resp.result:
            counts = [{"reason_code": a, "tier": b, "n_conversations": int(c)} for a, b, c in resp.result.data_array or []]
    return {"query": query, "filters": filters, "passages": passages, "transcript_counts": counts,
            "rows": [[p["doc_title"], p["section"], p["quote"]] for p in passages],
            "columns": ["doc_title", "section", "quote"], "sql": None}


def citations_of(results: list[dict]) -> list[dict]:
    """The answer card's "From your documents" entries, from search_knowledge results only (code, not the model)."""
    out, seen = [], set()
    for res in results:
        counts = {(c["reason_code"], c["tier"]): c["n_conversations"] for c in res.get("transcript_counts", [])}
        for p in res.get("passages", [])[:1]:
            key = (p["doc_title"], p["section"])
            if key in seen or not p["quote"]:
                continue
            seen.add(key)
            n = sum(v for (rc, _), v in counts.items() if rc == p.get("reason_code")) if p["doc_type"] == "transcript" else None
            out.append({"doc_type": p["doc_type"], "doc_title": p["doc_title"], "section": p["section"], "quote": p["quote"],
                        "n_conversations": n, "tier": p.get("tier"), "reason_code": p.get("reason_code")})
    return out


# ---- answer --------------------------------------------------------------------------------------------------

def _frame(question: str, day: str, tier: str, result: dict | None) -> dict:
    """The answer card's numbers, from the cancel_drivers rows only."""
    recs = [dict(zip(result["columns"], r)) for r in result["rows"]] if result else []
    first = recs[0] if recs else {}
    observed, expected = first.get("observed"), first.get("expected")
    status = first.get("recon_status")
    drivers = [{"driver": r["driver"], "label": DRIVER_LABELS.get(r["driver"], r["driver"]), "n_cancels": r["n_cancels"],
                "pct": r["pct"], "evidence_type": r["evidence_type"], "confidence": r["confidence"],
                "evidence": r["evidence"],
                "sql": f"{result['sql']} -- driver = '{r['driver']}'"}
               for r in recs if r.get("driver") and r["driver"] != "baseline"]
    declined = status == "sources_disagree" or not recs
    reason = None
    if status == "sources_disagree":
        reason = (f"{tier} cancellations disagree between Subscriber Live ({observed:,}) and the Finance ledger "
                  f"({first.get('ledger_cancels'):,}), a {first.get('recon_pct_diff', 0):.1%} gap, so the number is unconfirmed.")
    elif not recs:
        reason = f"No cancellation data for {tier} on {day}."
    return {"question": question, "day": day, "tier": tier,
            "total": {"observed": observed, "expected": expected,
                      "excess": observed - expected if observed is not None and expected is not None else None,
                      "pct_over": round(observed / expected - 1, 4) if observed and expected else None},
            "drivers": [] if declined else drivers,
            "reconciliation": {"status": status, "subscriber_live": observed, "ledger": first.get("ledger_cancels"),
                               "pct_diff": first.get("recon_pct_diff")},
            "declined": declined, "declined_reason": reason, "sql": result["sql"] if result else None}


def detect_tier(question: str) -> str:
    q = question.lower()
    return next((t for t in TIERS if t.lower() in q), "Sports")


def _client(w):
    from openai import OpenAI

    token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
    return OpenAI(api_key=token, base_url=w.config.host.rstrip("/") + "/ai-gateway/mlflow/v1", timeout=60, max_retries=1)


async def run(question: str, w, s: Settings):
    """Yield tool_start / tool_done / answer events for one question."""
    t0 = time.monotonic()
    ms = lambda: int((time.monotonic() - t0) * 1000)  # noqa: E731
    llm = _client(w)
    tier = detect_tier(question)
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": f"{question}\n(The Big Game was {s.game_date}; 'yesterday' is {s.day_after}.)"}]
    drivers_result, step, reply = None, 0, None
    knowledge: list[dict] = []

    async def call_tool(name: str, args: dict) -> dict:
        nonlocal step, drivers_result
        step += 1
        label = {"cancel_drivers": "Cancellation drivers", "search_knowledge": "AI Search: policies and support transcripts"}.get(
            name, "Genie Agent: StreamCo Subscriber Insights")
        events.append({"type": "tool_start", "data": {"step": step, "tool": name, "label": label, "args": args}, "offset_ms": ms()})
        started = time.monotonic()
        with _span(name, args) as span:
            try:
                if name == "cancel_drivers":
                    out = await asyncio.to_thread(cancel_drivers, w, s, args.get("day") or s.day_after, args.get("tier") or tier)
                    drivers_result = out
                    summary = f"{len(out['rows'])} rows"
                elif name == "search_knowledge":
                    out = await asyncio.to_thread(search_knowledge, w, s, str(args.get("query") or question),
                                                  args.get("doc_type"), args.get("tier"))
                    knowledge.append(out)
                    summary = "; ".join(f"{p['doc_title']} / {p['section']}" for p in out["passages"])[:200]
                elif name == "ask_genie":
                    out = await asyncio.to_thread(ask_genie, w, s, str(args.get("question", question)))
                    summary = (out.get("text") or "")[:200]
                else:
                    raise ValueError(f"unknown tool {name}")
                status = "ok" if out["rows"] or out.get("text") else "empty"
            except Exception as exc:  # noqa: BLE001 - a failed tool is reported, never raised
                out, status, summary = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}, "error", str(exc)[:200]
            if span is not None:
                span.set_outputs(out)
        events.append({"type": "tool_done", "data": {
            "step": step, "tool": name, "status": status, "duration_ms": int((time.monotonic() - started) * 1000),
            "sql": out.get("sql"), "columns": out.get("columns", []), "rows": out.get("rows", []), "summary": summary},
            "offset_ms": ms()})
        return out

    events: list[dict] = []
    for _ in range(6):  # model turns
        resp = await asyncio.to_thread(
            llm.chat.completions.create, model=s.gateway_model, messages=messages, tools=TOOLS,
            extra_body={"reasoning_effort": "none"})
        m = resp.choices[0].message
        if not m.tool_calls:
            reply = m.content or ""
            break
        messages.append({"role": "assistant", "content": m.content or "", "tool_calls": [
            {"id": c.id, "type": "function", "function": {"name": c.function.name, "arguments": c.function.arguments}}
            for c in m.tool_calls]})
        for c in m.tool_calls:
            try:
                args = json.loads(c.function.arguments or "{}")
            except ValueError:
                args = {}
            out = await call_tool(c.function.name, args)
            messages.append({"role": "tool", "tool_call_id": c.id, "content": json.dumps(out, default=str)[:12000]})
            while events:
                yield events.pop(0)

    if drivers_result is None:  # the card always rests on the decomposition tool
        await call_tool("cancel_drivers", {"day": s.day_after, "tier": tier})
        while events:
            yield events.pop(0)
        reply = None
    frame = _frame(question, s.day_after, tier, drivers_result)
    if reply is None:
        messages.append({"role": "user", "content": "Now reply with the JSON object."})
        resp = await asyncio.to_thread(llm.chat.completions.create, model=s.gateway_model, messages=messages,
                                       extra_body={"reasoning_effort": "none"})
        reply = resp.choices[0].message.content or ""
    try:
        written = json.loads(re.sub(r"^```(?:json)?|```$", "", reply.strip()).strip())
    except ValueError:
        written = {"headline": "", "narrative": reply.strip()}
    headline = str(written.get("headline") or "").strip()
    narrative = str(written.get("narrative") or "").strip()
    if frame["declined"]:
        headline = headline or f"I can't give a confident {tier} cancellation number yet"
        narrative = narrative or frame["declined_reason"]
    citations = [] if frame["declined"] else citations_of(knowledge)
    yield {"type": "answer", "data": {**frame, "headline": headline, "narrative": narrative, "citations": citations,
                                      "model": s.gateway_model},
           "offset_ms": ms()}
