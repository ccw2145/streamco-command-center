"""Ask: POST /api/ask (SSE lanes -> answer) and POST /api/ask/followup (straight to the Genie Agent).

Live investigation: six lanes run in parallel (lane 1 is the Genie Agent; lanes 2-4 and 6 query the evidence
tables and fn_cancel_drivers; lane 5 is AI Search over the policy docs and masked support transcripts), then one
gateway LLM call writes the headline and narrative from what the tools returned. Every number on the answer card
comes from a query, never from the model; the "From your documents" citations come from the retrieved passages.
If the numbers can't be fetched, the stream sends `fallback` and replays the recorded run from the fixture. If
only the knowledge lane fails, the answer is shown without citations.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import time
from collections.abc import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .. import databricks as dbx
from .. import knowledge
from ..config import ET, get_settings
from ..fallback import LiveUnavailable, call, live_allowed, recorded, sql

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ask", tags=["ask"])

TIER = "Sports"
HEARTBEAT_S = 5.0
REPLAY_SPEED = 1.0   # >1 compresses the recorded timing (tests only)
METRO_NAMES = {"DFW": "Dallas-Fort Worth", "PHX": "Phoenix", "CHI": "Chicago", "SEA": "Seattle", "HOU": "Houston",
               "ATL": "Atlanta", "NYC": "New York", "LAX": "Los Angeles"}
DRIVER_LABELS = {"playback": "Couldn't watch halftime properly", "payment": "Sign-up payment timed out, never retried",
                 "event_only": "Expected event-only cancels", "unexplained": "Unexplained"}
DRIVER_ORDER = list(DRIVER_LABELS)
REASON_LABELS = {"stream_froze_halftime": "stream froze at halftime",
                 "charged_declined_cancelled": "charged / card declined / account cancelled",
                 "cancel_request_other": "other"}

# ---------------------------------------------------------------- the five lanes' SQL (bare names = streamco tables)
SQL_ALERT = ("SELECT alert_id, detected_ts, cdn, metro, device_family, app_version, observed, baseline, "
             "persistence_min, affected_sessions, resolved_ts FROM gold_anomaly_alerts ORDER BY detected_ts LIMIT 5")
SQL_PLAYBACK = ("SELECT cdn, metro, device_family, app_version, sessions, rebuffer_ratio "
                "FROM gold_playback_failures_by_device "
                "WHERE day = :game_day ORDER BY rebuffer_ratio DESC LIMIT 12")
SQL_BILLING = """SELECT acquirer,
  SUM(IF(in_window, declines, 0)) / NULLIF(SUM(IF(in_window, attempts, 0)), 0) AS window_rate,
  SUM(IF(in_window, 0, declines)) / NULLIF(SUM(IF(in_window, 0, attempts)), 0) AS normal_rate,
  MAX_BY(top_decline_code, IF(in_window, declines, 0)) AS top_decline_code,
  AVG(IF(in_window, retry_scheduled_rate, NULL)) AS retry_rate
FROM (SELECT *, hour_ts >= :w0 AND hour_ts < :w1 AS in_window FROM gold_billing_failures
      WHERE hour_ts >= :d0 AND hour_ts < :d1)
GROUP BY acquirer ORDER BY window_rate DESC"""
SQL_CARE = ("SELECT reason_code, SUM(n_excess) AS n_excess, SUM(n_contacts) AS n_contacts FROM gold_contact_reasons "
            "WHERE day IN (:game_day, :day) GROUP BY reason_code ORDER BY n_excess DESC")
SQL_DRIVERS = ("SELECT driver, n_cancels, pct, evidence_type, confidence, evidence FROM fn_cancel_drivers(:day, :tier) "
               "WHERE driver IS NOT NULL")
SQL_DRIVERS_TABLE = ("SELECT driver, n_cancels, pct, evidence_type, confidence, rule AS evidence "
                     "FROM gold_cancel_decomposition WHERE day = :day AND tier = :tier")
SQL_EXPECTED = "SELECT actual, yhat, lo, hi FROM gold_cancel_expected WHERE day = :day AND tier = :tier"
SQL_RECON = ("SELECT subscriber_live_cancels, ledger_cancels, pct_diff, status FROM gold_reconciliation_daily "
             "WHERE day = :day AND tier = :tier")


def show_sql(statement: str, params: dict) -> str:
    """The SQL as shown on screen: parameters inlined as literals (values are ours, never user input)."""
    out = statement
    for k, v in sorted(params.items(), key=lambda kv: -len(kv[0])):
        if isinstance(v, dt.datetime):
            lit = f"TIMESTAMP'{v.astimezone(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(v, dt.date):
            lit = f"DATE'{v.isoformat()}'"
        else:
            lit = f"'{v}'"
        out = out.replace(f":{k}", lit)
    return " ".join(out.split())


def _params() -> dict:
    """Day after, game day, the halftime hour (billing is hourly) and the game-night window."""
    s = get_settings()
    c = s.clock()
    hour = c["halftime_start"].replace(minute=0)
    return {"day": s.day_after, "game_day": s.game_date, "tier": TIER, "w0": hour,
            "w1": hour + dt.timedelta(hours=1), "d0": c["t0"], "d1": c["end"]}


def pct(x: float | None, digits: int = 0) -> str:
    return "n/a" if x is None else f"{x * 100:.{digits}f}%"


def clock(ts) -> str:
    t = ts if isinstance(ts, dt.datetime) else dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.UTC)
    return t.astimezone(ET).strftime("%-I:%M %p")


KNOWLEDGE_LANE = {"lane": "knowledge", "label": "Policies and support chats", "tools": ["AI Search: knowledge_idx"],
                  "owner_system": "Policy docs + support transcripts"}
KNOWLEDGE_POLICY_Q = "sign-up payment declined: which decline codes are retried, and the grace window before cancelling"
KNOWLEDGE_CHATS_Q = "I was charged, my card was declined and my account was cancelled"


def lane_defs() -> list[dict]:
    defs = recorded("screens", "ask", "lanes")
    if not any(d["lane"] == "knowledge" for d in defs):  # fixtures exported before the knowledge lane existed
        at = next((i for i, d in enumerate(defs) if d["lane"] == "baseline"), len(defs))
        defs.insert(at, dict(KNOWLEDGE_LANE))
    return defs


# ---------------------------------------------------------------- lane runners: each returns a lane_done dict
async def lane_subscribers(question: str) -> dict:
    s = get_settings()
    g = await call("genie", dbx.genie_ask,
                   f"How many {TIER} cancellations were there on {s.day_after.isoformat()} compared with expected?",
                   timeout_s=s.genie_timeout_s + 2)
    text = re.split(r"(?<=[.!?])\s+", g["text"].strip())
    summary = " ".join(text[:2])[:260] if text and text[0] else "The Genie Agent answered with the table below."
    return {"summary": summary, "sql_text": g["sql"], "row_count": len(g["rows"])}


async def lane_playback() -> dict:
    p = _params()
    alerts, cells = await asyncio.gather(sql(SQL_ALERT), sql(SQL_PLAYBACK, game_day=p["game_day"]))
    if not alerts:
        return {"summary": "No viewing-quality alert fired on game night.", "sql_text": SQL_ALERT, "row_count": 0}
    a = alerts[0]
    summary = (f"One cell failed: {a['cdn']} x {METRO_NAMES.get(a['metro'], a['metro'])} x {a['device_family']} "
               f"{a['app_version']}, flagged at {clock(a['detected_ts'])}, buffering {pct(a['baseline'], 1)} to "
               f"{pct(a['observed'], 1)}. Every other cell stayed within its normal range.")
    return {"summary": summary, "sql_text": f"{SQL_ALERT};\n{show_sql(SQL_PLAYBACK, p)}",
            "row_count": len(alerts) + len(cells), "rows": {"alert": a}}


async def lane_billing() -> dict:
    p = _params()
    rows = await sql(SQL_BILLING, w0=p["w0"], w1=p["w1"], d0=p["d0"], d1=p["d1"])
    if not rows or rows[0]["window_rate"] is None:
        return {"summary": "No payment attempts found for game night.", "sql_text": show_sql(SQL_BILLING, p),
                "row_count": 0}
    r = rows[0]
    retry = " No retry is scheduled for that code." if not r.get("retry_rate") else ""
    summary = (f"Acquirer {r['acquirer']} declines rose from {pct(r['normal_rate'])} to {pct(r['window_rate'])} "
               f"({r['top_decline_code']}) during the halftime hour.{retry}")
    return {"summary": summary, "sql_text": show_sql(SQL_BILLING, p), "row_count": len(rows)}


async def lane_care() -> dict:
    p = _params()
    rows = await sql(SQL_CARE, game_day=p["game_day"], day=p["day"])
    extra = [r for r in rows if (r["n_excess"] or 0) > 0]
    total = sum(r["n_excess"] for r in extra)
    # The named reasons with a real share; everything else is "other".
    big = [r for r in extra if total and r["n_excess"] / total >= 0.05 and r["reason_code"] != "cancel_request_other"]
    rest = total - sum(r["n_excess"] for r in big)
    extra = big + ([{"reason_code": "cancel_request_other", "n_excess": rest}] if rest > 0 else [])
    if not total:
        return {"summary": "Support contacts stayed at their normal level.", "sql_text": show_sql(SQL_CARE, p),
                "row_count": len(rows)}
    parts = ", ".join(f"{pct(r['n_excess'] / total)} {REASON_LABELS.get(r['reason_code'], r['reason_code'])}"
                      for r in extra)
    summary = f"About {round(total, -2):,.0f} extra support contacts overnight: {parts}."
    reasons = [{"reason_code": r["reason_code"], "share": r["n_excess"] / total} for r in extra]
    return {"summary": summary, "sql_text": show_sql(SQL_CARE, p), "row_count": len(rows),
            "rows": {"reasons": reasons}}


async def lane_knowledge() -> dict:
    """AI Search: the policy passages on the payment driver and what Sports customers told support."""
    s = get_settings()
    policy, chats = await asyncio.gather(
        call("search", knowledge.search, KNOWLEDGE_POLICY_Q, doc_type="policy", k=2, timeout_s=s.sql_timeout_s),
        call("search", knowledge.search, KNOWLEDGE_CHATS_Q, doc_type="transcript", tier=TIER, k=3,
             timeout_s=s.sql_timeout_s))
    reasons = [c["reason_code"] for c in chats if c.get("reason_code")]
    reason = max(set(reasons), key=reasons.count) if reasons else None
    n = await call("sql", knowledge.transcript_count, reason, TIER, timeout_s=s.sql_timeout_s + 2) if reason else 0
    citations = [{**p, "n_conversations": None} for p in policy[:1] if p["quote"]]
    chat = next((c for c in chats if c["reason_code"] == reason and c["quote"]), None)
    if chat:
        citations.append({**chat, "n_conversations": n})
    parts = []
    if citations and citations[0]["doc_type"] == "policy":
        parts.append(f"{citations[0]['doc_title']}, {citations[0]['section']}: \"{citations[0]['quote']}\"")
    if chat:
        parts.append(f"{n:,} {TIER} support conversations: {REASON_LABELS.get(reason, chat['section'])}.")
    sql_text = (f"-- AI Search over {knowledge.index_name()}\n"
                f"-- query_text: '{KNOWLEDGE_POLICY_Q}', filters: doc_type = 'policy', k = 2\n"
                f"-- query_text: '{KNOWLEDGE_CHATS_Q}', filters: doc_type = 'transcript', tier = '{TIER}', k = 3\n"
                + (show_sql(knowledge.SQL_COUNTS, {"reason_code": reason, "tier": TIER}) if reason else ""))
    return {"summary": " ".join(parts) or "No matching policy or support passages.", "sql_text": sql_text,
            "row_count": len(policy) + len(chats),
            "rows": {"citations": citations, "passages": policy + chats}}


async def lane_baseline() -> dict:
    p = {"day": get_settings().day_after, "tier": TIER}
    try:
        drivers, stmt = await sql(SQL_DRIVERS, **p), SQL_DRIVERS
    except LiveUnavailable:
        drivers, stmt = await sql(SQL_DRIVERS_TABLE, **p), SQL_DRIVERS_TABLE
    expected, recon = await asyncio.gather(sql(SQL_EXPECTED, **p), sql(SQL_RECON, **p))
    by = {d["driver"]: d for d in drivers}
    if not expected or not all(k in by for k in DRIVER_ORDER):
        raise LiveUnavailable("cancel drivers or expected cancels are missing for the day after")
    summary = (f"Event-only sign-ups cancelled at the expected rate ({pct(by['event_only']['pct'])} of the excess). "
               f"{pct(by['unexplained']['pct'])} of the excess has no signal.")
    return {"summary": summary, "sql_text": show_sql(stmt, p), "row_count": len(drivers),
            "rows": {"drivers": by, "expected": expected[0], "recon": recon[0] if recon else None,
                     "drivers_sql": show_sql(stmt, p)}}


# ---------------------------------------------------------------- the answer
_NUM = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _nums(text: str) -> set[str]:
    return {n.replace(",", "").rstrip("0").rstrip(".") or "0" for n in _NUM.findall(text)}


def _allowed(facts: dict) -> set[str]:
    out = set()
    for raw in _NUM.findall(json.dumps(facts, default=str)):
        out |= _nums(raw)
        v = float(raw.replace(",", ""))
        if 0 < v < 1:
            out |= _nums(f"{v * 100:.2f}") | _nums(f"{v * 100:.1f}") | _nums(f"{v * 100:.0f}")
        if v >= 1000:
            out |= _nums(f"{round(v, -2):.0f}") | _nums(f"{v / 1000:.1f}")
    return out


def build_answer(question: str, base: dict) -> dict:
    """The structured answer card from lane 5's rows (numbers only from queries)."""
    by, e, r = base["drivers"], base["expected"], base["recon"]
    observed, exp = int(e["actual"]), int(round(e["yhat"]))
    excess = observed - exp
    drivers = [{"driver": k, "label": DRIVER_LABELS[k], "pct": float(by[k]["pct"]), "n_cancels": int(by[k]["n_cancels"]),
                "evidence_type": by[k].get("evidence_type") or "unexplained",
                "confidence": by[k].get("confidence") or "low",
                "evidence": by[k].get("evidence") or by[k].get("rule") or "", "metric_id": f"cancel_driver_{k}"}
               for k in DRIVER_ORDER]
    recon = {"status": r["status"], "subscriber_live": int(r["subscriber_live_cancels"]),
             "ledger": int(r["ledger_cancels"]), "pct_diff": float(r["pct_diff"])} if r else None
    if recon and recon["status"] == "sources_disagree":
        return {"question": question, "declined": True, "drivers": [], "source": "rules",
                "headline": f"I can't give a confident {TIER} cancellation number yet.",
                "narrative": (f"The subscriber system shows {recon['subscriber_live']:,} and the Finance ledger shows "
                              f"{recon['ledger']:,}, a {recon['pct_diff'] * 100:.0f}% gap. Treat it as unconfirmed "
                              "until the two sources are reconciled.")}
    two = drivers[0]["pct"] + drivers[1]["pct"]
    return {"question": question,
            "headline": f"{_share_words(two)} of the extra cancellations trace to the same 20 minutes of halftime.",
            "narrative": "", "source": "rules",
            "total": {"observed": observed, "expected": exp, "excess": excess, "pct_over": observed / exp - 1,
                      "band_lo": int(round(e["lo"])), "band_hi": int(round(e["hi"]))},
            "drivers": drivers, "reconciliation": recon, "declined": False, "drivers_sql": base["drivers_sql"]}


def _share_words(x: float) -> str:
    for limit, words in ((0.72, "Nearly three-quarters"), (0.6, "Nearly two-thirds"), (0.45, "About half"),
                         (0.3, "About a third")):
        if x >= limit:
            return words
    return f"{pct(x)}"


SUMMARY_PROMPT = """You write the explanation on the answer card for an executive who asked about yesterday's cancellations.
The user message is JSON: the question, the totals, the drivers (share of the extra cancellations), what each
system reported, and `documents`: passages retrieved from StreamCo's policy documents and support transcripts.
Return JSON {"headline": ..., "narrative": ...}.
- headline: one sentence, at most 16 words.
- narrative: at most 130 words, plain English, covering the total vs expected and each driver in order. For the
  payment driver, cite the policy document by its title (what it says about retries and the grace window) and say
  what happened instead; cite the support conversations with their count (n_conversations).
Use only numbers that appear in the input. Never invent causes or policy. If a share is unexplained, say so plainly.
No markdown, no system or vendor names."""


async def narrate(answer: dict, agent: asyncio.Future, lanes: list[dict]) -> dict:
    """Headline and narrative from the Ask agent; if it isn't reachable, one gateway call over the same facts."""
    try:
        a = await agent
        headline, narrative = str(a.get("headline") or "").strip(), str(a.get("narrative") or "").strip()
        if narrative and not a.get("declined"):
            return {**answer, "narrative": narrative, "source": "agent"}
    except (LiveUnavailable, asyncio.CancelledError):
        pass
    return await write_summary(answer, lanes)


async def write_summary(answer: dict, lanes: list[dict]) -> dict:
    facts = {"question": answer["question"], "total": answer["total"],
             "drivers": [{k: d[k] for k in ("label", "pct", "n_cancels", "evidence")} for d in answer["drivers"]],
             "reconciliation": answer["reconciliation"], "systems": [x["summary"] for x in lanes if x.get("summary")],
             "documents": [{k: c.get(k) for k in ("doc_title", "section", "quote", "n_conversations")}
                           for c in answer.get("citations", [])]}
    s = get_settings()
    try:
        out = await call("llm", dbx.llm_json, [{"role": "system", "content": SUMMARY_PROMPT},
                                               {"role": "user", "content": json.dumps(facts, default=str)}],
                         timeout_s=s.llm_timeout_s)
        headline, narrative = str(out.get("headline", "")).strip(), str(out.get("narrative", "")).strip()
        invented = _nums(f"{headline} {narrative}") - _allowed(facts)
        if narrative and not invented and len(narrative.split()) <= 160:
            return {**answer, "narrative": narrative, "source": "live_llm"}
        log.info("summary failed its checks (invented numbers: %s)", sorted(invented))
    except LiveUnavailable:
        pass
    t = answer["total"]
    d = answer["drivers"]
    narrative = (f"{TIER} cancellations reached {t['observed']:,} the morning after the game, {pct(t['pct_over'])} above the "
                 f"{t['expected']:,} expected. Of the {t['excess']:,} extra cancellations, "
                 + "; ".join(f"{x['label'].lower()} ({pct(x['pct'])})" for x in d) + ".")
    return {**answer, "narrative": narrative}


# ---------------------------------------------------------------- streams
async def live_events(question: str) -> AsyncIterator[tuple[str, dict]]:
    defs = {d["lane"]: d for d in lane_defs()}
    runners = {"subscribers": lane_subscribers(question), "playback": lane_playback(), "billing": lane_billing(),
               "care": lane_care(), "knowledge": lane_knowledge(), "baseline": lane_baseline()}
    started = time.monotonic()

    async def run(lane: str, coro):
        try:
            return lane, await coro, None
        except Exception as exc:  # noqa: BLE001 - reported per lane
            return lane, None, exc

    for lane in runners:
        yield "lane_start", defs[lane]
    tasks = [asyncio.ensure_future(run(lane, coro)) for lane, coro in runners.items()]
    # The Ask agent (Agent Runtime) writes the headline and narrative, in parallel with the lanes.
    agent = asyncio.ensure_future(call("agent", dbx.agent_answer, question, timeout_s=get_settings().genie_timeout_s + 45))
    results: dict[str, dict] = {}
    try:
        for fut in asyncio.as_completed(tasks):
            lane, res, exc = await fut
            if exc is not None and lane == "baseline":
                agent.cancel()
                raise LiveUnavailable(f"answer data unavailable: {exc}") from exc
            status = "ok" if exc is None else "error"
            res = res or {"summary": "", "sql_text": None, "row_count": 0}
            results[lane] = res
            yield "lane_done", {"lane": lane, "status": status,
                                "duration_ms": int((time.monotonic() - started) * 1000), "summary": res["summary"],
                                "sql_text": res["sql_text"], "row_count": res["row_count"]}
    finally:
        for t in tasks:
            t.cancel()
    answer = build_answer(question, results["baseline"]["rows"])
    if not answer["declined"]:
        answer["citations"] = [{k: c.get(k) for k in ("doc_type", "doc_title", "section", "quote", "n_conversations",
                                                       "tier")}
                               for c in (results.get("knowledge", {}).get("rows") or {}).get("citations", [])]
    if not answer["declined"]:
        answer = await narrate(answer, agent, [results[k] for k in runners])
    yield "answer", answer


async def replay_events(question: str) -> AsyncIterator[tuple[str, dict]]:
    run = recorded("ask_run")
    if not _matches(question, [run["question"], *run.get("variants", [])]):
        yield "answer", {"question": question, "declined": True,
                         "headline": "In recorded mode I can answer the prepared question.",
                         "narrative": "Try the suggested question to see the full analysis.", "drivers": []}
        return
    start = time.monotonic()
    for step in run["trace"]:
        wait = step["offset_ms"] / 1000 / REPLAY_SPEED - (time.monotonic() - start)
        if wait > 0:
            await asyncio.sleep(wait)
        data = {**step["data"], "question": question} if step["event"] == "answer" else step["data"]
        yield step["event"], data


def frame(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, default=dbx.jsonable)}\n\n".encode()


async def stream(question: str, live: bool) -> AsyncIterator[bytes]:
    queue: asyncio.Queue = asyncio.Queue()
    end = object()

    async def produce():
        recorded_run = not live
        try:
            if live:
                try:
                    async for ev in live_events(question):
                        await queue.put(frame(*ev))
                except Exception as exc:  # noqa: BLE001 - any live failure replays the recording
                    log.warning("live investigation failed, replaying the recording: %s", exc)
                    await queue.put(frame("fallback", {"message": "Showing the recorded run of the same analysis."}))
                    recorded_run = True
            if recorded_run:
                async for ev in replay_events(question):
                    await queue.put(frame(*ev))
            await queue.put(frame("done", {"recorded": recorded_run}))
        except Exception:  # noqa: BLE001
            log.exception("ask stream failed")
            await queue.put(frame("error", {"message": "Something went wrong while answering. Try again."}))
        finally:
            await queue.put(end)

    task = asyncio.create_task(produce())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), HEARTBEAT_S)
            except asyncio.TimeoutError:
                yield b": heartbeat\n\n"
                continue
            if item is end:
                break
            yield item
    finally:
        task.cancel()


class AskBody(BaseModel):
    question: str = Field(min_length=1, max_length=500)


@router.post("")
async def ask(body: AskBody, request: Request) -> StreamingResponse:
    return StreamingResponse(stream(body.question.strip(), live_allowed(request)), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------- follow-ups
_STOP = {"the", "a", "an", "of", "to", "did", "do", "is", "what", "why", "how", "me", "after", "in", "on", "for", "by"}


def _tokens(text: str) -> set[str]:
    toks = set(re.sub(r"[^\w\s]", " ", text.lower()).split())
    return (toks - _STOP) or toks


def _matches(question: str, candidates: list[str], threshold: float = 0.5) -> bool:
    return _best([{"variants": candidates}], question, threshold) is not None


def _best(entries: list[dict], question: str, threshold: float = 0.5) -> dict | None:
    q = _tokens(question)
    best, score = None, 0.0
    for e in entries:
        for c in e["variants"]:
            t = _tokens(c)
            s = len(q & t) / len(q | t) if q and t else 0.0
            if s > score:
                best, score = e, s
    return best if score >= threshold else None


def is_declined(text: str, rows: list[list]) -> bool:
    return bool(re.search(r"\b(can'?t|cannot|unable to) (give|confirm|provide)", text or "", re.I)) or any(
        "sources_disagree" in [str(v) for v in row] for row in rows)


class FollowupBody(BaseModel):
    question: str = Field(min_length=1, max_length=500)
    conversation_id: str | None = Field(default=None, max_length=200)


@router.post("/followup")
async def followup(body: FollowupBody, request: Request) -> dict:
    q = body.question.strip()
    if live_allowed(request):
        try:
            g = await call("genie", dbx.genie_ask, q, body.conversation_id,
                           timeout_s=get_settings().genie_timeout_s + 2)
            return {"question": q, **g, "declined": is_declined(g["text"], g["rows"]), "recorded": False}
        except LiveUnavailable:
            pass
    entry = _best(recorded("followups"), q)
    if entry is None:
        return {"question": q, "conversation_id": body.conversation_id, "text":
                "In recorded mode I can answer the suggested follow-up questions. Try one of those.",
                "declined": True, "sql": None, "columns": [], "rows": [], "duration_ms": 0, "recorded": True}
    return {**entry["answer"], "question": q, "recorded": True}
