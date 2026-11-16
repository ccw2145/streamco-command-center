"""Save Subscribers: POST /api/save/approve and POST /api/save/regenerate/{sample_id}.

Approve writes one row to the app's Lakebase table `approvals`; if that fails the approval is kept in memory (and
says so). GET /api/save/serving/{sample_id} reads one subscriber's score and message from the Lakebase synced tables.
Regenerate makes one live gateway call with the same prompt and rules as notebooks/05_personalize, checks the
output, and falls back to the recorded message.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re

from fastapi import APIRouter, HTTPException, Request

from .. import databricks as dbx
from .. import lakebase
from ..config import APP_DIR, get_settings
from ..fallback import LiveUnavailable, call, live_allowed, recorded

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/save", tags=["save"])

PROMPT_PATH = APP_DIR / "backend" / "prompts" / "personalized_message.md"
APPROVAL: dict | None = None  # the approval on screen; cleared by R (POST /api/reset)

# Lakebase. The app's service principal creates and owns its schema (identifiers are validated in config).
def _app_sql() -> tuple[str, str]:
    schema = get_settings().lakebase_app_schema
    create = (f"CREATE SCHEMA IF NOT EXISTS {schema}; "
              f"CREATE TABLE IF NOT EXISTS {schema}.approvals (approval_id BIGSERIAL PRIMARY KEY, approved_by TEXT NOT NULL, "
              "approved_at TIMESTAMPTZ NOT NULL DEFAULT now(), plan_snapshot JSONB NOT NULL)")
    insert = (f"INSERT INTO {schema}.approvals (approved_by, plan_snapshot) VALUES (%(by)s, %(snapshot)s) "
              "RETURNING approval_id, approved_at")
    return create, insert


def _serving_sql() -> str:
    schema = get_settings().lakebase_serving_schema
    return ("SELECT s.subscriber_id, s.churn_prob, s.risk_decile, s.top_drivers, s.card_id, s.model_version, "
            "m.subject, m.watch_next FROM " + schema + ".churn_scores_serving s "
            "LEFT JOIN " + schema + ".personalized_messages_serving m USING (subscriber_id) "
            "WHERE s.subscriber_id = %(id)s")


def approver(request: Request) -> str:
    return (request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-preferred-username")
            or "presenter")


@router.post("/approve")
async def approve(request: Request) -> dict:
    global APPROVAL
    if APPROVAL:
        return APPROVAL                      # idempotent: a double click doesn't write twice
    save = recorded("screens", "save")
    snapshot = json.dumps({"headline": save["headline"], "cards": save["cards"]})
    by = approver(request)
    stored, note, approval_id = "memory", "Kept in this session; Lakebase wasn't reachable.", None
    if live_allowed(request):
        try:
            create, insert = _app_sql()
            await asyncio.to_thread(lakebase.query, create)     # a no-op once the table exists
            row = (await asyncio.to_thread(lakebase.query, insert, {"by": by, "snapshot": snapshot}))[0]
            stored, note, approval_id = "lakebase", None, row["approval_id"]
        except Exception as exc:  # noqa: BLE001 - any Lakebase failure keeps the approval in memory
            log.warning("approval kept in memory: %s", exc)
    APPROVAL = {"approved_by": by, "approved_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                "stored": stored, "note": note, "recorded": stored != "lakebase", "approval_id": approval_id}
    return APPROVAL


@router.get("/serving/{sample_id}")
async def serving(sample_id: str, request: Request) -> dict:
    """One subscriber's churn score and message, read from the Lakebase synced tables (the operational copy)."""
    sample = next((x for x in recorded("screens", "save")["samples"] if x["sample_id"] == sample_id), None)
    if sample is None or not sample.get("subscriber_id"):
        raise HTTPException(404, "unknown sample")
    if not live_allowed(request):
        return {"source": "recorded"}
    try:
        rows, ms = await asyncio.to_thread(lakebase.timed_query, _serving_sql(), {"id": sample["subscriber_id"]})
    except Exception as exc:  # noqa: BLE001 - the card hides the line when Lakebase isn't reachable
        log.warning("Lakebase lookup failed: %s", exc)
        return {"source": "unavailable"}
    if not rows:
        return {"source": "lakebase", "found": False}
    r = rows[0]
    return {"source": "lakebase", "found": True, "ms": round(ms, 1),
            "table": f"{get_settings().lakebase_serving_schema}.churn_scores_serving",
            "churn_prob": float(r["churn_prob"]), "risk_decile": r["risk_decile"], "card_id": r["card_id"],
            "top_drivers": list(r["top_drivers"] or []), "model_version": r["model_version"],
            "has_message": r["subject"] is not None}


def reset() -> None:
    global APPROVAL
    APPROVAL = None


# ---------------------------------------------------------------- personalized message
FIELDS = ("subject", "body", "watch_next", "agent_note")
MAX_WORDS = {"subject": 12, "body": 80, "agent_note": 55}
MAX_CHARS = {"subject": 120, "body": 900, "watch_next": 120, "agent_note": 700}
_NUM = re.compile(r"\d[\d,]*(?:\.\d+)?")
_RISK = re.compile(r"\b(churn\w*|cancel\w*|risk\w*|scores?|model|likely to leave|propensity)\b", re.I)
_OFFER_WORDS = ("discount", "credit", "refund", "free month", "coupon", "promo code", "% off", "waive")
_CHILDREN = re.compile(r"\b(kids?|children|child|son|daughter)\b", re.I)


class CheckFailed(ValueError):
    pass


def _nums(text: str) -> set[str]:
    return {n.replace(",", "").rstrip("0").rstrip(".") or "0" for n in _NUM.findall(text)}


def variables(sample: dict, card: dict) -> dict:
    """The prompt input: the subscriber's own facts and what their plan card allows. No ids, no churn score."""
    sub = {k: sample[k] for k in ("first_name", "metro_name", "tier", "device", "tenure_months", "what_happened",
                                  "fav_genre", "recent_titles")}
    sub["watch_next"] = sample["message"]["watch_next"]
    return {"subscriber": sub, "card": {"card_id": card["card_id"], "title": card["title"], "offer": card["offer"],
                                        "policy_passage": card["policy"]}}


def check_message(msg: dict, v: dict) -> dict:
    """The prompt's rules, enforced. Raises CheckFailed with the first broken rule."""
    if not isinstance(msg, dict):
        raise CheckFailed("not a JSON object")
    out = {}
    for k in FIELDS:
        val = msg.get(k)
        if not isinstance(val, str) or not val.strip():
            raise CheckFailed(f"missing {k}")
        out[k] = val.strip()
        if len(out[k]) > MAX_CHARS[k] or len(out[k].split()) > MAX_WORDS.get(k, 99):
            raise CheckFailed(f"{k} too long")
    sub, card = v["subscriber"], v["card"]
    if not out["body"].startswith(f"Hi {sub['first_name']},"):
        raise CheckFailed("body must start with 'Hi <first_name>,'")
    if out["watch_next"] != sub["watch_next"]:
        raise CheckFailed("watch_next must be the subscriber's title")
    customer = f"{out['subject']} {out['body']}"
    invented = _nums(f"{customer} {out['agent_note']}") - _nums(json.dumps(v))
    if invented:
        raise CheckFailed(f"numbers not in the input: {sorted(invented)}")
    if m := _RISK.search(f"{customer} {out['agent_note']}"):
        raise CheckFailed(f"mentions '{m.group(0)}'")
    offer = card["offer"].lower()
    for word in _OFFER_WORDS:
        if word in customer.lower() and word not in offer:
            raise CheckFailed(f"offers '{word}', which the card doesn't include")
    if "$" in customer and "$" not in offer:
        raise CheckFailed("mentions a price")
    if m := _CHILDREN.search(customer):
        raise CheckFailed(f"mentions '{m.group(0)}'")
    return out


def system_prompt() -> str:
    return re.sub(r"^<!--.*?-->\s*", "", PROMPT_PATH.read_text(), flags=re.DOTALL)


@router.post("/regenerate/{sample_id}")
async def regenerate(sample_id: str, request: Request) -> dict:
    save = recorded("screens", "save")
    sample = next((x for x in save["samples"] if x["sample_id"] == sample_id), None)
    if sample is None:
        raise HTTPException(404, f"There is no sample subscriber '{sample_id}'.")
    card = next(c for c in save["cards"] if c["card_id"] == sample["card_id"])
    note = "Live AI is off (recorded mode), so this is the recorded message."
    if live_allowed(request):
        v = variables(sample, card)
        s = get_settings()
        for _ in range(2):                    # one retry if the first answer breaks a rule
            try:
                raw = await call("llm", dbx.llm_json, [{"role": "system", "content": system_prompt()},
                                                       {"role": "user", "content": json.dumps(v)}],
                                 timeout_s=s.llm_timeout_s)
                return {"sample_id": sample_id, "message": {**check_message(raw, v), "model": s.gateway_model},
                        "recorded": False, "note": None}
            except CheckFailed as exc:
                log.info("regenerated message for %s failed a check: %s", sample_id, exc)
                note = "The new message didn't pass its checks, so this is the recorded one."
            except LiveUnavailable:
                note = "The AI didn't answer in time, so this is the recorded message."
                break
    return {"sample_id": sample_id, "message": sample["message"], "recorded": True, "note": note}
