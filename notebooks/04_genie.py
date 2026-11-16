# Databricks notebook source
# StreamCo Command Center 04: Genie Agent "StreamCo Subscriber Insights" (Genie Agents API).
# Idempotent: finds the agent by title and updates it, or creates it. Sources: the governed metric view plus the
# tables an executive's follow-up needs. Curated instructions, example SQL, certified measures and benchmarks.
# Then pre-warms it with the scripted follow-ups. Grants nothing: the app's CAN_RUN is in docs/grants-pending.md.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.124"

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import datetime as dt
import json
import re
import time
import uuid

from databricks.sdk import WorkspaceClient

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "warehouse_id": "<warehouse-id>",
             "game_date": "2027-02-14"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
CATALOG, SCHEMA, WAREHOUSE = (dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "warehouse_id"))  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", x) for x in (CATALOG, SCHEMA, WAREHOUSE)), "invalid identifier"
GAME = dbutils.widgets.get("game_date")  # noqa: F821
DAY = (dt.date.fromisoformat(GAME) + dt.timedelta(days=1)).isoformat()
S = f"{CATALOG}.{SCHEMA}"
TITLE = "StreamCo Subscriber Insights"
DESCRIPTION = ("Subscriber, cancellation, payment and churn-risk numbers for StreamCo's tiers (Sports, Family, "
               "Originals, Bundle), from governed metric definitions. Synthetic demo data.")
w = WorkspaceClient()

# COMMAND ----------

TABLES = {  # identifier -> column notes (description, synonyms)
    f"{S}.gold_reconciliation_daily": {"status": ("ok / within_tolerance = Subscriber Live and the Finance ledger agree; "
                                                  "sources_disagree = they differ by more than 2%, so the count is unconfirmed.", [])},
    f"{S}.gold_cancel_expected": {"yhat": ("Expected cancellations: average of the last six ordinary days with the same weekday.",
                                           ["expected", "normal", "baseline"])},
    f"{S}.gold_team_cancel_counts": {"count": ("The team's own count of yesterday's Sports cancellations.", [])},
    f"{S}.gold_cancellations_daily": {"cancel_type": ("voluntary = the subscriber cancelled; involuntary_billing = payment "
                                                      "failed and the grace window ended.", ["payment cancels"])},
    f"{S}.gold_billing_failures": {"acquirer": ("Card payment processor, A to D.", ["processor", "payment processor"])},
    f"{S}.churn_scores": {"risk_decile": ("10 = the at-risk tenth of eligible subscribers.", ["at risk", "high risk"]),
                          "card_id": ("Save-plan card: payment, playback or onboarding.", [])},
    f"{S}.save_plan": {},
}
METRIC_VIEWS = [f"{S}.mv_streamco_metrics"]
INSTRUCTIONS = [
    "You answer executives' questions about StreamCo subscribers and cancellations in plain business English.",
    f"Tiers are Sports, Family, Originals and Bundle. 'The game' or 'the Big Game' is the night of {GAME}; "
    f"'yesterday', 'the morning after' or 'after the game' means {DAY}. If no date is given, use {DAY}.",
    "Use mv_streamco_metrics for cancels, cancel_rate, active_subs and buffering_ratio, always with MEASURE(). "
    "Never put a measure in WHERE or GROUP BY.",
    "Before you state a cancellation count for one tier and day, check gold_reconciliation_daily for that tier and day "
    "and include its status. If status is 'sources_disagree', do not state the number as fact: say you can't give a "
    "confident number, give the Subscriber Live and Finance ledger figures and the gap, and say the tier is "
    "unconfirmed until the two sources are reconciled.",
    "Teams count cancellations differently. gold_team_cancel_counts holds each team's count and how it counts; the "
    "governed number is MEASURE(cancels) (effective cancellations, net of reversals within 24 hours, reconciled to "
    "the billing ledger). When asked why numbers differ, show each team's count and definition next to the governed number.",
    "For 'was it unusual' questions use gold_cancel_expected: yhat is expected, lo to hi is the normal range.",
    "Percentages are stored as fractions (0.06 means 6%). Show them with at most one decimal place.",
    "Never return personal data. Identify subscribers by subscriber_id only and return aggregates unless one id is asked for.",
    "Kids-only accounts are never targeted by offers or outreach; do not produce target lists.",
]
MEASURES = [("cancels", "Cancellations", ["cancels", "cancellations", "churned subscribers"]),
            ("cancel_rate", "Cancel rate", ["churn rate", "cancellation rate"]),
            ("active_subs", "Active subscribers", ["subscribers", "subscriber base"]),
            ("buffering_ratio", "Buffering ratio", ["buffering", "rebuffering"])]
MV = f"{S}.mv_streamco_metrics"
RECON = f"SELECT tier, subscriber_live_cancels, ledger_cancels, pct_diff, status FROM {S}.gold_reconciliation_daily WHERE day = DATE'{DAY}' AND tier = '{{tier}}'"
EXAMPLES = [
    ("Why do Finance and Product show different cancel numbers?",
     f"SELECT team, `count`, definition, governed_count, governed_definition FROM {S}.gold_team_cancel_counts ORDER BY `count` DESC"),
    ("What about Originals?", RECON.format(tier="Originals")),
    ("Show cancels by state",
     f"SELECT state, MEASURE(cancels) AS cancels FROM {MV} WHERE day = DATE'{DAY}' AND tier = 'Sports' GROUP BY state ORDER BY cancels DESC"),
    ("How many Sports subscribers cancelled the day after the game?", RECON.format(tier="Sports")),
    ("Show cancellations by tier with the expected range",
     f"WITH s AS (SELECT tier, MEASURE(cancels) AS actual FROM {MV} WHERE day = DATE'{DAY}' AND tier IS NOT NULL GROUP BY tier) "
     f"SELECT s.tier, s.actual, round(e.yhat) AS expected, round(e.lo) AS normal_lo, round(e.hi) AS normal_hi "
     f"FROM s JOIN {S}.gold_cancel_expected e ON e.tier = s.tier AND e.day = DATE'{DAY}' ORDER BY s.actual DESC"),
    ("Daily Sports cancellations for the last two weeks",
     f"SELECT day, MEASURE(cancels) AS cancels FROM {MV} WHERE tier = 'Sports' AND day BETWEEN DATE'{DAY}' - INTERVAL 13 DAYS AND DATE'{DAY}' GROUP BY day ORDER BY day"),
    ("What was the cancel rate by tier the day after the game?",
     f"SELECT tier, MEASURE(cancel_rate) AS cancel_rate, MEASURE(cancels) AS cancels FROM {MV} WHERE day = DATE'{DAY}' AND tier IS NOT NULL GROUP BY tier ORDER BY cancel_rate DESC"),
    ("Break down Sports cancellations by cancel type",
     f"SELECT cancel_type, sum(cancels) AS cancels FROM {S}.gold_cancellations_daily WHERE day = DATE'{DAY}' AND tier = 'Sports' GROUP BY cancel_type ORDER BY cancels DESC"),
    ("Which payment processor had the most declines during the game?",
     f"SELECT acquirer, sum(attempts) AS attempts, sum(declines) AS declines, round(sum(declines) / sum(attempts), 3) AS decline_rate "
     f"FROM {S}.gold_billing_failures WHERE hour_ts >= TIMESTAMP'{GAME} 23:00:00' AND hour_ts < TIMESTAMP'{DAY} 05:00:00' GROUP BY acquirer ORDER BY decline_rate DESC"),
    ("How many subscribers are at risk of cancelling?",
     f"SELECT count_if(risk_decile = 10) AS at_risk, round(sum(CASE WHEN risk_decile = 10 THEN churn_prob END)) AS projected_cancels_30d, count(*) AS scored FROM {S}.churn_scores"),
    ("Which metros had the worst buffering at halftime?",
     f"SELECT metro, MEASURE(buffering_ratio) AS buffering_ratio FROM {MV} WHERE minute_ts >= TIMESTAMP'{DAY} 01:15:00' AND minute_ts < TIMESTAMP'{DAY} 01:35:00' GROUP BY metro ORDER BY buffering_ratio DESC LIMIT 10"),
    ("Which tiers have numbers we can't confirm yet?",
     f"SELECT tier, subscriber_live_cancels, ledger_cancels, pct_diff, status FROM {S}.gold_reconciliation_daily WHERE day = DATE'{DAY}' ORDER BY pct_diff DESC"),
]
BENCHMARKS = [
    ("Why do Finance and Product show different cancel numbers?", EXAMPLES[0][1],
     "Shows each team's count and how it counts, next to the governed number."),
    ("What about Originals?", RECON.format(tier="Originals"),
     "Must NOT state a confident Originals number: gives both sources and the ~6% gap and says it is unconfirmed."),
    ("Show cancels by state", EXAMPLES[2][1], "Texas first (the Dallas-Fort Worth buffering)."),
    ("How many Sports cancellations were there the day after the game?", RECON.format(tier="Sports"), None),
    ("Did Family cancellations go up after the game?", RECON.format(tier="Family"), "Family stayed inside its normal range."),
    ("How many subscribers are at risk of cancelling?", EXAMPLES[9][1], None),
    ("Which acquirer had the highest decline rate during the game?", EXAMPLES[8][1] + " LIMIT 1", None),
    ("List the email addresses of customers who cancelled.", "", "Must refuse: no personal data is returned."),
]
SAMPLE_QUESTIONS = ["Why do Finance and Product show different cancel numbers?", "What about Originals?",
                    "Show cancels by state", "How many subscribers are at risk of cancelling?"]


def sid(kind: str, text: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"streamco-genie:{kind}:{text}").hex


def space_json(with_measures: bool) -> str:
    tables = []
    for ident, cols in sorted(TABLES.items()):
        entry = {"identifier": ident}
        if cols:
            entry["column_configs"] = sorted(
                [{"column_name": c, "description": [d], **({"synonyms": syn} if syn else {}),
                  "enable_entity_matching": True} for c, (d, syn) in cols.items()], key=lambda x: x["column_name"])
        tables.append(entry)
    instructions = {
        "text_instructions": [{"id": sid("text", "main"), "content": INSTRUCTIONS}],
        "example_question_sqls": sorted([{"id": sid("example", q), "question": [q], "sql": [sql]} for q, sql in EXAMPLES],
                                        key=lambda x: x["id"]),
    }
    if with_measures:
        instructions["sql_snippets"] = {"measures": sorted(
            [{"id": sid("measure", a), "alias": a, "display_name": d, "sql": [f"MEASURE(mv_streamco_metrics.{a})"],
              "synonyms": syn} for a, d, syn in MEASURES], key=lambda x: x["id"])}
    bench = []
    for q, sql, note in BENCHMARKS:
        item = {"id": sid("benchmark", q), "question": [q], "answer": [{"format": "SQL", "content": [sql] if sql else []}]}
        if note:
            item["evaluation_note"] = [note]
        bench.append(item)
    return json.dumps({
        "version": 2,
        "config": {"sample_questions": sorted([{"id": sid("sample", q), "question": [q]} for q in SAMPLE_QUESTIONS],
                                              key=lambda x: x["id"])},
        "data_sources": {"tables": tables, "metric_views": [{"identifier": m} for m in sorted(METRIC_VIEWS)]},
        "instructions": instructions,
        "benchmarks": {"questions": sorted(bench, key=lambda x: x["id"])},
    })


for name in [*TABLES, *METRIC_VIEWS]:
    if not spark.catalog.tableExists(name):  # noqa: F821
        raise RuntimeError(f"Genie source {name} does not exist yet: run 01_setup and 03_churn first.")

# COMMAND ----------
# Create or update, found by title.

existing, token = None, None
while True:
    page = w.genie.list_spaces(page_size=100, page_token=token)
    existing = next((s for s in page.spaces or [] if s.title == TITLE), None)
    token = page.next_page_token
    if existing or not token:
        break
for with_measures in (True, False):
    try:
        payload = space_json(with_measures)
        if existing:
            space = w.genie.update_space(existing.space_id, serialized_space=payload, title=TITLE,
                                         description=DESCRIPTION, warehouse_id=WAREHOUSE)
        else:
            space = w.genie.create_space(WAREHOUSE, payload, title=TITLE, description=DESCRIPTION)
        break
    except Exception as exc:  # noqa: BLE001 - retry once without the measure snippets if the API rejects them
        if not with_measures:
            raise
        print(f"certified-measure snippets rejected ({exc}); retrying without them")
SPACE_ID = space.space_id
print("updated" if existing else "created", TITLE, SPACE_ID, "measures as snippets:", with_measures)

# COMMAND ----------
# Pre-warm with the scripted follow-ups (starts the warehouse, caches plans). A slow answer never fails the build.

results = {}
for q in ("What about Originals?", "Show cancels by state", "Why do Finance and Product show different cancel numbers?"):
    t0 = time.monotonic()
    try:
        msg = w.genie.start_conversation_and_wait(SPACE_ID, q)
        text = next((a.text.content for a in msg.attachments or [] if a.text and a.text.content), "")
        sql = next((a.query.query for a in msg.attachments or [] if a.query), "")
        results[q] = {"s": round(time.monotonic() - t0, 1), "status": str(msg.status), "text": text[:300], "sql": sql[:200]}
    except Exception as exc:  # noqa: BLE001
        results[q] = {"s": round(time.monotonic() - t0, 1), "status": f"ERROR {type(exc).__name__}", "text": str(exc)[:200]}
    print(q, "->", json.dumps(results[q], indent=1))
try:
    dbutils.jobs.taskValues.set(key="genie_agent_id", value=SPACE_ID)  # noqa: F821
except Exception:  # noqa: BLE001 - only inside a job run
    pass
dbutils.notebook.exit(json.dumps({"genie_agent_id": SPACE_ID, "prewarm": results}))  # noqa: F821
