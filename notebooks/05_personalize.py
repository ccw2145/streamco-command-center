# Databricks notebook source
# StreamCo Command Center 05: personalized save messages with batch ai_query through Unity Gateway.
# Input per at-risk subscriber with a save card: what happened (from the scoring features), what they watch
# (dim_subscriber_viewing), the card's offer and the card's Save-offer policy passage, retrieved from the AI Search
# index knowledge_idx by 07_knowledge (knowledge_citations). No PII leaves the table: the name is the placeholder
# "{first_name}", filled at send time; no email, phone, address or ids go into the prompt. Kids-only accounts
# never reach this step (the scoring view excludes them, and the input inner-joins the kids-filtered v_subscribers).
# Every output is checked (fields, length, offer, numbers, no risk talk, no children); a row that fails gets the
# card's deterministic template. The three sample subscribers are generated the same way with their first name.
# Writes personalized_messages and personalization_run. With limit > 0 it writes personalized_messages_preview only,
# so a trial run never replaces the full table.

# COMMAND ----------

import datetime as dt
import json
import re

from pyspark.sql import functions as F

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "src_schema": "streamco",
             "gateway_model": "system.ai.gpt-5-6-sol", "limit": "0"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
CATALOG, SCHEMA, SRC, MODEL = (dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema", "gateway_model"))  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", x) for x in (CATALOG, SCHEMA, SRC)), "invalid identifier"
assert re.fullmatch(r"[A-Za-z0-9_.\-]+", MODEL), "invalid gateway model"
LIMIT = int(dbutils.widgets.get("limit") or 0)  # noqa: F821 - 0 = every at-risk subscriber
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"  # noqa: E731
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
OUT = fq("personalized_messages_preview" if LIMIT else "personalized_messages")
PROMPT = open("../prompts/personalized_message.md").read().strip()
SCHEMA_JSON = json.dumps({"type": "json_schema", "json_schema": {"name": "personalized_message", "strict": True, "schema": {
    "type": "object", "additionalProperties": False, "required": ["subject", "body", "watch_next", "agent_note"],
    "properties": {k: {"type": "string"} for k in ("subject", "body", "watch_next", "agent_note")}}}})
sql_str = lambda s: "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"  # noqa: E731

# COMMAND ----------
# Inputs: at-risk subscribers with a card + the three samples (with first names).

plan = {r.card_id: r for r in spark.table(fq("save_plan")).collect()}  # noqa: F821
policy = {r.card_id: r for r in spark.table(fq("knowledge_citations")).collect()}  # noqa: F821 - from 07_knowledge
missing = sorted(set(plan) - set(policy))
assert not missing, f"no retrieved policy passage for cards {missing}: run 07_knowledge first"
for c, r in policy.items():
    print(f"{c:<11} {r.doc_title} / {r.passage} (score {r.score:.3f})")
cards_sql = " ".join(f"WHEN {sql_str(c)} THEN named_struct('card_id', {sql_str(c)}, 'title', {sql_str(r.title)}, "
                     f"'offer', {sql_str(r.offer)}, 'policy_doc', {sql_str(policy[c].doc_title)}, "
                     f"'policy_passage', {sql_str(policy[c].passage)})" for c, r in plan.items())
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW pm_input AS
WITH base AS (
  SELECT s.subscriber_id, s.card_id, coalesce(x.first_name, '{{first_name}}') AS first_name, x.first_name IS NOT NULL AS is_sample,
         named_struct(
           'first_name', coalesce(x.first_name, '{{first_name}}'),
           'tier', f.tier,
           'tenure_months', CAST(floor(f.tenure_days / 30) AS INT),
           'what_happened', coalesce(x.what_happened, CASE s.card_id
              WHEN 'playback' THEN concat(CAST(round(f.max_session_rebuffer_min) AS INT), ' minutes of buffering in one stream during the Big Game')
              WHEN 'payment' THEN 'a payment declined by a processor timeout during the Big Game and never retried'
              ELSE 'signed up during the Big Game and has watched little since' END),
           'fav_genre', v.fav_genre, 'recent_titles', v.recent_titles, 'watch_next', v.watch_next) AS subscriber,
         CASE s.card_id {cards_sql} END AS card
  FROM {fq('churn_scores')} s
  JOIN {fq('v_scoring_features')} f USING (subscriber_id)
  JOIN {fq('v_subscribers')} k USING (subscriber_id)
  JOIN {src('dim_subscriber_viewing')} v USING (subscriber_id)
  LEFT JOIN {src('ref_sample_subscribers')} x USING (subscriber_id)
  WHERE s.risk_decile = 10 AND s.card_id IS NOT NULL
)
SELECT * FROM base ORDER BY is_sample DESC, subscriber_id {f'LIMIT {LIMIT}' if LIMIT else ''}
""")  # noqa: F821
n_in = spark.sql("SELECT count(*) AS n, count_if(is_sample) AS s FROM pm_input").first()  # noqa: F821
print(f"inputs: {n_in.n:,} (samples {n_in.s})")

# COMMAND ----------
# Batch ai_query (failOnError => false: errors come back per row).

started = dt.datetime.now(dt.timezone.utc)
spark.sql(f"""
CREATE OR REPLACE TABLE {fq('tmp_personalized_raw')} AS
SELECT subscriber_id, card_id, is_sample, to_json(named_struct('subscriber', subscriber, 'card', card)) AS variables,
       ai_query('{MODEL}', concat({sql_str(PROMPT + chr(10) + chr(10) + 'Input: ')},
                to_json(named_struct('subscriber', subscriber, 'card', card))),
                responseFormat => {sql_str(SCHEMA_JSON)}, failOnError => false) AS r
FROM pm_input""")  # noqa: F821
elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
print(f"ai_query: {n_in.n:,} rows in {elapsed / 60:.1f} min")

# COMMAND ----------
# Guards, row by row. Failures get the card's template.

NUM = re.compile(r"\d[\d,]*(?:\.\d+)?")
RISK = re.compile(r"\b(churn\w*|cancel\w*|risk\w*|scores?|model|propensity)\b", re.I)
CHILD = re.compile(r"\b(kids?|children|child|son|daughter)\b", re.I)
OFFER_WORDS = ("discount", "refund", "free month", "coupon", "promo code", "% off", "waive", "credit")
MAX_WORDS = {"subject": 12, "body": 80, "agent_note": 55, "watch_next": 20}
TEMPLATE = {
    "payment": ("We fixed a payment problem on our side",
                "Hi {first_name}, a payment problem on our side stopped your renewal during the Big Game. {offer}. {watch} is ready when you are."),
    "playback": ("Sorry your Big Game kept buffering",
                 "Hi {first_name}, your stream buffered during the Big Game, and that's on us. {offer}. {watch} is ready when you are."),
    "onboarding": ("Your season starts here",
                   "Hi {first_name}, thanks for joining us for the Big Game. {offer}. {watch} is ready when you are."),
}


def nums(text: str) -> set:
    return {n.replace(",", "") for n in NUM.findall(text)}


def check(msg: dict, v: dict) -> dict:
    sub, card = v["subscriber"], v["card"]
    out = {k: str(msg.get(k) or "").strip() for k in ("subject", "body", "watch_next", "agent_note")}
    for k, s in out.items():
        if not s or len(s.split()) > MAX_WORDS[k]:
            raise ValueError(f"{k} missing or too long")
    if not out["body"].startswith(f"Hi {sub['first_name']},"):
        raise ValueError("greeting")
    if out["watch_next"] != sub["watch_next"]:
        raise ValueError("watch_next")
    text = " ".join(out.values())
    if nums(text) - nums(json.dumps(v)):
        raise ValueError("invented number")
    if RISK.search(text) or CHILD.search(out["subject"] + " " + out["body"]):
        raise ValueError("risk or children talk")
    customer = (out["subject"] + " " + out["body"]).lower()
    if any(w in customer and w not in card["offer"].lower() for w in OFFER_WORDS) or "$" in customer:
        raise ValueError("offer outside the card")
    return out


def template(v: dict) -> dict:
    sub, card = v["subscriber"], v["card"]
    subject, body = TEMPLATE[card["card_id"]]
    return {"subject": subject, "body": body.format(first_name=sub["first_name"], offer=card["offer"], watch=sub["watch_next"]),
            "watch_next": sub["watch_next"],
            "agent_note": f"{sub['what_happened'][0].upper()}{sub['what_happened'][1:]}. Offer: {card['offer']}. Suggest {sub['watch_next']}."}


raw = spark.table(fq("tmp_personalized_raw")).selectExpr(  # noqa: F821
    "subscriber_id", "card_id", "is_sample", "variables", "r.result AS result", "r.errorMessage AS error").toPandas()
rows, fails = [], {}
now = dt.datetime.now(dt.timezone.utc)
for r in raw.itertuples():
    v = json.loads(r.variables)
    try:
        if r.error:
            raise ValueError("ai_query error")
        msg, model = check(json.loads(r.result), v), MODEL
    except (ValueError, TypeError, KeyError) as exc:
        fails[str(exc)] = fails.get(str(exc), 0) + 1
        msg, model = template(v), "template"
    rows.append((int(r.subscriber_id), r.card_id, msg["subject"], msg["body"], msg["watch_next"], msg["agent_note"], model, now))
spark.createDataFrame(rows, "subscriber_id BIGINT, card_id STRING, subject STRING, body STRING, watch_next STRING, "  # noqa: F821
                      "agent_note STRING, model STRING, generated_at TIMESTAMP").write.mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable(OUT)
spark.sql(f"COMMENT ON TABLE {OUT} IS 'One save message per at-risk subscriber with a card "  # noqa: F821
          "(batch ai_query through Unity Gateway; each prompt carries the Save-offer policy passage retrieved from "
          "knowledge_idx; kids-only accounts excluded; no PII sent: {first_name} is filled at "
          "send time). model = template when the generated text failed a guard. Sample subscribers carry their first name.'")

# Cost: tokens estimated from characters (~4 per token) at ref_model_prices; system.ai_gateway.usage is authoritative.
price = spark.table(src("ref_model_prices")).where(F.col("model") == MODEL).first()  # noqa: F821
in_tok = (raw.variables.str.len().sum() + len(PROMPT) * len(raw)) / 4
out_tok = raw.result.fillna("").str.len().sum() / 4
cost = round(in_tok / 1000 * price.usd_per_1k_in + out_tok / 1000 * price.usd_per_1k_out, 2) if price else None
if not LIMIT:  # a preview run doesn't replace the record of the full run
    spark.createDataFrame([(len(rows), cost, MODEL, now)], "n_messages INT, cost_usd DOUBLE, model STRING, run_at TIMESTAMP")\
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("personalization_run"))  # noqa: F821
spark.sql(f"DROP TABLE IF EXISTS {fq('tmp_personalized_raw')}")  # noqa: F821

# COMMAND ----------

n_llm = sum(1 for r in rows if r[6] == MODEL)
summary = {"table": OUT, "n_messages": len(rows), "from_model": n_llm,
           "policy": {c: f"{r.doc_title}: {r.passage}" for c, r in policy.items()}, "template": len(rows) - n_llm, "fail_reasons": fails,
           "cost_usd_estimated": cost, "minutes": round(elapsed / 60, 1)}
print(json.dumps(summary, indent=1))
display(spark.sql(f"""SELECT x.first_name, m.* FROM {OUT} m
                      JOIN {src('ref_sample_subscribers')} x USING (subscriber_id)"""))  # noqa: F821
kids = spark.sql(f"""SELECT count(*) AS n FROM {OUT} m JOIN {src('dim_subscribers')} d
                     USING (subscriber_id) WHERE d.is_kids_only_account""").first().n  # noqa: F821
assert kids == 0, "a kids-only account received a message"
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821
