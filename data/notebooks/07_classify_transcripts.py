# Databricks notebook source
# OWNER: Stream C (GenAI+Governance). Task classify_transcripts (after gen_transcripts and probe_gateway).
# Batch classification of support transcripts with ai_query structured output -> silver_contact_reasons.
#   1. (Re)create the governance functions (sql/governance/01_functions.sql, idempotent).
#   2. Redact first: the model only ever sees fn_redact_pii(transcript_text).
#   3. If ops.app_config.llm_available_structured: ai_query(gateway_model, ..., responseFormat => json_schema)
#      with failOnError => false; rows that error or return an unknown code fall back to the rules classifier.
#      Otherwise every row uses the rules classifier (source='rules').
#   4. Rules classifier = nearest phrase-bank variant by token overlap (seed/phrase_banks/contact_reasons.yaml),
#      asserted >= 90% against eval_gt_contact_reason (build-plan Stream C wave 1).
# Columns (contracts/tables.md §3): contact_id, reason_code, confidence, summary, n_redactions, source, model,
# classified_ts.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import os
import re
import sys

import yaml

sys.path.insert(0, os.path.abspath("../src"))

from cc_common import config, llm, sqlfile  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
sqlfile.run(spark, os.path.abspath("../sql/governance/01_functions.sql"), cfg)  # noqa: F821

RULES_MIN_ACCURACY = 0.90
TARGET = cfg.fq("silver_contact_reasons")
SOURCE = cfg.fq("fact_support_contacts")

# COMMAND ----------

# Reason codes and phrasings come from the phrase bank Stream A generates transcripts from. Accepted shapes:
#   {reason_code: [phrase, ...]}  or  {reason_code: {label: str, phrases|variants: [phrase, ...]}}
with open(os.path.abspath("../seed/phrase_banks/contact_reasons.yaml")) as fh:
    bank_raw = yaml.safe_load(fh)
bank_raw = bank_raw.get("reasons", bank_raw) if isinstance(bank_raw, dict) else {}
BANK: dict[str, dict] = {}
for code, spec in bank_raw.items():
    phrases = spec if isinstance(spec, list) else (spec.get("phrases") or spec.get("variants") or [])
    label = spec.get("label", code.replace("_", " ")) if isinstance(spec, dict) else code.replace("_", " ")
    BANK[code] = {"label": label, "phrases": [str(p) for p in phrases]}
if not BANK:
    raise RuntimeError("seed/phrase_banks/contact_reasons.yaml has no reasons; Stream A must populate it first.")
CODES = sorted(BANK)
print(f"{len(CODES)} reason codes: {CODES}")

# COMMAND ----------

# Rules classifier: token-overlap nearest phrase (transcripts are generated from these phrases, design deviation 5).
_TOKEN = re.compile(r"[a-z']+")
_STOP = {"the", "a", "an", "and", "or", "to", "of", "my", "i", "it", "is", "was", "me", "at", "in", "on", "for",
         "you", "can", "reach", "card", "ends", "email", "phone", "address", "customer", "agent"}


def _tokens(text: str) -> set[str]:
    return {t for t in _TOKEN.findall((text or "").lower()) if t not in _STOP and len(t) > 2}


PHRASE_TOKENS = [(code, _tokens(p)) for code in CODES for p in BANK[code]["phrases"]]


def rules_classify(text: str) -> tuple[str, float]:
    toks = _tokens(text)
    best, score = CODES[0], 0.0
    for code, ptoks in PHRASE_TOKENS:
        if not ptoks:
            continue
        s = len(toks & ptoks) / len(ptoks)
        if s > score:
            best, score = code, s
    return best, round(score, 3)

# COMMAND ----------

# StreamCo: no Lakebase flags. The deterministic rules classifier is used (the transcripts are generated
# from the phrase bank); set use_llm = True to classify with ai_query instead.
flags = {"llm_available_structured": False}
use_llm = flags["llm_available_structured"]
print("llm flags:", flags, "-> classifier:", "ai_query structured" if use_llm else "rules")

schema = {
    "type": "object",
    "properties": {
        "reason_code": {"type": "string", "enum": CODES},
        "confidence": {"type": "number"},
        "summary": {"type": "string"},
    },
    "required": ["reason_code", "confidence", "summary"],
    "additionalProperties": False,
}
instructions = (
    "Classify this customer-support transcript from a streaming service into exactly one reason code. "
    "Personal data has already been replaced with tokens such as [EMAIL]. Reason codes: "
    + "; ".join(f"{c} = {BANK[c]['label']}" for c in CODES)
    + ". Return reason_code, confidence between 0 and 1, and a summary of at most 20 words with no personal data."
    " Transcript: "
)
redacted = spark.sql(  # noqa: F821
    f"SELECT contact_id, {cfg.fq('fn_redact_pii')}(transcript_text) AS redacted FROM {SOURCE} "
    "WHERE transcript_text IS NOT NULL")
redacted.createOrReplaceTempView("cc_redacted")

if use_llm:
    expr = llm.ai_query_expr(cfg, f"concat({llm.sql_string(instructions)}, redacted)",
                             response_format_json=llm.response_format("contact_reason", schema))
    classified = spark.sql(f"""
        SELECT contact_id, redacted,
               get_json_object(r.result, '$.reason_code') AS reason_code,
               CAST(get_json_object(r.result, '$.confidence') AS DOUBLE) AS confidence,
               get_json_object(r.result, '$.summary') AS summary,
               r.errorMessage AS error
        FROM (SELECT contact_id, redacted, {expr} AS r FROM cc_redacted)""")  # noqa: F821
    classified.write.mode("overwrite").saveAsTable(cfg.fq("tmp_contact_reasons_llm"))
    pdf = spark.table(cfg.fq("tmp_contact_reasons_llm")).toPandas()  # noqa: F821
    ok = pdf["error"].isna() & pdf["reason_code"].isin(CODES)
    pdf["source"] = "live_llm"
    pdf["model"] = cfg.gateway_model
else:
    pdf = redacted.toPandas()
    pdf["reason_code"], pdf["confidence"], pdf["summary"] = None, None, None
    ok = pdf["contact_id"].isna()  # all False

fallback = ~ok
if fallback.any():
    ruled = pdf.loc[fallback, "redacted"].map(rules_classify)
    pdf.loc[fallback, "reason_code"] = ruled.map(lambda x: x[0])
    pdf.loc[fallback, "confidence"] = ruled.map(lambda x: x[1])
    pdf.loc[fallback, "summary"] = pdf.loc[fallback, "reason_code"].map(lambda c: BANK[c]["label"])
    pdf.loc[fallback, "source"] = "rules"
    pdf.loc[fallback, "model"] = "rules:phrase_bank_overlap"
print(f"classified {len(pdf):,} transcripts: {int(ok.sum()):,} by the model, {int(fallback.sum()):,} by rules")

# COMMAND ----------

pdf["n_redactions"] = pdf["redacted"].str.count(r"\[(?:EMAIL|PHONE|CARD|ADDRESS)\]").astype("int32")
out = pdf[["contact_id", "reason_code", "confidence", "summary", "n_redactions", "source", "model"]]
(spark.createDataFrame(out)  # noqa: F821
 .selectExpr("CAST(contact_id AS BIGINT) contact_id", "reason_code", "CAST(confidence AS DOUBLE) confidence",
             "summary", "CAST(n_redactions AS INT) n_redactions", "source", "model",
             "current_timestamp() AS classified_ts")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET))
spark.sql(f"DROP TABLE IF EXISTS {cfg.fq('tmp_contact_reasons_llm')}")  # noqa: F821
spark.sql(f"COMMENT ON TABLE {TARGET} IS 'Contact reason per support transcript, classified from redacted text "  # noqa: F821
          "(ai_query structured output, rules fallback). source = live_llm | rules.'")

# COMMAND ----------

# Validation: row counts, source split, reason mix, and accuracy against the generator's ground truth.
display(spark.sql(f"SELECT source, count(*) n, round(avg(confidence), 3) avg_conf, sum(n_redactions) redactions "  # noqa: F821
                  f"FROM {TARGET} GROUP BY source"))
display(spark.sql(f"SELECT reason_code, count(*) n FROM {TARGET} GROUP BY reason_code ORDER BY n DESC"))  # noqa: F821
acc = spark.sql(f"""
    SELECT s.source, avg(CASE WHEN s.reason_code = g.reason_code THEN 1.0 ELSE 0.0 END) AS accuracy, count(*) AS n
    FROM {TARGET} s JOIN {cfg.fq('eval_gt_contact_reason')} g USING (contact_id) GROUP BY s.source""").collect()  # noqa: F821
for r in acc:
    print(f"accuracy {r['source']}: {r['accuracy']:.3f} on {r['n']:,}")
rules_acc = {r["source"]: r["accuracy"] for r in acc}.get("rules")
n_target = spark.table(TARGET).count()  # noqa: F821
n_source = spark.table(SOURCE).where("transcript_text IS NOT NULL").count()  # noqa: F821
assert n_target == n_source, f"silver_contact_reasons has {n_target} rows, expected {n_source}"
if rules_acc is not None:
    assert rules_acc >= RULES_MIN_ACCURACY, f"rules classifier accuracy {rules_acc:.3f} < {RULES_MIN_ACCURACY}"
