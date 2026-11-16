# Databricks notebook source
# Owner: Stream A (Data + Pipelines). Schema, volumes and the small reference tables every later
# task reads: ref_assumptions, ref_team_budgets, ref_model_prices, ref_thresholds, ref_metric_provenance.
# No grants here (26 owns grants). Idempotent: CREATE ... IF NOT EXISTS and overwrite.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.abspath("../src"))

import yaml  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from cc_common import config, planted as P  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
DAY_AFTER = (date.fromisoformat(cfg.game_date) + timedelta(days=1)).isoformat()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema} "  # noqa: F821
          "COMMENT 'Streaming Command Center demo (synthetic data)'")
for vol, comment in (("raw_landing", "CDN and session JSON landing zone (Auto Loader source)"),
                     ("app_assets", "Frozen app bundle exported by 28_stage_demo")):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.fq(vol)} COMMENT '{comment}'")  # noqa: F821

# COMMAND ----------
# ref_assumptions: keys are exactly planted.ASSUMPTIONS. ARPU and retention honour the job parameters.

NOTES = {
    "arpu_monthly": "Average revenue per subscriber per month (USD), synthetic assumption",
    "retention_months": "Months of revenue counted for a saved subscriber",
    "reprocess_cost_usd": "Processing cost per payment retry (USD)",
    "credit_redemption_rate": "Share of offered one-month credits that are redeemed",
    "nudge_cost_usd": "Cost per onboarding message (USD)",
}
values = dict(P.ASSUMPTIONS, arpu_monthly=cfg.arpu_monthly, retention_months=cfg.retention_months)
assert set(values) == set(P.ASSUMPTIONS) == set(NOTES)
spark.createDataFrame(  # noqa: F821
    [(k, float(v), NOTES[k]) for k, v in values.items()],
    "key STRING, value DOUBLE, note STRING",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq("ref_assumptions"))

spark.createDataFrame(  # noqa: F821
    [(team, budget, alert) for team, (budget, _used, alert) in P.TEAM_BUDGETS.items()],
    "team STRING, monthly_budget_usd DOUBLE, alert_pct DOUBLE",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq("ref_team_budgets"))

# One row for the gateway model. List-price placeholder (USD per 1k tokens) used only for cost tiles.
spark.createDataFrame(  # noqa: F821
    [(cfg.gateway_model, 0.00125, 0.01)], "model STRING, usd_per_1k_in DOUBLE, usd_per_1k_out DOUBLE",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq("ref_model_prices"))

# Thresholds the pipelines read (SQL cannot import planted.py): one row per key.
spark.createDataFrame(  # noqa: F821
    [("recon_disagree_threshold", float(P.RECON_DISAGREE_THRESHOLD),
      "Subscriber Live vs ledger: above this relative difference the sources disagree")],
    "key STRING, value DOUBLE, note STRING",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq("ref_thresholds"))

# ref_source_systems: the simulated systems of record (planted.SOURCE_SYSTEMS). row_count and
# last_ingested_at are filled from the bronze tables after cc_batch (sql/09_metric_views.sql).
spark.createDataFrame(  # noqa: F821
    [(sid, name, stype, conn, list(tables), None, None) for sid, name, stype, conn, tables in P.SOURCE_SYSTEMS],
    "source_id STRING, display_name STRING, system_type STRING, prod_connector STRING, bronze_tables ARRAY<STRING>, "
    "row_count BIGINT, last_ingested_at TIMESTAMP",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq("ref_source_systems"))

# COMMAND ----------
# ref_metric_provenance from seed/metric_provenance.yaml. The YAML uses two value tokens, filled here
# from validated config / planted values: {day_after} (ISO date) and {alert_id}.

with open(os.path.abspath("../seed/metric_provenance.yaml")) as fh:
    prov = yaml.safe_load(fh)["metrics"]
tokens = {"{day_after}": DAY_AFTER, "{alert_id}": config.validate_identifier(P.INCIDENT["alert_id"], "alert_id")}


def fill(sql: str) -> str:
    for token, value in tokens.items():
        sql = sql.replace(token, value)
    return " ".join(sql.split())


rows = [(m["metric_id"], m["display_name"], fill(m["sql_text"]), list(m["source_tables"]), m["owner_system"])
        for m in prov]
assert len({r[0] for r in rows}) == len(rows), "duplicate metric_id in seed/metric_provenance.yaml"
schema = T.StructType([
    T.StructField("metric_id", T.StringType()), T.StructField("display_name", T.StringType()),
    T.StructField("sql_text", T.StringType()), T.StructField("source_tables", T.ArrayType(T.StringType())),
    T.StructField("owner_system", T.StringType()),
])
spark.createDataFrame(rows, schema).write.mode("overwrite").option(  # noqa: F821
    "overwriteSchema", "true").saveAsTable(cfg.fq("ref_metric_provenance"))

# COMMAND ----------
# Validation

for t in ("ref_assumptions", "ref_team_budgets", "ref_model_prices", "ref_thresholds", "ref_source_systems", "ref_metric_provenance"):
    print(t, spark.table(cfg.fq(t)).count())  # noqa: F821
assert spark.table(cfg.fq("ref_metric_provenance")).count() >= 18, "expected the 18 fixture metric_ids plus v3 ones"  # noqa: F821
assert spark.table(cfg.fq("ref_team_budgets")).count() == len(P.TEAM_BUDGETS)  # noqa: F821
display(spark.table(cfg.fq("ref_metric_provenance")))  # noqa: F821
