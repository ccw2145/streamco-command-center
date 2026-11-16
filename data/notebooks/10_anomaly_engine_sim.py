# Databricks notebook source
# Owner: Stream A (Data + Pipelines). Stand-in for the customer's Anomaly Engine (same output contract).
#
# Rule (planted.ANOMALY_RULE): per cell (CDN x metro x device x app version), robust z of the minute
# rebuffer ratio against the median / MAD of the previous 60 minutes; an alert fires when z > 4 for
# 3 consecutive minutes. detected_ts = the minute the rule is satisfied (onset + 2 minutes, e.g. 20:19
# for an onset at 20:17);
# resolved_ts = first later minute back under 1.5x the alert baseline (the runbook's verification).
# affected_sessions = weighted sessions in the cell overlapping [detected_ts, resolved_ts]. A resolved cell
# is not re-alerted for COOLDOWN_MIN minutes (its recovery tail is still above the 60-minute median).
# Then the rules-based traffic shift (cdn_failover_runbook.md section 2): same metro, the CDN with the
# most forecast headroom at the alert time, provided the headroom exceeds the load being moved.
# Reads gold_cdn_region_minute, silver_sessions, forecast_viewers (StreamCo notebooks/02_forecast), ref_cdn_capacity.
# Writes gold_anomaly_alerts, gold_shift_recommendation.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.abspath("../src"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from cc_common import config, planted as P, policies  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
GAME_DATE = date.fromisoformat(cfg.game_date)
ET = ZoneInfo(P.TZ)
T0 = datetime(GAME_DATE.year, GAME_DATE.month, GAME_DATE.day, 18, tzinfo=ET).astimezone(ZoneInfo("UTC")).replace(
    tzinfo=None)
T_END = T0 + timedelta(hours=4, minutes=30)
Z_MAX, PERSIST = P.ANOMALY_RULE["z_threshold"], P.ANOMALY_RULE["persistence_min"]
WINDOW_MIN = 60
COOLDOWN_MIN = 30     # no re-alert on the same cell for 30 minutes after it resolves (recovery tail)
CELL = ["cdn", "metro", "device_family", "app_version"]


def write(df, name: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq(name))


minutes = (spark.table(cfg.fq("gold_cdn_region_minute"))  # noqa: F821
           .where((F.col("minute_ts") >= F.lit(T0)) & (F.col("minute_ts") < F.lit(T_END))
                  & F.col("app_version").isNotNull())
           .select("minute_ts", *CELL, "rebuffer_ratio", "concurrent_sessions"))
if minutes.limit(1).count() == 0:
    raise RuntimeError("gold_cdn_region_minute has no game-night rows: run 04_gen_game_night and the cc_stream pipeline.")

# COMMAND ----------

ALERT_SCHEMA = ("cdn STRING, metro STRING, device_family STRING, app_version STRING, detected_ts TIMESTAMP, "
                "resolved_ts TIMESTAMP, observed DOUBLE, baseline DOUBLE, z_score DOUBLE, max_z DOUBLE")


def detect(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("minute_ts").reset_index(drop=True)
    x = pdf.rebuffer_ratio.to_numpy(float)
    z = np.full(len(x), np.nan)
    med = np.full(len(x), np.nan)
    for i in range(WINDOW_MIN, len(x)):
        hist = x[i - WINDOW_MIN:i]
        m = np.median(hist)
        mad = np.median(np.abs(hist - m)) * 1.4826
        med[i] = m
        z[i] = (x[i] - m) / max(mad, 1e-4 * m, 1e-9)
    out = []
    run = 0
    i = WINDOW_MIN
    while i < len(x):
        run = run + 1 if z[i] > Z_MAX else 0
        if run == PERSIST:
            start = i - PERSIST + 1
            base = med[start]
            later = np.flatnonzero((np.arange(len(x)) > i) & (x < 1.5 * base))
            end = int(later[0]) if len(later) else None
            out.append({**{c: pdf[c].iloc[0] for c in CELL}, "detected_ts": pdf.minute_ts[i],
                        "resolved_ts": pdf.minute_ts[end] if end is not None else None,
                        "observed": float(np.max(x[start:end if end is not None else len(x)])), "baseline": base,
                        "z_score": float(z[start]), "max_z": float(np.nanmax(z[start:end if end is not None else len(x)]))})
            i = end + COOLDOWN_MIN if end is not None else len(x)
            run = 0
        i += 1
    return pd.DataFrame(out, columns=[c.split()[0] for c in ALERT_SCHEMA.split(", ")])


raw = minutes.groupBy(*CELL).applyInPandas(detect, ALERT_SCHEMA).toPandas()
print(raw)

# COMMAND ----------
# Alert rows. The planted incident gets planted.INCIDENT["alert_id"]; any other alert gets a derived id.

sessions = spark.table(cfg.fq("silver_sessions"))  # noqa: F821
rows = []
for a in raw.itertuples():
    is_incident = all(getattr(a, k) == P.INCIDENT[k] for k in CELL)
    alert_id = P.INCIDENT["alert_id"] if is_incident else \
        f"ALR-{GAME_DATE:%Y%m%d}-{a.metro}-{a.cdn[-1]}-{a.device_family.upper()}{a.app_version.replace('.', '')}"
    end = a.resolved_ts if pd.notna(a.resolved_ts) else T_END
    onset = a.detected_ts - pd.Timedelta(minutes=PERSIST - 1)
    affected = sessions.where(" AND ".join(f"{c} = '{getattr(a, c)}'" for c in CELL)).where(
        (F.col("start_ts") <= F.lit(end)) & (F.col("end_ts") >= F.lit(onset))).agg(
        F.sum("sample_weight")).first()[0] or 0
    rows.append((alert_id, a.detected_ts.to_pydatetime(), a.cdn, a.metro, a.device_family, a.app_version,
                 "rebuffer_ratio", float(a.observed), float(a.baseline), float(a.z_score), PERSIST,
                 int(round(affected)), "high" if a.max_z > 3 * Z_MAX else "medium",
                 "resolved" if pd.notna(a.resolved_ts) else "open",
                 a.resolved_ts.to_pydatetime() if pd.notna(a.resolved_ts) else None))
alerts = spark.createDataFrame(rows, (  # noqa: F821
    "alert_id STRING, detected_ts TIMESTAMP, cdn STRING, metro STRING, device_family STRING, app_version STRING, "
    "metric STRING, observed DOUBLE, baseline DOUBLE, z_score DOUBLE, persistence_min INT, affected_sessions BIGINT, "
    "severity STRING, status STRING, resolved_ts TIMESTAMP"))
write(alerts, "gold_anomaly_alerts")

# COMMAND ----------
# Traffic-shift recommendation for each high alert (rules only; the LLM just phrases it later).

RULE = policies.citation("cdn_failover_runbook.md", "2. Same-metro failover")
fc = spark.table(cfg.fq("forecast_viewers")).selectExpr(  # noqa: F821 - StreamCo 02_forecast output
    "ts AS forecast_ts", "metro", "cdn", "yhat AS p50")
cap = spark.table(cfg.fq("ref_cdn_capacity"))  # noqa: F821
recs = []
for a in [r for r in rows if r[12] == "high"]:
    alert_id, detected, cdn, metro, dev, ver = a[0], a[1], a[2], a[3], a[4], a[5]
    shift_load = spark.table(cfg.fq("gold_cdn_region_minute")).where(  # noqa: F821
        (F.col("minute_ts") == F.lit(detected)) & (F.col("cdn") == cdn) & (F.col("metro") == metro)
        & (F.col("device_family") == dev) & (F.col("app_version") == ver)).first().concurrent_sessions
    cand = (fc.where((F.col("metro") == metro) & (F.col("cdn") != cdn)
                     & (F.col("forecast_ts") <= F.lit(detected)))
            .withColumn("rk", F.row_number().over(Window.partitionBy("cdn").orderBy(F.desc("forecast_ts"))))
            .where("rk = 1").join(cap.select("cdn", "metro", F.col("capacity_concurrent").alias("cap")),
                                  ["cdn", "metro"])
            .withColumn("headroom", F.col("cap") - F.col("p50")).orderBy(F.desc("headroom")).toPandas())
    if cand.empty:
        raise RuntimeError(f"No forecast for other CDNs in {metro} at {detected}: run notebooks/02_forecast first.")
    best = cand.iloc[0]
    if best.headroom < shift_load:
        print(f"{alert_id}: no same-metro CDN has headroom for {shift_load:,} viewers; no shift recommended")
        continue
    recs.append((alert_id, cdn, best.cdn, metro, dev, ver, int(shift_load), int(best.cap), int(best.p50),
                 float(round(best.headroom / best.cap, 3)),
                 f"Move {dev} {ver} viewers in {metro} from {cdn} to {best.cdn}: same-metro CDN with the most "
                 f"forecast headroom ({best.headroom:,.0f}) above the {shift_load:,} viewers moved.",
                 RULE["doc"], RULE["section"], RULE["passage"]))
write(spark.createDataFrame(recs, (  # noqa: F821
    "alert_id STRING, from_cdn STRING, to_cdn STRING, metro STRING, device_family STRING, app_version STRING, "
    "shift_load BIGINT, target_capacity BIGINT, target_forecast_p50 BIGINT, target_headroom_pct DOUBLE, rule STRING, "
    "policy_doc STRING, policy_section STRING, policy_passage STRING")), "gold_shift_recommendation")

# COMMAND ----------
# Self-asserts

al = spark.table(cfg.fq("gold_anomaly_alerts")).toPandas()  # noqa: F821
high = al[al.severity == "high"]
inc = al[al.alert_id == P.INCIDENT["alert_id"]]
display(al)  # noqa: F821
assert len(high) == 1 and len(inc) == 1, f"expected exactly the planted incident as the one high alert, got {len(high)}"
detected = pd.Timestamp(inc.detected_ts.iloc[0]).tz_localize("UTC").tz_convert(ET)
onset = detected - pd.Timedelta(minutes=PERSIST - 1)
resolved = pd.Timestamp(inc.resolved_ts.iloc[0]).tz_localize("UTC").tz_convert(ET)
assert onset.strftime("%H:%M") == P.INCIDENT["onset"], onset
expected_detect = (datetime.strptime(P.INCIDENT["onset"], "%H:%M") + timedelta(minutes=PERSIST - 1)).strftime("%H:%M")
assert detected.strftime("%H:%M") == expected_detect, detected   # 20:19
assert (resolved - onset).total_seconds() / 60 == P.INCIDENT_MINUTES_TO_RESOLVE, resolved
assert abs(inc.affected_sessions.iloc[0] - P.AFFECTED_SESSIONS) / P.AFFECTED_SESSIONS <= 0.02
assert not ((al.cdn == P.DECOY["cdn"]) & (al.metro == P.DECOY["metro"])).any(), "the Seattle decoy must not fire"
rec = spark.table(cfg.fq("gold_shift_recommendation")).toPandas()  # noqa: F821
assert len(rec) == 1 and rec.to_cdn.iloc[0] == P.BACKUP_CELL["cdn"], rec
assert abs(rec.shift_load.iloc[0] - P.SHIFT_LOAD) / P.SHIFT_LOAD <= 0.05, rec.shift_load.iloc[0]
print("10_anomaly_engine_sim: all self-asserts passed")
