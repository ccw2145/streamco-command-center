# Databricks notebook source
# StreamCo Command Center 02: game-night viewership forecast by metro x CDN, issued at kickoff.
# LightGBM quantile models (p10/p50/p90) of a cell's concurrency relative to its own pre-kickoff level, trained on
# the past big events. For game night the forecast uses only the 6:00-6:29 PM ET telemetry. Utilization is the
# forecast p50 load divided by the cell's capacity; nothing is set by construction.
# Writes forecast_viewers and forecast_flags; registers {catalog}.{schema}.viewer_forecast (alias champion).

# COMMAND ----------

# MAGIC %pip install -q lightgbm>=4.5 mlflow>=3.4

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import json
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "src_schema": "streamco", "game_date": "2027-02-14"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
CATALOG, SCHEMA, SRC = (dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema"))  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", x) for x in (CATALOG, SCHEMA, SRC)), "invalid identifier"
GAME_DATE = date.fromisoformat(dbutils.widgets.get("game_date"))  # noqa: F821
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"  # noqa: E731
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821

ET, UTC = ZoneInfo("America/New_York"), ZoneInfo("UTC")
KICKOFF_MIN_ET, HALF_LO, HALF_HI, START, END = 18 * 60 + 30, 105, 125, -30, 240  # minutes after kickoff
to_utc = lambda m: (datetime.combine(GAME_DATE, datetime.min.time(), tzinfo=ET)  # noqa: E731
                    + timedelta(minutes=KICKOFF_MIN_ET + m)).astimezone(UTC).replace(tzinfo=None)
KICKOFF_UTC, T0_UTC = to_utc(0), to_utc(START)
FLAG_AT = 0.90
QUANTILES = {"yhat_lo": 0.1, "yhat": 0.5, "yhat_hi": 0.9}
FEATURES = ["minutes_since_kickoff", "is_halftime", "game_period", "log_ordinary", "metro", "cdn"]
PARAMS = dict(objective="quantile", n_estimators=500, learning_rate=0.05, num_leaves=15, min_child_samples=20,
              random_state=7, deterministic=True, force_row_wise=True, verbose=-1)
MODEL_NAME = fq("viewer_forecast")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user() AS u').first().u}/streamco-command-center")  # noqa: F821

# COMMAND ----------
# Training data: past big events at 5-minute grain, each cell's ordinary prime-time level as a size feature.

ev = spark.table(src("fact_concurrency_event_5min")).where(F.col("ts") < F.lit(T0_UTC)).toPandas()  # noqa: F821
if ev.empty:
    raise RuntimeError("fact_concurrency_event_5min has no past events: run the data generators first.")
ordinary = (spark.table(src("fact_concurrency_hourly"))  # noqa: F821
            .where(~F.col("is_event") & F.hour(F.from_utc_timestamp("hour_ts", "America/New_York")).between(19, 22))
            .groupBy("hour_ts", "metro", "cdn").agg(F.sum("concurrent_avg").alias("c"))
            .groupBy("metro", "cdn").agg(F.avg("c").alias("ordinary")).toPandas())
anchor = ev[ev.minutes_since_kickoff < 0].groupby(["event_id", "metro", "cdn"]).concurrent.mean().rename("anchor")
train = ev.merge(anchor.reset_index(), on=["event_id", "metro", "cdn"]).merge(ordinary, on=["metro", "cdn"])
train = train[(train.anchor > 0) & (train.concurrent > 0)].copy()
train["y"] = np.log(train.concurrent / train.anchor)
CATS = {"metro": sorted(train.metro.unique()), "cdn": sorted(train.cdn.unique())}


def make_features(df: pd.DataFrame, cats: dict) -> pd.DataFrame:
    m = df["minutes_since_kickoff"].astype(int)
    return pd.DataFrame({
        "minutes_since_kickoff": m, "is_halftime": m.between(HALF_LO, HALF_HI).astype(int),
        "game_period": np.select([m < 0, m < HALF_LO, m <= HALF_HI], [0, 1, 2], 3),
        "log_ordinary": np.log(df["ordinary"].astype(float)),
        "metro": pd.Categorical(df["metro"], categories=cats["metro"]),
        "cdn": pd.Categorical(df["cdn"], categories=cats["cdn"])})[FEATURES]


class ViewerForecast(mlflow.pyfunc.PythonModel):
    """Input: metro, cdn, minutes_since_kickoff, anchor (pre-kickoff mean viewers), ordinary (normal prime-time
    level). Output: yhat_lo, yhat, yhat_hi concurrent viewers (p10/p50/p90, monotone)."""

    def __init__(self, models: dict, cats: dict):
        self.models, self.cats = models, cats

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        X = make_features(model_input, self.cats)
        raw = np.sort(np.column_stack([np.exp(self.models[k].predict(X)) for k in QUANTILES]), axis=1)
        raw = raw * model_input["anchor"].astype(float).to_numpy()[:, None]
        return pd.DataFrame(np.round(raw), columns=list(QUANTILES)).astype("int64")


def fit(df: pd.DataFrame) -> ViewerForecast:
    X = make_features(df, CATS)
    return ViewerForecast({k: lgb.LGBMRegressor(alpha=a, **PARAMS).fit(X, df.y) for k, a in QUANTILES.items()}, CATS)


# Holdout check: train without the most recent past event, forecast its national peak.
last = sorted(train.event_id.unique())[-1]
te = train[train.event_id == last]
pr = fit(train[train.event_id != last]).predict(None, te)
nat = te.assign(yhat=pr.yhat.values).groupby("minutes_since_kickoff")[["concurrent", "yhat"]].sum()
holdout_peak_err = float(nat.yhat.max() / nat.concurrent.max() - 1)
print(f"training rows={len(train):,} events={train.event_id.nunique()}  holdout {last} peak error {holdout_peak_err:+.1%}")

# COMMAND ----------
# Game-night inputs available at kickoff: each cell's 6:00-6:29 PM ET telemetry.

anchor_gn = (spark.table(src("gold_cdn_region_minute"))  # noqa: F821
             .where((F.col("minute_ts") >= F.lit(T0_UTC)) & (F.col("minute_ts") < F.lit(KICKOFF_UTC)))
             .groupBy("minute_ts", "metro", "cdn").agg(F.sum("concurrent_sessions").alias("c"))
             .groupBy("metro", "cdn").agg(F.avg("c").alias("anchor")).toPandas())
if anchor_gn.empty:
    raise RuntimeError("No pre-kickoff telemetry in gold_cdn_region_minute: run the stream pipeline first.")
cap = spark.table(src("ref_cdn_capacity")).toPandas().rename(columns={"capacity_concurrent": "capacity"})  # noqa: F821
cells = anchor_gn.merge(ordinary, on=["metro", "cdn"]).merge(cap, on=["metro", "cdn"])
missing = len(anchor_gn) - len(cells)
if missing:
    print(f"WARNING: {missing} cells have no history or capacity and are not forecast")
grid = cells.merge(pd.DataFrame({"minutes_since_kickoff": range(START, END, 5)}), how="cross")
grid["ts"] = [to_utc(int(m)) for m in grid.minutes_since_kickoff]

# COMMAND ----------
# Train on all past events, register, forecast.

with mlflow.start_run(run_name="viewer_forecast"):
    model = fit(train)
    inp = grid[["metro", "cdn", "minutes_since_kickoff", "anchor", "ordinary"]].reset_index(drop=True)
    fc = model.predict(None, inp)
    mlflow.log_params({"features": ",".join(FEATURES), "target": "log(concurrent / pre-kickoff level)",
                       "n_events": int(train.event_id.nunique()), "issued_at": "kickoff"})
    mlflow.log_metric("holdout_peak_err", holdout_peak_err)
    info = mlflow.pyfunc.log_model(
        name="model", python_model=model, input_example=inp.head(5),
        signature=infer_signature(inp.head(50), fc.head(50)), registered_model_name=MODEL_NAME,
        pip_requirements=[f"lightgbm=={lgb.__version__}", f"pandas=={pd.__version__}", f"numpy=={np.__version__}"])
client = MlflowClient()
version = str(info.registered_model_version)
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.update_registered_model(MODEL_NAME, description="Game-night viewership forecast per metro x CDN (LightGBM "
                               "quantile p10/p50/p90 relative to the pre-kickoff level). Issued at kickoff.")

# COMMAND ----------
# forecast_viewers and forecast_flags

out = grid[["ts", "metro", "cdn", "capacity"]].reset_index(drop=True).join(fc)
out["util_at_peak"] = (out.groupby(["metro", "cdn"]).yhat.transform("max") / out.capacity).round(4)
(spark.createDataFrame(out[["ts", "metro", "cdn", "yhat", "yhat_lo", "yhat_hi", "capacity", "util_at_peak"]])  # noqa: F821
 .selectExpr("CAST(ts AS TIMESTAMP) ts", "metro", "cdn", "CAST(yhat AS BIGINT) yhat", "CAST(yhat_lo AS BIGINT) yhat_lo",
             "CAST(yhat_hi AS BIGINT) yhat_hi", "CAST(capacity AS BIGINT) capacity", "CAST(util_at_peak AS DOUBLE) util_at_peak")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("forecast_viewers")))
spark.sql(f"COMMENT ON TABLE {fq('forecast_viewers')} IS 'Game-night viewer forecast per metro x CDN, 5-minute grain, "  # noqa: F821
          f"issued at kickoff by viewer_forecast v{version}. util_at_peak = max yhat over the night / capacity.'")
spark.sql(f"""CREATE OR REPLACE TABLE {fq('forecast_flags')}
  COMMENT 'Capacity flags at kickoff: flagged when the forecast peak reaches {FLAG_AT:.0%} of capacity.'
  AS SELECT metro, cdn, max(util_at_peak) AS util_at_peak, max(util_at_peak) >= {FLAG_AT} AS flagged
  FROM {fq('forecast_viewers')} GROUP BY metro, cdn""")  # noqa: F821

# COMMAND ----------
# Validation (numbers reported; only structural failures stop the task)

g = spark.table(fq("forecast_viewers")).toPandas()  # noqa: F821
nat = g.groupby("ts")[["yhat", "yhat_lo", "yhat_hi"]].sum()
actual = spark.sql(f"""SELECT max(n) AS peak FROM (SELECT minute_ts, sum(concurrent_sessions) AS n
                       FROM {src('gold_cdn_region_minute')} WHERE minute_ts >= '{T0_UTC}' GROUP BY minute_ts)""").first().peak  # noqa: F821
flags = spark.table(fq("forecast_flags")).orderBy(F.desc("util_at_peak")).toPandas()  # noqa: F821
dfw = flags[(flags.metro == "DFW") & (flags.cdn == "CDN-C")].util_at_peak
summary = {"model": MODEL_NAME, "version": version, "rows": len(g), "national_peak_forecast": int(nat.yhat.max()),
           "national_peak_actual": int(actual or 0),
           "peak_err": round(nat.yhat.max() / actual - 1, 4) if actual else None,
           "dallas_cdn_c_util": round(float(dfw.iloc[0]), 4) if len(dfw) else None,
           "flagged": flags[flags.flagged][["metro", "cdn", "util_at_peak"]].values.tolist(),
           "max_other_cell": flags[~((flags.metro == "DFW") & (flags.cdn == "CDN-C"))].head(1)[["metro", "cdn", "util_at_peak"]].values.tolist(),
           "holdout_peak_err": round(holdout_peak_err, 4)}
print(json.dumps(summary, indent=1, default=str))
display(flags.head(8))  # noqa: F821
assert ((g.yhat_lo <= g.yhat) & (g.yhat <= g.yhat_hi)).all(), "quantiles are not monotone"
assert len(g) > 0 and g.capacity.gt(0).all(), "empty forecast or missing capacity"
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
