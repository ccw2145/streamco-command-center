# Databricks notebook source
# Owner: Stream A (Data + Pipelines). History before the Big Game, plus the churn training labels.
#
# Writes fact_concurrency_hourly (90 days), fact_concurrency_event_5min (8 past events),
# fact_daily_cancels (18 months, through game day), ref_price_changes, ref_app_releases, the
# 14-day history slice of fact_subscription_events / fact_billing_attempts / billing_ledger_cancels_daily,
# and marks the history cancels on dim_subscribers.
#
# Churn labels (design deviation 2): latent frailty z ~ N(0,1), observed only through noisy proxies
# (engagement trend, days since last session, pauses, support contacts), on top of the planted
# multipliers. A calibration loop bisects beta_z (frailty strength) and the QoE dose slope on a
# 200k sample with a logistic-regression oracle until full-feature AUC = 0.80 and no-QoE AUC = 0.77.
# Generator tables (not part of the app contract):
#   gen_subscriber_features  training panel, same columns as fs_subscriber_features (B 11 reads it)
#   gen_calibration          chosen parameters, the risk SQL and the oracle (04 reuses them)
#   gen_pregame_features     Sports subscribers as of the day after, before game-night effects, with latents
# All randomness is a hash of (seed, salt, row key), so output does not depend on partitioning.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import json
import math
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.abspath("../src"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from scipy import special, stats  # noqa: E402
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402

from cc_common import config, planted as P, seeds  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
rng = seeds.rng(cfg.seed, "03")
ET = ZoneInfo(P.TZ)
GAME_DATE = date.fromisoformat(cfg.game_date)
DAY_AFTER = GAME_DATE + timedelta(days=1)
W_START = GAME_DATE - timedelta(days=14)
SCALE = cfg.scale_subscribers / P.N_SUBSCRIBERS


def u(salt: str, *cols):
    """Deterministic U(0,1) per row from (seed, salt, cols); independent of partitioning."""
    h = F.xxhash64(F.lit(cfg.seed), F.lit(salt), *cols)
    return (F.pmod(h, F.lit(1 << 40)).cast("double") + 0.5) / float(1 << 40)


def et_ts(day_col, seconds_col):
    """ET wall clock (DATE column + seconds after midnight) -> UTC TIMESTAMP column."""
    return F.to_utc_timestamp(F.to_timestamp(day_col) + F.make_interval(secs=seconds_col), P.TZ)


def et_to_utc(d: date, seconds: float = 0) -> datetime:
    local = datetime(d.year, d.month, d.day, tzinfo=ET) + timedelta(seconds=float(seconds))
    return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def write(df, name: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq(name))


cell_mix = spark.table(cfg.fq("gen_cell_mix")).toPandas()  # noqa: F821
events = spark.table(cfg.fq("gen_events")).toPandas()  # noqa: F821
curve = spark.table(cfg.fq("gen_event_curve")).toPandas().sort_values("offset_s")  # noqa: F821
past_events = events[~events.is_target].copy()
target = events[events.is_target].iloc[0]

# COMMAND ----------
# fact_concurrency_hourly: 90 days through game day 17:00 ET, metro x CDN x device. Regular hours follow
# a local-time diurnal curve with weekday seasonality; event nights (18:00-22:59 ET) follow the event curve.

DIURNAL = np.array([.25, .15, .08, .05, .04, .05, .10, .18, .22, .25, .28, .30, .33, .33, .34, .36, .40, .50,
                    .65, .82, .95, 1.0, .85, .55])
WEEKDAY = {0: .95, 1: .93, 2: .95, 3: .97, 4: 1.02, 5: 1.08, 6: 1.10}  # Monday=0
PRIME_TIME_LEVEL = 2_000_000  # national concurrent viewers at 21:00 local on an ordinary weeknight
pairs_dev = cell_mix.groupby(["metro", "cdn", "device_family"], as_index=False).share.sum()
metro_tz = {m: tz for m, (_n, _la, _lo, tz) in P.METROS.items()}

hours = pd.date_range(et_to_utc(GAME_DATE - timedelta(days=90)), et_to_utc(GAME_DATE, 17 * 3600), freq="h")
ev_dates = {d: r for d, r in zip(past_events.event_date, past_events.itertuples())}


def event_hour_level(ev, et_hour: int) -> float:
    """Mean of pre_level * pre_shape * (1 + (m-1) h) over one ET clock hour of an event night."""
    seg = curve[(curve.offset_s >= (et_hour - 18) * 3600) & (curve.offset_s < (et_hour - 17) * 3600)]
    level = ev.pre_level * (seg.pre_shape * (1 + (ev.surge_mult - 1) * seg.h)).mean()
    return level if et_hour < 22 else 0.5 * level + 0.5 * PRIME_TIME_LEVEL * 0.85


rows = []
for ts in hours:
    et = ts.tz_localize("UTC").tz_convert(ET)
    ev = ev_dates.get(et.date())
    for metro in P.METROS:
        local = ts.tz_localize("UTC").tz_convert(metro_tz[metro])
        if ev is not None and 18 <= et.hour <= 22:
            level, is_event = event_hour_level(ev, et.hour), True
        else:
            level, is_event = PRIME_TIME_LEVEL * DIURNAL[local.hour] * WEEKDAY[local.weekday()], False
        rows.append((ts.to_pydatetime(), metro, level, is_event))
hour_df = pd.DataFrame(rows, columns=["hour_ts", "metro", "level", "is_event"]).merge(pairs_dev, on="metro")
hour_df["concurrent_avg"] = np.round(hour_df.level * hour_df.share
                                     * (1 + 0.04 * rng.standard_normal(len(hour_df)))).astype(int)
write(spark.createDataFrame(hour_df[["hour_ts", "metro", "cdn", "device_family", "concurrent_avg",  # noqa: F821
                                     "is_event"]]).withColumn("concurrent_avg", F.col("concurrent_avg").cast("int")),
      "fact_concurrency_hourly")

# fact_concurrency_event_5min: 8 past events x 54 five-minute buckets (18:00-22:30 ET) x metro-CDN pairs.
pairs = cell_mix.groupby(["metro", "cdn"], as_index=False).share.sum()
curve["bucket"] = curve.offset_s // 300
bucket_shape = curve.groupby("bucket").agg(pre_shape=("pre_shape", "mean"), h=("h", "mean")).reset_index()
ev5 = []
for ev in past_events.itertuples():
    for b in bucket_shape.itertuples():
        total = ev.pre_level * b.pre_shape * (1 + (ev.surge_mult - 1) * b.h)
        for pr in pairs.itertuples():
            ev5.append((et_to_utc(ev.event_date, 18 * 3600 + b.bucket * 300), ev.event_id, pr.metro, pr.cdn,
                        int(b.bucket * 5 - 30), int(round(total * pr.share * (1 + 0.02 * rng.standard_normal())))))
write(spark.createDataFrame(ev5, "ts TIMESTAMP, event_id STRING, metro STRING, cdn STRING, "  # noqa: F821
                            "minutes_since_kickoff INT, concurrent INT"), "fact_concurrency_event_5min")

# COMMAND ----------
# Reference tables for the ruled-out hypotheses.

write(spark.createDataFrame([  # noqa: F821
    (et_to_utc(GAME_DATE - timedelta(days=21), 3 * 3600), "Sports", 1.00),
    (et_to_utc(GAME_DATE - timedelta(days=120), 3 * 3600), "Family", 1.00),
    (et_to_utc(GAME_DATE - timedelta(days=200), 3 * 3600), "Originals", 2.00),
], "change_ts TIMESTAMP, tier STRING, delta_usd DOUBLE").withColumn("delta_usd", F.col("delta_usd").cast(
    "decimal(6,2)")), "ref_price_changes")
releases = [(-10, "Roku", "7.2.0"), (-45, "Roku", "7.1.5"), (-120, "Roku", "7.1.2"), (-240, "Roku", "7.0.4"),
            (-20, "FireTV", "5.4.1"), (-90, "FireTV", "5.3.9"), (-12, "iOS", "12.3.0"), (-60, "iOS", "12.2.1"),
            (-30, "Android", "12.2.4"), (-75, "Android", "12.1.0"), (-25, "SmartTV", "3.9.0"),
            (-110, "SmartTV", "3.8.2"), (-16, "Web", "2027.02"), (-47, "Web", "2027.01")]
write(spark.createDataFrame([(et_to_utc(GAME_DATE + timedelta(days=d), 10 * 3600), dev, v)  # noqa: F821
                             for d, dev, v in releases], "release_ts TIMESTAMP, device_family STRING, version STRING"),
      "ref_app_releases")

# COMMAND ----------
# fact_daily_cancels: 18 months per tier, negative binomial with weekday seasonality and a mild
# annual cycle. The day after each big event adds its event-only cancels at the same rate r as the
# Big Game (r = planted event-only count / Big Game event-promo sign-ups), so Lane 5 can show that
# "event-only" churn is expected. The last six ordinary same-weekday days before the game average
# exactly planted.CANCELS_BASELINE for Sports (the decomposition baseline).

DOW_MEAN = {  # tier -> mean cancels by weekday (Monday first), anchored so the day-after weekday hits the baseline
    "Sports": [9000, 8700, 8550, 8500, 8450, 8300, 8600],
    "Originals": [4000, 3880, 3820, 3800, 3780, 3700, 3860],
    "Family": [1800, 1750, 1720, 1710, 1700, 1680, 1740],
    "Bundle": [300, 292, 288, 286, 284, 280, 290],
}
DOW_SD = {"Sports": 450, "Originals": 200, "Family": 90, "Bundle": 25}
sports_baseline_shift = P.CANCELS_BASELINE - DOW_MEAN["Sports"][DAY_AFTER.weekday()]
DOW_MEAN = {t: [x + (sports_baseline_shift if t == "Sports" else 0) for x in v] for t, v in DOW_MEAN.items()}
# Event-promo Sports sign-ups on an event evening (from 18:00 ET); the same filter is the rate denominator
# in 04's self-check and in sql/14's rule text.
ep_evening = {r.night: r.n for r in spark.table(cfg.fq("dim_subscribers")).where(  # noqa: F821
    "signup_source = 'event_promo' AND tier = 'Sports'").withColumn(
    "et", F.from_utc_timestamp("signup_ts", P.TZ)).where(F.hour("et") >= 18).groupBy(
    F.to_date("et").alias("night")).agg(F.count("*").alias("n")).collect()}
EVENT_ONLY_RATE = P.CANCEL_DRIVERS["event_only"][1] / ep_evening[GAME_DATE]
next_day_event = {ev.event_date + timedelta(days=1): ev for ev in past_events.itertuples()}


# Each past night's event-only rate is the Big Game rate +/- a few percent (the same rate, not identical).
EV_RATE_JITTER = {ev.event_id: float(rng.uniform(-0.07, 0.07)) for ev in past_events.itertuples()}


def event_only_quota(ev) -> int:
    """Next-day event-only cancels for a past event (in-window nights: from the actual sign-up count)."""
    return round(EVENT_ONLY_RATE * (1 + EV_RATE_JITTER[ev.event_id]) * ep_evening.get(ev.event_date, ev.n_event_promo))

days = [GAME_DATE - timedelta(days=i) for i in range(547, -1, -1)]
anchor = 1 + 0.03 * math.cos(2 * math.pi * (DAY_AFTER.timetuple().tm_yday - 15) / 365)
daily = []
for tier, means in DOW_MEAN.items():
    for d in days:
        mu = SCALE * means[d.weekday()] * (1 + 0.03 * math.cos(2 * math.pi * (d.timetuple().tm_yday - 15) / 365)) / anchor
        k = mu * mu / max(DOW_SD[tier] ** 2 * SCALE - mu, 1.0)
        n = int(rng.negative_binomial(k, k / (k + mu)))
        ev = next_day_event.get(d)
        evt_only = event_only_quota(ev) if (ev is not None and tier == "Sports") else 0
        daily.append([d, tier, n, evt_only, ev is not None])
daily_df = pd.DataFrame(daily, columns=["day", "tier", "base", "event_only", "is_big_event_next_day"])

# The expected band (gold_cancel_expected) and the decomposition baseline are the mean and spread of the
# last six ordinary same-weekday days. Those six days are set so the Sports mean gives the planted +76%
# (15,800 / mean - 1) and mean +/- BAND_K x sd lands on the planted 8.4k-9.7k band; other tiers keep their
# weekday mean, so their day-after counts sit inside their own bands.
BAND_K = 1.5
BASELINE_MEAN = {t: m[DAY_AFTER.weekday()] * SCALE for t, m in DOW_MEAN.items()}
BASELINE_MEAN["Sports"] = P.CANCELS_OBSERVED / (1 + P.CANCELS_PCT_OVER)
BASELINE_SD = {t: DOW_SD[t] * SCALE for t in DOW_MEAN}
BASELINE_SD["Sports"] = (P.CANCEL_FORECAST_BAND[1] - P.CANCEL_FORECAST_BAND[0]) / (2 * BAND_K)
q6 = np.array([-1.3830, -0.6745, -0.2104, 0.2104, 0.6745, 1.3830])
q6 = (q6 - q6.mean()) / q6.std(ddof=1)
for tier in DOW_MEAN:
    six = daily_df[(daily_df.tier == tier) & (daily_df.day < DAY_AFTER) & (~daily_df.is_big_event_next_day)
                   & (daily_df.day.map(lambda d: d.weekday()) == DAY_AFTER.weekday())].tail(6)
    daily_df.loc[six.index, "base"] = np.round(BASELINE_MEAN[tier] + BASELINE_SD[tier] * rng.permutation(q6)).astype(int)
daily_df["cancels"] = daily_df.base + daily_df.event_only
write(spark.createDataFrame(daily_df[["day", "tier", "cancels", "is_big_event_next_day"]])  # noqa: F821
      .withColumn("cancels", F.col("cancels").cast("int")), "fact_daily_cancels")

# COMMAND ----------
# History slice of fact_subscription_events (15 days, game day before 18:00 ET).
# Starts: every subscriber who signed up in the window. Cancels: the daily counts above, drawn from
# base subscribers (event-only ones from the prior game nights' event-promo sign-ups). A few pauses.

subs = spark.table(cfg.fq("dim_subscribers"))  # noqa: F821
win_lo, game_t0 = et_to_utc(W_START), et_to_utc(GAME_DATE, 18 * 3600)
window_days = daily_df[(daily_df.day >= W_START) & (daily_df.day <= GAME_DATE)]
REASONS_VOL = {"too_expensive": .30, "not_watching": .22, "content_finished": .18, "switching_service": .12,
               "technical_issues": .08, P.PLAYBACK_SURVEY_REASON: P.PLAYBACK_SURVEY_SHARE[1], "other": .06}
REASONS_EVENT = {"event_over": .55, "too_expensive": .25, "not_watching": .15, "other": .05}
INVOLUNTARY_SHARE = 0.12
assert abs(sum(REASONS_VOL.values()) - 1) < 1e-9


def pick(uc, items: dict):
    total, acc, expr = sum(items.values()), 0.0, None
    keys = list(items)
    for k in keys[:-1]:
        acc += items[k] / total
        expr = F.when(uc < acc, F.lit(k)) if expr is None else expr.when(uc < acc, F.lit(k))
    return expr.otherwise(F.lit(keys[-1]))




def cancel_seconds(salt: str):
    """Seconds after ET midnight for a cancel: clustered in the day and evening (06:00-23:30)."""
    return (6 + 17.5 * F.pow(u(salt, "subscriber_id"), 0.8)) * 3600


# Event-only cancels after the two in-window prior game nights
prior_in_window = past_events[past_events.event_date >= W_START]
ev_quota = {ev.event_id: event_only_quota(ev) for ev in prior_in_window.itertuples()}
ev_night = {ev.event_id: ev.event_date for ev in prior_in_window.itertuples()}
ep = (subs.where((F.col("signup_source") == "event_promo") & (F.col("tier") == "Sports"))
      .withColumn("et_day", F.to_date(F.from_utc_timestamp("signup_ts", P.TZ)))
      .where(F.col("et_day").isin(list(ev_night.values())) & (F.hour(F.from_utc_timestamp("signup_ts", P.TZ)) >= 18))
      .withColumn("rk", F.row_number().over(Window.partitionBy("et_day").orderBy(u("ev_only", "subscriber_id")))))
quota_expr = None
for eid, d in ev_night.items():
    cond = F.col("et_day") == F.lit(d)
    quota_expr = F.when(cond, ev_quota[eid]) if quota_expr is None else quota_expr.when(cond, ev_quota[eid])
next_midnight = F.to_utc_timestamp(F.to_timestamp(F.date_add("et_day", 1)), P.TZ)
ev_cancels = (ep.where(F.col("rk") <= quota_expr)
              .withColumn("lo", F.greatest(next_midnight, F.col("signup_ts") + F.expr("INTERVAL 1 HOUR")))
              .withColumn("event_ts", F.col("lo") + F.make_interval(secs=(
                  F.unix_timestamp(F.col("signup_ts") + F.expr("INTERVAL 24 HOURS")) - F.unix_timestamp("lo"))
                  * u("ev_t", "subscriber_id")))
              .select("subscriber_id", "tier", "event_ts", F.lit("voluntary").alias("cancel_type"),
                      pick(u("ev_r", "subscriber_id"), REASONS_EVENT).alias("cancel_reason_code")))

# Baseline cancels: rank base subscribers per tier, give the first n_d of them day d.
quota = window_days.sort_values(["tier", "day"]).copy()
quota["hi"] = quota.groupby("tier").base.cumsum()
quota["lo"] = quota.hi - quota.base
bins = spark.createDataFrame(quota[["tier", "day", "lo", "hi"]])  # noqa: F821
pool = (subs.where(F.col("signup_ts") < F.lit(win_lo)).select("subscriber_id", "tier")
        .withColumn("rk", F.row_number().over(Window.partitionBy("tier").orderBy(u("hist_cancel", "subscriber_id"))))
        .where(F.col("rk") <= int(quota.hi.max())))
base_cancels = (pool.join(F.broadcast(bins), (pool.tier == bins.tier) & (pool.rk > bins.lo) & (pool.rk <= bins.hi))
                .select("subscriber_id", pool.tier, "day")
                .withColumn("event_ts", et_ts(F.col("day"), cancel_seconds("hc_t")))
                .withColumn("invol", u("hc_inv", "subscriber_id") < INVOLUNTARY_SHARE)
                .select("subscriber_id", "tier", "event_ts",
                        F.when(F.col("invol"), "involuntary_billing").otherwise("voluntary").alias("cancel_type"),
                        F.when(F.col("invol"), "payment_failed").otherwise(pick(u("hc_r", "subscriber_id"), REASONS_VOL))
                        .alias("cancel_reason_code")))
cancels = base_cancels.unionByName(ev_cancels)

starts = (subs.where((F.col("signup_ts") >= F.lit(win_lo)) & (F.col("signup_ts") < F.lit(game_t0)))
          .select("subscriber_id", "tier", F.col("signup_ts").alias("event_ts")))
pauses = (subs.where((F.col("status") == "paused") & (u("pause_ev", "subscriber_id") < 0.3))
          .select("subscriber_id", "tier", (F.lit(win_lo) + F.make_interval(
              secs=u("pause_t", "subscriber_id") * 14 * 86400)).alias("event_ts")))
resumes = (subs.where((F.col("status") == "active") & (F.col("signup_ts") < F.lit(win_lo))
                      & (u("resume_ev", "subscriber_id") < 0.002))
           .select("subscriber_id", "tier", (F.lit(win_lo) + F.make_interval(
               secs=u("resume_t", "subscriber_id") * 14 * 86400)).alias("event_ts")))

EVENT_ID_BASE = {"start": 10_000_000_000, "cancel": 20_000_000_000, "pause": 30_000_000_000,
                 "resume": 40_000_000_000}


def as_events(df, event_type: str):
    out = df
    for c in ("cancel_type", "cancel_reason_code"):
        if c not in out.columns:
            out = out.withColumn(c, F.lit(None).cast("string"))
    return out.select((F.col("subscriber_id") + EVENT_ID_BASE[event_type]).alias("event_id"), "subscriber_id",
                      "tier", F.lit(event_type).alias("event_type"), "event_ts", "cancel_type", "cancel_reason_code",
                      F.lit("subscriber_live").alias("source_system"))


hist_events = (as_events(starts, "start").unionByName(as_events(cancels, "cancel"))
               .unionByName(as_events(pauses, "pause")).unionByName(as_events(resumes, "resume")))
write(hist_events, "fact_subscription_events")

# Mark history cancels on the dimension (idempotent MERGE: same rows on every re-run).
spark.table(cfg.fq("fact_subscription_events")).where("event_type = 'cancel'").createOrReplaceTempView(  # noqa: F821
    "hist_cancels")
spark.sql(f"""MERGE INTO {cfg.fq('dim_subscribers')} d USING hist_cancels c ON d.subscriber_id = c.subscriber_id
              WHEN MATCHED THEN UPDATE SET d.status = 'cancelled', d.canceled_ts = c.event_ts""")  # noqa: F821

# COMMAND ----------
# fact_billing_attempts for history-window sign-ups: declines at 3-4% by acquirer (never processor_timeout
# before the game), each retried once after 1 hour and approved (payment_retry_policy section 1).

DECLINE_RATE = {"A": 0.032, "B": P.ACQUIRER_B["decline_rate_baseline"], "C": 0.036, "D": 0.039}
DECLINE_CODES = {"insufficient_funds": .45, "do_not_honor": .30, "expired_card": .15, "card_velocity": .10}
ATTEMPT_ID_BASE = {"signup": 10_000_000_000, "signup_retry": 20_000_000_000}
rate_expr = F.element_at(F.create_map(*[x for k, v in DECLINE_RATE.items() for x in (F.lit(k), F.lit(v))]),
                         F.col("acquirer"))
signups = (subs.where((F.col("signup_ts") >= F.lit(win_lo)) & (F.col("signup_ts") < F.lit(game_t0)))
           .withColumn("declined", u("bill_dec", "subscriber_id") < rate_expr))
first = signups.select(
    (F.col("subscriber_id") + ATTEMPT_ID_BASE["signup"]).alias("attempt_id"), "subscriber_id",
    F.col("signup_ts").alias("attempt_ts"), F.lit("signup").alias("attempt_type"), "tier",
    F.col("plan_price").alias("amount_usd"), "acquirer",
    F.when(F.col("declined"), "declined").otherwise("approved").alias("result"),
    F.when(F.col("declined"), pick(u("bill_code", "subscriber_id"), DECLINE_CODES)).alias("decline_code"),
    F.lit(None).cast("bigint").alias("retry_of"), F.col("declined").alias("retry_scheduled"),
    F.when(F.col("declined"), F.col("signup_ts") + F.expr("INTERVAL 24 HOURS")).alias("grace_expires_ts"))
retry = signups.where("declined").select(
    (F.col("subscriber_id") + ATTEMPT_ID_BASE["signup_retry"]).alias("attempt_id"), "subscriber_id",
    (F.col("signup_ts") + F.expr("INTERVAL 1 HOUR")).alias("attempt_ts"), F.lit("retry").alias("attempt_type"),
    "tier", F.col("plan_price").alias("amount_usd"), "acquirer", F.lit("approved").alias("result"),
    F.lit(None).cast("string").alias("decline_code"),
    (F.col("subscriber_id") + ATTEMPT_ID_BASE["signup"]).alias("retry_of"), F.lit(False).alias("retry_scheduled"),
    F.lit(None).cast("timestamp").alias("grace_expires_ts"))
write(first.unionByName(retry), "fact_billing_attempts")

# Finance ledger for the history days: within 0.3% of Subscriber Live (late events), never > 2%.
ledger = window_days.assign(
    ledger_cancels=(window_days.cancels * (1 - rng.uniform(0.0, 0.003, len(window_days)))).round().astype(int),
    ledger_source="finance_gl")
write(spark.createDataFrame(ledger[["day", "tier", "ledger_cancels", "ledger_source"]])  # noqa: F821
      .withColumn("ledger_cancels", F.col("ledger_cancels").cast("int")), "billing_ledger_cancels_daily")

# COMMAND ----------
# Churn feature generator. One function maps per-row uniforms to the fs_subscriber_features columns plus
# the latent frailty, so the training panel (here) and the scoring snapshot (04) come from one recipe.

LN = {k: math.log(v) for k, v in P.CHURN_MULTIPLIERS.items()}
FEATURE_COLS = ["tenure_days", "plan_price", "is_bundle", "signup_source", "event_only_signup", "hours_30d",
                "active_days_30d", "engagement_trend", "days_since_last_session", "pauses_90d", "rebuffer_min_30d",
                "max_session_rebuffer_min", "failed_starts_30d", "declines_30d", "decline_no_retry",
                "support_contacts_30d"]
UNIFORMS = ["u_z", "u_eps", "u_label", "u_hours", "u_active", "u_trend", "u_dsl", "u_pause", "u_supp",
            "u_qoe", "u_reb", "u_reb30", "u_fail", "u_decl", "u_decl2"]
PROXY_NOISE = 0.5     # engagement-trend noise relative to the frailty loading (fixed; calibration tunes beta_z)
EPS_SD = 0.3          # unobserved idiosyncratic risk
QOE_PREVALENCE = sum(P.QOE_EXPOSURE_PREVALENCE) / 2
QOE_SCHEMA = ("subscriber_id BIGINT, snapshot_date DATE, tenure_days INT, plan_price DECIMAL(6,2), is_bundle BOOLEAN, "
              "signup_source STRING, event_only_signup BOOLEAN, hours_30d DOUBLE, active_days_30d INT, "
              "engagement_trend DOUBLE, days_since_last_session INT, pauses_90d INT, rebuffer_min_30d DOUBLE, "
              "max_session_rebuffer_min DOUBLE, failed_starts_30d INT, declines_30d INT, decline_no_retry BOOLEAN, "
              "support_contacts_30d INT, z DOUBLE, eps DOUBLE, u_label DOUBLE, snapshot_lift DOUBLE")


def features_from_uniforms(pdf: pd.DataFrame) -> pd.DataFrame:
    """pdf has subscriber_id, snapshot_date, tenure_days, plan_price, is_bundle, signup_source and UNIFORMS."""
    z = special.ndtri(pdf.u_z.values)
    ten = pdf.tenure_days.values.astype(float)
    tenure_frac = np.clip(ten, 1, 30) / 30.0
    hours = np.exp(2.6 - 0.35 * z + 0.5 * special.ndtri(pdf.u_hours.values)) * tenure_frac
    p_active = np.clip(hours / (hours + 8.0), 0.05, 0.97)
    active = stats.binom.ppf(pdf.u_active.values, np.ceil(30 * tenure_frac).astype(int), p_active)
    exposed = pdf.u_qoe.values < QOE_PREVALENCE
    u_reb = pdf.u_reb.values
    max_reb = np.where(exposed, 5.0 + stats.gamma.ppf(u_reb, 1.5, scale=3.0),
                       np.minimum(stats.gamma.ppf(u_reb, 1.2, scale=1.0), 4.9))
    decline_no_retry = pdf.u_decl.values < 0.04
    out = pdf[["subscriber_id", "snapshot_date", "tenure_days", "plan_price", "is_bundle", "signup_source"]].copy()
    out["event_only_signup"] = pdf.signup_source.values == "event_promo"
    out["hours_30d"] = np.round(hours, 2)
    out["active_days_30d"] = active.astype(int)
    out["engagement_trend"] = np.round(-0.5 * z + PROXY_NOISE * special.ndtri(pdf.u_trend.values), 4)
    out["days_since_last_session"] = np.minimum(
        np.round(np.exp(1.2 + 0.45 * z + 0.3 * special.ndtri(pdf.u_dsl.values))), np.maximum(ten, 0)).astype(int)
    out["pauses_90d"] = stats.poisson.ppf(pdf.u_pause.values, np.exp(-1.6 + 0.55 * z)).astype(int)
    out["rebuffer_min_30d"] = np.round(max_reb * (1.3 + stats.gamma.ppf(pdf.u_reb30.values, 2.0, scale=0.4)), 2)
    out["max_session_rebuffer_min"] = np.round(max_reb, 2)
    out["failed_starts_30d"] = stats.poisson.ppf(pdf.u_fail.values, 0.3 + 0.25 * max_reb).astype(int)
    out["declines_30d"] = np.where(decline_no_retry, 1 + (pdf.u_decl2.values < 0.25),
                                   (pdf.u_decl2.values < 0.06).astype(int)).astype(int)
    out["decline_no_retry"] = decline_no_retry
    out["support_contacts_30d"] = stats.poisson.ppf(pdf.u_supp.values, np.exp(-1.4 + 0.5 * z)).astype(int)
    out["z"] = z
    out["eps"] = special.ndtri(pdf.u_eps.values)
    out["u_label"] = pdf.u_label.values
    out["snapshot_lift"] = pdf.snapshot_lift.values if "snapshot_lift" in pdf else 0.0
    return out


def gen_features(batches):
    for pdf in batches:
        yield features_from_uniforms(pdf)


def with_uniforms(df, key_cols):
    for name in UNIFORMS:
        df = df.withColumn(name, u(name, *key_cols))
    return df


# Risk model, kept as SQL so 04 evaluates the same expressions on the scoring cohort.
BASE_SQL = " + ".join([
    f"{LN['rebuffer_gt_5min']} * pow(least(max_session_rebuffer_min / 5.0, 1.0), 1.5)",
    f"CASE WHEN decline_no_retry THEN {LN['decline_no_retry']} ELSE 0 END",
    f"CASE WHEN event_only_signup AND tenure_days < 30 THEN {LN['event_only_signup_30d']} ELSE 0 END",
    f"CASE WHEN tenure_days < 30 THEN {LN['tenure_lt_30d']} ELSE 0 END",
    f"CASE WHEN hours_30d * 30.0 / least(greatest(tenure_days, 1), 30) < 5 THEN {LN['low_engagement']} ELSE 0 END",
    f"CASE WHEN is_bundle THEN {LN['bundle']} ELSE 0 END",
])
DOSE_SQL = "0.06 * greatest(max_session_rebuffer_min - 5.0, 0)"   # rebuffer beyond 5 min keeps adding risk
P_SQL = ("least(exp({log_p0} + ({base}) + {qoe_slope} * ({dose}) + {beta_z} * z + {eps_sd} * eps + {extra}), "
         "0.95)")   # extra: snapshot_lift in training, 0 for the scoring snapshot (04)
ORACLE_NOQOE = ["ln(1 + tenure_days)", "CAST(tenure_days < 30 AS INT)",
                "CAST(event_only_signup AND tenure_days < 30 AS INT)", "ln(1 + hours_30d)",
                "CAST(hours_30d * 30.0 / least(greatest(tenure_days, 1), 30) < 5 AS INT)", "engagement_trend",
                "ln(1 + days_since_last_session)", "pauses_90d", "support_contacts_30d",
                "CAST(decline_no_retry AS INT)", "CAST(is_bundle AS INT)", "declines_30d", "active_days_30d / 30.0",
                "CAST(plan_price AS DOUBLE)"]
ORACLE_QOE = ["ln(1 + rebuffer_min_30d)", "ln(1 + max_session_rebuffer_min)", "least(max_session_rebuffer_min, 5)",
              "CAST(max_session_rebuffer_min >= 5 AS INT)", "greatest(max_session_rebuffer_min - 5, 0)",
              "failed_starts_30d"]

# COMMAND ----------
# Training panel: 200k subscribers x 6 monthly snapshots. Historical accounts (ids 9e9+), many of which
# have since churned, so they are deliberately not rows of today's dim_subscribers.

SNAPSHOTS = [GAME_DATE - timedelta(days=30 * k) for k in range(P.TRAIN_SNAPSHOTS, 0, -1)]
snap_map = F.element_at(F.array(*[F.lit(d) for d in SNAPSHOTS]), (F.col("id") / P.TRAIN_COHORT).cast("int") + 1)
# Churn runs a little higher or lower month to month (log-lift per snapshot; the overall mean stays 5%).
SNAPSHOT_LIFT = [0.92, 1.06, 0.97, 1.10, 0.94, 1.02]
lift_map = F.element_at(F.array(*[F.lit(math.log(x)) for x in SNAPSHOT_LIFT]),
                        (F.col("id") / P.TRAIN_COHORT).cast("int") + 1)
panel = (spark.range(P.TRAIN_COHORT * P.TRAIN_SNAPSHOTS, numPartitions=48)  # noqa: F821
         .withColumn("subscriber_id", 9_000_000_000 + F.col("id") % P.TRAIN_COHORT)
         .withColumn("snapshot_date", snap_map)
         .withColumn("snapshot_lift", lift_map)
         .withColumn("tenure_days", F.when(u("ten_new", "id") < 0.08, 1 + F.floor(u("ten_d", "id") * 29))
                     .otherwise(30 + F.floor(-F.log(u("ten_d", "id")) * 500)).cast("int"))
         .withColumn("is_bundle", u("bundle", "id") < 0.08)
         .withColumn("plan_price", F.when(F.col("is_bundle"), 16.99).otherwise(
             F.element_at(F.array(*[F.lit(x) for x in (11.99, 11.99, 11.99, 11.99, 11.99, 11.99, 11.99, 9.99,
                                                       7.99, 17.99)]),
                          (F.floor(u("price", "id") * 10) + 1).cast("int"))).cast("decimal(6,2)"))
         .withColumn("signup_source", F.when(F.col("is_bundle"), "bundle_partner")
                     .when((F.col("tenure_days") < 30) & (u("src", "id") < 0.55), "event_promo")
                     .when(u("src", "id") < 0.12, "event_promo").when(u("src2", "id") < 0.13, "winback")
                     .otherwise("organic"))
         # decoy: event-promo sign-ups often pay a promo price (correlated with a driver, no effect itself)
         .withColumn("plan_price", F.when((F.col("signup_source") == "event_promo") & ~F.col("is_bundle")
                                          & (u("promo_price", "id") < 0.5), F.lit(9.99).cast("decimal(6,2)"))
                     .otherwise(F.col("plan_price"))))
panel = with_uniforms(panel, ["id"]).drop("id")
panel_feats = (panel.mapInPandas(gen_features, QOE_SCHEMA)
               .withColumn("base", F.expr(BASE_SQL)).withColumn("dose", F.expr(DOSE_SQL)))
for i, e in enumerate(ORACLE_NOQOE + ORACLE_QOE):
    panel_feats = panel_feats.withColumn(f"o{i}", F.expr(e).cast("double"))
write(panel_feats, "gen_training_panel_tmp")
panel_feats = spark.table(cfg.fq("gen_training_panel_tmp"))  # noqa: F821

# COMMAND ----------
# Calibration loop on a 200k sample: bisect beta_z for full AUC, outer bisection on the QoE dose slope
# for the no-QoE AUC (more dose -> QoE features matter more -> bigger gap).

N_ORACLE = len(ORACLE_NOQOE) + len(ORACLE_QOE)
sample = panel_feats.where(u("calib", "subscriber_id", "snapshot_date") < 200_000 / panel_feats.count()).select(
    "base", "dose", "z", "eps", "u_label", "snapshot_lift", *[f"o{i}" for i in range(N_ORACLE)]).toPandas()
X_raw = sample[[f"o{i}" for i in range(N_ORACLE)]].to_numpy(dtype=float)
X_MEAN, X_STD = X_raw.mean(0), np.where(X_raw.std(0) > 0, X_raw.std(0), 1.0)
X = (X_raw - X_MEAN) / X_STD
half = len(sample) // 2
nq_idx = list(range(len(ORACLE_NOQOE)))


def labels(beta_z: float, qoe_slope: float):
    lin = (sample.base.values + qoe_slope * sample.dose.values + beta_z * sample.z.values + EPS_SD * sample.eps.values
           + sample.snapshot_lift.values)
    lo, hi = -15.0, 0.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if np.minimum(np.exp(mid + lin), 0.95).mean() > P.TRAIN_PREVALENCE:
            hi = mid
        else:
            lo = mid
    log_p0 = (lo + hi) / 2
    return log_p0, sample.u_label.values < np.minimum(np.exp(log_p0 + lin), 0.95)


def oracle_auc(y, cols=None):
    Xc = X if cols is None else X[:, cols]
    m = LogisticRegression(max_iter=500).fit(Xc[:half], y[:half])
    return roc_auc_score(y[half:], m.predict_proba(Xc[half:])[:, 1])


def fit_beta(qoe_slope: float):
    lo, hi = 0.2, 2.5
    for _ in range(9):
        mid = (lo + hi) / 2
        if oracle_auc(labels(mid, qoe_slope)[1]) < P.AUC_CHAMPION:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


log = []
qs_lo, qs_hi = 0.0, 5.0
for _ in range(7):
    qs = (qs_lo + qs_hi) / 2
    bz = fit_beta(qs)
    y = labels(bz, qs)[1]
    a_full, a_nq = oracle_auc(y), oracle_auc(y, nq_idx)
    log.append((qs, bz, a_full, a_nq))
    print(f"qoe_slope={qs:.3f} beta_z={bz:.3f} auc_full={a_full:.4f} auc_noqoe={a_nq:.4f}")
    if a_nq > P.AUC_CHALLENGER:
        qs_lo = qs
    else:
        qs_hi = qs
QOE_SLOPE, BETA_Z, *_ = min(log, key=lambda r: abs(r[2] - P.AUC_CHAMPION) + abs(r[3] - P.AUC_CHALLENGER))
LOG_P0, y_cal = labels(BETA_Z, QOE_SLOPE)
oracle = LogisticRegression(max_iter=1000).fit(X, y_cal)   # on standardized features (mean/std stored)

calib = {"beta_z": BETA_Z, "qoe_slope": QOE_SLOPE, "log_p0": LOG_P0, "eps_sd": EPS_SD, "proxy_noise": PROXY_NOISE,
         "qoe_prevalence": QOE_PREVALENCE, "base_sql": BASE_SQL, "dose_sql": DOSE_SQL, "p_sql": P_SQL,
         "oracle_features": ORACLE_NOQOE + ORACLE_QOE, "oracle_coef": oracle.coef_[0].tolist(),
         "oracle_mean": X_MEAN.tolist(), "oracle_std": X_STD.tolist(),
         "oracle_intercept": float(oracle.intercept_[0]), "search_log": log}
P_EXPR = P_SQL.format(log_p0=LOG_P0, base=BASE_SQL, qoe_slope=QOE_SLOPE, dose=DOSE_SQL, beta_z=BETA_Z, eps_sd=EPS_SD,
                      extra="snapshot_lift")

# COMMAND ----------
# Write the panel features (fs_subscriber_features columns only) and the labels.

labelled = panel_feats.withColumn("p_true", F.expr(P_EXPR)).withColumn("churned_30d", F.col("u_label") < F.col("p_true"))
write(labelled.select("subscriber_id", "snapshot_date", *FEATURE_COLS), "gen_subscriber_features")
write(labelled.select("subscriber_id", "snapshot_date", "churned_30d"), "labels_churn_30d")

# Independent check on a different 200k sample of the written data.
chk = (spark.table(cfg.fq("gen_subscriber_features")).join(spark.table(cfg.fq("labels_churn_30d")),  # noqa: F821
                                                            ["subscriber_id", "snapshot_date"])
       .where(u("verify", "subscriber_id", "snapshot_date") < 200_000 / (P.TRAIN_COHORT * P.TRAIN_SNAPSHOTS)))
for i, e in enumerate(ORACLE_NOQOE + ORACLE_QOE):
    chk = chk.withColumn(f"o{i}", F.expr(e).cast("double"))
chk_pd = chk.select("churned_30d", *[f"o{i}" for i in range(N_ORACLE)]).toPandas()
Xv = chk_pd[[f"o{i}" for i in range(N_ORACLE)]].to_numpy(float)
Xv = (Xv - Xv.mean(0)) / np.where(Xv.std(0) > 0, Xv.std(0), 1)
yv = chk_pd.churned_30d.values
hv = len(yv) // 2


def auc_v(cols):
    m = LogisticRegression(max_iter=500).fit(Xv[:hv][:, cols], yv[:hv])
    return roc_auc_score(yv[hv:], m.predict_proba(Xv[hv:][:, cols])[:, 1])


AUC_FULL, AUC_NOQOE = auc_v(list(range(N_ORACLE))), auc_v(nq_idx)
prev = float(yv.mean())
qoe_prev = float((chk_pd[f"o{len(ORACLE_NOQOE) + 3}"] > 0).mean())
calib.update({"verify_auc_full": AUC_FULL, "verify_auc_noqoe": AUC_NOQOE, "verify_prevalence": prev,
              "verify_qoe_prevalence": qoe_prev})
write(spark.createDataFrame([(json.dumps(calib),)], "params_json STRING")  # noqa: F821
      .withColumn("created_ts", F.current_timestamp()), "gen_calibration")
spark.sql(f"DROP TABLE IF EXISTS {cfg.fq('gen_training_panel_tmp')}")  # noqa: F821
print(f"calibrated beta_z={BETA_Z:.3f} qoe_slope={QOE_SLOPE:.3f}; verify AUC full={AUC_FULL:.4f} "
      f"no-QoE={AUC_NOQOE:.4f} prevalence={prev:.3%} QoE exposure={qoe_prev:.1%}")

# COMMAND ----------
# Pre-game state for every Sports and Bundle subscriber still on the books (as of the day after), from the same recipe.
# 04 overrides the game-night columns (QoE, sign-up declines) and evaluates the stored risk SQL.

pre = (spark.table(cfg.fq("dim_subscribers")).where("tier IN ('Sports', 'Bundle') AND status <> 'cancelled'")  # noqa: F821
       .select("subscriber_id", F.lit(DAY_AFTER).alias("snapshot_date"),
               F.greatest(F.datediff(F.lit(DAY_AFTER), F.to_date(F.from_utc_timestamp("signup_ts", P.TZ))), F.lit(0))
               .alias("tenure_days"), "plan_price", "is_bundle", "signup_source"))
pre = with_uniforms(pre, ["subscriber_id", F.lit("pregame")])
write(pre.mapInPandas(gen_features, QOE_SCHEMA), "gen_pregame_features")

# COMMAND ----------
# Self-asserts

fdc = spark.table(cfg.fq("fact_daily_cancels"))  # noqa: F821
evs = spark.table(cfg.fq("fact_subscription_events"))  # noqa: F821
n_days = fdc.select("day").distinct().count()
ev_counts = {(r.day, r.tier): r.n for r in evs.where("event_type = 'cancel'").groupBy(
    F.to_date(F.from_utc_timestamp("event_ts", P.TZ)).alias("day"), "tier").agg(F.count("*").alias("n")).collect()}
mismatch = [(d, t) for d, t, c in window_days[["day", "tier", "cancels"]].itertuples(index=False)
            if ev_counts.get((d, t), 0) != c]
same_wd_now = daily_df[(daily_df.tier == "Sports") & (daily_df.day < DAY_AFTER) & (~daily_df.is_big_event_next_day)
                       & (daily_df.day.map(lambda d: d.weekday()) == DAY_AFTER.weekday())].tail(6)
ev_rates = {eid: ev_quota[eid] / ep_evening[ev_night[eid]] for eid in ev_quota}
hourly_n = spark.table(cfg.fq("fact_concurrency_hourly")).count()  # noqa: F821
ev5_n = spark.table(cfg.fq("fact_concurrency_event_5min")).count()  # noqa: F821
print(f"fact_daily_cancels days={n_days}; Sports same-weekday baseline={same_wd_now.cancels.mean():,.0f}; "
      f"event-only rate big game={EVENT_ONLY_RATE:.4f}, prior nights={ev_rates}")
print(f"fact_concurrency_hourly={hourly_n:,}  fact_concurrency_event_5min={ev5_n:,}  "
      f"fact_subscription_events={evs.count():,}")

assert n_days == 548
assert not mismatch, f"event counts differ from fact_daily_cancels: {mismatch[:5]}"
assert abs(same_wd_now.cancels.mean() - BASELINE_MEAN["Sports"]) < 1
assert round(P.CANCELS_OBSERVED / same_wd_now.cancels.mean() - 1, 2) == P.CANCELS_PCT_OVER
assert all(abs(r / EVENT_ONLY_RATE - 1) <= 0.08 for r in ev_rates.values()), ev_rates   # same rate, +/- a little
snap_prev = {str(r.snapshot_date): r.p for r in spark.table(cfg.fq("labels_churn_30d")).groupBy(  # noqa: F821
    "snapshot_date").agg(F.avg(F.col("churned_30d").cast("double")).alias("p")).collect()}
print("training prevalence by snapshot:", {k: round(v, 4) for k, v in sorted(snap_prev.items())})
assert max(snap_prev.values()) - min(snap_prev.values()) > 0.005, "prevalence should vary across snapshots"
assert spark.table(cfg.fq("fact_billing_attempts")).where("decline_code = 'processor_timeout'").count() == 0  # noqa: F821
assert abs(AUC_FULL - P.AUC_CHAMPION) <= P.AUC_TOL, f"full-model AUC {AUC_FULL:.4f}"
assert abs(AUC_NOQOE - P.AUC_CHALLENGER) <= P.AUC_TOL, f"no-QoE AUC {AUC_NOQOE:.4f}"
assert AUC_FULL - AUC_NOQOE >= P.AUC_MIN_DELTA
assert abs(prev - P.TRAIN_PREVALENCE) < 0.005
assert P.QOE_EXPOSURE_PREVALENCE[0] - 0.01 <= qoe_prev <= P.QOE_EXPOSURE_PREVALENCE[1] + 0.01
print("03_gen_history_facts: all self-asserts passed")
