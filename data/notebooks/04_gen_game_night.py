# Databricks notebook source
# Owner: Stream A (Data + Pipelines). The Big Game night and the day after (design section 6.3).
#
#  * CDN telemetry JSON (10 s on game night, 1 min on the two prior game nights, hourly snapshots for
#    14 days) and session-event JSON into raw_landing/{cdn,sessions}/dt=/hh=/, plus a copy of game
#    night in raw_landing/replay_staged/ for the replayer. ~3% late (ingest up to 90 s), ~1% null
#    app_version on telemetry, ~0.2% duplicate rows (the pipeline dedupes).
#  * Halftime surge 4.0M -> 9.5M; incident CDN-C x DFW x Roku 7.2.0 from 20:17 (ops shift 20:26,
#    resolved 20:36); clean controls; CDN-F Seattle decoy inside its chronic noise.
#  * Acquirer B processor_timeout 3% -> 17% for 20:16-20:40 sign-ups, never retried, 24 h grace.
#  * Day-after Sports cancels 15,800 = 9,000 baseline + 2,300 playback + 2,000 payment + 2,050
#    event-only + 450 unexplained, with the state split, kids-only and reactivated accounts planted.
#  * Ledger (Sports 0.25%, Originals 6%), renewals, eval_gt_incident / eval_gt_cancel_cause /
#    eval_gt_churn_future, and the 310k post-game scoring snapshot in gen_subscriber_features.
# Every planted number is asserted at the end, computed from the generated rows.

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

from cc_common import config, planted as P, seeds  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
rng = seeds.rng(cfg.seed, "04")
ET = ZoneInfo(P.TZ)
GAME_DATE = date.fromisoformat(cfg.game_date)
DAY_AFTER = GAME_DATE + timedelta(days=1)
W_START = GAME_DATE - timedelta(days=14)


def u(salt: str, *cols):
    """Deterministic U(0,1) per row from (seed, salt, cols); independent of partitioning."""
    h = F.xxhash64(F.lit(cfg.seed), F.lit(salt), *cols)
    return (F.pmod(h, F.lit(1 << 40)).cast("double") + 0.5) / float(1 << 40)


def normal(salt: str, *cols):
    return F.sqrt(-2 * F.log(u(salt + "_a", *cols))) * F.cos(2 * math.pi * u(salt + "_b", *cols))


def pick(uc, items: dict):
    total, acc, expr = sum(items.values()), 0.0, None
    keys = list(items)
    for k in keys[:-1]:
        acc += items[k] / total
        expr = F.when(uc < acc, F.lit(k)) if expr is None else expr.when(uc < acc, F.lit(k))
    return expr.otherwise(F.lit(keys[-1]))


def et_to_utc(d: date, seconds: float = 0) -> datetime:
    local = datetime(d.year, d.month, d.day, tzinfo=ET) + timedelta(seconds=float(seconds))
    return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def clk(hhmm: str) -> int:
    """Seconds after 18:00 ET (the replay T0) for an ET clock label on game night."""
    h, m = (int(x) for x in hhmm.split(":"))
    return (h - 18) * 3600 + m * 60


def write(df, name: str, mode: str = "overwrite"):
    w = df.write.mode(mode)
    if mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(cfg.fq(name))


def tmp(df, name: str):
    """Materialize an intermediate (serverless has no cache); dropped at the end of the notebook."""
    write(df, name)
    return spark.table(cfg.fq(name))  # noqa: F821


T0 = et_to_utc(GAME_DATE, 18 * 3600)
T0_LIT = F.lit(T0).cast("timestamp")
DAY_AFTER_T0 = et_to_utc(DAY_AFTER)
DAY_AFTER_END = et_to_utc(DAY_AFTER + timedelta(days=1))
ONSET, RAMP_END, SHIFT, RESOLVED = (clk(P.INCIDENT[k]) for k in ("onset", "ramp_end", "shift_approved", "resolved"))
DECOY_LO, DECOY_HI = clk(P.DECOY["start"]), clk(P.DECOY["end"])
ACQ_LO, ACQ_HI = clk(P.ACQUIRER_B["window"][0]), clk(P.ACQUIRER_B["window"][1]) + 60
NIGHT_S = clk(P.CLOCK["end"])
COHORT = {k: P.INCIDENT[k] for k in ("cdn", "metro", "device_family", "app_version")}
BACKUP = dict(COHORT, cdn=P.BACKUP_CELL["cdn"])
DECAY_TAU_S = 3.4 * 60      # post-shift recovery; crosses 1.5x baseline in the 20:36 minute
SHIFT_RAMP_S = 8 * 60       # sessions reconnect to CDN-A over ~8 minutes after the 20:26 approval
SHIFT_MAX = 0.97
# Sign-up grace before an unpaid account auto-cancels: 8-10 h, so the timed-out halftime sign-ups cancel
# at 04:15-06:50 ET and are in the 07:00 morning-after picture. (planted.ACQUIRER_B["grace_hours"] is 24:
# reported to the lead for update together with seed/docs/payment_retry_policy.md section 2.)
GRACE_LO_H, GRACE_HI_H = 8.0, 10.0
assert cfg.scale_subscribers == P.N_SUBSCRIBERS, "planted day-after counts assume 4,000,000 subscribers"

# Driver counts: planted value +/- under 1% (seeded), with unexplained = what is left above the baseline,
# kept within 0.5 pt of the planted 7%. The governed total stays planted.CANCELS_OBSERVED.
fdc_hist = spark.table(cfg.fq("fact_daily_cancels")).where(F.col("day") < F.lit(DAY_AFTER)).toPandas()  # noqa: F821
fdc_hist["wd"] = pd.to_datetime(fdc_hist.day).dt.weekday
wd_mean = fdc_hist[(fdc_hist.wd == DAY_AFTER.weekday()) & ~fdc_hist.is_big_event_next_day].groupby(
    "tier").cancels.apply(lambda x: x.tail(6).mean())
BASELINE_EXPECTED = float(wd_mean["Sports"])
EXCESS = P.CANCELS_OBSERVED - BASELINE_EXPECTED
while True:
    jit = {k: float(rng.uniform(-0.009, 0.009)) for k in ("payment", "playback", "event_only")}
    drv = {k: round(P.CANCEL_DRIVERS[k][1] * (1 + jit[k])) for k in jit}
    unexpl = P.CANCELS_OBSERVED - round(BASELINE_EXPECTED) - sum(drv.values())
    if abs(unexpl / EXCESS - P.CANCEL_DRIVERS["unexplained"][0]) <= 0.005:
        break
print(f"baseline expected {BASELINE_EXPECTED:,.1f}; drivers {drv}; unexplained {unexpl}")

cell_mix = spark.table(cfg.fq("gen_cell_mix"))  # noqa: F821
events_pd = spark.table(cfg.fq("gen_events")).toPandas()  # noqa: F821
curve_pd = spark.table(cfg.fq("gen_event_curve")).toPandas().sort_values("offset_s")  # noqa: F821
calib = json.loads(spark.table(cfg.fq("gen_calibration")).first().params_json)  # noqa: F821
target = events_pd[events_pd.is_target].iloc[0]
prior_nights = events_pd[(~events_pd.is_target) & (events_pd.event_date >= W_START)]
subs = spark.table(cfg.fq("dim_subscribers"))  # noqa: F821
# Day-after cancels written by an earlier run of this notebook count as still active (idempotent re-run).
alive = (F.col("status") != "cancelled") | (F.col("canceled_ts") >= F.lit(DAY_AFTER_T0))

# COMMAND ----------
# Game-night sign-ups and their billing. Exactly planted.CANCEL_DRIVERS["payment"] Acquirer-B
# sign-ups in the window time out (processor_timeout, no retry, 24 h grace); 3% of the B window
# sign-ups decline normally (retried); other sign-ups decline at their acquirer's 3-4% and are retried.

N_PAY = drv["payment"]
N_RECOVERED = 37            # timed-out sign-ups who re-entered a card themselves within minutes (no cancel)
N_TIMEOUT = N_PAY + N_RECOVERED
N_OVERLAP = round(N_PAY * P.PAYMENT_PLAYBACK_OVERLAP_MAX * 0.8)   # ~80: payment and playback, kept < 5%
DECLINE_RATE = {"A": 0.032, "B": P.ACQUIRER_B["decline_rate_baseline"], "C": 0.036, "D": 0.039}
DECLINE_CODES = {"insufficient_funds": .45, "do_not_honor": .30, "expired_card": .15, "card_velocity": .10}
rate_map = F.create_map(*[x for k, v in DECLINE_RATE.items() for x in (F.lit(k), F.lit(v))])

gn = (subs.where(F.col("signup_ts") >= T0_LIT)
      .withColumn("t_s", F.unix_timestamp("signup_ts") - F.unix_timestamp(T0_LIT))
      .withColumn("in_b_window", (F.col("acquirer") == "B") & (F.col("t_s") >= ACQ_LO) & (F.col("t_s") < ACQ_HI))
      .withColumn("rk_b", F.when(F.col("in_b_window"), F.row_number().over(
          Window.partitionBy("in_b_window").orderBy(u("timeout", "subscriber_id"))))))
n_b_window = gn.where("in_b_window").count()
n_b_normal = int(round(n_b_window * P.ACQUIRER_B["decline_rate_baseline"]))
gn = (gn.withColumn("timeout", F.coalesce(F.col("rk_b") <= N_TIMEOUT, F.lit(False)))
      .withColumn("declined_normal", F.when(F.col("in_b_window"), (F.col("rk_b") > N_TIMEOUT)
                                            & (F.col("rk_b") <= N_TIMEOUT + n_b_normal))
                  .otherwise(u("gn_dec", "subscriber_id") < F.element_at(rate_map, F.col("acquirer"))))
      .withColumn("rk_ov", F.when(F.col("timeout") & (F.col("t_s") < clk("20:19")), F.row_number().over(
          Window.partitionBy("timeout", F.col("t_s") < clk("20:19")).orderBy(u("overlap", "subscriber_id")))))
      .withColumn("overlap", F.coalesce(F.col("rk_ov") <= N_OVERLAP, F.lit(False)))
      .withColumn("rk_rec", F.when(F.col("timeout") & ~F.col("overlap"), F.row_number().over(
          Window.partitionBy("timeout", "overlap").orderBy(u("recover", "subscriber_id")))))
      .withColumn("recovered", F.coalesce(F.col("rk_rec") <= N_RECOVERED, F.lit(False)))
      .withColumn("grace_expires_ts", F.col("signup_ts") + F.make_interval(
          secs=(GRACE_LO_H + (GRACE_HI_H - GRACE_LO_H) * u("grace", "subscriber_id")) * 3600))
      .drop("rk_b", "rk_ov", "rk_rec"))
gn = tmp(gn, "gen_gn_signups_tmp")

# COMMAND ----------
# The three sample subscribers (planted.SAMPLE_SUBSCRIBERS): real rows chosen deterministically, then
# given exactly the story the planted record tells. All stay active, non-kids, never cancelled.
SAMPLES = {sp["card_id"]: sp for sp in P.SAMPLE_SUBSCRIBERS}
DAYS_PER_MONTH = 30.44
signup_day = F.to_date(F.from_utc_timestamp("signup_ts", P.TZ))


def tenure_between(months: int, slack_days: int):
    d = F.datediff(F.lit(GAME_DATE), signup_day)
    return (d >= months * DAYS_PER_MONTH - slack_days) & (d <= months * DAYS_PER_MONTH + slack_days)


def first_by_hash(df, label: str):
    row = df.orderBy(u("sample", "subscriber_id")).select("subscriber_id").first()
    if row is None:
        raise RuntimeError(f"No candidate subscriber for the {label} sample")
    return int(row.subscriber_id)


not_kids = alive & ~F.col("is_kids_only_account") & (F.col("status") == "active")
SAMPLE_ID = {
    "playback": first_by_hash(subs.where(not_kids & (F.col("tier") == SAMPLES["playback"]["tier"])
                                         & (F.col("metro") == SAMPLES["playback"]["metro"])
                                         & tenure_between(SAMPLES["playback"]["tenure_months"], 10)), "playback"),
    # Priya renews on game day: sign-up day of month = game day, about 9 months ago
    "payment": first_by_hash(subs.where(not_kids & (F.col("tier") == SAMPLES["payment"]["tier"])
                                        & (F.col("metro") == SAMPLES["payment"]["metro"])
                                        & (F.dayofmonth(signup_day) == GAME_DATE.day)
                                        & tenure_between(SAMPLES["payment"]["tenure_months"], 35)), "payment"),
    "onboarding": first_by_hash(gn.where(~F.col("is_kids_only_account") & (F.col("metro") == SAMPLES["onboarding"]["metro"])
                                         & (F.col("signup_source") == "event_promo") & ~F.col("timeout")
                                         & ~F.col("declined_normal")), "onboarding"),
}
SAMPLE_IDS = list(SAMPLE_ID.values())
not_sample = ~F.col("subscriber_id").isin(SAMPLE_IDS)
MARCUS, PRIYA, JORDAN = SAMPLE_ID["playback"], SAMPLE_ID["payment"], SAMPLE_ID["onboarding"]
MARCUS_REBUFFER_S = 14 * 60                       # "14 minutes of buffering during the halftime show"
SAMPLE_DEVICE = {MARCUS: "Roku", PRIYA: "SmartTV", JORDAN: "iOS"}
print("sample subscribers:", SAMPLE_ID)

ATT = {"signup": 10_000_000_000, "signup_retry": 20_000_000_000, "renewal": 30_000_000_000,
       "renewal_retry": 40_000_000_000}
gn_bill = gn.select(
    (F.col("subscriber_id") + ATT["signup"]).alias("attempt_id"), "subscriber_id", F.col("signup_ts").alias("attempt_ts"),
    F.lit("signup").alias("attempt_type"), "tier", F.col("plan_price").alias("amount_usd"), "acquirer",
    F.when(F.col("timeout") | F.col("declined_normal"), "declined").otherwise("approved").alias("result"),
    F.when(F.col("timeout"), F.lit(P.ACQUIRER_B["decline_code"]))
    .when(F.col("declined_normal"), pick(u("gn_code", "subscriber_id"), DECLINE_CODES)).alias("decline_code"),
    F.lit(None).cast("bigint").alias("retry_of"),
    F.when(F.col("timeout"), F.lit(P.ACQUIRER_B["retry_scheduled"])).otherwise(F.col("declined_normal"))
    .alias("retry_scheduled"),
    F.when(F.col("timeout") | F.col("declined_normal"), F.col("grace_expires_ts")).alias("grace_expires_ts"))
gn_retry = gn.where(F.col("declined_normal") | F.col("recovered")).select(
    (F.col("subscriber_id") + ATT["signup_retry"]).alias("attempt_id"), "subscriber_id",
    # scheduled retry after 1 h (normal declines); recovered timeouts: the customer re-entered a card
    F.when(F.col("recovered"), F.col("signup_ts") + F.make_interval(secs=300 + 3000 * u("rec_t", "subscriber_id")))
    .otherwise(F.col("signup_ts") + F.expr("INTERVAL 1 HOUR")).alias("attempt_ts"), F.lit("retry").alias("attempt_type"),
    "tier", F.col("plan_price").alias("amount_usd"), "acquirer", F.lit("approved").alias("result"),
    F.lit(None).cast("string").alias("decline_code"), (F.col("subscriber_id") + ATT["signup"]).alias("retry_of"),
    F.lit(False).alias("retry_scheduled"), F.lit(None).cast("timestamp").alias("grace_expires_ts"))

# Renewals on game day and the day after (billing runs 02:00-06:00 ET), declines retried the next day.
renew = (subs.where(alive & not_sample & (F.col("signup_ts") < F.lit(et_to_utc(GAME_DATE))))
         .withColumn("dom", F.dayofmonth(F.from_utc_timestamp("signup_ts", P.TZ)))
         .withColumn("rday", F.when(F.col("dom") == GAME_DATE.day, F.lit(GAME_DATE))
                     .when(F.col("dom") == DAY_AFTER.day, F.lit(DAY_AFTER)))
         .where(F.col("rday").isNotNull())
         .withColumn("attempt_ts", F.to_utc_timestamp(F.to_timestamp("rday") + F.make_interval(
             secs=(2 + 4 * u("rn_t", "subscriber_id")) * 3600), P.TZ))
         .withColumn("declined", u("rn_dec", "subscriber_id") < F.element_at(rate_map, F.col("acquirer"))))
renew_bill = renew.select(
    (F.col("subscriber_id") + ATT["renewal"]).alias("attempt_id"), "subscriber_id", "attempt_ts",
    F.lit("renewal").alias("attempt_type"), "tier", F.col("plan_price").alias("amount_usd"), "acquirer",
    F.when(F.col("declined"), "declined").otherwise("approved").alias("result"),
    F.when(F.col("declined"), pick(u("rn_code", "subscriber_id"), DECLINE_CODES)).alias("decline_code"),
    F.lit(None).cast("bigint").alias("retry_of"), F.col("declined").alias("retry_scheduled"),
    F.when(F.col("declined"), F.col("attempt_ts") + F.expr("INTERVAL 7 DAYS")).alias("grace_expires_ts"))
renew_retry = renew.where(F.col("declined") & (F.col("rday") == F.lit(GAME_DATE))).select(
    (F.col("subscriber_id") + ATT["renewal_retry"]).alias("attempt_id"), "subscriber_id",
    (F.col("attempt_ts") + F.expr("INTERVAL 1 DAY")).alias("attempt_ts"), F.lit("retry").alias("attempt_type"),
    "tier", F.col("plan_price").alias("amount_usd"), "acquirer",
    F.when(u("rn_retry", "subscriber_id") < 0.6, "approved").otherwise("declined").alias("result"),
    F.when(u("rn_retry", "subscriber_id") >= 0.6, pick(u("rn_code2", "subscriber_id"), DECLINE_CODES))
    .alias("decline_code"), (F.col("subscriber_id") + ATT["renewal"]).alias("retry_of"),
    (u("rn_retry", "subscriber_id") >= 0.6).alias("retry_scheduled"),
    F.when(u("rn_retry", "subscriber_id") >= 0.6, F.col("attempt_ts") + F.expr("INTERVAL 6 DAYS"))
    .alias("grace_expires_ts"))

# Priya: her game-day renewal hit the same Acquirer-B overload at 8:20 PM, failed twice with
# processor_timeout, and was never retried (7-day renewal grace, so she has not cancelled yet).
priya_row = subs.where(F.col("subscriber_id") == PRIYA).select("subscriber_id", "tier", "plan_price").first()
priya_t1 = T0 + timedelta(seconds=clk("20:20") + 14)
priya_bill = spark.createDataFrame([  # noqa: F821
    (PRIYA + ATT["renewal"], PRIYA, priya_t1, "renewal", priya_row.tier, priya_row.plan_price, "B", "declined",
     P.ACQUIRER_B["decline_code"], None, False, priya_t1 + timedelta(days=7)),
    (PRIYA + ATT["renewal_retry"], PRIYA, priya_t1 + timedelta(seconds=49), "retry", priya_row.tier,
     priya_row.plan_price, "B", "declined", P.ACQUIRER_B["decline_code"], PRIYA + ATT["renewal"], False,
     priya_t1 + timedelta(days=7))],
    "attempt_id BIGINT, subscriber_id BIGINT, attempt_ts TIMESTAMP, attempt_type STRING, tier STRING, "
    "amount_usd DECIMAL(6,2), acquirer STRING, result STRING, decline_code STRING, retry_of BIGINT, "
    "retry_scheduled BOOLEAN, grace_expires_ts TIMESTAMP")

# COMMAND ----------
# Incident cohort viewers: existing Sports subscribers in DFW watching on Roku 7.2.0 via CDN-C at halftime
# (one per concurrent stream at the onset), plus the planted overlap of timed-out sign-ups.
# ~25% are severely hit (>= 5 min of rebuffering in one session); the rest see 10-100 s.

N_VIEWERS = P.SHIFT_LOAD
SEVERE_SHARE = 0.25
viewers = (subs.where(alive & (F.col("tier") == "Sports") & (F.col("metro") == COHORT["metro"])
                      & (F.col("signup_ts") < F.lit(et_to_utc(GAME_DATE - timedelta(days=2)))))
           .withColumn("rk", F.row_number().over(Window.orderBy(u("viewer", "subscriber_id"))))
           .where(F.col("rk") <= N_VIEWERS)
           .select("subscriber_id", "state", "is_kids_only_account", F.lit(False).alias("is_signup"),
                   (u("severe", "subscriber_id") < SEVERE_SHARE).alias("severe"), F.lit(6300).alias("t_avail_s")))
viewers = viewers.where(F.col("subscriber_id") != MARCUS).unionByName(
    subs.where(F.col("subscriber_id") == MARCUS).select(
        "subscriber_id", "state", "is_kids_only_account", F.lit(False).alias("is_signup"), F.lit(True).alias("severe"),
        F.lit(6300).alias("t_avail_s")))
viewers = viewers.unionByName(gn.where("overlap").select(
    "subscriber_id", "state", "is_kids_only_account", F.lit(True).alias("is_signup"), F.lit(True).alias("severe"),
    (F.col("t_s") + 60).cast("int").alias("t_avail_s")))
viewers = tmp(viewers.withColumn("t_mig_s", F.when(F.col("subscriber_id") == MARCUS, SHIFT + SHIFT_RAMP_S).otherwise(
    SHIFT + 30 + (SHIFT_RAMP_S - 30) * u("mig", "subscriber_id")).cast("int")), "gen_viewers_tmp")

# COMMAND ----------
# Day-after Sports cancels.

N_PLAYBACK, N_EVENT_ONLY, N_UNEXPLAINED = drv["playback"], drv["event_only"], unexpl
N_BASELINE = P.CANCELS_OBSERVED - N_PLAYBACK - N_PAY - N_EVENT_ONLY - N_UNEXPLAINED
KIDS_TOTAL, REACTIVATED = P.KIDS_ONLY_IN_SEGMENT, P.REACTIVATED_IN_SEGMENT
# Retro segment = payment cancels + cancels by any cohort viewer (gross 4,700, of which 160 kids-only
# and 290 reactivated, eligible 4,250). With 2,000 payment and 2,300 playback that leaves 400 baseline
# cancels by mildly affected viewers (< 5 min, so not playback by the rule).
N_MILD_CANCEL = P.SEGMENT_GROSS - N_PAY - N_PLAYBACK
KIDS_PLAYBACK = int(round(KIDS_TOTAL * N_PLAYBACK / (N_PLAYBACK + N_MILD_CANCEL)))
KIDS_MILD = KIDS_TOTAL - KIDS_PLAYBACK
assert N_PAY + N_PLAYBACK + N_MILD_CANCEL - KIDS_TOTAL - REACTIVATED == P.SEGMENT_ELIGIBLE

ranked_viewers = (viewers.where(~F.col("is_signup") & not_sample)
                  .withColumn("rk", F.row_number().over(Window.partitionBy("severe", "is_kids_only_account")
                                                         .orderBy(u("pb_cancel", "subscriber_id")))))
quota_v = (F.when(F.col("severe") & F.col("is_kids_only_account"), KIDS_PLAYBACK)
           .when(F.col("severe"), N_PLAYBACK - KIDS_PLAYBACK)
           .when(F.col("is_kids_only_account"), KIDS_MILD).otherwise(N_MILD_CANCEL - KIDS_MILD))
viewer_cancels = (ranked_viewers.where(F.col("rk") <= quota_v)
                  .select("subscriber_id", "state", "is_kids_only_account",
                          F.when(F.col("severe"), "playback").otherwise("baseline").alias("cause")))

ep_pool = (gn.where((F.col("signup_source") == "event_promo") & ~F.col("timeout") & not_sample)
           .withColumn("rk", F.row_number().over(Window.orderBy(u("ev_cancel", "subscriber_id")))))
event_cancels = ep_pool.where(F.col("rk") <= N_EVENT_ONLY).select("subscriber_id", "state", "signup_ts",
                                                                  F.lit("event_only").alias("cause"))
pay_cancels = gn.where(F.col("timeout") & ~F.col("recovered")).select(
    "subscriber_id", "state", "signup_ts", "grace_expires_ts", "overlap", F.lit("payment").alias("cause"))

# Baseline + unexplained cancels by state quota so the day-after state split matches planted.CANCELS_BY_STATE.
TX = P.METRO_STATE[COHORT["metro"]]
fixed_by_state = {}
for df in (pay_cancels, event_cancels, viewer_cancels):
    for r in df.groupBy("state").count().collect():
        fixed_by_state[r.state] = fixed_by_state.get(r.state, 0) + r["count"]
state_quota = {s: P.CANCELS_BY_STATE[s] - fixed_by_state.get(s, 0) for s in P.CANCELS_BY_STATE}
assert min(state_quota.values()) >= 0, state_quota
assert sum(state_quota.values()) == N_BASELINE - N_MILD_CANCEL + N_UNEXPLAINED
qmap = F.create_map(*[x for s, q in state_quota.items() for x in (F.lit(s), F.lit(q))])
excluded = viewers.select("subscriber_id")
base_pool = (subs.where(alive & not_sample & (F.col("tier") == "Sports")
                        & (F.col("signup_ts") < F.lit(et_to_utc(GAME_DATE - timedelta(days=2)))))
             .join(excluded, "subscriber_id", "left_anti")
             .withColumn("rk", F.row_number().over(Window.partitionBy("state").orderBy(u("bl_cancel", "subscriber_id"))))
             .where(F.col("rk") <= F.element_at(qmap, F.col("state")))
             .withColumn("rk2", F.row_number().over(Window.orderBy(u("unexpl", "subscriber_id"))))
             .select("subscriber_id", "state", F.when(F.col("rk2") <= N_UNEXPLAINED, "unexplained")
                     .otherwise("baseline").alias("cause")))

REASONS_VOL = {"too_expensive": .30, "not_watching": .22, "content_finished": .18, "switching_service": .12,
               "technical_issues": .08, P.PLAYBACK_SURVEY_REASON: P.PLAYBACK_SURVEY_SHARE[1], "other": .06}
REASONS_NO_BUFFER = {k: v for k, v in REASONS_VOL.items() if k != P.PLAYBACK_SURVEY_REASON}
REASONS_EVENT = {"event_over": .55, "too_expensive": .25, "not_watching": .15, "other": .05}
INVOLUNTARY_SHARE = 0.12
N_PB_SURVEY = int(round(N_PLAYBACK * P.PLAYBACK_SURVEY_SHARE[0]))


def secs_after(day_utc_start: datetime, lo_h: float, hi_h: float, salt: str):
    return F.lit(day_utc_start).cast("timestamp") + F.make_interval(
        secs=(lo_h + (hi_h - lo_h) * u(salt, "subscriber_id")) * 3600)


# Ordinary cancels over an ET day: a smooth curve with ~30% before 07:00 (overnight billing runs, app
# cancels), drawn by inverse CDF so the hourly shape has no steps.
CANCEL_HOUR_W = [4, 4, 4, 5, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4]
_cdf = np.concatenate([[0], np.cumsum(CANCEL_HOUR_W) / sum(CANCEL_HOUR_W)])
CANCEL_HOUR_Q = np.interp(np.linspace(0, 1, 241), _cdf, np.arange(25)).tolist()


def diurnal_ts(day_utc_start: datetime, salt: str):
    q = u(salt, "subscriber_id") * 240
    hour = F.element_at(F.array(*[F.lit(x) for x in CANCEL_HOUR_Q]), F.floor(q).cast("int") + 1) + (
        q - F.floor(q)) * 0.1
    return F.lit(day_utc_start).cast("timestamp") + F.make_interval(secs=F.least(hour, F.lit(23.99)) * 3600)


def night_or_day(day_utc_start: datetime, night_share: float, night_hi: float, day_hi: float, salt: str):
    """Most in [00:15, night_hi) ET (people who woke up to the problem), the rest in [07:00, day_hi)."""
    w = u(salt + "_n", "subscriber_id") < night_share
    return F.when(w, secs_after(day_utc_start, 0.25, night_hi, salt)).otherwise(
        secs_after(day_utc_start, 7, day_hi, salt))


next_midnight = F.lit(DAY_AFTER_T0).cast("timestamp")
all_cancels = (
    pay_cancels.select("subscriber_id", "state", "cause", "overlap",
                       (F.col("grace_expires_ts") + F.make_interval(secs=600 * u("pay_t", "subscriber_id")))
                       .alias("event_ts"),
                       F.lit("involuntary_billing").alias("cancel_type"), F.lit("payment_failed").alias("reason"))
    .unionByName(event_cancels.select(
        "subscriber_id", "state", "cause", F.lit(False).alias("overlap"),
        F.least(night_or_day(DAY_AFTER_T0, 0.8, 7, 17, "ev_t"),
                F.col("signup_ts") + F.expr("INTERVAL 23 HOURS")).alias("event_ts"),
        F.lit("voluntary").alias("cancel_type"), pick(u("ev_r", "subscriber_id"), REASONS_EVENT).alias("reason")))
    .unionByName(viewer_cancels.where("cause = 'playback'")
                 .withColumn("rk", F.row_number().over(Window.orderBy(u("pb_survey", "subscriber_id"))))
                 .select("subscriber_id", "state", "cause", F.lit(False).alias("overlap"),
                         night_or_day(DAY_AFTER_T0, 0.7, 6.75, 13, "pb_t").alias("event_ts"),
                         F.lit("voluntary").alias("cancel_type"),
                         F.when(F.col("rk") <= N_PB_SURVEY, F.lit(P.PLAYBACK_SURVEY_REASON))
                         .otherwise(pick(u("pb_r", "subscriber_id"), REASONS_NO_BUFFER)).alias("reason")))
    .unionByName(viewer_cancels.where("cause = 'baseline'").select("subscriber_id", "state", "cause")
                 .unionByName(base_pool)
                 .withColumn("invol", u("bl_inv", "subscriber_id") < INVOLUNTARY_SHARE)
                 .select("subscriber_id", "state", "cause", F.lit(False).alias("overlap"),
                         F.when(F.col("invol"), secs_after(DAY_AFTER_T0, 2, 6, "bl_t"))
                         .otherwise(diurnal_ts(DAY_AFTER_T0, "bl_t")).alias("event_ts"),
                         F.when(F.col("invol"), "involuntary_billing").otherwise("voluntary").alias("cancel_type"),
                         F.when(F.col("invol"), F.lit("payment_failed"))
                         .otherwise(pick(u("bl_r", "subscriber_id"), REASONS_VOL)).alias("reason"))))
sports_cancels = tmp(all_cancels.withColumn("tier", F.lit("Sports")), "gen_sports_cancels_tmp")

# Reactivated: 290 non-kids cohort-viewer cancellers came back the same day (excluded from the retro plan).
reactivated = (sports_cancels.join(viewers.select("subscriber_id", "is_kids_only_account", "is_signup"), "subscriber_id")
               .where(~F.col("is_kids_only_account") & ~F.col("is_signup"))
               .withColumn("rk", F.row_number().over(Window.orderBy(u("react", "subscriber_id"))))
               .where(F.col("rk") <= REACTIVATED)
               .select("subscriber_id", "tier", F.least(F.col("event_ts") + F.make_interval(
                   secs=(1 + 3 * u("react_t", "subscriber_id")) * 3600), F.lit(DAY_AFTER_END).cast("timestamp")
                   - F.expr("INTERVAL 1 SECOND")).alias("event_ts")))

# Other tiers on the day after: inside their expected range (Originals exactly planted RECON_ORIGINALS).
other_counts = {"Originals": round(P.RECON_ORIGINALS["subscriber_live"] * (1 + rng.uniform(-0.005, 0.005)))}
for tier in ("Family", "Bundle"):
    other_counts[tier] = int(round(wd_mean[tier] * (1 + rng.uniform(-0.01, 0.01))))
other_pool = subs.where(alive & not_sample & F.col("tier").isin(list(other_counts))
                        & (F.col("signup_ts") < F.lit(et_to_utc(GAME_DATE - timedelta(days=2)))))
other_q = F.create_map(*[x for t, n in other_counts.items() for x in (F.lit(t), F.lit(n))])
other_cancels = (other_pool.withColumn("rk", F.row_number().over(
    Window.partitionBy("tier").orderBy(u("ot_cancel", "subscriber_id"))))
    .where(F.col("rk") <= F.element_at(other_q, F.col("tier")))
    .withColumn("invol", u("ot_inv", "subscriber_id") < INVOLUNTARY_SHARE)
    .select("subscriber_id", "tier", F.when(F.col("invol"), secs_after(DAY_AFTER_T0, 2, 6, "ot_t"))
            .otherwise(diurnal_ts(DAY_AFTER_T0, "ot_t")).alias("event_ts"),
            F.when(F.col("invol"), "involuntary_billing").otherwise("voluntary").alias("cancel_type"),
            F.when(F.col("invol"), F.lit("payment_failed")).otherwise(pick(u("ot_r", "subscriber_id"), REASONS_VOL))
            .alias("reason")))

# COMMAND ----------
# Cancel funnel (source of Product analytics' number): every voluntary cancel starts with a cancel click;
# some clicks end in a save offer (no cancel) and some cancels are reversed within 24 h (customer
# changed their mind). Involuntary cancels have no click. Day after: clicks land on the planted Product
# count; history days: saved ~18% and reversed ~9% of voluntary cancels.
TEAM = P.TEAM_CANCEL_COUNTS
vol_eff = sports_cancels.where("cancel_type = 'voluntary'").count()
product_target = round(TEAM["Product analytics"][0] * (1 + rng.uniform(-0.008, 0.008)))
n_extra = product_target - vol_eff
N_REVERSED = round(n_extra * 0.33)
N_SAVED = n_extra - N_REVERSED
HIST_SAVED, HIST_REVERSED = 0.18, 0.09
day_after_cancellers = sports_cancels.select("subscriber_id").unionByName(other_cancels.select("subscriber_id"))
funnel_pool = (subs.where(alive & not_sample & (F.col("signup_ts") < F.lit(et_to_utc(GAME_DATE - timedelta(days=2)))))
               .join(day_after_cancellers, "subscriber_id", "left_anti")
               .join(viewers.select("subscriber_id"), "subscriber_id", "left_anti"))
hist_vol = (spark.table(cfg.fq("fact_subscription_events"))  # noqa: F821
            .where("event_type = 'cancel' AND cancel_type = 'voluntary'")
            .withColumn("day", F.to_date(F.from_utc_timestamp("event_ts", P.TZ)))
            .where(F.col("day") < F.lit(DAY_AFTER)).groupBy("day", "tier").count().toPandas())
plan = [(DAY_AFTER, "Sports", "saved", N_SAVED), (DAY_AFTER, "Sports", "reversed", N_REVERSED)]
for r in hist_vol.itertuples():
    plan += [(r.day, r.tier, "saved", round(r.count * HIST_SAVED)), (r.day, r.tier, "reversed", round(r.count * HIST_REVERSED))]
plan = pd.DataFrame(plan, columns=["day", "tier", "outcome", "n"]).sort_values(["tier", "day", "outcome"])
plan["hi"] = plan.groupby("tier").n.cumsum()
plan["lo"] = plan.hi - plan.n
bins = spark.createDataFrame(plan[["day", "tier", "outcome", "lo", "hi"]])  # noqa: F821
ranked = funnel_pool.select("subscriber_id", "tier").withColumn(
    "rk", F.row_number().over(Window.partitionBy("tier").orderBy(u("funnel", "subscriber_id"))))
extra_clicks = (ranked.join(F.broadcast(bins), (ranked.tier == bins.tier) & (ranked.rk > bins.lo) & (ranked.rk <= bins.hi))
                .select("subscriber_id", ranked.tier, "day", "outcome")
                .withColumn("request_ts", F.to_utc_timestamp(F.to_timestamp("day") + F.make_interval(
                    secs=(6 + 17.5 * u("fn_t", "subscriber_id", "day")) * 3600), P.TZ))
                .withColumn("cancel_ts", F.when(F.col("outcome") == "reversed",
                                                F.col("request_ts") + F.make_interval(secs=60 + 420 * u("fn_c", "subscriber_id"))))
                .withColumn("reversed_ts", F.col("cancel_ts") + F.make_interval(
                    secs=600 + 70000 * u("fn_r", "subscriber_id"))))
effective_clicks = (spark.table(cfg.fq("fact_subscription_events")).where(  # noqa: F821
    "event_type = 'cancel' AND cancel_type = 'voluntary'").select("subscriber_id", "tier", F.col("event_ts").alias("ets"))
    .where(F.col("ets") < F.lit(DAY_AFTER_T0))
    .unionByName(sports_cancels.where("cancel_type = 'voluntary'").select("subscriber_id", "tier", F.col("event_ts").alias("ets")))
    .unionByName(other_cancels.where("cancel_type = 'voluntary'").select("subscriber_id", "tier", F.col("event_ts").alias("ets")))
    .select("subscriber_id", "tier", F.to_date(F.from_utc_timestamp("ets", P.TZ)).alias("day"),
            F.lit("cancelled").alias("outcome"),
            (F.col("ets") - F.make_interval(secs=60 + 420 * u("fn_c", "subscriber_id"))).alias("request_ts"),
            F.col("ets").alias("cancel_ts"), F.lit(None).cast("timestamp").alias("reversed_ts")))
funnel = tmp(extra_clicks.select("subscriber_id", "tier", "day", "outcome", "request_ts", "cancel_ts", "reversed_ts")
             .unionByName(effective_clicks), "gen_cancel_funnel_tmp")

# COMMAND ----------
# Write subscription events, billing, daily cancels, ledger; update the dimension. Each write first
# removes the rows this notebook owns, so a re-run is idempotent.

EVID = {"start": 10_000_000_000, "cancel": 20_000_000_000, "reactivate": 50_000_000_000}


def sub_events(df, event_type: str, id_base: int, ctype=None, reason=None):
    return df.select((F.col("subscriber_id") + id_base).alias("event_id"), "subscriber_id", "tier",
                     F.lit(event_type).alias("event_type"), "event_ts",
                     (F.col(ctype) if ctype else F.lit(None)).cast("string").alias("cancel_type"),
                     (F.col(reason) if reason else F.lit(None)).cast("string").alias("cancel_reason_code"),
                     F.lit("subscriber_live").alias("source_system"))


new_events = (sub_events(gn.withColumnRenamed("signup_ts", "event_ts"), "start", EVID["start"])
              .unionByName(sub_events(sports_cancels, "cancel", EVID["cancel"], "cancel_type", "reason"))
              .unionByName(sub_events(other_cancels, "cancel", EVID["cancel"], "cancel_type", "reason"))
              .unionByName(sub_events(reactivated, "resume", EVID["reactivate"])))
spark.sql(f"""DELETE FROM {cfg.fq('fact_subscription_events')}
              WHERE event_ts >= TIMESTAMP'{DAY_AFTER_T0}' OR (event_type = 'start' AND event_ts >= TIMESTAMP'{T0}')
                 OR event_id >= {EVID['reactivate']}""")  # noqa: F821
write(new_events, "fact_subscription_events", "append")

spark.sql(f"""DELETE FROM {cfg.fq('fact_billing_attempts')} WHERE attempt_id >= {ATT['renewal']}
              OR subscriber_id IN (SELECT subscriber_id FROM {cfg.fq('gen_gn_signups_tmp')})""")  # noqa: F821
write(gn_bill.unionByName(gn_retry).unionByName(renew_bill).unionByName(renew_retry).unionByName(priya_bill),
      "fact_billing_attempts", "append")

day_counts = {"Sports": P.CANCELS_OBSERVED, **other_counts}
spark.sql(f"DELETE FROM {cfg.fq('fact_daily_cancels')} WHERE day >= DATE'{DAY_AFTER}'")  # noqa: F821
write(spark.createDataFrame([(DAY_AFTER, t, n, True) for t, n in day_counts.items()],  # noqa: F821
                            "day DATE, tier STRING, cancels INT, is_big_event_next_day BOOLEAN"),
      "fact_daily_cancels", "append")
ledger = {"Sports": P.RECON_SPORTS["ledger"],
          "Originals": round(other_counts["Originals"] * (1 + P.RECON_ORIGINALS["pct_diff"] + rng.uniform(-0.003, 0.003)))}
for tier in ("Family", "Bundle"):
    ledger[tier] = int(round(other_counts[tier] * (1 - rng.uniform(0.0, 0.003))))
spark.sql(f"DELETE FROM {cfg.fq('billing_ledger_cancels_daily')} WHERE day >= DATE'{DAY_AFTER}'")  # noqa: F821
write(spark.createDataFrame([(DAY_AFTER, t, n, "finance_gl") for t, n in ledger.items()],  # noqa: F821
                            "day DATE, tier STRING, ledger_cancels INT, ledger_source STRING"),
      "billing_ledger_cancels_daily", "append")

sports_cancels.select("subscriber_id", "event_ts").unionByName(other_cancels.select("subscriber_id", "event_ts")) \
    .join(reactivated.select("subscriber_id", F.lit(True).alias("react")), "subscriber_id", "left") \
    .createOrReplaceTempView("day_after_cancels")
spark.sql(f"""MERGE INTO {cfg.fq('dim_subscribers')} d USING day_after_cancels c ON d.subscriber_id = c.subscriber_id
              WHEN MATCHED THEN UPDATE SET d.status = CASE WHEN c.react THEN 'active' ELSE 'cancelled' END,
                                           d.canceled_ts = c.event_ts""")  # noqa: F821
# Samples: the device and acquirer their story needs, and an email with their first name.
for card, sid in SAMPLE_ID.items():
    first = SAMPLES[card]["first_name"].lower()
    spark.sql(f"""UPDATE {cfg.fq('dim_subscribers')}
                  SET device_primary = '{SAMPLE_DEVICE[sid]}', status = 'active', canceled_ts = NULL,
                      acquirer = CASE WHEN subscriber_id = {PRIYA} THEN 'B' ELSE acquirer END,
                      email = concat('{first}.', CAST(subscriber_id AS STRING), '@',
                                     coalesce(nullif(regexp_extract(email, '@(.*)$', 1), ''), 'gmail.com'))
                  WHERE subscriber_id = {sid}""")  # noqa: F821

# COMMAND ----------
# CDN telemetry. Total viewers N(t) = pre_level * pre_shape(t) * (1 + (m - 1) h(t)) split by gen_cell_mix.
# Ratios: rebuffer 1.0% and start failures 1.8% everywhere, with 3% multiplicative noise per row.
#   cohort cell: ramps to 10.5% / 15% over 20:17-20:20, holds, the ops shift at 20:26 moves its sessions
#                to CDN-A over ~8 min and quality recovers (below 1.5x baseline from 20:36).
#   CDN-F Seattle: chronically flaky (+/-0.9 pt slow oscillation all night) plus +0.8 pt 20:20-20:40,
#                so its robust z stays below 2 and the anomaly rule never fires.

RB0, SF0 = P.REBUFFER_BASELINE, P.START_FAIL_BASELINE
DECOY_AMP = 0.009
mix = cell_mix.select("metro", "cdn", "device_family", "app_version", "share")
is_cohort = " AND ".join(f"{k} = '{v}'" for k, v in COHORT.items())
is_backup = " AND ".join(f"{k} = '{v}'" for k, v in BACKUP.items())
cohort_share = mix.where(is_cohort).first().share


def telemetry(grid, pre_level: float, surge: float, bucket_s: int, incident: bool):
    """grid: t (seconds after 18:00 ET), event_ts, h, pre_shape. Returns bronze_cdn_telemetry-shaped rows."""
    g = F.when(F.col("t") < ONSET, 0.0).when(F.col("t") < RAMP_END, (F.col("t") - ONSET) / (RAMP_END - ONSET)) \
        .when(F.col("t") < SHIFT, 1.0).otherwise(F.exp(-(F.col("t") - SHIFT) / DECAY_TAU_S)) if incident else F.lit(0.0)
    moved = F.least(F.greatest((F.col("t") - SHIFT) / SHIFT_RAMP_S, F.lit(0.0)), F.lit(SHIFT_MAX)) \
        if incident else F.lit(0.0)
    bump = (F.least(F.greatest((F.col("t") - DECOY_LO) / 60.0, F.lit(0.0)), F.lit(1.0))
            * F.least(F.greatest((DECOY_HI - F.col("t")) / 60.0, F.lit(0.0)), F.lit(1.0))) if incident else F.lit(0.0)
    key = ("event_ts", "cdn", "metro", "device_family", "app_version")
    df = (grid.crossJoin(F.broadcast(mix))
          .withColumn("total", F.lit(pre_level) * F.col("pre_shape") * (1 + (surge - 1) * F.col("h")))
          .withColumn("conc", F.col("total") * F.col("share") * (1 + 0.01 * normal("conc", *key)))
          .withColumn("conc", F.when(F.expr(is_cohort), F.col("conc") * (1 - moved))
                      .when(F.expr(is_backup), F.col("conc") + F.col("total") * cohort_share * moved)
                      .otherwise(F.col("conc")))
          .withColumn("rb", F.when(F.expr(is_cohort), RB0 + (P.REBUFFER_INCIDENT - RB0) * g)
                      .when(F.expr("cdn = 'CDN-F' AND metro = 'SEA'"),
                            RB0 + DECOY_AMP * F.sin(2 * math.pi * F.col("t") / 480.0) + P.DECOY["rebuffer_lift_pts"]
                            * bump)
                      .otherwise(RB0 * (1 + 0.04 * (u("cellrb", "cdn", "metro", "device_family", "app_version")
                                                    - 0.5)))
                      * (1 + 0.03 * normal("rb", *key)))
          .withColumn("sf", F.when(F.expr(is_cohort), SF0 + (P.START_FAIL_INCIDENT - SF0) * g)
                      .otherwise(SF0 * (1 + 0.04 * (u("cellsf", "cdn", "metro", "device_family", "app_version")
                                                    - 0.5)))
                      * (1 + 0.03 * normal("sf", *key)))
          .withColumn("starts", F.round(F.col("conc") * bucket_s / 1800.0 * (1 + 2 * F.col("h"))).cast("int"))
          # failed starts: error-diffusion rounding along each cell's timeline, so small cells are not biased
          .withColumn("cum_fail", F.sum(F.col("starts") * F.col("sf")).over(
              Window.partitionBy("cdn", "metro", "device_family", "app_version").orderBy("event_ts")))
          .withColumn("failed", (F.round("cum_fail") - F.round(F.col("cum_fail") - F.col("starts") * F.col("sf")))
                      .cast("int"))
          .select("event_ts", "cdn", "metro", "device_family",
                  F.when(u("nullver", *key) < 0.01, F.lit(None)).otherwise(F.col("app_version")).alias("app_version"),
                  F.round("conc").cast("int").alias("concurrent_sessions"), F.col("starts").alias("session_starts"),
                  F.col("failed").alias("failed_starts"),
                  F.round(F.col("conc") * bucket_s * 1000 * F.col("rb")).cast("bigint").alias("rebuffer_ms"),
                  F.round(F.col("conc") * bucket_s * 1000).cast("bigint").alias("play_ms"),
                  F.round(6200 - 25000 * F.col("rb") + 80 * normal("br", *key)).cast("int").alias("bitrate_kbps_avg"),
                  (F.col("event_ts") + F.make_interval(secs=F.when(u("late", *key) < 0.03, 30 + 60 * u("lag", *key))
                                                        .otherwise(2 + 10 * u("lag", *key)))).alias("ingest_ts")))
    dups = df.where(u("dup", "event_ts", "cdn", "metro", "device_family") < 0.002) \
        .withColumn("ingest_ts", F.col("ingest_ts") + F.expr("INTERVAL 5 SECONDS"))
    return df.unionByName(dups)


def night_grid(night: date, step_s: int):
    """Event-night grid with a national jitter: a mean-reverting ~0.8% wobble on the audience curve."""
    rows = curve_pd[curve_pd.offset_s % step_s == 0]
    t0 = et_to_utc(night, 18 * 3600)
    r = np.random.default_rng([cfg.seed, 43, night.toordinal(), step_s])
    wob = np.zeros(len(rows))
    for i in range(1, len(rows)):
        wob[i] = 0.97 * wob[i - 1] + 0.0025 * r.standard_normal()
    return spark.createDataFrame(pd.DataFrame({  # noqa: F821
        "t": rows.offset_s.astype(int), "event_ts": [t0 + timedelta(seconds=int(o)) for o in rows.offset_s],
        "h": rows.h, "pre_shape": rows.pre_shape * np.exp(wob)}))


game_tel = tmp(telemetry(night_grid(GAME_DATE, 10), float(target.pre_level), float(target.surge_mult), 10, True),
               "gen_game_telemetry_tmp")
prior_tel = None
for ev in prior_nights.itertuples():
    t = telemetry(night_grid(ev.event_date, 60), float(ev.pre_level), float(ev.surge_mult), 60, False)
    prior_tel = t if prior_tel is None else prior_tel.unionByName(t)

# One-minute snapshots every 30 minutes for the 14-day baseline (from fact_concurrency_hourly, the half
# hours interpolated with a little noise), outside event nights.
hourly = (spark.table(cfg.fq("fact_concurrency_hourly"))  # noqa: F821
          .where((F.col("hour_ts") >= F.lit(et_to_utc(W_START))) & ~F.col("is_event"))
          .join(cell_mix.select("metro", "cdn", "device_family", "app_version", "version_share"),
                ["metro", "cdn", "device_family"]))
nxt = Window.partitionBy("metro", "cdn", "device_family", "app_version").orderBy("hour_ts")
half = (hourly.withColumn("next_avg", F.lead("concurrent_avg").over(nxt))
        .where(F.col("next_avg").isNotNull())
        .select((F.col("hour_ts") + F.expr("INTERVAL 30 MINUTES")).alias("hour_ts"), "metro", "cdn", "device_family",
                "app_version", "version_share", "is_event",
                F.round((F.col("concurrent_avg") + F.col("next_avg")) / 2
                        * (1 + 0.02 * normal("half", "hour_ts", "metro", "cdn", "device_family", "app_version")))
                .cast("int").alias("concurrent_avg")))
hourly = hourly.unionByName(half)
hkey = ("hour_ts", "cdn", "metro", "device_family", "app_version")
hourly_tel = hourly.select(
    F.col("hour_ts").alias("event_ts"), "cdn", "metro", "device_family",
    F.when(u("nullver", *hkey) < 0.01, F.lit(None)).otherwise(F.col("app_version")).alias("app_version"),
    F.round(F.col("concurrent_avg") * F.col("version_share")).cast("int").alias("concurrent_sessions"),
    F.round(F.col("concurrent_avg") * F.col("version_share") * 60 / 1800.0).cast("int").alias("session_starts"),
    F.round(F.col("concurrent_avg") * F.col("version_share") * 60 / 1800.0 * SF0 * (1 + 0.03 * normal("sf", *hkey)))
    .cast("int").alias("failed_starts"),
    F.round(F.col("concurrent_avg") * F.col("version_share") * 60000 * RB0 * (1 + 0.03 * normal("rb", *hkey)))
    .cast("bigint").alias("rebuffer_ms"),
    F.round(F.col("concurrent_avg") * F.col("version_share") * 60000).cast("bigint").alias("play_ms"),
    F.round(6200 - 25000 * RB0 + 80 * normal("br", *hkey)).cast("int").alias("bitrate_kbps_avg"),
    (F.col("hour_ts") + F.make_interval(secs=2 + 10 * u("lag", *hkey))).alias("ingest_ts"))

LANDING = cfg.volume_path("raw_landing")


def write_json(df, path: str, chunk_s: int):
    out = (df.withColumn("dt", F.date_format("event_ts", "yyyy-MM-dd")).withColumn("hh", F.date_format("event_ts", "HH"))
           .withColumn("chunk", F.floor(F.unix_timestamp("event_ts") / chunk_s))
           .withColumn("event_ts", F.date_format("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
           .withColumn("ingest_ts", F.date_format("ingest_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
    (out.repartition("dt", "hh", "chunk").drop("chunk").write.mode("overwrite").partitionBy("dt", "hh").json(path))


all_tel = game_tel.unionByName(prior_tel).unionByName(hourly_tel)
write_json(all_tel, f"{LANDING}/cdn", 600)
write_json(game_tel, f"{LANDING}/replay_staged/cdn", 600)

# COMMAND ----------
# Session events. Cohort viewers are individual subscribers (sample_weight 1): one session in progress
# at the onset, restarts during the incident (failed starts are extra short sessions), then a session on
# CDN-A after the shift. Severe viewers sit through >= 5 min of rebuffering in one session; mild viewers
# restart more. Rebuffering is a 10-second heartbeat event while stalled. Everyone else is a 1-in-100
# sample of sessions (sample_weight 100) drawn from each cell's concurrency.

HB_S = 10
SAMPLE_W = 100.0
MEAN_SESSION_S = 2700.0
P_FAIL_INCIDENT = 0.15
P_DROP, P_DROP_INCIDENT = 0.04, 0.04 * (1 + P.SESSION_DROP_LIFT_REL)
SESSION_SCHEMA = ("session_id STRING, subscriber_id BIGINT, cdn STRING, metro STRING, device_family STRING, "
                  "app_version STRING, start_s DOUBLE, end_s DOUBLE, failed BOOLEAN, dropped BOOLEAN, "
                  "n_hb_a INT, a_lo DOUBLE, a_hi DOUBLE, n_hb_b INT, b_lo DOUBLE, b_hi DOUBLE, sample_weight DOUBLE")

vc = viewers.groupBy("severe").count().collect()
n_sev = next(r["count"] for r in vc if r.severe)
n_mild = next(r["count"] for r in vc if not r.severe)
fail_extra = P_FAIL_INCIDENT / (1 - P_FAIL_INCIDENT)       # expected failed attempts before each restart
SEVERE_RESTART_P = 0.3
MU_MILD = ((P.AFFECTED_SESSIONS - n_sev * (1 + SEVERE_RESTART_P * (1 + fail_extra))) / n_mild - 1) / (1 + fail_extra)
# Drop probability per completed incident session, so that drops / all sessions overlapping the window
# (failed attempts included) is the baseline rate x (1 + planted lift).
n_window_sessions = n_sev * (1 + SEVERE_RESTART_P * (1 + fail_extra)) + n_mild * (1 + MU_MILD * (1 + fail_extra))
n_failed_attempts = fail_extra * (n_sev * SEVERE_RESTART_P + n_mild * MU_MILD)
P_DROP_WINDOW = P_DROP_INCIDENT * n_window_sessions / (n_window_sessions - n_failed_attempts)
COHORT_T = (COHORT["cdn"], COHORT["metro"], COHORT["device_family"], COHORT["app_version"])
BACKUP_T = (BACKUP["cdn"], BACKUP["metro"], BACKUP["device_family"], BACKUP["app_version"])


def cohort_sessions(batches):
    cols = SESSION_SCHEMA.replace(",", "").split()[::2]
    for pdf in batches:
        rows = []
        for v in pdf.itertuples(index=False):
            r = np.random.default_rng([cfg.seed, 41, int(v.subscriber_id)])
            s0 = max(float(v.t_avail_s), 6300 + r.uniform(0, 1500))
            k = 1 + int(r.random() < SEVERE_RESTART_P) if v.severe else 1 + int(r.poisson(MU_MILD))
            if v.subscriber_id == MARCUS:
                k = 1
            lo_w = max(float(ONSET), s0)
            bounds = sorted(r.uniform(lo_w, v.t_mig_s, k - 1)) if k > 1 else []
            starts, ends = [s0, *bounds], [*bounds, float(v.t_mig_s)]
            win_len = np.array([max(e - max(s, ONSET), 0.0) for s, e in zip(starts, ends)])
            reb = r.uniform(305, 420) if v.severe else r.uniform(10, 100)
            if v.subscriber_id == MARCUS:
                reb = float(MARCUS_REBUFFER_S)
            if v.severe:
                alloc = np.zeros(k)
                alloc[int(win_len.argmax())] = reb
            else:
                alloc = reb * win_len / max(win_len.sum(), 1.0)
            for i, (s, e) in enumerate(zip(starts, ends)):
                if i > 0:
                    n_fail = 0
                    while r.random() < P_FAIL_INCIDENT:
                        n_fail += 1
                    for j in range(n_fail):
                        fs = s - 5.0 * (j + 1)
                        rows.append((f"S{v.subscriber_id}-{i:02d}f{j}", v.subscriber_id, *COHORT_T, fs, fs + 3,
                                     True, False, 0, fs, fs, 0, fs, fs, 1.0))
                pre = max(min(e, ONSET) - s, 0.0)
                rows.append((f"S{v.subscriber_id}-{i:02d}", v.subscriber_id, *COHORT_T, s, e, False,
                             bool(r.random() < P_DROP_WINDOW),
                             int(r.poisson(pre * RB0 / HB_S)), s, s + pre,
                             int(round(alloc[i] / HB_S)), max(s, ONSET), e, 1.0))
            ps = float(v.t_mig_s) + r.uniform(5, 30)
            pe = r.uniform(clk("20:50"), clk("21:15"))
            rows.append((f"S{v.subscriber_id}-A", v.subscriber_id, *BACKUP_T, ps, pe, False, bool(r.random() < P_DROP),
                         int(r.poisson((pe - ps) * RB0 / HB_S)), ps, pe, 0, ps, ps, 1.0))
        yield pd.DataFrame(rows, columns=cols)


cohort_sess = viewers.repartition(32, "subscriber_id").sortWithinPartitions("subscriber_id").mapInPandas(
    cohort_sessions, SESSION_SCHEMA)

# Sampled sessions for every other cell: arrivals follow concurrency, exponential durations.
curve_pd["total"] = float(target.pre_level) * curve_pd.pre_shape * (1 + (float(target.surge_mult) - 1) * curve_pd.h)
viewer_s = float((curve_pd.total * 10).sum())
cdf = np.cumsum(curve_pd.total.values) / curve_pd.total.sum()
START_Q = np.interp(np.linspace(0, 1, 1001), np.concatenate([[0], cdf]),
                    np.concatenate([[0], curve_pd.offset_s.values + 10])).tolist()
cells = mix.where(f"NOT ({is_cohort})").withColumn(
    "n_sess", F.round(F.lit(viewer_s / MEAN_SESSION_S / SAMPLE_W) * F.col("share")).cast("int"))
skey = ("cdn", "metro", "device_family", "app_version", "k")
sampled = (cells.withColumn("k", F.explode(F.sequence(F.lit(1), F.greatest("n_sess", F.lit(1)))))
           .withColumn("q", u("st", *skey) * 1000)
           .withColumn("start_s", F.element_at(F.array(*[F.lit(x) for x in START_Q]), F.floor("q").cast("int") + 1)
                       + (F.col("q") - F.floor("q")) * 16.2)
           .withColumn("end_s", F.least(F.col("start_s") - F.log(u("dur", *skey)) * MEAN_SESSION_S,
                                        F.lit(float(NIGHT_S))))
           .withColumn("failed", u("fail", *skey) < SF0)
           .withColumn("end_s", F.when(F.col("failed"), F.col("start_s") + 3).otherwise(F.col("end_s")))
           .withColumn("rk_sub", F.floor(u("who", *skey) * 1e9)))
metro_subs = (subs.where(F.col("metro").isNotNull() & F.col("tier").isin("Sports", "Bundle") & not_sample)
              .withColumn("n", F.count("*").over(Window.partitionBy("metro")))
              .withColumn("rk", F.row_number().over(Window.partitionBy("metro").orderBy(u("samp", "subscriber_id"))) - 1)
              .select("metro", "subscriber_id", "n", "rk"))
metro_n = {r.metro: r.n for r in metro_subs.select("metro", "n").distinct().collect()}
nmap = F.create_map(*[x for m, n in metro_n.items() for x in (F.lit(m), F.lit(n))])
sampled = (sampled.withColumn("rk", F.pmod(F.col("rk_sub"), F.element_at(nmap, F.col("metro"))).cast("int"))
           .join(metro_subs.drop("n"), ["metro", "rk"])
           .withColumn("decoy_overlap", F.when(F.expr("cdn = 'CDN-F' AND metro = 'SEA'"), F.greatest(
               F.least("end_s", F.lit(float(DECOY_HI))) - F.greatest("start_s", F.lit(float(DECOY_LO))), F.lit(0.0)))
               .otherwise(0.0))
           .withColumn("lam", ((F.col("end_s") - F.col("start_s")) * RB0 + F.col("decoy_overlap")
                               * P.DECOY["rebuffer_lift_pts"]) / HB_S)
           .withColumn("n_hb", F.when(F.col("failed"), 0).otherwise(
               F.round(F.col("lam") + F.sqrt(F.col("lam")) * normal("hb", *skey)).cast("int")))
           .select(F.concat_ws("-", F.lit("W"), "cdn", "metro", "device_family", "app_version",
                               F.col("k").cast("string")).alias("session_id"), "subscriber_id", "cdn", "metro",
                   "device_family", "app_version", "start_s", "end_s", "failed",
                   (~F.col("failed") & (u("drop", *skey) < P_DROP)).alias("dropped"),
                   F.greatest("n_hb", F.lit(0)).alias("n_hb_a"), F.col("start_s").alias("a_lo"),
                   F.col("end_s").alias("a_hi"), F.lit(0).alias("n_hb_b"), F.col("start_s").alias("b_lo"),
                   F.col("start_s").alias("b_hi"), F.lit(SAMPLE_W).alias("sample_weight")))
sessions = tmp(cohort_sess.unionByName(sampled), "gen_sessions_tmp")

EVENT_SCHEMA = ("event_id STRING, event_ts TIMESTAMP, session_id STRING, subscriber_id BIGINT, profile_id BIGINT, "
                "event_type STRING, cdn STRING, metro STRING, device_family STRING, app_version STRING, "
                "stream_id STRING, sample_weight DOUBLE, ingest_ts TIMESTAMP")
T0_NP = np.datetime64(T0, "ms")


def to_events(batches):
    for s in batches:
        r = np.random.default_rng([cfg.seed, 42, len(s), int(s.subscriber_id.iloc[0]) if len(s) else 0])
        parts = []

        def emit(mask, t, etype):
            sub = s[mask]
            parts.append(pd.DataFrame({"session_id": sub.session_id.values, "t": np.asarray(t)[mask.values]
                                       if np.ndim(t) else t, "event_type": etype, "row": np.flatnonzero(mask.values)}))

        all_rows = pd.Series(True, index=s.index)
        emit(all_rows, s.start_s.values, "start")
        emit(s.failed, s.start_s.values + 2, "error")
        emit(s.failed, s.end_s.values, "end")
        ok = ~s.failed
        emit(ok & s.dropped, s.end_s.values, "drop")
        emit(ok & ~s.dropped, s.end_s.values, "end")
        for n_col, lo, hi in (("n_hb_a", "a_lo", "a_hi"), ("n_hb_b", "b_lo", "b_hi")):
            n = s[n_col].values.astype(int)
            idx = np.repeat(np.arange(len(s)), n)
            if len(idx):
                frac = r.random(len(idx))
                t = s[lo].values[idx] + (s[hi].values[idx] - s[lo].values[idx]) * frac
                parts.append(pd.DataFrame({"session_id": s.session_id.values[idx], "t": t,
                                           "event_type": "rebuffer", "row": idx}))
        ev = pd.concat(parts, ignore_index=True).sort_values(["row", "t"], kind="stable")
        src = s.iloc[ev.row.values].reset_index(drop=True)
        ev = ev.reset_index(drop=True)
        seq = ev.groupby("session_id").cumcount()
        lag = np.where(r.random(len(ev)) < 0.03, r.uniform(30, 90, len(ev)), r.uniform(2, 12, len(ev)))
        yield pd.DataFrame({
            "event_id": ev.session_id + "-" + seq.astype(str), "event_ts": T0_NP + (ev.t.values * 1000).astype(
                "timedelta64[ms]"),
            "session_id": ev.session_id, "subscriber_id": src.subscriber_id, "profile_id": src.subscriber_id * 10,
            "event_type": ev.event_type, "cdn": src.cdn, "metro": src.metro, "device_family": src.device_family,
            "app_version": src.app_version, "stream_id": "SPORTS-BIG-GAME-LIVE", "sample_weight": src.sample_weight,
            "ingest_ts": T0_NP + ((ev.t.values + lag) * 1000).astype("timedelta64[ms]")})


session_events = (sessions.repartition(64, "subscriber_id").sortWithinPartitions("session_id")
                  .mapInPandas(to_events, EVENT_SCHEMA))
session_events = tmp(session_events, "gen_session_events_tmp")
write_json(session_events, f"{LANDING}/sessions", 600)
write_json(session_events, f"{LANDING}/replay_staged/sessions", 600)

# COMMAND ----------
# Ground-truth tables (only 25 and 27 read these).

tel_dedup = game_tel.dropDuplicates(["event_ts", "cdn", "metro", "device_family", "app_version"])
cell_minutes = tel_dedup.where(F.col("app_version").isNotNull()).withColumn("minute_ts", F.date_trunc("minute", "event_ts"))
win_sessions = sessions.where(F.expr(is_cohort) & (F.col("start_s") <= RESOLVED) & (F.col("end_s") >= ONSET))
affected = int(win_sessions.agg(F.sum("sample_weight")).first()[0])
write(spark.createDataFrame([(COHORT["cdn"], COHORT["metro"], COHORT["device_family"], COHORT["app_version"],  # noqa: F821
                              T0 + timedelta(seconds=ONSET), T0 + timedelta(seconds=RESOLVED), affected,
                              P.DECOY["cdn"], P.DECOY["metro"])],
                            "cdn STRING, metro STRING, device_family STRING, app_version STRING, onset_ts TIMESTAMP, "
                            "resolved_ts TIMESTAMP, affected_sessions BIGINT, decoy_cdn STRING, decoy_metro STRING"),
      "eval_gt_incident")

write(sports_cancels.select(
    (F.col("subscriber_id") + EVID["cancel"]).alias("cancel_event_id"), F.col("cause").alias("cause_primary"),
    F.when(F.col("overlap"), F.lit("playback")).alias("cause_secondary"), F.col("overlap").alias("is_overlap")),
    "eval_gt_cancel_cause")

# COMMAND ----------
# Post-game scoring snapshot (310k Sports subscribers who have not cancelled): cohort viewers, game-night
# sign-ups and a random fill. Pre-game state comes from 03 (gen_pregame_features, same recipe as the
# training panel); game-night sessions add QoE exposure; sign-up declines add declines_30d. The risk
# SQL stored by 03 gives each subscriber's true 30-day cancel probability for eval_gt_churn_future.

cancelled_ids = sports_cancels.select("subscriber_id")
viewer_qoe = (sessions.where("sample_weight = 1").groupBy("subscriber_id").agg(
    (F.sum((F.col("n_hb_a") + F.col("n_hb_b")) * HB_S) / 60.0).alias("game_reb_min"),
    F.max((F.col("n_hb_a") + F.col("n_hb_b")) * HB_S / 60.0).alias("game_max_min"),
    F.sum(F.col("failed").cast("int")).alias("game_failed")))
core = (viewers.select("subscriber_id").unionByName(gn.select("subscriber_id"))
        .unionByName(spark.createDataFrame([(i,) for i in SAMPLE_IDS], "subscriber_id BIGINT"))  # noqa: F821
        .distinct().join(cancelled_ids, "subscriber_id", "left_anti"))
n_core = core.count()
fill = (spark.table(cfg.fq("gen_pregame_features")).select("subscriber_id")  # noqa: F821
        .join(core, "subscriber_id", "left_anti").join(cancelled_ids, "subscriber_id", "left_anti")
        .withColumn("rk", F.row_number().over(Window.orderBy(u("fill", "subscriber_id"))))
        .where(F.col("rk") <= P.SCORING_COHORT - n_core).select("subscriber_id"))
FEATURE_COLS = ["tenure_days", "plan_price", "is_bundle", "signup_source", "event_only_signup", "hours_30d",
                "active_days_30d", "engagement_trend", "days_since_last_session", "pauses_90d", "rebuffer_min_30d",
                "max_session_rebuffer_min", "failed_starts_30d", "declines_30d", "decline_no_retry",
                "support_contacts_30d"]
scored_pre = tmp(
    spark.table(cfg.fq("gen_pregame_features")).join(core.unionByName(fill), "subscriber_id")  # noqa: F821
    .join(viewer_qoe, "subscriber_id", "left")
    .join(gn.select("subscriber_id", "declined_normal"), "subscriber_id", "left")
    .withColumn("rebuffer_min_30d", F.round(F.col("rebuffer_min_30d") + F.coalesce("game_reb_min", F.lit(0.0)), 2))
    .withColumn("max_session_rebuffer_min", F.round(F.greatest(
        "max_session_rebuffer_min", F.coalesce("game_max_min", F.lit(0.0))), 2))
    .withColumn("failed_starts_30d", F.col("failed_starts_30d") + F.coalesce("game_failed", F.lit(0)).cast("int"))
    .withColumn("declines_30d", F.col("declines_30d") + F.coalesce(F.col("declined_normal").cast("int"), F.lit(0))
                + F.when(F.col("subscriber_id") == PRIYA, 2).otherwise(0))
    .withColumn("decline_no_retry", F.col("decline_no_retry") | (F.col("subscriber_id") == PRIYA))
    .withColumn("is_viewer", F.col("game_reb_min").isNotNull() | F.col("subscriber_id").isin(SAMPLE_IDS))
    .withColumn("u_watched", u("watched", "subscriber_id")), "gen_scored_pre_tmp")

# Last night's viewing: subscribers who streamed the game have days_since_last_session = 0 and two more
# viewing hours. Every cohort viewer did; for the rest, the share that streamed is calibrated (bisection)
# so the oracle's top decile projects planted.PROJECTED_CANCELS_30D, the same way 03 calibrates AUC.
WATCH_HOURS = 2.0
P_EXPR = calib["p_sql"].format(log_p0=calib["log_p0"], base=calib["base_sql"], qoe_slope=calib["qoe_slope"],
                               dose=calib["dose_sql"], beta_z=calib["beta_z"], eps_sd=calib["eps_sd"], extra="0")
logit = F.lit(calib["oracle_intercept"])
for i, (e, c) in enumerate(zip(calib["oracle_features"], calib["oracle_coef"])):
    logit = logit + c * (F.expr(e).cast("double") - calib["oracle_mean"][i]) / calib["oracle_std"][i]


def with_watched(df, share: float):
    w = F.col("is_viewer") | (F.col("u_watched") < share)
    return (df.withColumn("days_since_last_session", F.when(w, 0).otherwise(F.col("days_since_last_session")))
            .withColumn("hours_30d", F.when(w, F.round(F.col("hours_30d") + WATCH_HOURS, 2)).otherwise(F.col("hours_30d")))
            .withColumn("active_days_30d", F.when(w, F.least(F.col("active_days_30d") + 1, F.greatest(
                F.least("tenure_days", F.lit(30)), F.lit(1)))).otherwise(F.col("active_days_30d")).cast("int"))
            .withColumn("p_oracle", 1 / (1 + F.exp(-logit))))


def top_decile_sum(share: float) -> float:
    return with_watched(scored_pre, share).orderBy(F.desc("p_oracle")).limit(P.AT_RISK).agg(
        F.sum("p_oracle")).first()[0]


lo, hi = 0.0, 1.0
for _ in range(8):
    mid = (lo + hi) / 2
    if top_decile_sum(mid) > P.PROJECTED_CANCELS_30D:
        lo = mid
    else:
        hi = mid
WATCHED_SHARE = (lo + hi) / 2
print(f"calibrated share of non-cohort subscribers who streamed the game: {WATCHED_SHARE:.3f}")
# Samples: solve each one's latent frailty k so the oracle scores them near the planted churn_prob, and
# set its observable proxies the way 03's recipe does (engagement trend, support contacts, pauses, hours,
# z itself). Everything else about them is what the record says happened.
watched = with_watched(scored_pre, WATCHED_SHARE)


def with_frailty(df, k):
    return (df.withColumn("engagement_trend", -0.5 * k)
            .withColumn("support_contacts_30d", F.round(F.exp(-1.4 + 0.5 * k)).cast("int"))
            .withColumn("pauses_90d", F.round(F.exp(-1.6 + 0.55 * k)).cast("int"))
            .withColumn("hours_30d", F.round(F.col("hours_30d") * F.exp(-0.35 * k), 2))
            .withColumn("z", k))


grid = spark.createDataFrame([(float(x),) for x in np.round(np.arange(0.0, 6.01, 0.05), 2)], "k DOUBLE")  # noqa: F821
cand = (with_frailty(watched.where(F.col("subscriber_id").isin(SAMPLE_IDS)).crossJoin(grid), F.col("k"))
        .withColumn("p_oracle", 1 / (1 + F.exp(-logit))).select("subscriber_id", "k", "p_oracle").toPandas())
planted_p = {SAMPLE_ID[c]: SAMPLES[c]["churn_prob"] for c in SAMPLES}
frailty = {sid: float(g.iloc[(g.p_oracle - planted_p[sid]).abs().argmin()].k) for sid, g in cand.groupby("subscriber_id")}
k_expr = F.lit(None).cast("double")
for sid, k in frailty.items():
    k_expr = F.when(F.col("subscriber_id") == sid, F.lit(k)).otherwise(k_expr)
samples_df = with_frailty(watched.where(F.col("subscriber_id").isin(SAMPLE_IDS)).withColumn("k", k_expr), F.col("k")).drop("k")
scored = tmp(watched.where(~F.col("subscriber_id").isin(SAMPLE_IDS)).unionByName(samples_df)
             .withColumn("p_oracle", 1 / (1 + F.exp(-logit))).withColumn("p_true", F.expr(P_EXPR)), "gen_scored_tmp")

spark.sql(f"DELETE FROM {cfg.fq('gen_subscriber_features')} WHERE snapshot_date = DATE'{DAY_AFTER}'")  # noqa: F821
write(scored.select("subscriber_id", F.lit(DAY_AFTER).alias("snapshot_date"), *FEATURE_COLS),
      "gen_subscriber_features", "append")
SAVE_IF_OFFERED = {"payment": P.SAVE_CARDS["payment"]["save_rate"], "playback": P.SAVE_CARDS["playback"]["save_rate"],
                   "onboarding": P.SAVE_CARDS["onboarding"]["save_rate"], "other": 0.2}
save_rate = (F.when(F.col("decline_no_retry"), SAVE_IF_OFFERED["payment"])
             .when(F.col("max_session_rebuffer_min") >= P.PLAYBACK_REBUFFER_MIN_THRESHOLD, SAVE_IF_OFFERED["playback"])
             .when(F.col("tenure_days") < 30, SAVE_IF_OFFERED["onboarding"]).otherwise(SAVE_IF_OFFERED["other"]))
write(scored.select("subscriber_id", (F.col("u_label") < F.col("p_true")).alias("true_cancel_30d_no_action"),
                    ((F.col("u_label") < F.col("p_true")) & (u("save", "subscriber_id") < save_rate))
                    .alias("true_save_if_offered")), "eval_gt_churn_future")

# COMMAND ----------
# Viewing history for personalization (scoring cohort): favourite genre, up to three recent titles and a
# "watch next" pick from the same genre. Titles are invented. Game viewers lean to sports and have
# "The Big Game" in their recent titles. The samples get exactly their planted record.

TITLES = {
    "Live sports": ["The Big Game", "Sunday Rewind", "Monday Night Matchups", "Prime Time Tip-Off", "Overtime Live",
                    "The Rivalry Hour"],
    "Sports docs": ["Fourth Down Stories", "Fourth Down Stories: Season 2", "Road to the Big Game", "The Long Season",
                    "Underdogs", "Hall of Grit", "Final Whistle"],
    "Drama": ["Harbor Lights", "Harbor Lights: Season 3", "Quiet Hours", "The Glass Orchard", "Northbound",
              "Salt and Iron", "Second Shift"],
    "Comedy": ["Roommates Anonymous", "The Office Plant", "Wrong Number", "Cul-de-Sac", "Late Checkout"],
    "Animation": ["Star Pup Patrol", "The Lantern Fox", "Moon Garden", "Pocket Robots", "Captain Puddle"],
    "Reality": ["Island Swap", "Kitchen Rivals", "Tiny House Wars", "Love on Layover", "Flip or Fold"],
    "True crime": ["Cold Case County", "The Vanishing at Pine Lake", "Alibi", "Paper Trail", "Night Shift Detectives"],
}
assert set(TITLES) == set(P.GENRES)
for sp in P.SAMPLE_SUBSCRIBERS:
    assert all(t in sum(TITLES.values(), []) for t in (*sp["recent_titles"], sp["watch_next"])), sp["first_name"]
GAME_GENRE_W = {"Live sports": 45, "Sports docs": 20, "Drama": 10, "Comedy": 8, "Reality": 7, "True crime": 6,
                "Animation": 4}
title_map = F.create_map(*[x for g, ts in TITLES.items() for x in (F.lit(g), F.array(*[F.lit(t) for t in ts]))])
genre_titles = F.element_at(title_map, F.col("fav_genre"))


def nth(arr, salt: str):
    return F.element_at(arr, (F.pmod(F.xxhash64(F.lit(cfg.seed), F.lit(salt), "subscriber_id"), F.size(arr)) + 1)
                        .cast("int"))


game_watchers = F.col("is_viewer") | (F.col("u_watched") < WATCHED_SHARE) | (F.col("tenure_days") < 2)
viewing = (scored.select("subscriber_id", "is_viewer", "u_watched", "tenure_days")
           .withColumn("fav_genre", F.when(game_watchers, pick(u("genre", "subscriber_id"), GAME_GENRE_W))
                       .otherwise(pick(u("genre", "subscriber_id"), {g: 1 for g in P.GENRES})))
           .withColumn("recent_titles", F.array_distinct(F.filter(F.array(
               F.when(game_watchers, F.lit("The Big Game")), nth(genre_titles, "t1"), nth(genre_titles, "t2")),
               lambda x: x.isNotNull())))
           .withColumn("unseen", F.array_except(genre_titles, F.col("recent_titles")))
           .withColumn("watch_next", nth(F.col("unseen"), "t3")))
sample_rows = [(SAMPLE_ID[sp["card_id"]], sp["fav_genre"], list(sp["recent_titles"]), sp["watch_next"])
               for sp in P.SAMPLE_SUBSCRIBERS]
viewing = (viewing.where(not_sample).select("subscriber_id", "fav_genre", "recent_titles", "watch_next")
           .unionByName(spark.createDataFrame(sample_rows, "subscriber_id BIGINT, fav_genre STRING, "  # noqa: F821
                                              "recent_titles ARRAY<STRING>, watch_next STRING")))
write(viewing, "dim_subscriber_viewing")

ranks = (scored.withColumn("risk_rank", F.row_number().over(Window.orderBy(F.desc("p_oracle"))))
         .where(F.col("subscriber_id").isin(SAMPLE_IDS)).select("subscriber_id", "risk_rank", "p_oracle").toPandas()
         .set_index("subscriber_id"))
acct = {r.subscriber_id: r.account_id for r in subs.where(F.col("subscriber_id").isin(SAMPLE_IDS)).collect()}
write(spark.createDataFrame([  # noqa: F821
    (sp["card_id"], sp["first_name"], SAMPLE_ID[sp["card_id"]], acct[SAMPLE_ID[sp["card_id"]]], sp["metro"], sp["tier"],
     sp["device"], sp["tenure_months"], sp["what_happened"], sp["fav_genre"], list(sp["recent_titles"]),
     sp["watch_next"], float(sp["churn_prob"]), float(ranks.p_oracle[SAMPLE_ID[sp["card_id"]]]),
     int(ranks.risk_rank[SAMPLE_ID[sp["card_id"]]])) for sp in P.SAMPLE_SUBSCRIBERS],
    "card_id STRING, first_name STRING, subscriber_id BIGINT, account_id BIGINT, metro STRING, tier STRING, "
    "device STRING, tenure_months INT, what_happened STRING, fav_genre STRING, recent_titles ARRAY<STRING>, "
    "watch_next STRING, planted_churn_prob DOUBLE, oracle_churn_prob DOUBLE, risk_rank INT"), "ref_sample_subscribers")

# COMMAND ----------
# Simulated source systems (planted.SOURCE_SYSTEMS). The tables above are the generator's state; here
# each one is exported the way its system of record would land it in the raw_landing Volume, with the
# source's own naming and CDC texture. The cc_stream pipeline ingests these into bronze_* and silver_*,
# and the governed gold tables and metric view read only that chain.
#   subscriptions (MySQL CDC, camelCase): accounts + subscription_events, op I/U, tier codes; cancel
#                 reversals arrive as U rows on the CANCEL event; cancel clicks as CANCEL_REQUEST rows.
#   billing (PostgreSQL CDC, snake_case): billing_attempts in cents; declines arrive I 'pending' then U.
#   payments (processor webhooks): nested JSON events, epoch-second timestamps.
TIER_CODE = {"Sports": "SPT", "Family": "FAM", "Originals": "ORG", "Bundle": "BDL"}
tier_code = F.element_at(F.create_map(*[x for k, v in TIER_CODE.items() for x in (F.lit(k), F.lit(v))]), F.col("tier"))
TS = "yyyy-MM-dd HH:mm:ss.SSS"   # MySQL / Postgres DATETIME text in UTC


def land(df, source_id: str, entity: str):
    df.repartition(8).write.mode("overwrite").json(f"{LANDING}/{source_id}/{entity}")


dim_now = spark.table(cfg.fq("dim_subscribers"))  # noqa: F821
acct_cols = [F.col("account_id").alias("accountId"), F.col("subscriber_id").alias("subscriberId"),
             tier_code.alias("tierCode"), F.col("is_bundle").cast("int").alias("isBundle"),
             F.date_format("signup_ts", TS).alias("signupAt"), F.upper("signup_source").alias("signupChannel"),
             F.format_number(F.col("plan_price").cast("double"), 2).alias("monthlyPrice"),
             F.col("metro").alias("homeMetro"), F.col("state").alias("stateName"), F.col("tz").alias("tzName"),
             F.col("device_primary").alias("deviceType"), F.concat(F.lit("ACQ-"), "acquirer").alias("acquirerId"),
             F.col("is_kids_only_account").cast("int").alias("kidsOnly")]
# Contact PII (email, phone, address) stays out of the extract: its governed home is the masked
# dim_subscribers, so bronze never holds an unmasked copy.
react_ts = (spark.table(cfg.fq("fact_subscription_events")).where(  # noqa: F821
    f"event_type = 'resume' AND event_id >= {EVID['reactivate']}").select("subscriber_id", F.col("event_ts").alias("rts")))
acct_i = dim_now.select(F.lit("I").alias("op"), *acct_cols,
                        F.when(F.col("status") == "paused", "PAUSED").otherwise("ACTIVE").alias("status"),
                        F.lit(None).cast("string").alias("canceledAt"), F.date_format("signup_ts", TS).alias("_commit_ts"))
acct_u = dim_now.where(F.col("canceled_ts").isNotNull()).select(
    F.lit("U").alias("op"), *acct_cols, F.lit("CANCELLED").alias("status"),
    F.date_format("canceled_ts", TS).alias("canceledAt"), F.date_format("canceled_ts", TS).alias("_commit_ts"))
acct_r = dim_now.join(react_ts, "subscriber_id").select(
    F.lit("U").alias("op"), *acct_cols, F.lit("ACTIVE").alias("status"),
    F.date_format("canceled_ts", TS).alias("canceledAt"), F.date_format("rts", TS).alias("_commit_ts"))
land(acct_i.unionByName(acct_u).unionByName(acct_r), "subscriptions", "accounts")

ev = spark.table(cfg.fq("fact_subscription_events"))  # noqa: F821
day_off = F.datediff(F.col("day"), F.lit(W_START))
ev_rows = ev.select(F.lit("I").alias("op"), F.col("event_id").alias("eventId"), F.col("subscriber_id").alias("subscriberId"),
                    tier_code.alias("tierCode"), F.upper("event_type").alias("eventType"),
                    F.date_format("event_ts", TS).alias("eventAt"),
                    F.when(F.col("cancel_type") == "voluntary", "VOL").when(F.col("cancel_type") == "involuntary_billing",
                                                                         "INVOL").alias("cancelType"),
                    F.col("cancel_reason_code").alias("reasonCode"), F.lit(None).cast("string").alias("outcome"),
                    F.lit(0).alias("isReversed"), F.lit(None).cast("string").alias("reversedAt"),
                    F.date_format("event_ts", TS).alias("_commit_ts"))
clicks = funnel.select(F.lit("I").alias("op"), (F.lit(60_000_000_000) + day_off * 10_000_000 + F.col("subscriber_id"))
                       .alias("eventId"), F.col("subscriber_id").alias("subscriberId"), tier_code.alias("tierCode"),
                       F.lit("CANCEL_REQUEST").alias("eventType"), F.date_format("request_ts", TS).alias("eventAt"),
                       F.lit(None).cast("string").alias("cancelType"), F.lit(None).cast("string").alias("reasonCode"),
                       F.upper(F.when(F.col("outcome") == "reversed", "cancelled").otherwise(F.col("outcome")))
                       .alias("outcome"), F.lit(0).alias("isReversed"), F.lit(None).cast("string").alias("reversedAt"),
                       F.date_format("request_ts", TS).alias("_commit_ts"))
rev = funnel.where("outcome = 'reversed'")
rev_cols = [(F.lit(70_000_000_000) + day_off * 10_000_000 + F.col("subscriber_id")).alias("eventId"),
            F.col("subscriber_id").alias("subscriberId"), tier_code.alias("tierCode"), F.lit("CANCEL").alias("eventType"),
            F.date_format("cancel_ts", TS).alias("eventAt"), F.lit("VOL").alias("cancelType"),
            pick(u("rev_r", "subscriber_id"), REASONS_VOL).alias("reasonCode"), F.lit(None).cast("string").alias("outcome")]
rev_i = rev.select(F.lit("I").alias("op"), *rev_cols, F.lit(0).alias("isReversed"),
                   F.lit(None).cast("string").alias("reversedAt"), F.date_format("cancel_ts", TS).alias("_commit_ts"))
rev_u = rev.select(F.lit("U").alias("op"), *rev_cols, F.lit(1).alias("isReversed"),
                   F.date_format("reversed_ts", TS).alias("reversedAt"), F.date_format("reversed_ts", TS).alias("_commit_ts"))
land(ev_rows.unionByName(clicks).unionByName(rev_i).unionByName(rev_u), "subscriptions", "subscription_events")

bill = spark.table(cfg.fq("fact_billing_attempts"))  # noqa: F821
bill_cols = [F.col("attempt_id"), F.col("subscriber_id"), F.date_format("attempt_ts", TS).alias("attempt_ts"),
             "attempt_type", F.lower(tier_code).alias("tier_code"),
             (F.col("amount_usd") * 100).cast("int").alias("amount_cents"), F.lit("USD").alias("currency"),
             F.concat(F.lit("acq_"), F.lower("acquirer")).alias("acquirer"), "decline_code", "retry_of",
             "retry_scheduled", F.date_format("grace_expires_ts", TS).alias("grace_expires_ts")]
lsn = (F.unix_micros("attempt_ts") * 10)
bill_i = bill.select(F.lit("I").alias("op"), *bill_cols,
                     F.when(F.col("result") == "declined", "pending").otherwise(F.col("result")).alias("status"),
                     lsn.alias("_lsn"))
bill_u = bill.where("result = 'declined'").select(F.lit("U").alias("op"), *bill_cols, F.lit("declined").alias("status"),
                                                  (lsn + 20_000_000).alias("_lsn"))
land(bill_i.unionByName(bill_u), "billing", "billing_attempts")

proc = bill.select(
    F.concat(F.lit("evt_"), F.col("attempt_id").cast("string")).alias("id"),
    F.when(F.col("result") == "declined", "charge.failed").otherwise("charge.succeeded").alias("type"),
    F.unix_timestamp("attempt_ts").alias("created"), F.lit(True).alias("livemode"),
    F.struct(F.struct(F.concat(F.lit("ch_"), F.col("attempt_id").cast("string")).alias("id"),
                      (F.col("amount_usd") * 100).cast("int").alias("amount"), F.lit("usd").alias("currency"),
                      F.concat(F.lit("cus_"), F.col("subscriber_id").cast("string")).alias("customer"),
                      F.when(F.col("result") == "declined", "failed").otherwise("succeeded").alias("status"),
                      F.col("decline_code").alias("failure_code"),
                      F.struct(F.col("acquirer").alias("acquirer"), F.col("attempt_type").alias("attempt_type"))
                      .alias("metadata")).alias("object")).alias("data"))
land(proc, "payments", "processor_events")

# COMMAND ----------
# Self-asserts: every planted number this notebook owns, recomputed from the generated rows.

ok = []


def check(name: str, cond: bool, detail: str):
    print(("PASS " if cond else "FAIL ") + f"{name}: {detail}")
    ok.append((name, cond, detail))


# Surge and sign-ups
tot = tel_dedup.groupBy("event_ts").agg(F.sum("concurrent_sessions").alias("n")).toPandas()
peak = tot.n.max()
pre = tot[(tot.event_ts >= T0 + timedelta(seconds=3600)) & (tot.event_ts < T0 + timedelta(seconds=clk("20:10")))]
pre_level = pre.n.mean()
check("peak concurrency", abs(peak - P.PEAK_CONCURRENT) / P.PEAK_CONCURRENT <= P.PEAK_TOL_REL, f"{peak:,.0f}")
check("halftime multiplier", abs(peak / pre_level - P.SURGE_MULTIPLIER) <= P.SURGE_MULT_TOL, f"x{peak / pre_level:.2f}")
spm = (gn.groupBy(F.floor(F.col("t_s") / 60).alias("m")).count().toPandas().set_index("m")["count"]
       .reindex(range(270), fill_value=0))
check("sign-up multiplier", abs(spm.max() / spm.loc[30:119].median() - P.SIGNUP_MULTIPLIER) <= P.SIGNUP_MULT_TOL,
      f"x{spm.max() / spm.loc[30:119].median():.1f}")

# Incident, controls, decoy (minute grain, play-time weighted)
mins = cell_minutes.groupBy("minute_ts", "cdn", "metro", "device_family", "app_version").agg(
    F.sum("rebuffer_ms").alias("rb"), F.sum("play_ms").alias("pl"), F.sum("failed_starts").alias("fs"),
    F.sum("session_starts").alias("ss")).toPandas()
mins["t"] = (pd.to_datetime(mins.minute_ts) - pd.Timestamp(T0)).dt.total_seconds()


def rates(df, lo, hi):
    w = df[(df.t >= lo) & (df.t < hi)]
    return w.rb.sum() / w.pl.sum(), w.fs.sum() / w.ss.sum()


def sel(cdn, metro, device="*", version="*"):
    m = (mins.cdn == cdn) & (mins.metro == metro)
    if device != "*":
        m &= mins.device_family == device
    if version == "7.1.x":
        m &= mins.app_version.str.startswith("7.1.")
    elif version != "*":
        m &= mins.app_version == version
    return mins[m]


coh = sel(**{k: COHORT[k] for k in ("cdn", "metro")}, device=COHORT["device_family"], version=COHORT["app_version"])
rb_plat, sf_plat = rates(coh, RAMP_END, SHIFT)
cell_plat = rates(sel(COHORT["cdn"], COHORT["metro"]), RAMP_END, SHIFT)[0]
check("cohort rebuffer plateau", abs(rb_plat - P.REBUFFER_INCIDENT) <= P.REBUFFER_TOL, f"{rb_plat:.4f}")
check("cohort start failures", abs(sf_plat - P.START_FAIL_INCIDENT) <= 0.01, f"{sf_plat:.4f}")
check("whole-cell rebuffer", abs(cell_plat - P.HOT_CELL_REBUFFER_INCIDENT) <= P.HOT_CELL_REBUFFER_TOL, f"{cell_plat:.4f}")
coh_min = coh.groupby("t").apply(lambda g: g.rb.sum() / g.pl.sum())
baseline_rb = coh_min[(coh_min.index >= ONSET - 3600) & (coh_min.index < ONSET)].median()
recovered = coh_min[coh_min.index > ONSET + 300]
first_ok = recovered[recovered < 1.5 * baseline_rb].index.min()
check("minutes to resolve", (first_ok - ONSET) / 60 == P.INCIDENT_MINUTES_TO_RESOLVE, f"{(first_ok - ONSET) / 60:.0f}")
for c in P.CONTROLS:
    df = sel(c["cdn"], c["metro"], c["device_family"], c["app_version"])
    (rb_b, sf_b), (rb_d, sf_d) = rates(df, ONSET - 1800, ONSET), rates(df, ONSET, RESOLVED)
    worst = max(abs(rb_d / rb_b - 1), abs(sf_d / sf_b - 1))
    check(f"control {c['cdn']} {c['metro']} {c['device_family']} {c['app_version']}", worst <= P.CONTROL_TOL_REL,
          f"max change {worst:.2%}")
dec = sel(P.DECOY["cdn"], P.DECOY["metro"]).groupby("t").apply(lambda g: g.rb.sum() / g.pl.sum()).sort_index()
z = []
for t, x in dec.items():
    hist = dec[(dec.index >= t - 3600) & (dec.index < t)]
    if len(hist) >= 30:
        med = hist.median()
        z.append((t, (x - med) / (1.4826 * (hist - med).abs().median())))
z = pd.Series(dict(z))
pre_dec = dec[(dec.index >= DECOY_LO - 3600) & (dec.index < DECOY_LO)]
window_z = (dec[(dec.index >= DECOY_LO) & (dec.index < DECOY_HI)].mean() - pre_dec.mean()) / pre_dec.std()
check("decoy max robust z", z.max() < P.DECOY["z_max"], f"{z.max():.2f}")
check("decoy window z", abs(window_z - P.DECOY["window_z"]) <= 0.3, f"{window_z:.2f}")
drop_by = {r.k: r.rate for r in sessions.where(F.col("metro") == COHORT["metro"]).withColumn(
    "k", F.when(F.expr(is_cohort) & (F.col("start_s") <= RESOLVED) & (F.col("end_s") >= ONSET), "cohort")
    .when(F.col("sample_weight") > 1, "baseline")).where(F.col("k").isNotNull()).groupBy("k").agg(
    (F.sum(F.col("sample_weight") * F.col("dropped").cast("double")) / F.sum("sample_weight")).alias("rate")).collect()}
lift = drop_by["cohort"] / drop_by["baseline"] - 1
check("session drop lift", abs(lift - P.SESSION_DROP_LIFT_REL) <= 0.05,
      f"{drop_by['cohort']:.4f} vs {drop_by['baseline']:.4f} (+{lift:.0%})")
check("affected sessions", abs(affected - P.AFFECTED_SESSIONS) / P.AFFECTED_SESSIONS <= 0.02, f"{affected:,}")

# Acquirer B and billing
bill = spark.table(cfg.fq("fact_billing_attempts")).where("attempt_type = 'signup'").join(  # noqa: F821
    gn.select("subscriber_id", "t_s"), "subscriber_id")
win_rates = {r.acquirer: r.rate for r in bill.where((F.col("t_s") >= ACQ_LO) & (F.col("t_s") < ACQ_HI)).groupBy(
    "acquirer").agg(F.avg((F.col("result") == "declined").cast("double")).alias("rate")).collect()}
check("Acquirer B window decline rate", abs(win_rates["B"] - P.ACQUIRER_B["decline_rate_incident"])
      <= P.ACQUIRER_DECLINE_TOL, f"{win_rates['B']:.3f}")
check("other acquirers normal", all(0.02 <= v <= 0.05 for k, v in win_rates.items() if k != "B"), str(win_rates))
pt = spark.table(cfg.fq("fact_billing_attempts")).where(  # noqa: F821
    f"decline_code = '{P.ACQUIRER_B['decline_code']}'").agg(F.count("*").alias("n"), F.max(
        F.col("retry_scheduled").cast("int")).alias("any_retry")).first()
check("processor_timeout never retried", pt.n == N_TIMEOUT + 2 and pt.any_retry == 0,
      f"{pt.n} declines (incl. 2 renewal timeouts)")
check("timeouts are not all cancels", pt.n > N_PAY, f"{pt.n} timeouts vs {N_PAY} payment cancels")

# Day-after cancels: total, split by the evidence rules, survey reason, state split, segment
ec = sports_cancels.toPandas()
expected = {"payment": N_PAY, "playback": N_PLAYBACK, "event_only": N_EVENT_ONLY}
excess = P.CANCELS_OBSERVED - BASELINE_EXPECTED
check("+76% vs baseline", round(P.CANCELS_OBSERVED / BASELINE_EXPECTED - 1, 2) == P.CANCELS_PCT_OVER,
      f"baseline {BASELINE_EXPECTED:,.1f}, +{P.CANCELS_OBSERVED / BASELINE_EXPECTED - 1:.1%}")
counts = ec.cause.value_counts().to_dict()
check("Sports day-after cancels", len(ec) == P.CANCELS_OBSERVED, f"{len(ec):,}")
ec_et = pd.to_datetime(ec.event_ts).dt.tz_localize("UTC").dt.tz_convert(ET)
by7 = (ec_et.dt.hour < 7).mean()
check("most cancels in by 07:00", by7 > 0.5 and (ec_et[ec.cause == "payment"].dt.hour < 7).all(),
      f"{by7:.0%} before 07:00 ET; payment all before 07:00")
for drv, (pct, n, _e, _c) in P.CANCEL_DRIVERS.items():
    got = counts.get(drv, 0) if drv != "unexplained" else len(ec) - round(BASELINE_EXPECTED) - sum(expected.values())
    check(f"driver {drv}", abs(got / excess - pct) <= P.CANCEL_SPLIT_TOL_PTS, f"{got:,} ({got / excess:.1%})")
check("payment/playback overlap", ec.overlap.sum() / N_TIMEOUT <= P.PAYMENT_PLAYBACK_OVERLAP_MAX, f"{ec.overlap.sum()}")
pb = ec[ec.cause == "playback"]
vol = ec[(ec.cause.isin(["baseline", "unexplained"])) & (ec.cancel_type == "voluntary")]
check("playback survey reason", abs((pb.reason == P.PLAYBACK_SURVEY_REASON).mean() - P.PLAYBACK_SURVEY_SHARE[0]) < 0.01
      and abs((vol.reason == P.PLAYBACK_SURVEY_REASON).mean() - P.PLAYBACK_SURVEY_SHARE[1]) < 0.01,
      f"{(pb.reason == P.PLAYBACK_SURVEY_REASON).mean():.0%} vs {(vol.reason == P.PLAYBACK_SURVEY_REASON).mean():.1%}")
by_state = ec.state.value_counts()
bad_states = [s for s, n in P.CANCELS_BY_STATE.items() if n >= 200
              and abs(by_state.get(s, 0) - n) / n > P.CANCELS_BY_STATE_TOL_REL]
check("cancels by state", not bad_states and tuple(by_state.index[:4]) == P.CANCELS_BY_STATE_TOP4,
      f"top4 {tuple(by_state.index[:4])}")
ec_days = pd.to_datetime(ec.event_ts).dt.tz_localize("UTC").dt.tz_convert(ET).dt.date
check("all Sports cancels on the day after", (ec_days == DAY_AFTER).all(), str(ec_days.value_counts().to_dict()))
# Event-only rate (voluntary cancel within 24 h / event-promo sign-ups) equals the prior big game nights.
ep_all = (subs.where("signup_source = 'event_promo' AND tier = 'Sports'")
          .withColumn("night", F.to_date(F.from_utc_timestamp("signup_ts", P.TZ)))
          .where(F.col("night").isin([GAME_DATE, *prior_nights.event_date])
                 & (F.hour(F.from_utc_timestamp("signup_ts", P.TZ)) >= 18)))
ev_c = spark.table(cfg.fq("fact_subscription_events")).where(  # noqa: F821
    "event_type = 'cancel' AND cancel_type = 'voluntary'").select("subscriber_id", F.col("event_ts").alias("c_ts"))
ev_rates = {str(r.night): r.rate for r in ep_all.join(ev_c, "subscriber_id", "left").groupBy("night").agg(
    (F.sum(((F.col("c_ts") - F.col("signup_ts")) <= F.expr("INTERVAL 24 HOURS")).cast("int"))
     / F.count("*")).alias("rate")).collect()}
check("event-only rate matches prior big nights", 0 < max(ev_rates.values()) / min(ev_rates.values()) - 1 <= 0.16,
      str({k: round(v, 4) for k, v in ev_rates.items()}))
seg = ec.merge(viewers.select("subscriber_id", "is_kids_only_account").toPandas(), on="subscriber_id", how="left")
seg = seg[(seg.cause == "payment") | seg.is_kids_only_account.notna()]
react_ids = set(reactivated.select("subscriber_id").toPandas().subscriber_id)
kids_in_seg = int(seg.is_kids_only_account.fillna(False).sum())
check("retro segment", len(seg) == P.SEGMENT_GROSS and kids_in_seg == P.KIDS_ONLY_IN_SEGMENT
      and len(react_ids) == P.REACTIVATED_IN_SEGMENT
      and len(seg) - kids_in_seg - len(react_ids) == P.SEGMENT_ELIGIBLE,
      f"gross {len(seg):,}, kids-only {kids_in_seg}, reactivated {len(react_ids)}")
for tier in ("Family", "Originals"):
    check(f"{tier} inside its range", abs(day_counts[tier] / wd_mean[tier] - 1) <= 0.03,
          f"{day_counts[tier]:,} vs {wd_mean[tier]:,.0f}")
check("ledger Sports", abs((P.CANCELS_OBSERVED - ledger["Sports"]) / P.CANCELS_OBSERVED - P.RECON_SPORTS["pct_diff"])
      <= P.RECON_SPORTS_TOL, f"{ledger['Sports']:,}")
check("ledger Originals", abs((ledger["Originals"] - day_counts["Originals"]) / day_counts["Originals"] - P.RECON_ORIGINALS["pct_diff"])
      <= P.RECON_ORIGINALS_TOL, f"{ledger['Originals']:,}")

# Scoring cohort
sc = scored.agg(F.count("*").alias("n"), F.sum("p_true").alias("p_sum")).first()
top = scored.withColumn("rk", F.row_number().over(Window.orderBy(F.desc("p_oracle")))).where(
    F.col("rk") <= P.AT_RISK).agg(F.avg("p_oracle").alias("mean_p"), F.sum("p_oracle").alias("sum_p")).first()
check("scoring cohort size", sc.n == P.SCORING_COHORT, f"{sc.n:,}")
check("top-decile projected cancels (oracle)", abs(top.sum_p - P.PROJECTED_CANCELS_30D) / P.PROJECTED_CANCELS_30D
      <= P.PROJECTED_TOL_REL, f"{top.sum_p:,.0f} (mean p {top.mean_p:.3f})")

n_tel = spark.read.json(f"{LANDING}/cdn").count()  # noqa: F821
n_sess = session_events.count()
check("telemetry rows", 1.5e6 <= n_tel <= 2.2e6, f"{n_tel:,}")
# Team numbers that come from this notebook's rows (Care is planted and checked in 05).
clicks_day = funnel.where((F.col("day") == F.lit(DAY_AFTER)) & (F.col("tier") == "Sports")).count()
check("Product analytics clicks", abs(clicks_day / TEAM["Product analytics"][0] - 1) <= P.TEAM_CANCEL_TOL_REL,
      f"{clicks_day:,} (saved {N_SAVED:,}, reversed {N_REVERSED:,})")
check("Finance ledger", abs(ledger["Sports"] / TEAM["Finance"][0] - 1) <= P.TEAM_CANCEL_TOL_REL, f"{ledger['Sports']:,}")
# Samples: active, non-kids, never cancelled, in the at-risk top decile, and their stories hold in the data.
smp = subs.where(F.col("subscriber_id").isin(SAMPLE_IDS)).toPandas().set_index("subscriber_id")
check("samples active", (smp.status == "active").all() and smp.canceled_ts.isna().all()
      and (~smp.is_kids_only_account).all(), smp[["status", "canceled_ts", "is_kids_only_account"]].to_dict())
check("samples at risk", (ranks.risk_rank <= P.AT_RISK).all(), ranks.round(3).to_dict())
m_reb = sessions.where((F.col("subscriber_id") == MARCUS) & F.expr(is_cohort)).agg(
    F.sum(F.col("n_hb_b") * HB_S)).first()[0]
check("Marcus: 14 min buffering on Roku 7.2.0 in DFW", m_reb == MARCUS_REBUFFER_S and smp.metro[MARCUS] == "DFW",
      f"{m_reb / 60:.0f} min")
p_att = spark.table(cfg.fq("fact_billing_attempts")).where(F.col("subscriber_id") == PRIYA).toPandas()  # noqa: F821
check("Priya: two processor timeouts, no retry", (p_att.decline_code == P.ACQUIRER_B["decline_code"]).sum() == 2
      and not p_att.retry_scheduled.any() and smp.tier[PRIYA] == "Bundle" and smp.metro[PRIYA] == "PHX", f"{len(p_att)} attempts")
j_sess = sessions.where(F.col("subscriber_id") == JORDAN).count()
check("Jordan: event sign-up in CHI, nothing else watched", smp.signup_source[JORDAN] == "event_promo"
      and smp.metro[JORDAN] == "CHI" and j_sess == 0, f"{j_sess} sampled sessions")
n_view = spark.table(cfg.fq("dim_subscriber_viewing")).count()  # noqa: F821
check("viewing history covers the scoring cohort", n_view == P.SCORING_COHORT, f"{n_view:,}")
check("session event rows", 2.2e6 <= n_sess <= 3.3e6, f"{n_sess:,}")

failed = [f"{n}: {d}" for n, c, d in ok if not c]
assert not failed, "planted-signal checks failed (intermediate gen_*_tmp tables kept for debugging): " + "; ".join(failed)
for t in ("gen_gn_signups_tmp", "gen_viewers_tmp", "gen_sports_cancels_tmp", "gen_game_telemetry_tmp",
          "gen_sessions_tmp", "gen_session_events_tmp", "gen_scored_pre_tmp", "gen_scored_tmp"):
    spark.sql(f"DROP TABLE IF EXISTS {cfg.fq(t)}")  # noqa: F821
print("04_gen_game_night: all self-asserts passed")
