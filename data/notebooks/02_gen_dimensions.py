# Databricks notebook source
# Owner: Stream A (Data + Pipelines). The synthetic "world": who the subscribers are and how traffic
# is spread across metros, CDNs and devices.
#
# Writes dim_subscribers (scale_subscribers rows, incl. the history-window and game-night sign-ups),
# dim_profiles (~7M, ~15% kids), ref_cdn_capacity, ref_incidents_history, offer_holdout_results,
# eval_gt_kids_accounts, plus three generator tables read by 03/04 (not part of the app contract):
#   gen_cell_mix     metro x CDN x device x version traffic shares (sum 1.0)
#   gen_events       the Big Game and the 8 past big events (audience, surge multiplier, sign-up rate)
#   gen_event_curve  10-second event-night curve: pre-game shape and the halftime bump h(t)
# dbldatagen (partitions=64, hash_fieldname seeds) for the big dimension; numpy on the driver only
# for small reference tables. Idempotent: every table is overwritten.

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

import dbldatagen as dg  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from cc_common import config, planted as P, seeds  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
rng = seeds.rng(cfg.seed, "02")
ET = ZoneInfo(P.TZ)
GAME_DATE = date.fromisoformat(cfg.game_date)
N = cfg.scale_subscribers
# Planted counts (day-after cancels, 31k at-risk, segment sizes) are absolute, so the population is fixed.
assert N == P.N_SUBSCRIBERS, f"scale_subscribers must be {P.N_SUBSCRIBERS:,} (planted counts assume it), got {N:,}"
SCALE = N / P.N_SUBSCRIBERS


def u(salt: str, *cols):
    """Deterministic U[0,1) per row from (seed, salt, cols); independent of partitioning."""
    h = F.xxhash64(F.lit(cfg.seed), F.lit(salt), *cols)
    return F.pmod(h, F.lit(1 << 40)).cast("double") / float(1 << 40)


def pick(uc, items: dict):
    """CASE expression choosing a key of `items` by cumulative weight against the uniform column uc."""
    total, acc, expr = sum(items.values()), 0.0, None
    keys = list(items)
    for k in keys[:-1]:
        acc += items[k] / total
        expr = F.when(uc < acc, F.lit(k)) if expr is None else expr.when(uc < acc, F.lit(k))
    return expr.otherwise(F.lit(keys[-1]))


def et_to_utc(d: date, seconds: float) -> datetime:
    """ET wall-clock (date + seconds after midnight) to a naive UTC datetime."""
    local = datetime(d.year, d.month, d.day, tzinfo=ET) + timedelta(seconds=float(seconds))
    return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def write(df, name: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq(name))

# COMMAND ----------
# Traffic mix. DFW's share is fixed by the planted cell loads: CDN-C DFW peak 232k = 45% of DFW.

DFW_SHARE = P.CDN_C_DFW_PEAK_LOAD / (P.DFW_CDN_SHARE["CDN-C"] * P.PEAK_CONCURRENT)
OTHER_METRO_W = {"NYC": 14.0, "LAX": 10.5, "CHI": 7.5, "HOU": 5.5, "ATL": 5.2, "WAS": 5.5, "PHL": 5.3,
                 "MIA": 5.0, "BOS": 4.6, "PHX": 4.4, "SEA": 4.2, "SFO": 4.8, "DEN": 3.8, "MSP": 3.6}
METRO_SHARE = {m: (1 - DFW_SHARE) * w / sum(OTHER_METRO_W.values()) for m, w in OTHER_METRO_W.items()}
METRO_SHARE["DFW"] = DFW_SHARE
assert set(METRO_SHARE) == set(P.METROS)

METRO_CDNS = {
    "NYC": {"CDN-A": .35, "CDN-B": .25, "CDN-D": .20, "CDN-E": .20},
    "LAX": {"CDN-A": .30, "CDN-D": .25, "CDN-E": .25, "CDN-G": .20},
    "CHI": {"CDN-A": .30, "CDN-B": .30, "CDN-D": .20, "CDN-E": .20},
    "DFW": dict(P.DFW_CDN_SHARE),
    "HOU": {"CDN-C": .40, "CDN-B": .35, "CDN-D": .25},
    "ATL": {"CDN-C": .38, "CDN-A": .32, "CDN-E": .30},
    "WAS": {"CDN-A": .35, "CDN-B": .35, "CDN-D": .30},
    "PHL": {"CDN-A": .40, "CDN-B": .30, "CDN-E": .30},
    "MIA": {"CDN-B": .40, "CDN-D": .35, "CDN-G": .25},
    "BOS": {"CDN-A": .40, "CDN-B": .30, "CDN-E": .30},
    "PHX": {"CDN-D": .40, "CDN-E": .35, "CDN-G": .25},
    "SEA": {"CDN-A": .40, "CDN-F": .30, "CDN-G": .30},
    "SFO": {"CDN-A": .35, "CDN-G": .35, "CDN-D": .30},
    "DEN": {"CDN-B": .40, "CDN-D": .35, "CDN-G": .25},
    "MSP": {"CDN-A": .40, "CDN-B": .35, "CDN-E": .25},
}
DEVICE_SHARE = {"Roku": .35, "FireTV": .15, "SmartTV": .18, "iOS": .12, "Android": .12, "Web": .08}
_r720 = P.ROKU_720_SHARE_OF_CELL / DEVICE_SHARE["Roku"]          # 7.2.0 = 22% of the cell's traffic
VERSION_SHARE = {
    "Roku": {"7.0.4": .06, "7.1.2": .12, "7.1.5": 1 - .18 - _r720, "7.2.0": _r720},
    "FireTV": {"5.4.1": .7, "5.3.9": .3}, "SmartTV": {"3.9.0": .7, "3.8.2": .3},
    "iOS": {"12.3.0": .75, "12.2.1": .25}, "Android": {"12.2.4": .7, "12.1.0": .3},
    "Web": {"2027.02": .8, "2027.01": .2},
}
assert tuple(VERSION_SHARE["Roku"]) == P.ROKU_VERSIONS and tuple(DEVICE_SHARE) == P.DEVICE_FAMILIES

cell_rows = [(m, c, d, v, METRO_SHARE[m], cs, DEVICE_SHARE[d], vs, METRO_SHARE[m] * cs * DEVICE_SHARE[d] * vs)
             for m, cdns in METRO_CDNS.items() for c, cs in cdns.items()
             for d, vers in VERSION_SHARE.items() for v, vs in vers.items()]
cell_mix = pd.DataFrame(cell_rows, columns=["metro", "cdn", "device_family", "app_version", "metro_share",
                                            "cdn_share", "device_share", "version_share", "share"])
assert abs(cell_mix.share.sum() - 1) < 1e-9
write(spark.createDataFrame(cell_mix), "gen_cell_mix")  # noqa: F821
print(f"cells: {len(cell_mix)}  metro x CDN pairs: {cell_mix[['metro', 'cdn']].drop_duplicates().shape[0]}")

# COMMAND ----------
# Event-night curve (10 s grain, 18:00-22:30 ET). Total concurrent viewers on an event night:
#   N(t) = pre_level * pre_shape(t) * (1 + (m - 1) * h(t))
# h(t) is the halftime bump: logistic rise from 20:14 over ~3 min, plateau, ~5 min decay after 20:30.
# Sign-ups per minute: lambda0 * (1 + 10 * h(t)), so the halftime peak is 11x the pre-halftime rate.


def pre_shape(t_min: np.ndarray) -> np.ndarray:
    return np.interp(t_min, [0, 30, 60, 134, 155, 240, 270], [0.55, 0.95, 1.0, 1.0, 1.02, 1.02, 0.45])


def halftime(t_min: np.ndarray) -> np.ndarray:
    x = t_min - 120.0                                  # minutes after 20:00
    h = 1 / (1 + np.exp(-(x - 15.5) / 0.5)) / (1 + np.exp((x - 32.5) / 0.6))
    return h


CURVE_STEP_S = 10
offsets = np.arange(0, 270 * 60, CURVE_STEP_S)
h_grid = halftime(offsets / 60.0)
H_MAX = h_grid.max()
curve = pd.DataFrame({
    "offset_s": offsets.astype(int),
    "et_clock": [f"{18 + o // 3600:02d}:{(o % 3600) // 60:02d}:{o % 60:02d}" for o in offsets],
    "h": h_grid / H_MAX,
    "pre_shape": pre_shape(offsets / 60.0),
})
write(spark.createDataFrame(curve), "gen_event_curve")  # noqa: F821
SIGNUP_LAMBDA0 = 244.0  # pre-halftime sign-ups per minute on the Big Game night (Sports)
minute_h = halftime(np.arange(270) + 0.5) / H_MAX


# Past big events (Sundays, same weekday as the game). The two most recent are inside the 14-day
# history window, so their sign-ups and next-day event-only cancels exist row by row. Past events sell far
# fewer sign-ups than the Big Game, so their next-day bump in cancels stays modest.
PAST_OFFSETS = (7, 14, 28, 98, 133, 168, 203, 364)
assert len(PAST_OFFSETS) == P.N_PAST_BIG_EVENTS
EVENT_PROMO_SHARE_EVENT = 0.65
events = [{"event_id": f"EVT-{GAME_DATE:%Y%m%d}", "event_date": GAME_DATE, "pre_level": P.PRE_HALFTIME_CONCURRENT,
           "surge_mult": P.SURGE_MULTIPLIER, "signup_lambda0": SIGNUP_LAMBDA0, "is_target": True}]
for off in PAST_OFFSETS:
    d = GAME_DATE - timedelta(days=off)
    events.append({"event_id": f"EVT-{d:%Y%m%d}", "event_date": d,
                   "pre_level": int(round(rng.uniform(2.4e6, 3.4e6), -3)),
                   "surge_mult": float(np.round(rng.uniform(*P.PAST_EVENT_MULT_RANGE), 3)),
                   "signup_lambda0": float(np.round(rng.uniform(12, 20), 1)), "is_target": False})
events_df = pd.DataFrame(events)
assert all(P.PAST_EVENT_MULT_RANGE[0] <= m <= P.PAST_EVENT_MULT_RANGE[1] for m in events_df.surge_mult)

# COMMAND ----------
# Sign-up segments with exact row counts. Ids: base 0..N_base-1, then window / event sign-ups.

W_START = GAME_DATE - timedelta(days=14)                     # first day of the 14-day history window
WINDOW_DAYS = [W_START + timedelta(days=i) for i in range(15)]  # through game day
WINDOW_DAILY = {"Sports": 4000, "Originals": 1200, "Family": 2000, "Bundle": 150}
TIER_TARGET = {"Sports": P.N_SPORTS_TIER, "Family": 800_000, "Originals": 450_000, "Bundle": 250_000}
assert sum(TIER_TARGET.values()) == P.N_SUBSCRIBERS
TIER_TARGET = {k: v * SCALE for k, v in TIER_TARGET.items()}

seg_rows = []  # kind, tier, signup_utc, event_id, minute (event nights only)
for d in WINDOW_DAYS:
    end_s = 18 * 3600 if d == GAME_DATE else 24 * 3600   # game night sign-ups are their own segment
    for tier, n in WINDOW_DAILY.items():
        n = int(round(n * SCALE))
        secs = rng.uniform(7 * 3600, end_s, n)
        seg_rows += [("window", tier, et_to_utc(d, s), None, -1) for s in secs]
for ev in events:
    if ev["is_target"] or (GAME_DATE - ev["event_date"]).days <= 14:
        counts = rng.poisson(ev["signup_lambda0"] * SCALE * (1 + (P.SIGNUP_MULTIPLIER - 1) * minute_h))
        for m, c in enumerate(counts):
            secs = 18 * 3600 + 60 * m + rng.uniform(0, 60, c)
            seg_rows += [("event", "Sports", et_to_utc(ev["event_date"], s), ev["event_id"], m) for s in secs]
seg = pd.DataFrame(seg_rows, columns=["kind", "tier", "signup_ts", "event_id", "minute"])
seg = seg.sort_values(["kind", "signup_ts"], kind="stable").reset_index(drop=True)
N_SEG = len(seg)
N_BASE = N - N_SEG
seg["id"] = np.arange(N_BASE, N, dtype=np.int64)

# Acquirer routing for the Big Game: exactly n_B sign-ups in the Acquirer-B window go to B, so that
# planted.CANCEL_DRIVERS["payment"] processor timeouts give the planted 17% in-window decline rate.
ACQ = P.ACQUIRER_B
N_TIMEOUTS = P.CANCEL_DRIVERS["payment"][1]
N_B_WINDOW = round(N_TIMEOUTS / (ACQ["decline_rate_incident"] - ACQ["decline_rate_baseline"]))
win_lo = et_to_utc(GAME_DATE, 3600 * int(ACQ["window"][0][:2]) + 60 * int(ACQ["window"][0][3:]))
win_hi = et_to_utc(GAME_DATE, 3600 * int(ACQ["window"][1][:2]) + 60 * (int(ACQ["window"][1][3:]) + 1))
target_id = events[0]["event_id"]
in_win = (seg.event_id == target_id) & (seg.signup_ts >= win_lo) & (seg.signup_ts < win_hi)
assert in_win.sum() > N_B_WINDOW / 0.6, f"Acquirer-B window too thin: {in_win.sum()} sign-ups"
order = rng.permutation(int(in_win.sum()))
seg["acquirer_seg"] = None
seg.loc[in_win, "acquirer_seg"] = np.where(order < N_B_WINDOW, "B", rng.choice(["A", "C", "D"], int(in_win.sum())))
src_draw = rng.random(N_SEG)
seg["source_seg"] = np.where(
    seg.kind == "event",
    np.select([src_draw < EVENT_PROMO_SHARE_EVENT, src_draw < 0.95], ["event_promo", "organic"], "winback"),
    np.select([seg.tier == "Bundle", src_draw < 0.80, src_draw < 0.95], ["bundle_partner", "organic", "winback"],
              "event_promo"))

for ev in events:
    sel = seg.event_id == ev["event_id"]
    ev["n_signups"] = int(sel.sum()) if sel.any() else int(round(
        ev["signup_lambda0"] * SCALE * (1 + (P.SIGNUP_MULTIPLIER - 1) * minute_h).sum()))
    ev["n_event_promo"] = int((sel & (seg.source_seg == "event_promo")).sum()) if sel.any() else int(
        round(ev["n_signups"] * EVENT_PROMO_SHARE_EVENT))
events_df = pd.DataFrame(events)
write(spark.createDataFrame(events_df), "gen_events")  # noqa: F821

# Signup multiplier self-assert (minute counts of the Big Game segment)
gm = seg[seg.event_id == target_id].groupby("minute").size().reindex(range(270), fill_value=0)
pre_rate = gm.loc[30:119].median()
signup_mult = gm.max() / pre_rate
print(f"Big Game sign-ups {int(gm.sum()):,}; peak {gm.max()}/min vs {pre_rate:.0f}/min -> x{signup_mult:.1f}")
assert abs(signup_mult - P.SIGNUP_MULTIPLIER) <= P.SIGNUP_MULT_TOL

# COMMAND ----------
# Base attributes for every id with dbldatagen (deterministic: fixed seed, hash_fieldname, 64 partitions).

seg_tier = seg.tier.value_counts().to_dict()
base_tier_w = {t: max(TIER_TARGET[t] - seg_tier.get(t, 0), 0) for t in TIER_TARGET}
FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
         "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Carlos", "Karen",
         "Daniel", "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Sandra", "Luis", "Ashley", "Steven",
         "Kimberly", "Andrew", "Emily", "Kevin", "Donna", "Brian", "Michelle", "Jose", "Carol", "Wei", "Amanda",
         "Ryan", "Melissa", "Jacob", "Deborah", "Gary", "Stephanie", "Nicholas", "Maria", "Eric", "Priya"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
        "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
        "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Patel", "Chen", "Kim"]
STREETS = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill", "Park", "View",
           "Sunset", "Ridge", "Highland", "Willow", "Meadow", "River", "Forest", "Spring", "Church", "Lincoln"]
SUFFIX = ["St", "Ave", "Rd", "Ln", "Dr", "Ct", "Blvd", "Way"]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "aol.com", "hotmail.com", "proton.me"]

spec = (dg.DataGenerator(spark, name="dim_subscribers_base", rows=N, partitions=64,  # noqa: F821
                         randomSeedMethod="hash_fieldname", randomSeed=cfg.seed)
        .withIdOutput()
        .withColumn("tier_base", "string", values=list(base_tier_w),
                    weights=[int(v) for v in base_tier_w.values()], random=True)
        .withColumn("device_primary", "string", values=list(DEVICE_SHARE),
                    weights=[int(v * 1000) for v in DEVICE_SHARE.values()], random=True)
        .withColumn("acquirer_base", "string", values=["A", "B", "C", "D"], weights=[35, 25, 25, 15], random=True)
        .withColumn("source_base", "string", values=["organic", "winback", "event_promo"],
                    weights=[72, 12, 16], random=True)
        .withColumn("first_i", "int", minValue=0, maxValue=len(FIRST) - 1, random=True)
        .withColumn("last_i", "int", minValue=0, maxValue=len(LAST) - 1, random=True)
        .withColumn("street_i", "int", minValue=0, maxValue=len(STREETS) - 1, random=True)
        .withColumn("suffix_i", "int", minValue=0, maxValue=len(SUFFIX) - 1, random=True)
        .withColumn("domain_i", "int", minValue=0, maxValue=len(DOMAINS) - 1, random=True)
        .withColumn("house_no", "int", minValue=10, maxValue=9899, random=True)
        .withColumn("email_n", "int", minValue=1, maxValue=99, random=True)
        .withColumn("area_code", "int", minValue=201, maxValue=989, random=True)
        .withColumn("phone_n", "int", minValue=2000000, maxValue=9999999, random=True)
        .withColumn("zip_n", "int", minValue=10001, maxValue=99950, random=True))
base = spec.build()

# COMMAND ----------
# State follows planted.STATE_SHARE; metro is the home metro when the state has one (METRO_STATE), else null.

STATE_TZ_CENTRAL = {"Texas", "Illinois", "Minnesota", "Wisconsin", "Iowa", "Missouri", "Arkansas", "Louisiana",
                    "Mississippi", "Alabama", "Tennessee", "Oklahoma", "Kansas", "Nebraska", "South Dakota",
                    "North Dakota"}
STATE_TZ_MOUNTAIN = {"Colorado", "Utah", "New Mexico", "Wyoming", "Montana", "Idaho"}
STATE_TZ_PACIFIC = {"California", "Washington", "Oregon", "Nevada"}
STATE_TZ_OTHER = {"Arizona": "America/Phoenix", "Alaska": "America/Anchorage", "Hawaii": "Pacific/Honolulu"}


def state_tz(s: str) -> str:
    if s in STATE_TZ_OTHER:
        return STATE_TZ_OTHER[s]
    for group, tz in ((STATE_TZ_CENTRAL, "America/Chicago"), (STATE_TZ_MOUNTAIN, "America/Denver"),
                      (STATE_TZ_PACIFIC, "America/Los_Angeles")):
        if s in group:
            return tz
    return "America/New_York"


STATE_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO",
    "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"}
assert set(STATE_ABBR) == set(P.STATE_SHARE)
# Real city, area codes and ZIP prefix per state (non-metro subscribers); metro subscribers use METRO_PHONE.
STATE_INFO = {
    "Alabama": ("Birmingham", (205, 659), 352), "Alaska": ("Anchorage", (907,), 995), "Arizona": ("Tucson", (520,), 857),
    "Arkansas": ("Little Rock", (501,), 722), "California": ("Sacramento", (916,), 958),
    "Colorado": ("Colorado Springs", (719,), 809), "Connecticut": ("Hartford", (860, 959), 61),
    "Delaware": ("Wilmington", (302,), 198), "District of Columbia": ("Washington", (202,), 200),
    "Florida": ("Tampa", (813, 727), 336), "Georgia": ("Savannah", (912,), 314), "Hawaii": ("Honolulu", (808,), 968),
    "Idaho": ("Boise", (208, 986), 837), "Illinois": ("Springfield", (217,), 627), "Indiana": ("Indianapolis", (317,), 462),
    "Iowa": ("Des Moines", (515,), 503), "Kansas": ("Wichita", (316,), 672), "Kentucky": ("Louisville", (502,), 402),
    "Louisiana": ("New Orleans", (504,), 701), "Maine": ("Portland", (207,), 41), "Maryland": ("Baltimore", (410, 443), 212),
    "Massachusetts": ("Worcester", (508,), 16), "Michigan": ("Detroit", (313,), 482), "Minnesota": ("Duluth", (218,), 558),
    "Mississippi": ("Jackson", (601,), 392), "Missouri": ("Kansas City", (816,), 641), "Montana": ("Billings", (406,), 591),
    "Nebraska": ("Omaha", (402,), 681), "Nevada": ("Las Vegas", (702, 725), 891), "New Hampshire": ("Manchester", (603,), 31),
    "New Jersey": ("Newark", (973, 862), 71), "New Mexico": ("Albuquerque", (505,), 871), "New York": ("Buffalo", (716,), 142),
    "North Carolina": ("Charlotte", (704, 980), 282), "North Dakota": ("Fargo", (701,), 581),
    "Ohio": ("Columbus", (614, 380), 432), "Oklahoma": ("Oklahoma City", (405,), 731), "Oregon": ("Portland", (503, 971), 972),
    "Pennsylvania": ("Pittsburgh", (412,), 152), "Rhode Island": ("Providence", (401,), 29),
    "South Carolina": ("Columbia", (803,), 292), "South Dakota": ("Sioux Falls", (605,), 571),
    "Tennessee": ("Nashville", (615, 629), 372), "Texas": ("San Antonio", (210,), 782), "Utah": ("Salt Lake City", (801, 385), 841),
    "Vermont": ("Burlington", (802,), 54), "Virginia": ("Richmond", (804,), 232), "Washington": ("Spokane", (509,), 992),
    "West Virginia": ("Charleston", (304,), 253), "Wisconsin": ("Milwaukee", (414,), 532), "Wyoming": ("Cheyenne", (307,), 820)}
METRO_PHONE = {"NYC": ((212, 718, 917, 646), 100), "LAX": ((213, 310, 323, 818), 900), "CHI": ((312, 773, 872), 606),
               "DFW": ((214, 469, 972, 817), 752), "HOU": ((713, 281, 832), 770), "ATL": ((404, 678, 770), 303),
               "WAS": ((202,), 200), "PHL": ((215, 267), 191), "MIA": ((305, 786), 331), "BOS": ((617, 857), 21),
               "PHX": ((602, 480, 623), 850), "SEA": ((206, 425), 981), "SFO": ((415, 628), 941),
               "DEN": ((303, 720), 802), "MSP": ((612, 651), 554)}
assert set(STATE_INFO) == set(P.STATE_SHARE) and set(METRO_PHONE) == set(P.METROS)
NULL_RATE = 0.02   # missing email / phone / device in the source system (join keys are never null)


def lookup(mapping: dict, key_col, idx_col=None):
    """element_at over a literal map; with idx_col, picks the idx-th element of a tuple value (mod length)."""
    if idx_col is None:
        return F.element_at(F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))]), key_col)
    arr = F.element_at(F.create_map(*[x for k, v in mapping.items()
                                      for x in (F.lit(k), F.array(*[F.lit(i) for i in v]))]), key_col)
    return F.element_at(arr, (F.pmod(idx_col, F.size(arr)) + 1).cast("int"))


STATE_METROS = {}
for m, s in P.METRO_STATE.items():
    STATE_METROS.setdefault(s, {})[m] = METRO_SHARE[m]
state_map = F.create_map(*[x for s in P.STATE_SHARE for x in (F.lit(s), F.lit(state_tz(s)))])
abbr_map = F.create_map(*[x for s, a in STATE_ABBR.items() for x in (F.lit(s), F.lit(a))])


def metro_for_state(uc):
    expr = None
    for s, metros in STATE_METROS.items():
        choice = pick(uc, metros) if len(metros) > 1 else F.lit(next(iter(metros)))
        expr = F.when(F.col("state") == s, choice) if expr is None else expr.when(F.col("state") == s, choice)
    return expr.otherwise(F.lit(None).cast("string"))


metro_tz = F.create_map(*[x for m, (_n, _la, _lo, tz) in P.METROS.items() for x in (F.lit(m), F.lit(tz))])
METRO_CITY = {m: name for m, (name, *_r) in P.METROS.items()} | {"DFW": "Dallas"}   # a real city for the address
metro_name = F.create_map(*[x for m, c in METRO_CITY.items() for x in (F.lit(m), F.lit(c))])

# COMMAND ----------

seg_sdf = spark.createDataFrame(seg[["id", "kind", "tier", "signup_ts", "event_id", "acquirer_seg",  # noqa: F821
                                     "source_seg"]])
w_start_local = f"{W_START.isoformat()} 00:00:00"
kids_rate = P.KIDS_ONLY_ACCOUNT_SHARE * N / (N - int((seg.kind == "event").sum()))
is_event_signup = F.col("kind") == "event"
first_arr = F.array(*[F.lit(x) for x in FIRST])
last_arr = F.array(*[F.lit(x) for x in LAST])

subs = (base.join(seg_sdf, "id", "left")
        .withColumn("subscriber_id", F.col("id") + 1)
        .withColumn("account_id", F.col("id") + 100_000_001)
        .withColumn("tier", F.coalesce("tier", "tier_base"))
        .withColumn("is_bundle", F.col("tier") == "Bundle")
        # base sign-ups: exponential look-back from the window start, evening-weighted time of day
        .withColumn("signup_ts", F.coalesce("signup_ts", F.to_utc_timestamp(
            F.to_timestamp(F.lit(w_start_local))
            - F.make_interval(days=F.least(F.floor(-F.log(1 - u("signup_age", "id")) * 500), F.lit(2190)).cast("int"))
            + F.make_interval(secs=(7 + 17 * u("signup_tod", "id")) * 3600), P.TZ)))
        .withColumn("signup_source", F.when(F.col("source_seg").isNotNull(), F.col("source_seg"))
                    .when(F.col("tier") == "Bundle", F.lit("bundle_partner")).otherwise(F.col("source_base")))
        # event-promo Sports sign-ups often get a discounted first price (a decoy: correlated with the
        # event-only driver, no effect of its own)
        .withColumn("plan_price", F.when((F.col("tier") == "Sports") & (F.col("signup_source") == "event_promo")
                                         & (u("promo_price", "id") < 0.5), 9.99)
                    .when(F.col("tier") == "Sports", 11.99).when(F.col("tier") == "Family", 9.99)
                    .when(F.col("tier") == "Bundle", 16.99)
                    .when(u("originals_plan", "id") < 0.6, 7.99).otherwise(17.99).cast("decimal(6,2)"))
        .withColumn("state", pick(u("state", "id"), P.STATE_SHARE))
        .withColumn("metro", metro_for_state(u("metro", "id")))
        .withColumn("tz", F.coalesce(F.element_at(metro_tz, F.col("metro")), F.element_at(state_map, F.col("state"))))
        .withColumn("acquirer", F.coalesce("acquirer_seg", F.when(is_event_signup & (u("acq", "id") < 0.25),
                                                                  F.lit("B")),
                                           F.when(is_event_signup, pick(u("acq2", "id"), {"A": 4, "C": 4, "D": 2})),
                                           "acquirer_base"))
        .withColumn("is_kids_only_account", (F.col("kind").isNull() | (F.col("kind") == "window"))
                    & (u("kids_only", "id") < kids_rate))
        .withColumn("status", F.when(F.col("kind").isNull() & (u("paused", "id") < 0.015), F.lit("paused"))
                    .otherwise(F.lit("active")))
        .withColumn("canceled_ts", F.lit(None).cast("timestamp"))
        .withColumn("first", F.element_at(first_arr, F.col("first_i") + 1))
        .withColumn("last", F.element_at(last_arr, F.col("last_i") + 1))
        # unique per subscriber: the local part carries the subscriber id
        .withColumn("email", F.when(u("null_email", "id") >= NULL_RATE, F.lower(F.concat(
            F.when(F.col("email_n") < 50, F.concat_ws(".", "first", "last"))
            .otherwise(F.concat(F.substring("first", 1, 1), F.col("last"))),
            F.col("subscriber_id").cast("string"), F.lit("@"),
            F.element_at(F.array(*[F.lit(x) for x in DOMAINS]), F.col("domain_i") + 1)))))
        .withColumn("area", F.coalesce(
            lookup({m: v[0] for m, v in METRO_PHONE.items()}, F.col("metro"), F.col("area_code")),
            lookup({s: v[1] for s, v in STATE_INFO.items()}, F.col("state"), F.col("area_code"))))
        .withColumn("phone", F.when(u("null_phone", "id") >= NULL_RATE, F.format_string(
            "+1-%03d-%03d-%04d", F.col("area"), (F.col("phone_n") / 10000).cast("int"), F.col("phone_n") % 10000)))
        .withColumn("device_primary", F.when(u("null_device", "id") >= NULL_RATE, F.col("device_primary")))
        .withColumn("zip3", F.coalesce(lookup({m: v[1] for m, v in METRO_PHONE.items()}, F.col("metro")),
                                       lookup({s: v[2] for s, v in STATE_INFO.items()}, F.col("state"))))
        .withColumn("postal_address", F.format_string(
            "%d %s %s, %s, %s %05d", F.col("house_no"),
            F.element_at(F.array(*[F.lit(x) for x in STREETS]), F.col("street_i") + 1),
            F.element_at(F.array(*[F.lit(x) for x in SUFFIX]), F.col("suffix_i") + 1),
            F.coalesce(F.element_at(metro_name, F.col("metro")),
                       lookup({s: v[0] for s, v in STATE_INFO.items()}, F.col("state"))),
            F.element_at(abbr_map, F.col("state")), F.col("zip3") * 100 + F.pmod("zip_n", F.lit(100))))
        .select("subscriber_id", "account_id", "tier", "is_bundle", "signup_ts", "signup_source", "plan_price",
                "metro", "state", "tz", "device_primary", "acquirer", "status", "canceled_ts", "email", "phone",
                "postal_address", "is_kids_only_account"))
write(subs, "dim_subscribers")
subs = spark.table(cfg.fq("dim_subscribers"))  # noqa: F821

# COMMAND ----------
# dim_profiles: kids-only accounts have only kids profiles; other accounts have 1-3 adults and
# sometimes kids. profile_id = subscriber_id * 10 + k; profile 0 is the account owner.

profiles = (subs.select("subscriber_id", "signup_ts", "is_kids_only_account")
            .withColumn("n_adult", F.when(F.col("is_kids_only_account"), 0).otherwise(
                1 + (u("ad1", "subscriber_id") < 0.27).cast("int") + (u("ad2", "subscriber_id") < 0.27).cast("int")))
            .withColumn("n_kids", F.when(F.col("is_kids_only_account"),
                                         1 + (u("k1", "subscriber_id") < 0.2).cast("int"))
                        .otherwise((u("k2", "subscriber_id") < 0.17).cast("int")
                                   * (1 + (u("k3", "subscriber_id") < 0.34).cast("int"))))
            .withColumn("k", F.explode(F.sequence(F.lit(0), F.col("n_adult") + F.col("n_kids") - 1)))
            .select((F.col("subscriber_id") * 10 + F.col("k")).alias("profile_id"), "subscriber_id",
                    F.when(F.col("k") < F.col("n_adult"), "adult").otherwise("kids").alias("profile_type"),
                    (F.col("signup_ts") + F.make_interval(
                        days=(F.col("k") * 40 * u("pc", "subscriber_id", "k")).cast("int"))).alias("created_ts")))
write(profiles, "dim_profiles")
write(subs.where("is_kids_only_account").select("account_id"), "eval_gt_kids_accounts")

# COMMAND ----------
# ref_cdn_capacity: capacity = forecast peak / target utilization (forecast peak = planted p50 x traffic share).
# CDN-C DFW runs at 94%; CDN-A DFW is the planted backup (226k); CDN-B DFW is tight (0.78) so the
# same-metro headroom rule picks CDN-A; every other cell draws U(0.55, 0.78).

pairs = cell_mix.groupby(["cdn", "metro"], as_index=False).share.sum()
pairs["forecast_peak"] = P.FORECAST_PEAK_P50 * pairs.share
pairs["actual_peak"] = P.PEAK_CONCURRENT * pairs.share
util = rng.uniform(*P.OTHER_CELL_UTIL_RANGE, len(pairs))
is_hot = (pairs.cdn == P.HOT_CELL["cdn"]) & (pairs.metro == P.HOT_CELL["metro"])
is_backup = (pairs.cdn == P.BACKUP_CELL["cdn"]) & (pairs.metro == P.BACKUP_CELL["metro"])
util = np.where(is_hot, P.HOT_CELL_UTILIZATION, util)
util = np.where((pairs.cdn == "CDN-B") & (pairs.metro == "DFW"), P.OTHER_CELL_UTIL_RANGE[1], util)
pairs["capacity_concurrent"] = np.where(is_backup, P.CDN_A_DFW_CAPACITY,
                                        np.round(pairs.forecast_peak / util, -2)).astype(int)
write(spark.createDataFrame(pairs[["cdn", "metro", "capacity_concurrent"]]), "ref_cdn_capacity")  # noqa: F821

# COMMAND ----------
# ref_incidents_history (~40 past incidents). CDN-F Seattle is chronically noisy and self-recovers,
# which is why the game-night Seattle bump is a decoy. Same-metro failover is the standard fix.

causes = [("edge cache saturation", "same-metro failover to {alt}", 15, 30),
          ("app release regression", "rolled back app version", 25, 60),
          ("peering congestion", "same-metro failover to {alt}", 12, 25),
          ("origin shield failover", "vendor ticket, no traffic move", 20, 45),
          ("DNS misroute", "corrected routing policy", 8, 20)]
inc = []
for i in range(40):
    ts = et_to_utc(GAME_DATE - timedelta(days=int(rng.integers(20, 540))), int(rng.integers(17, 23)) * 3600)
    if i % 8 == 0:
        inc.append((f"INC-{i + 1:04d}", ts, "CDN-F", "SEA", "All", "edge flapping (chronic, within noise)",
                    "no action; recovered on its own", int(rng.integers(5, 15))))
        continue
    m = str(rng.choice(list(METRO_CDNS)))
    cdn = str(rng.choice(list(METRO_CDNS[m])))
    alt = max((c for c in METRO_CDNS[m] if c != cdn), key=lambda c: METRO_CDNS[m][c])
    cause, action, lo, hi = causes[int(rng.integers(0, len(causes)))]
    inc.append((f"INC-{i + 1:04d}", ts, cdn, m, str(rng.choice(list(DEVICE_SHARE))), cause,
                action.format(alt=alt), int(rng.integers(lo, hi))))
write(spark.createDataFrame(inc, "incident_id STRING, ts TIMESTAMP, cdn STRING, metro STRING, "  # noqa: F821
                            "device_family STRING, root_cause STRING, action_taken STRING, minutes_to_resolve INT"),
      "ref_incidents_history")

# COMMAND ----------
# offer_holdout_results: 6 randomized tests per (offer_type, phase). save rate = treated save rate
# minus holdout save rate. Per-test rates are spread so the across-test mean and 80% interval match
# planted SAVE_CARDS / RETRO (Beta fit by moments across tests reproduces them).

Z80 = 1.2815515655446004
TESTS = {  # (offer_type, phase, churn_type): (mean, sd across tests)
    ("reprocess", "post_cancel", "involuntary"): (P.RETRO["reprocess_rate"], 0.04),
    ("credit_replay", "post_cancel", "voluntary"): (
        P.RETRO["credit_rate"], (P.RETRO["credit_rate_hi80"] - P.RETRO["credit_rate_lo80"]) / (2 * Z80)),
}
for card, offer_type, churn_type in (("payment", "reprocess", "involuntary"),
                                     ("playback", "credit_replay", "voluntary"),
                                     ("onboarding", "onboarding_nudge", "voluntary")):
    c = P.SAVE_CARDS[card]
    TESTS[(offer_type, "pre_cancel", churn_type)] = (
        c["save_rate"], (c["saved_hi"] - c["saved_lo"]) / c["expected_cancels"] / (2 * Z80))
HOLDOUT_SAVE_RATE = 0.03
q = np.array([-1.3830, -0.6745, -0.2104, 0.2104, 0.6745, 1.3830])   # normal quantiles of (k - 0.5) / 6
q = (q - q.mean()) / q.std(ddof=1)
holdout = []
for (offer_type, phase, churn_type), (mean, sd) in TESTS.items():
    for k, z in enumerate(rng.permutation(q)):
        n_t, n_h = int(rng.integers(3000, 6000)), int(rng.integers(3000, 6000))
        rate = mean + sd * z
        holdout.append((f"HT-{offer_type[:4].upper()}-{phase[:3].upper()}-{k + 1}", offer_type, phase, churn_type,
                        n_t, n_h, int(round(n_t * (HOLDOUT_SAVE_RATE + rate))), int(round(n_h * HOLDOUT_SAVE_RATE)),
                        GAME_DATE - timedelta(days=int(rng.integers(40, 400)))))
hold_df = pd.DataFrame(holdout, columns=["test_id", "offer_type", "phase", "churn_type", "n_treated", "n_holdout",
                                         "saved_treated", "saved_holdout", "run_date"])
write(spark.createDataFrame(hold_df).selectExpr(  # noqa: F821
    "test_id", "offer_type", "phase", "churn_type", "CAST(n_treated AS INT) n_treated",
    "CAST(n_holdout AS INT) n_holdout", "CAST(saved_treated AS INT) saved_treated",
    "CAST(saved_holdout AS INT) saved_holdout", "run_date"), "offer_holdout_results")

# COMMAND ----------
# Self-asserts on the planted numbers this notebook owns.

stats = subs.agg(F.count("*").alias("n"), F.sum((F.col("tier") == "Sports").cast("int")).alias("sports"),
                 F.avg(F.col("is_kids_only_account").cast("int")).alias("kids_only"),
                 F.sum(((F.col("metro").isNotNull())
                        & (F.element_at(F.create_map(*[x for m, s in P.METRO_STATE.items()
                                                       for x in (F.lit(m), F.lit(s))]), F.col("metro"))
                           != F.col("state"))).cast("int")).alias("metro_state_mismatch")).first()
prof = spark.table(cfg.fq("dim_profiles")).agg(  # noqa: F821
    F.count("*").alias("n"), F.avg((F.col("profile_type") == "kids").cast("int")).alias("kids")).first()
state_share = {r.state: r.n / stats.n for r in subs.groupBy("state").agg(F.count("*").alias("n")).collect()}
worst_state = max(P.STATE_SHARE, key=lambda s: abs(state_share.get(s, 0) - P.STATE_SHARE[s]))
b_window = subs.where((F.col("signup_ts") >= F.lit(win_lo)) & (F.col("signup_ts") < F.lit(win_hi))
                      & (F.col("acquirer") == "B")).count()
cap = pairs.assign(util_forecast=pairs.forecast_peak / pairs.capacity_concurrent,
                   util_actual=pairs.actual_peak / pairs.capacity_concurrent)
hot, backup = cap[is_hot].iloc[0], cap[is_backup].iloc[0]
cohort = cell_mix.query("cdn == 'CDN-C' and metro == 'DFW' and device_family == 'Roku' and app_version == '7.2.0'")
cohort_load = P.PEAK_CONCURRENT * cohort.share.sum()
rates = hold_df.assign(r=hold_df.saved_treated / hold_df.n_treated - hold_df.saved_holdout / hold_df.n_holdout)
rate_by = rates.groupby(["offer_type", "phase"]).r.agg(["mean", "std"])

print(f"dim_subscribers {stats.n:,}  Sports {stats.sports:,}  kids-only {stats.kids_only:.2%}")
print(f"dim_profiles {prof.n:,}  kids {prof.kids:.2%}")
print(f"worst state share: {worst_state} {state_share.get(worst_state, 0):.4f} vs {P.STATE_SHARE[worst_state]:.4f}")
print(f"Acquirer-B window sign-ups {b_window:,} (target {N_B_WINDOW:,})")
print(f"CDN-C DFW util {hot.util_forecast:.3f}; CDN-A DFW capacity {backup.capacity_concurrent:,}, "
      f"headroom {backup.capacity_concurrent - backup.actual_peak:,.0f}; cohort load {cohort_load:,.0f}")
print(rate_by)

ids = subs.agg(F.count("email").alias("e"), F.countDistinct("email").alias("de"),
               F.avg(F.col("phone").isNull().cast("double")).alias("np"),
               F.avg(F.col("device_primary").isNull().cast("double")).alias("nd")).first()
print(f"emails {ids.e:,} distinct {ids.de:,}; null phone {ids.np:.2%}, null device {ids.nd:.2%}")
assert stats.n == N
assert ids.e == ids.de, "emails must be unique per subscriber"
assert 0.01 <= ids.np <= 0.03 and 0.01 <= ids.nd <= 0.03
assert abs(stats.sports - P.N_SPORTS_TIER * SCALE) / (P.N_SPORTS_TIER * SCALE) < 0.03
assert abs(stats.kids_only - P.KIDS_ONLY_ACCOUNT_SHARE) < 0.003
assert stats.metro_state_mismatch == 0, "metro must sit in METRO_STATE[metro]"
assert abs(prof.kids - P.KIDS_PROFILE_SHARE) < 0.01
assert abs(prof.n - P.N_PROFILES * SCALE) / (P.N_PROFILES * SCALE) < 0.05
assert abs(state_share.get(worst_state, 0) - P.STATE_SHARE[worst_state]) <= P.STATE_SHARE_TOL_PTS
assert b_window == N_B_WINDOW
assert abs(hot.util_forecast - P.HOT_CELL_UTILIZATION) <= P.HOT_CELL_UTIL_TOL
assert (cap[~is_hot].util_actual < P.OTHER_CELL_UTIL_MAX).all()
assert backup.capacity_concurrent == P.CDN_A_DFW_CAPACITY
assert backup.capacity_concurrent - backup.actual_peak >= P.SHIFT_LOAD
assert abs(cohort_load - P.SHIFT_LOAD) / P.SHIFT_LOAD < 0.02
assert abs(hot.actual_peak - P.CDN_C_DFW_PEAK_LOAD) / P.CDN_C_DFW_PEAK_LOAD < 0.01
for (offer_type, phase, _c), (mean, sd) in TESTS.items():
    assert abs(rate_by.loc[(offer_type, phase), "mean"] - mean) < 0.005, (offer_type, phase)
print("02_gen_dimensions: all self-asserts passed")
