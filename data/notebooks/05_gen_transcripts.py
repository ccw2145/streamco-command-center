# Databricks notebook source
# Owner: Stream A (Data + Pipelines). Template-generated support transcripts (design deviation 5).
#
# 25k contacts over 7 days (game day - 5 .. day after). Ordinary days carry an identical baseline mix
# from seed/phrase_banks/contact_reasons.yaml; the night after the Big Game adds the planted excess
# (planted.CONTACT_EXCESS_BY_REASON: 1,364 / 1,085 / 651), ~90% linked to the affected subscribers.
# PII slots are filled with Faker: phone 40%, email 50%, card last-4 30%, single-line street address 20%
# (never a full card number). Every inserted value is recorded in eval_gt_pii_spans:
#   start_idx / end_idx = 0-based character offsets into transcript_text, end exclusive;
#   value_hash = sha256 hex digest of the raw inserted value (UTF-8).
# Writes fact_support_contacts, eval_gt_pii_spans, eval_gt_contact_reason.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import hashlib
import os
import re
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.abspath("../src"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from faker import Faker  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from cc_common import config, planted as P  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
ET = ZoneInfo(P.TZ)
GAME_DATE = date.fromisoformat(cfg.game_date)
DAY_AFTER = GAME_DATE + timedelta(days=1)
DAYS = [GAME_DATE - timedelta(days=5) + timedelta(days=i) for i in range(7)]


def u(salt: str, *cols):
    h = F.xxhash64(F.lit(cfg.seed), F.lit(salt), *cols)
    return (F.pmod(h, F.lit(1 << 40)).cast("double") + 0.5) / float(1 << 40)


def et_to_utc(d: date, seconds: float = 0) -> datetime:
    local = datetime(d.year, d.month, d.day, tzinfo=ET) + timedelta(seconds=float(seconds))
    return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def write(df, name: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fq(name))


with open(os.path.abspath("../seed/phrase_banks/contact_reasons.yaml")) as fh:
    BANK = yaml.safe_load(fh)["reasons"]
BASE_SHARE = {k: v["baseline_share"] for k, v in BANK.items() if v["baseline_share"] > 0}
assert abs(sum(BASE_SHARE.values()) - 1) < 1e-9
assert set(P.CONTACT_EXCESS_BY_REASON) <= set(BANK)
assert all(8 <= len(v["variants"]) <= 12 for v in BANK.values())

# COMMAND ----------
# Contact plan per (day, ET hour, reason). Daily volume varies (weekday pattern), but the six days
# before the day after average exactly the day-after baseline, so the hourly baseline expectation
# (average of the prior days) still nets out to the planted excess split.

N_EXCESS = sum(P.CONTACT_EXCESS_BY_REASON.values())
assert N_EXCESS == P.CONTACTS_EXCESS
per_day = (P.N_TRANSCRIPTS - N_EXCESS) // len(DAYS)
extra = P.N_TRANSCRIPTS - N_EXCESS - per_day * len(DAYS)       # remainder goes to the day after
HOUR_W = dict(enumerate([1, 1, 1, 1, 1, 1, 2, 4, 6, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 5, 4, 3, 2]))
EXCESS_HOUR_W = dict(enumerate([9, 9, 8, 7, 6, 5, 6, 7, 8, 8, 7, 6, 5, 4]))   # 00:00-13:59 ET, overnight surge
DAY_FACTOR = [1.06, 0.97, 0.93, 1.02, 0.98, 1.04]           # the six days before the day after; mean 1.0
assert abs(np.mean(DAY_FACTOR) - 1) < 1e-9 and len(DAY_FACTOR) == len(DAYS) - 1
plan = []
for i, d in enumerate(DAYS):
    total = per_day + extra if d == DAY_AFTER else round(per_day * DAY_FACTOR[i])
    by_reason = P.apportion(total, BASE_SHARE)
    for reason, n in by_reason.items():
        for hour, k in P.apportion(n, HOUR_W).items():
            plan.append((d, hour, reason, k, False))
for reason, n in P.CONTACT_EXCESS_BY_REASON.items():
    for hour, k in P.apportion(n, EXCESS_HOUR_W).items():
        plan.append((DAY_AFTER, hour, reason, k, True))
plan = pd.DataFrame(plan, columns=["day", "hour", "reason_code", "n", "excess"])
plan = plan[plan.n > 0]
assert abs(plan.n.sum() - P.N_TRANSCRIPTS) <= 3

# COMMAND ----------
# Rows and subscriber links. Excess contacts link (~90%) to the people the night affected: cohort
# viewers (stream froze), processor_timeout sign-ups (charged/declined), day-after cancellers (cancel).

contacts = (spark.createDataFrame(plan)  # noqa: F821
            .withColumn("k", F.explode(F.sequence(F.lit(1), F.col("n"))))
            .withColumn("contact_id", F.row_number().over(Window.orderBy("day", "hour", "reason_code", "excess", "k")))
            .withColumn("contact_ts", F.to_utc_timestamp(F.to_timestamp("day") + F.make_interval(
                secs=F.col("hour") * 3600 + 3599 * u("ts", "contact_id")), P.TZ))
            .withColumn("channel", F.when(u("ch", "contact_id") < 0.55, "chat")
                        .when(u("ch", "contact_id") < 0.85, "phone").otherwise("email")))

LANDING = cfg.volume_path("raw_landing")
viewers = (spark.read.schema("subscriber_id BIGINT, sample_weight DOUBLE, cdn STRING, metro STRING, "  # noqa: F821
                             "device_family STRING, app_version STRING").json(f"{LANDING}/sessions")
           .where((F.col("sample_weight") == 1) & (F.col("cdn") == P.INCIDENT["cdn"])
                  & (F.col("metro") == P.INCIDENT["metro"]) & (F.col("app_version") == P.INCIDENT["app_version"]))
           .select("subscriber_id").distinct())
timeouts = (spark.table(cfg.fq("fact_billing_attempts"))  # noqa: F821
            .where(F.col("decline_code") == P.ACQUIRER_B["decline_code"]).select("subscriber_id").distinct())
cancellers = (spark.table(cfg.fq("fact_subscription_events"))  # noqa: F821
              .where((F.col("event_type") == "cancel") & (F.col("tier") == "Sports")
                     & (F.to_date(F.from_utc_timestamp("event_ts", P.TZ)) == F.lit(DAY_AFTER)))
              .select("subscriber_id").distinct())
everyone = spark.table(cfg.fq("dim_subscribers")).select("subscriber_id")  # noqa: F821
LINK_POOLS = {"stream_froze_halftime": viewers, "charged_declined_cancelled": timeouts,
              "cancel_request_other": cancellers}


def link(df, pool, salt: str, link_rate: float):
    """Attach a pseudo-random subscriber from pool to link_rate of the rows (the rest are non-subscribers)."""
    pool = pool.withColumn("pk", F.row_number().over(Window.orderBy(u(salt + "_pool", "subscriber_id"))) - 1)
    n_pool = pool.count()
    df = df.withColumn("pk", F.pmod(F.floor(u(salt + "_pick", "contact_id") * 1e9), F.lit(n_pool)).cast("int"))
    df = df.join(pool, "pk", "left").drop("pk")
    return df.withColumn("subscriber_id", F.when(u(salt + "_link", "contact_id") < link_rate, F.col("subscriber_id")))


parts = [link(contacts.where(F.col("excess") & (F.col("reason_code") == r)), pool, r, 0.9)
         for r, pool in LINK_POOLS.items()]
parts.append(link(contacts.where(~F.col("excess")), everyone, "baseline", 0.85))
contacts = parts[0]
for part in parts[1:]:
    contacts = contacts.unionByName(part)
# Linked contacts use the subscriber's own email, phone and address (so PII matches the account);
# unlinked ones (non-subscribers) get Faker values in a real metro's city, state and area code.
contacts = contacts.join(spark.table(cfg.fq("dim_subscribers")).select(  # noqa: F821
    "subscriber_id", F.col("email").alias("sub_email"), F.col("phone").alias("sub_phone"),
    F.col("postal_address").alias("sub_address")), "subscriber_id", "left")

# COMMAND ----------
# Transcript text. Faker is seeded per contact_id, so every row is reproducible on any partitioning.

SUFFIXES = ["St", "Ave", "Rd", "Blvd", "Ln", "Dr", "Ct", "Way", "Pl"]   # match fn_redact_pii's address pattern
METRO_CITY = [("New York", "NY", (212, 718, 917)), ("Los Angeles", "CA", (213, 310, 323)), ("Chicago", "IL", (312, 773)),
              ("Dallas", "TX", (214, 469, 972)), ("Houston", "TX", (713, 281, 832)), ("Atlanta", "GA", (404, 678)),
              ("Washington", "DC", (202,)), ("Philadelphia", "PA", (215, 267)), ("Miami", "FL", (305, 786)),
              ("Boston", "MA", (617, 857)), ("Phoenix", "AZ", (602, 480)), ("Seattle", "WA", (206, 425)),
              ("San Francisco", "CA", (415, 628)), ("Denver", "CO", (303, 720)), ("Minneapolis", "MN", (612, 651))]
CHANNEL_VERB = {"chat": "chatting with", "phone": "calling", "email": "writing to"}
OPENERS = {
    "chat": ["Hi, thanks for chatting with support. What can I help with today?",
             "Hello! You're chatting with the support team. How can I help?",
             "Thanks for reaching out on chat. What's going on with your account?",
             "Hi there, I'm here to help. What brings you to chat today?"],
    "phone": ["Thanks for calling support, how can I help you today?",
              "Hello, thank you for calling. Who do I have the pleasure of speaking with?",
              "Thanks for calling in. What can I do for you this morning?",
              "Support line, thanks for calling. How can I help?"],
    "email": ["Thanks for writing to support. I've read your message and I'm happy to help.",
              "Hello, and thanks for your email. Let me look into this for you.",
              "Thank you for writing in. I'm sorry for the trouble, let's sort it out."],
}
AGENT_ASK = ["Can you tell me a bit more about what happened?", "When did you first notice this?",
             "Which device were you using at the time?", "Let me pull up your account. Can you confirm a few details?",
             "I can look into that. Has this happened before?", "Thanks. Is there anything else I should know?"]
CUSTOMER_MORE = ["It started last night and it's still bugging me.", "This has never happened before.",
                 "I tried restarting everything already.", "Honestly I'm pretty frustrated.",
                 "I just want it to work like it normally does.", "My partner had the same issue on the other TV."]
CLOSE = ["I'm sorry about that. I've noted your account and someone will follow up shortly.",
         "Thanks for your patience. I've logged this and we'll get back to you.",
         "I understand. I'll look into this and update you by email.",
         "All set on my side. You'll get a confirmation shortly.",
         "I've escalated this to the right team. Anything else I can help with?"]
CUSTOMER_BYE = ["Okay, thanks.", "Thanks for the help.", "Alright, appreciate it.", "Great, thank you."]
# Which PII a customer volunteers depends on the topic; rates are rescaled so the overall slot rates stay
# planted.PII_SLOT_RATES.
AFFINITY = {"phone": {"default": 1.0}, "email": {"login_issue": 1.6, "account_update": 1.5, "default": 1.0},
            "card_last4": {"charged_declined_cancelled": 2.4, "billing_question": 2.2, "promo_question": 1.4,
                           "cancel_request_other": 1.2, "stream_froze_halftime": 0.3, "device_setup": 0.2,
                           "content_question": 0.2, "default": 0.7},
            "address": {"account_update": 2.6, "billing_question": 1.2, "device_setup": 0.4, "default": 0.9}}
reason_mix = plan.groupby("reason_code").n.sum() / plan.n.sum()
PII_P = {}
for slot, aff in AFFINITY.items():
    w = {r: aff.get(r, aff["default"]) for r in reason_mix.index}
    norm = sum(w[r] * reason_mix[r] for r in w)
    PII_P[slot] = {r: min(0.95, P.PII_SLOT_RATES[slot] * w[r] / norm) for r in w}
BANK_VARIANTS = {k: v["variants"] for k, v in BANK.items()}


def compose(contact_id: int, reason: str, channel: str, sub_email, sub_phone, sub_address, force_all: bool = False):
    fk = Faker("en_US")
    fk.seed_instance(cfg.seed * 1_000_003 + contact_id)
    r = np.random.default_rng([cfg.seed, 5, contact_id])
    city, st, codes = METRO_CITY[r.integers(0, len(METRO_CITY))]
    slots = []
    if force_all or r.random() < PII_P["phone"][reason]:
        phone = sub_phone if sub_phone else "{}-{}-{}".format(codes[r.integers(0, len(codes))], fk.numerify("###"),
                                                               fk.numerify("####"))
        if not sub_phone or r.random() < 0.3:          # how customers say it varies
            digits = "".join(ch for ch in phone if ch.isdigit())[-10:]
            phone = ("({}) {}-{}", "{}-{}-{}", "{}.{}.{}")[r.integers(0, 3)].format(digits[:3], digits[3:6], digits[6:])
        else:
            phone = phone.removeprefix("+1-")
        slots.append(("phone", phone, ("You can call me back at {}.", "My number is {}.",
                                       "Best number to reach me is {}.")[r.integers(0, 3)]))
    if force_all or r.random() < PII_P["email"][reason]:
        slots.append(("email", sub_email or fk.email(), ("My email on the account is {}.", "The account email is {}.",
                                                         "You can email me at {}.")[r.integers(0, 3)]))
    if force_all or r.random() < PII_P["card_last4"][reason]:
        slots.append(("card_last4", fk.numerify("####"), ("The charge was on my card ending in {}.",
                                                          "It's the card ending in {}.")[r.integers(0, 2)]))
    if force_all or r.random() < PII_P["address"][reason]:
        if sub_address:
            addr = sub_address
        else:
            street = next(n for n in iter(fk.last_name, None) if n.isalpha())
            addr = f"{r.integers(10, 9900)} {street} {SUFFIXES[r.integers(0, len(SUFFIXES))]}, {city}, {st} {fk.zipcode()}"
        slots.append(("address", addr, ("My address is {}.", "The billing address is {}.")[r.integers(0, 2)]))
    r.shuffle(slots)
    n_turns = int(r.integers(4, 11))
    turns = [("Agent", OPENERS[channel][r.integers(0, len(OPENERS[channel]))]),
             ("Customer", BANK_VARIANTS[reason][r.integers(0, len(BANK_VARIANTS[reason]))])]
    while len(turns) < n_turns - 2:
        turns.append(("Agent", AGENT_ASK[r.integers(0, len(AGENT_ASK))]))
        turns.append(("Customer", CUSTOMER_MORE[r.integers(0, len(CUSTOMER_MORE))]))
    turns.append(("Agent", CLOSE[r.integers(0, len(CLOSE))]))
    if len(turns) < n_turns:
        turns.append(("Customer", CUSTOMER_BYE[r.integers(0, len(CUSTOMER_BYE))]))
    # PII goes into the customer's turns, after the opening complaint
    cust_idx = [i for i, (who, _) in enumerate(turns) if who == "Customer"]
    pii_turn = {}
    for j, slot in enumerate(slots):
        pii_turn.setdefault(cust_idx[min(1 + j // 2, len(cust_idx) - 1) if len(cust_idx) > 1 else 0], []).append(slot)
    text, spans = "", []
    for i, (who, line) in enumerate(turns):
        text += ("\n" if text else "") + f"{who}: {line}"
        for pii_type, value, template in pii_turn.get(i, []):
            prefix, suffix = template.split("{}")
            text += " " + prefix
            spans.append((pii_type, len(text), len(text) + len(value), hashlib.sha256(value.encode()).hexdigest()))
            text += value + suffix
    return text, spans


def gen_text(batches):
    for pdf in batches:
        out = [compose(int(c), r, ch, e, ph, ad, bool(sc)) for c, r, ch, e, ph, ad, sc in zip(
            pdf.contact_id, pdf.reason_code, pdf.channel, pdf.sub_email, pdf.sub_phone, pdf.sub_address, pdf.is_showcase)]
        pdf = pdf.assign(transcript_text=[t for t, _ in out],
                         spans=[[{"pii_type": a, "start_idx": b, "end_idx": c, "value_hash": d} for a, b, c, d in sp]
                                for _, sp in out])
        yield pdf.drop(columns=["sub_email", "sub_phone", "sub_address"])


# The showcase transcript for the masked / unmasked pair: the first day-after "charged / declined" chat
# from a linked subscriber with a full address; it carries all four PII types.
showcase_id = (contacts.where((F.col("reason_code") == "charged_declined_cancelled") & (F.col("channel") == "chat")
                              & F.col("sub_email").isNotNull() & F.col("sub_phone").isNotNull()
                              & F.col("sub_address").isNotNull() & ~F.col("sub_address").contains("'"))
               .orderBy("contact_id").first().contact_id)
contacts = contacts.withColumn("is_showcase", F.col("contact_id") == showcase_id)

TEXT_SCHEMA = ("contact_id BIGINT, subscriber_id BIGINT, contact_ts TIMESTAMP, channel STRING, reason_code STRING, "
               "is_showcase BOOLEAN, "
               "transcript_text STRING, spans ARRAY<STRUCT<pii_type: STRING, start_idx: INT, end_idx: INT, "
               "value_hash: STRING>>")
gen = (contacts.select("contact_id", "subscriber_id", "contact_ts", "channel", "reason_code", "sub_email", "sub_phone",
                       "sub_address", "is_showcase")
       .repartition(16, "contact_id").sortWithinPartitions("contact_id")
       .mapInPandas(gen_text, TEXT_SCHEMA))
write(gen, "gen_contacts_tmp")
gen = spark.table(cfg.fq("gen_contacts_tmp"))  # noqa: F821

write(gen.select("contact_id", "subscriber_id", "contact_ts", "channel", "transcript_text", "is_showcase"),
      "fact_support_contacts")
write(gen.select("contact_id", F.explode("spans").alias("s")).select(
    "contact_id", "s.pii_type", "s.start_idx", "s.end_idx", "s.value_hash"), "eval_gt_pii_spans")
write(gen.select("contact_id", "reason_code"), "eval_gt_contact_reason")

# COMMAND ----------
# Simulated Salesforce Service Cloud source (planted.SOURCE_SYSTEMS "support"): every transcript is a
# case (the transcript text itself stays in the masked fact_support_contacts), plus cancellation cases that care agents opened while processing a cancel (no transcript kept).
# Customer care counts a cancellation as theirs when the subscriber had a case from 24 h before the
# cancel to 1 h after it; the extra cancellation cases bring that to planted TEAM_CANCEL_COUNTS["Customer care"].
CARE_BEFORE_H, CARE_AFTER_H = 24, 1
care_target = round(P.TEAM_CANCEL_COUNTS["Customer care"][0] * (1 + np.random.default_rng([cfg.seed, 51]).uniform(
    -0.008, 0.008)))
eff = (spark.table(cfg.fq("fact_subscription_events"))  # noqa: F821
       .where((F.col("event_type") == "cancel") & (F.col("tier") == "Sports")
              & (F.to_date(F.from_utc_timestamp("event_ts", P.TZ)) == F.lit(DAY_AFTER)))
       .select("subscriber_id", F.col("event_ts").alias("cancel_ts")))


def touched(cases):
    return (eff.join(cases.select("subscriber_id", "case_ts"), "subscriber_id")
            .where((F.col("case_ts") >= F.col("cancel_ts") - F.make_interval(hours=F.lit(CARE_BEFORE_H)))
                   & (F.col("case_ts") <= F.col("cancel_ts") + F.make_interval(hours=F.lit(CARE_AFTER_H))))
            .select("subscriber_id").distinct())


transcript_cases = gen.where(F.col("subscriber_id").isNotNull()).select("subscriber_id", F.col("contact_ts").alias("case_ts"))
already = touched(transcript_cases)
n_extra = care_target - already.count()
cancel_cases = (eff.join(already, "subscriber_id", "left_anti")
                .withColumn("rk", F.row_number().over(Window.orderBy(u("care", "subscriber_id"))))
                .where(F.col("rk") <= n_extra)
                .select("subscriber_id", (F.col("cancel_ts") - F.make_interval(
                    secs=120 + 1680 * u("care_t", "subscriber_id"))).alias("case_ts"),
                    F.when(u("care_ch", "subscriber_id") < 0.6, "Phone").otherwise("Chat").alias("Origin")))
SF_TS = "yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'"
sf_transcripts = gen.select(
    F.format_string("500%012d", F.col("contact_id")).alias("Id"), F.format_string("%08d", F.col("contact_id")).alias(
        "CaseNumber"), F.col("subscriber_id").cast("string").alias("Subscriber_Id__c"),
    F.initcap("channel").alias("Origin"), F.lit(None).cast("string").alias("Type"),
    F.substring(F.regexp_extract("transcript_text", "Customer: ([^\n]*)", 1), 1, 80).alias("Subject"),
    F.lit("Closed").alias("Status"),
    F.col("is_showcase").alias("Is_Showcase__c"), F.date_format("contact_ts", SF_TS).alias("CreatedDate"),
    F.date_format(F.col("contact_ts") + F.expr("INTERVAL 25 MINUTES"), SF_TS).alias("LastModifiedDate"))
sf_cancels = cancel_cases.withColumn("rn", F.row_number().over(Window.orderBy("subscriber_id"))).select(
    F.format_string("500C%011d", F.col("rn")).alias("Id"), F.format_string("C%07d", F.col("rn")).alias("CaseNumber"),
    F.col("subscriber_id").cast("string").alias("Subscriber_Id__c"), "Origin", F.lit("Cancellation").alias("Type"),
    F.lit("Cancel subscription").alias("Subject"),
    F.lit("Closed").alias("Status"), F.lit(False).alias("Is_Showcase__c"),
    F.date_format("case_ts", SF_TS).alias("CreatedDate"),
    F.date_format(F.col("case_ts") + F.expr("INTERVAL 8 MINUTES"), SF_TS).alias("LastModifiedDate"))
sf_cases = sf_transcripts.unionByName(sf_cancels)
sf_cases.repartition(4).write.mode("overwrite").json(cfg.volume_path("raw_landing", "support", "cases"))
care_count = touched(transcript_cases.unionByName(cancel_cases.select("subscriber_id", "case_ts"))).count()

# COMMAND ----------
# Self-asserts

pd_all = gen.select("contact_id", "contact_ts", "reason_code", "transcript_text", "spans").toPandas()
pd_all["day"] = pd.to_datetime(pd_all.contact_ts).dt.tz_localize("UTC").dt.tz_convert(ET).dt.date
counts = pd_all.groupby(["day", "reason_code"]).size().unstack(fill_value=0)
baseline = counts.loc[[d for d in DAYS if d < DAY_AFTER]].mean()
excess = (counts.loc[DAY_AFTER] - baseline).clip(lower=0)
excess = excess[excess > 0.5]
share = excess / excess.sum()
print("excess by reason:", excess.round().to_dict())
assert abs(len(pd_all) - P.N_TRANSCRIPTS) <= 3
assert abs(excess.sum() - P.CONTACTS_EXCESS) / P.CONTACTS_EXCESS <= P.CONTACTS_EXCESS_TOL_REL
for reason, s in P.CONTACT_EXCESS_SPLIT.items():
    assert abs(share.get(reason, 0) - s) <= P.CONTACT_SPLIT_TOL_PTS, (reason, share.get(reason, 0))
spans = pd_all.explode("spans").dropna(subset=["spans"])
rates = spans.spans.map(lambda s: s["pii_type"]).value_counts() / len(pd_all)
print("PII slot rates:", rates.round(3).to_dict())
for k, v in P.PII_SLOT_RATES.items():
    assert abs(rates.get(k, 0) - v) < 0.02, (k, rates.get(k, 0))
bad = [cid for cid, txt, s in zip(spans.contact_id, spans.transcript_text, spans.spans)
       if hashlib.sha256(txt[s["start_idx"]:s["end_idx"]].encode()).hexdigest() != s["value_hash"]]
assert not bad, f"span offsets do not match the text for {len(bad)} rows"
assert not pd_all.transcript_text.str.contains(r"\d{13,19}").any(), "full card numbers must never appear"
assert all(re.fullmatch(r"\d{1,5} [A-Z][A-Za-z]* \w+, [A-Za-z .]+, [A-Z]{2} \d{5}",
                        txt[s["start_idx"]:s["end_idx"]])
           for txt, s in zip(spans.transcript_text, spans.spans) if s["pii_type"] == "address")
print(f"Customer care cancels: {care_count:,} (target {care_target:,})")
assert abs(care_count / P.TEAM_CANCEL_COUNTS["Customer care"][0] - 1) <= P.TEAM_CANCEL_TOL_REL
assert gen.where("is_showcase").count() == 1
show = pd_all[pd_all.contact_id == showcase_id].iloc[0]
assert {sp["pii_type"] for sp in show.spans} == set(P.PII_SLOT_RATES), "showcase transcript needs all four PII types"
n_lines = pd_all.transcript_text.str.count("\n") + 1
assert n_lines.between(4, 10).all(), n_lines.describe()
ch = gen.select("channel", "transcript_text").toPandas()
assert ch[ch.channel == "phone"].transcript_text.str.split("\n").str[0].str.contains("calling").all()
assert not ch[ch.channel == "phone"].transcript_text.str.contains("chatting").any()
daily = pd_all.groupby("day").size()
assert daily[daily.index < DAY_AFTER].std() > 50, "daily contact volume should vary"
linked = gen.where("reason_code IN ('stream_froze_halftime', 'charged_declined_cancelled')").agg(
    F.avg(F.col("subscriber_id").isNotNull().cast("double"))).first()[0]
assert 0.85 <= linked <= 0.95, linked
spark.sql(f"DROP TABLE IF EXISTS {cfg.fq('gen_contacts_tmp')}")  # noqa: F821
print(f"05_gen_transcripts: {len(pd_all):,} contacts, {len(spans):,} PII spans; all self-asserts passed")
