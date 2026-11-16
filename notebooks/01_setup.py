# Databricks notebook source
# StreamCo Command Center 01: demo-layer views, the Ask tool function, the governed metric view, and checks.
# The data layer (data/) has already generated every table into the schema with StreamCo names. This notebook
# adds only what governance needs: masked, kids-filtered subscribers; the masked support transcript; the
# training and eligible scoring views; fn_cancel_drivers; and mv_streamco_metrics.
# Fails on structural problems (empty views, blocked terms in screen text, kids or ineligible rows). Numbers are reported.

# COMMAND ----------

import datetime as dt
import re

dbutils.widgets.text("catalog", "cindy_wu_catalog")  # noqa: F821
dbutils.widgets.text("schema", "streamco")  # noqa: F821
dbutils.widgets.text("src_schema", "streamco")  # noqa: F821
dbutils.widgets.text("game_date", "2027-02-14")  # noqa: F821
dbutils.widgets.text("blocked_terms", "")  # noqa: F821  comma-separated terms no screen text may contain
W = {k: dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema", "game_date", "blocked_terms")}  # noqa: F821
for k in ("catalog", "schema", "src_schema"):
    if not re.fullmatch(r"[A-Za-z0-9_]+", W[k]):
        raise ValueError(f"{k} must be a plain identifier, got {W[k]!r}")
CATALOG, SCHEMA, SRC = W["catalog"], W["schema"], W["src_schema"]
GAME_DATE = dt.date.fromisoformat(W["game_date"])
DAY_AFTER = GAME_DATE + dt.timedelta(days=1)
FEATURES = ["tenure_days", "plan_price", "is_bundle", "signup_source", "event_only_signup", "hours_30d",
            "active_days_30d", "engagement_trend", "days_since_last_session", "pauses_90d", "rebuffer_min_30d",
            "max_session_rebuffer_min", "failed_starts_30d", "declines_30d", "decline_no_retry",
            "support_contacts_30d"]
COLS = ["subscriber_id", "snapshot_date"] + FEATURES
fq = lambda name: f"{CATALOG}.{SCHEMA}.{name}"  # noqa: E731

spark.sql(f"USE CATALOG {CATALOG}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'StreamCo Command Center (synthetic demo data)'")  # noqa: F821

# COMMAND ----------


def run_sql_file(path: str) -> int:
    tokens = {"catalog": CATALOG, "schema": SCHEMA, "src": SRC, "game_date": GAME_DATE.isoformat(),
              "day_after": DAY_AFTER.isoformat(), "feature_cols_f": ", ".join(f"f.{c}" for c in COLS)}
    text = open(path).read()
    for k, v in tokens.items():
        text = text.replace("{" + k + "}", v)
    n = 0
    for chunk in re.split(r";\s*\n", text + "\n"):
        body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            spark.sql(body)  # noqa: F821
            n += 1
    return n


print("views.sql:", run_sql_file("../sql/views.sql"), "statements")
print("metric_view.sql:", run_sql_file("../sql/metric_view.sql"), "statements")

# COMMAND ----------
# Structural checks.

VIEWS = ["v_subscribers", "v_support_showcase", "v_training_features", "v_scoring_features"]
counts = {v: spark.sql(f"SELECT count(*) AS n FROM {fq(v)}").first().n for v in VIEWS}  # noqa: F821
for v, n in counts.items():
    print(f"{'OK ' if n else 'EMPTY'} {v:22s} {n:>12,}")

# Free text that reaches a screen must not contain any blocked term (widget blocked_terms; none by default).
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
TEXT = {src("gold_team_cancel_counts"): ["team", "definition", "governed_definition"],
        src("gold_cancel_decomposition"): ["tier", "rule"], src("gold_reconciliation_daily"): ["tier", "likely_cause"],
        src("gold_cancellations_daily"): ["tier"], src("ref_source_systems"): ["display_name"],
        src("ref_sample_subscribers"): ["tier", "what_happened", "watch_next", "array_join(recent_titles, ' ')"],
        src("dim_subscriber_viewing"): ["watch_next", "array_join(recent_titles, ' ')"],
        fq("v_support_showcase"): ["masked_text", "transcript_text"]}
leaks = {}
blocked = [t.strip().lower().replace("'", "") for t in W["blocked_terms"].split(",") if t.strip()]
for table, cols in (TEXT.items() if blocked else []):
    cond = " OR ".join(f"instr(lower(coalesce({c}, '')), '{t}') > 0" for c in cols for t in blocked)
    n = spark.sql(f"SELECT count(*) AS n FROM {table} WHERE {cond}").first().n  # noqa: F821
    if n:
        leaks[table] = n
tiers = sorted(r.tier for r in spark.sql(f"SELECT DISTINCT tier FROM {src('gold_subscriber_daily')}").collect())  # noqa: F821
kids = spark.sql(f"""SELECT count(*) AS n FROM {fq('v_subscribers')} s
                     JOIN {src('dim_subscribers')} d USING (subscriber_id) WHERE d.is_kids_only_account""").first().n  # noqa: F821
ineligible = spark.sql(f"""SELECT count(*) AS n FROM {fq('v_scoring_features')} s
                           JOIN {src('dim_subscribers')} d USING (subscriber_id)
                           WHERE d.is_kids_only_account OR d.status <> 'active' OR d.canceled_ts IS NOT NULL""").first().n  # noqa: F821
privileged = spark.sql("SELECT is_account_group_member('cc_privileged') AS p").first().p  # noqa: F821
unmasked = spark.sql(f"SELECT count_if(email NOT LIKE '%***@%') AS n FROM {fq('v_subscribers')} "  # noqa: F821
                     "WHERE email IS NOT NULL").first().n
showcase_pii = spark.sql(f"SELECT n_redactions FROM {fq('v_support_showcase')}").first()  # noqa: F821

checks = [
    ("every view returns rows", all(counts.values()), [v for v, n in counts.items() if not n]),
    ("no blocked terms in screen text", not leaks, leaks),
    ("tiers are StreamCo names", tiers == ["Bundle", "Family", "Originals", "Sports"], tiers),
    ("kids-only accounts filtered from v_subscribers", kids == 0, kids),
    ("scoring population is eligible only", ineligible == 0, ineligible),
    ("emails masked for this caller", unmasked == 0 or privileged, f"unmasked={unmasked} privileged={privileged}"),
    ("showcase transcript has redactions", bool(showcase_pii and showcase_pii.n_redactions), showcase_pii),
]
print("\n".join(f"{'PASS' if ok else 'FAIL'}  {name}  {'' if ok else detail}" for name, ok, detail in checks))

# COMMAND ----------
# Numbers (reported, not asserted: the app numbers are illustrative).

teams = spark.sql(f"SELECT team, `count`, governed_count FROM {src('gold_team_cancel_counts')} ORDER BY `count` DESC").collect()  # noqa: F821
mv = spark.sql(f"SELECT MEASURE(cancels) AS c, MEASURE(cancel_rate) AS r FROM {fq('mv_streamco_metrics')} "  # noqa: F821
               f"WHERE day = DATE'{DAY_AFTER}' AND tier = 'Sports'").first()
drivers = spark.sql(f"SELECT * FROM {fq('fn_cancel_drivers')}(DATE'{DAY_AFTER}', 'Sports')").collect()  # noqa: F821
originals = spark.sql(f"SELECT DISTINCT recon_status, recon_pct_diff FROM {fq('fn_cancel_drivers')}(DATE'{DAY_AFTER}', 'Originals')").collect()  # noqa: F821
print("team counts:", [(r.team, r["count"]) for r in teams], "governed:", sorted({r.governed_count for r in teams}))
print(f"metric view Sports cancels {DAY_AFTER}: {mv.c:,}  cancel_rate {mv.r:.4f}")
print("drivers:", [(r.driver, r.n_cancels, None if r.pct is None else round(r.pct, 3)) for r in drivers])
print("Originals reconciliation:", [(r.recon_status, round(r.recon_pct_diff, 4)) for r in originals])

failed = [name for name, ok, _ in checks if not ok]
if failed:
    raise AssertionError(f"01_setup checks failed: {failed}")
dbutils.notebook.exit(f"ok governed={mv.c}")  # noqa: F821
