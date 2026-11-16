# Databricks notebook source
# StreamCo Command Center 03: churn model, scores, save plan and model card.
# 1. Train LightGBM + isotonic calibration on v_training_features (historical snapshots with 30-day labels), split
#    by subscriber 70/10/20 (train/calibration/test). Register {catalog}.{schema}.churn_propensity@champion.
# 2. Score v_scoring_features (eligible post-game population only). risk_decile 10 = the at-risk set.
#    top_drivers = up to 3 features with a positive SHAP contribution. card_id (at-risk only), first rule that
#    matches the save-offer policy: payment (declined, never retried) -> playback (5+ minutes of buffering in one
#    session) -> onboarding (first 30 days). Otherwise no card: nothing is forced.
# 3. save_plan per card: expected cancels = sum of churn_prob; save rate = mean treated-minus-holdout save rate of
#    past pre-cancel offer tests (80% interval by Monte Carlo); costs from ref_assumptions.
# 4. churn_model_card.

# COMMAND ----------

# MAGIC %pip install -q lightgbm>=4.5 mlflow>=3.4

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import json
import os
import re
import sys

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "src_schema": "streamco"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
CATALOG, SCHEMA, SRC = (dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema"))  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", x) for x in (CATALOG, SCHEMA, SRC)), "invalid identifier"
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"  # noqa: E731
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
sys.path.insert(0, os.path.abspath("../data/src"))
from cc_common import planted as P  # noqa: E402  (targets, reported only)

MODEL_NAME = fq("churn_propensity")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user() AS u').first().u}/streamco-command-center")  # noqa: F821

FEATURES = ["tenure_days", "plan_price", "is_bundle", "signup_source", "event_only_signup", "hours_30d",
            "active_days_30d", "engagement_trend", "days_since_last_session", "pauses_90d", "rebuffer_min_30d",
            "max_session_rebuffer_min", "failed_starts_30d", "declines_30d", "decline_no_retry",
            "support_contacts_30d"]
BOOLS = ["is_bundle", "event_only_signup", "decline_no_retry"]
SIGNUP_SOURCES = ["organic", "event_promo", "bundle_partner", "winback"]
LABELS = {  # plain-English driver labels for the app
    "tenure_days": "New subscriber", "plan_price": "Plan price", "is_bundle": "Not on a bundle",
    "signup_source": "How they signed up", "event_only_signup": "Joined for the event",
    "hours_30d": "Low viewing hours", "active_days_30d": "Few active days", "engagement_trend": "Watching less",
    "days_since_last_session": "Days since last watched", "pauses_90d": "Paused recently",
    "rebuffer_min_30d": "Buffering minutes", "max_session_rebuffer_min": "Worst buffering in one session",
    "failed_starts_30d": "Streams that failed to start", "declines_30d": "Card declines",
    "decline_no_retry": "Failed payment, never retried", "support_contacts_30d": "Contacted support"}
PLAYBACK_MIN = 5.0  # minutes of buffering in one session (save-offer policy, section 2)
CARDS = {  # card_id -> (title, offer shown to the approver, offer_type in the holdout tests)
    "payment": ("Fix failed payments", "Retry the failed payment at no charge, so the account stays active", "reprocess"),
    "playback": ("Make good for buffering", "One month of service credit and a replay of the moments they missed", "credit_replay"),
    "onboarding": ("Welcome event sign-ups", "A welcome message with what to watch next", "onboarding_nudge"),
}
PARAMS = dict(n_estimators=400, learning_rate=0.03, num_leaves=31, min_child_samples=200, subsample=0.8,
              subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0, random_state=15, deterministic=True,
              force_row_wise=True, verbose=-1)


def to_matrix(df: pd.DataFrame) -> pd.DataFrame:
    x = df[FEATURES].copy()
    x["plan_price"] = x["plan_price"].astype(float)
    for c in BOOLS:
        x[c] = x[c].fillna(False).astype(int)
    x["signup_source"] = pd.Categorical(x["signup_source"], categories=SIGNUP_SOURCES)
    return x


class ChurnModel(mlflow.pyfunc.PythonModel):
    """Calibrated 30-day churn probability. predict() -> churn_prob; contributions() -> SHAP log-odds per feature."""

    def __init__(self, clf, calibrator):
        self.clf, self.calibrator = clf, calibrator

    def contributions(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.clf.predict_proba(to_matrix(df), pred_contrib=True)
        return pd.DataFrame(c[:, :-1], columns=FEATURES, index=df.index)

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        p = self.calibrator.predict(self.clf.predict_proba(to_matrix(model_input))[:, 1])
        return pd.DataFrame({"churn_prob": np.clip(p, 0.0, 1.0)})


# COMMAND ----------
# 1. Train and register

df = spark.table(fq("v_training_features")).select("subscriber_id", *FEATURES, "churned_30d").toPandas()  # noqa: F821
if df.empty:
    raise RuntimeError("v_training_features is empty: run the data generators and 01_setup first.")
bucket = pd.util.hash_pandas_object(df.subscriber_id, index=False).to_numpy() % 10
tr, ca, te = df[bucket < 7], df[(bucket >= 7) & (bucket < 8)], df[bucket >= 8]
with mlflow.start_run(run_name="churn_propensity"):
    clf = lgb.LGBMClassifier(**PARAMS).fit(to_matrix(tr), tr.churned_30d)
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0).fit(
        clf.predict_proba(to_matrix(ca))[:, 1], ca.churned_30d)
    model = ChurnModel(clf, iso)
    auc = float(roc_auc_score(te.churned_30d, model.predict(None, te).churn_prob))
    shap_abs = model.contributions(te.sample(n=min(50_000, len(te)), random_state=15)).abs().mean()
    importance = (shap_abs / shap_abs.sum()).sort_values(ascending=False)
    mlflow.log_params({"features": ",".join(FEATURES), "calibration": "isotonic, 10% subscriber holdout",
                       "train_rows": len(tr), "test_rows": len(te)})
    mlflow.log_metric("test_auc", auc)
    sig = te.head(200)[FEATURES].astype({"plan_price": float, **{c: bool for c in BOOLS}}).reset_index(drop=True)
    info = mlflow.pyfunc.log_model(
        name="model", python_model=model, input_example=sig.head(5), registered_model_name=MODEL_NAME,
        signature=infer_signature(sig, model.predict(None, sig)),
        pip_requirements=[f"lightgbm=={lgb.__version__}", f"scikit-learn=={sklearn.__version__}",
                          f"pandas=={pd.__version__}", f"numpy=={np.__version__}"])
client = MlflowClient()
version = str(info.registered_model_version)
client.set_registered_model_alias(MODEL_NAME, "challenger", version)

# Champion/challenger gate: promote only if the new version beats the current champion on the same test rows.
try:
    current = client.get_model_version_by_alias(MODEL_NAME, "champion")
except Exception:  # first run: no champion yet
    current = None
champion_auc = None
if current is not None and current.version != version:
    old = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion").unwrap_python_model()
    champion_auc = float(roc_auc_score(te.churned_30d, old.predict(None, te).churn_prob))
if champion_auc is None or auc > champion_auc:
    client.set_registered_model_alias(MODEL_NAME, "champion", version)
    client.update_registered_model(MODEL_NAME, description=f"30-day churn propensity (LightGBM + isotonic). Test AUC {auc:.3f}.")
    print(f"{MODEL_NAME} v{version} promoted @champion  test AUC {auc:.4f} (previous champion {champion_auc})")
else:
    print(f"{MODEL_NAME} v{version} stays @challenger: AUC {auc:.4f} <= champion v{current.version} {champion_auc:.4f}")
    version, auc = str(current.version), champion_auc
print(f"rows train/calib/test {len(tr):,}/{len(ca):,}/{len(te):,}")

# COMMAND ----------
# 2. Score the eligible population with the champion

champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion").unwrap_python_model()
pdf = spark.table(fq("v_scoring_features")).select("subscriber_id", *FEATURES).toPandas()  # noqa: F821
if pdf.empty:
    raise RuntimeError("v_scoring_features is empty: the post-game snapshot has not been generated.")
pdf = pdf.sort_values("subscriber_id").reset_index(drop=True)
pdf["churn_prob"] = champion.predict(None, pdf).churn_prob.to_numpy()
order = np.lexsort((pdf.subscriber_id.to_numpy(), -pdf.churn_prob.to_numpy()))  # prob desc, id asc
rank = np.empty(len(pdf), dtype=np.int64)
rank[order] = np.arange(len(pdf))
pdf["risk_decile"] = (10 - rank * 10 // len(pdf)).astype(int)
at_risk = pdf.risk_decile == 10

contrib = champion.contributions(pdf)
vals, cols = contrib.to_numpy(), np.array(FEATURES)
top = np.argsort(-vals, axis=1)[:, :3]
pdf["top_drivers"] = [[LABELS[cols[j]] for j in row if vals[i, j] > 0] for i, row in enumerate(top)]
rule = np.select([pdf.decline_no_retry.fillna(False).astype(bool),
                  pdf.max_session_rebuffer_min.fillna(0) >= PLAYBACK_MIN,
                  pdf.tenure_days.fillna(10**6) < 30], list(CARDS), "")
pdf["card_id"] = pd.Series(rule).where(at_risk & (rule != ""), None)
pdf["model_version"] = version

scores = spark.createDataFrame(pdf[["subscriber_id", "churn_prob", "risk_decile", "top_drivers", "card_id", "model_version"]])  # noqa: F821
(scores.selectExpr("CAST(subscriber_id AS BIGINT) subscriber_id", "CAST(churn_prob AS DOUBLE) churn_prob",
                   "CAST(risk_decile AS INT) risk_decile", "CAST(top_drivers AS ARRAY<STRING>) top_drivers",
                   "CAST(card_id AS STRING) card_id", "model_version")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("churn_scores")))
spark.sql(f"COMMENT ON TABLE {fq('churn_scores')} IS 'Post-game churn risk for eligible subscribers (active, not "  # noqa: F821
          f"kids-only, never cancelled), churn_propensity v{version}. risk_decile 10 = at risk; card_id = save-plan card.'")

# COMMAND ----------
# 3. Save plan

A = {r.key: r.value for r in spark.table(src("ref_assumptions")).collect()}  # noqa: F821
COST = {"reprocess": lambda n: n * A["reprocess_cost_usd"],
        "credit_replay": lambda n: n * A["credit_redemption_rate"] * A["arpu_monthly"],
        "onboarding_nudge": lambda n: n * A["nudge_cost_usd"]}
ho = spark.table(src("offer_holdout_results")).where("phase = 'pre_cancel' AND n_treated > 0 AND n_holdout > 0").toPandas()  # noqa: F821
rng = np.random.default_rng(18)
members = spark.table(fq("churn_scores")).where("card_id IS NOT NULL").groupBy("card_id").agg(  # noqa: F821
    F.count("*").alias("n"), F.sum("churn_prob").alias("expected")).toPandas().set_index("card_id")
plan = []
for card, (title, offer, offer_type) in CARDS.items():
    g = ho[ho.offer_type == offer_type].reset_index(drop=True)
    if g.empty:
        raise RuntimeError(f"No pre-cancel holdout tests for {offer_type}")
    rate = float((g.saved_treated / g.n_treated - g.saved_holdout / g.n_holdout).mean())
    pick = rng.integers(0, len(g), 10_000)  # 80% interval: resample a past test, Beta posteriors per arm
    draws = (rng.beta(g.saved_treated.to_numpy()[pick] + 1, (g.n_treated - g.saved_treated).to_numpy()[pick] + 1)
             - rng.beta(g.saved_holdout.to_numpy()[pick] + 1, (g.n_holdout - g.saved_holdout).to_numpy()[pick] + 1))
    lo, hi = (max(float(x), 0.0) for x in np.quantile(draws, [0.1, 0.9]))
    n = int(members.n.get(card, 0))
    expected = float(members.expected.get(card, 0.0))
    plan.append((card, title, offer, n, expected, rate, expected * rate, expected * lo, expected * hi,
                 float(COST[offer_type](n)), expected * rate * A["arpu_monthly"] * A["retention_months"]))
spark.createDataFrame(plan, "card_id STRING, title STRING, offer STRING, n_subscribers INT, expected_cancels DOUBLE, "  # noqa: F821
                      "save_rate DOUBLE, saved_point DOUBLE, saved_lo DOUBLE, saved_hi DOUBLE, cost_usd DOUBLE, "
                      "revenue_protected_usd DOUBLE").write.mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable(fq("save_plan"))

# COMMAND ----------
# 4. Model card

projected = float(pdf.churn_prob[at_risk].sum())
spark.createDataFrame(  # noqa: F821
    [(MODEL_NAME, version, auc, len(pdf), int(at_risk.sum()), projected, [LABELS[f] for f in importance.index[:5]])],
    "model_name STRING, version STRING, auc DOUBLE, n_scored BIGINT, n_at_risk BIGINT, projected_cancels_30d DOUBLE, "
    "top_drivers ARRAY<STRING>").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("churn_model_card"))

# COMMAND ----------
# Validation: structural checks fail the task; numbers are reported against the generator's targets.

sp = spark.table(fq("save_plan")).toPandas().set_index("card_id")  # noqa: F821
s = spark.table(fq("churn_scores"))  # noqa: F821
ineligible = s.join(spark.table(src("dim_subscribers")).where(  # noqa: F821
    "is_kids_only_account OR status <> 'active' OR canceled_ts IS NOT NULL").select("subscriber_id"), "subscriber_id").count()
samples = (spark.table(src("ref_sample_subscribers")).select("first_name", "subscriber_id", F.col("card_id").alias("planted_card"))  # noqa: F821
           .join(s, "subscriber_id", "left").select("first_name", "planted_card", "card_id", "risk_decile", "churn_prob")
           .toPandas())
summary = {
    "model": f"{MODEL_NAME} v{version}", "auc": round(auc, 4), "auc_target": P.AUC_CHAMPION,
    "n_scored": len(pdf), "at_risk": int(at_risk.sum()), "at_risk_target": P.AT_RISK,
    "projected_cancels_30d": round(projected), "projected_target": P.PROJECTED_CANCELS_30D,
    "at_risk_without_card": int((at_risk & pdf.card_id.isna()).sum()),
    "cards": {c: {"n": int(r.n_subscribers), "target": P.SAVE_CARDS[c]["n_subscribers"],
                  "expected": round(r.expected_cancels), "saved": round(r.saved_point), "cost": round(r.cost_usd)}
              for c, r in sp.iterrows()},
    "totals": {"saved": round(sp.saved_point.sum()), "saved_target": P.SAVED_TOTAL,
               "cost": round(sp.cost_usd.sum()), "cost_target": P.PLAN_COST_USD,
               "revenue": round(sp.revenue_protected_usd.sum()), "revenue_target": P.REVENUE_PROTECTED_USD},
    "samples": samples.assign(churn_prob=samples.churn_prob.round(3)).to_dict("records"),
    "top_drivers": [LABELS[f] for f in importance.index[:5]],
}
print(json.dumps(summary, indent=1, default=str))
missing_samples = samples[(samples.risk_decile != 10) | (samples.card_id != samples.planted_card)]
if len(missing_samples):
    print("WARNING: sample subscribers not at risk with their planted card:\n", missing_samples)
assert ineligible == 0, f"{ineligible} ineligible subscribers reached churn_scores"
assert int(at_risk.sum()) > 0 and len(sp) == 3, "no at-risk subscribers or an incomplete save plan"
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
