# Databricks notebook source
# StreamCo Command Center 08: churn model monitoring, the retrain rule, and the outcomes loop.
# Runs after 03_churn. Reports what it finds; it never retrains or changes the champion itself.
# 1. Champion: version, when it was trained, its test AUC at training (from the MLflow run).
# 2. Performance: AUC on each labelled monthly window for the subscribers held out of training (the same
#    hash split as 03), so the latest labelled window shows whether accuracy is holding.
# 3. Drift: PSI per feature between the training snapshots and the post-game scoring snapshot (bins = training
#    deciles; categories for booleans and signup_source), plus PSI of the model's own score.
# 4. Status by the retrain rule: retrain if PSI > 0.2 on a top-5 feature or AUC drops > 0.03 from training;
#    watch if PSI > 0.1 on a top-5 feature or AUC drops > 0.015; else OK. Retraining means: train a challenger on
#    the latest labelled windows, compare it with the champion on the same held-out window, promote only if better.
# 5. Outcomes loop: the past holdout tests behind each plan card's save rate (offer_holdout_results), so the
#    plan's numbers trace back to measured outcomes, and new test results change the next plan.
# Writes churn_monitoring (one row), churn_monitoring_features (one row per feature) and
# churn_monitoring_outcomes (one row per holdout test that feeds a card).

# COMMAND ----------

# MAGIC %pip install -q lightgbm>=4.5 mlflow>=3.4

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import datetime as dt
import json
import re

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import roc_auc_score

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "src_schema": "streamco"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
CATALOG, SCHEMA, SRC = (dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema"))  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_]+", x) for x in (CATALOG, SCHEMA, SRC)), "invalid identifier"
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"  # noqa: E731
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
spark.sql(f"USE CATALOG {CATALOG}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821

MODEL_NAME = fq("churn_propensity")
mlflow.set_registry_uri("databricks-uc")

# Same feature list and labels as 03_churn.
FEATURES = ["tenure_days", "plan_price", "is_bundle", "signup_source", "event_only_signup", "hours_30d",
            "active_days_30d", "engagement_trend", "days_since_last_session", "pauses_90d", "rebuffer_min_30d",
            "max_session_rebuffer_min", "failed_starts_30d", "declines_30d", "decline_no_retry",
            "support_contacts_30d"]
CATEGORICAL = ["is_bundle", "event_only_signup", "decline_no_retry", "signup_source"]
LABELS = {
    "tenure_days": "New subscriber", "plan_price": "Plan price", "is_bundle": "Not on a bundle",
    "signup_source": "How they signed up", "event_only_signup": "Joined for the event",
    "hours_30d": "Low viewing hours", "active_days_30d": "Few active days", "engagement_trend": "Watching less",
    "days_since_last_session": "Days since last watched", "pauses_90d": "Paused recently",
    "rebuffer_min_30d": "Buffering minutes", "max_session_rebuffer_min": "Worst buffering in one session",
    "failed_starts_30d": "Streams that failed to start", "declines_30d": "Card declines",
    "decline_no_retry": "Failed payment, never retried", "support_contacts_30d": "Contacted support"}
CARD_OFFER = {"payment": "reprocess", "playback": "credit_replay", "onboarding": "onboarding_nudge"}

# The retrain rule (shown on the Save screen).
TOP_K = 5
PSI_RETRAIN, PSI_WATCH = 0.2, 0.1
AUC_DROP_RETRAIN, AUC_DROP_WATCH = 0.03, 0.015
RETRAIN_RULE = (f"Retrain when PSI > {PSI_RETRAIN} on a top-{TOP_K} feature or AUC drops more than "
                f"{AUC_DROP_RETRAIN} from training; a challenger replaces the champion only if it beats it on the "
                "same held-out window.")

# COMMAND ----------
# 1. Champion

client = MlflowClient()
mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
version = str(mv.version)
trained_at = dt.datetime.fromtimestamp(int(mv.creation_timestamp) / 1000, tz=dt.timezone.utc)
run_metrics = client.get_run(mv.run_id).data.metrics if mv.run_id else {}
champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion").unwrap_python_model()
challengers = [v for v in client.search_model_versions(f"name = '{MODEL_NAME}'") if str(v.version) != version]
print(f"{MODEL_NAME} v{version} @champion, trained {trained_at:%Y-%m-%d %H:%M} UTC; "
      f"other versions: {[v.version for v in challengers] or 'none'}")

# COMMAND ----------
# 2. Performance on each labelled window (held-out subscribers only)

train = spark.table(fq("v_training_features")).select("subscriber_id", "snapshot_date", *FEATURES, "churned_30d").toPandas()  # noqa: F821
score = spark.table(fq("v_scoring_features")).select("subscriber_id", "snapshot_date", *FEATURES).toPandas()  # noqa: F821
if train.empty or score.empty:
    raise RuntimeError("v_training_features or v_scoring_features is empty: run 01_setup and 03_churn first.")
bucket = pd.util.hash_pandas_object(train.subscriber_id, index=False).to_numpy() % 10
held = train[bucket >= 8].copy()                      # 03's test split: never seen in training or calibration
held["churn_prob"] = champion.predict(None, held).churn_prob.to_numpy()
auc_train = float(run_metrics.get("test_auc", roc_auc_score(held.churned_30d, held.churn_prob)))
windows = []
for d, g in held.groupby("snapshot_date"):
    if g.churned_30d.nunique() == 2:
        windows.append({"snapshot_date": pd.Timestamp(d).date(), "n": int(len(g)),
                        "auc": float(roc_auc_score(g.churned_30d, g.churn_prob))})
windows.sort(key=lambda w: w["snapshot_date"])
recent = windows[-1]
auc_drop = auc_train - recent["auc"]
print(f"AUC at training {auc_train:.4f}; latest labelled window {recent['snapshot_date']} "
      f"(n={recent['n']:,}) {recent['auc']:.4f}; drop {auc_drop:+.4f}")
print("by window:", [(str(w["snapshot_date"]), round(w["auc"], 4)) for w in windows])

# COMMAND ----------
# 3. Drift: PSI per feature, training snapshots -> post-game scoring snapshot

EPS = 1e-4


def _psi(p: np.ndarray, q: np.ndarray) -> float:
    p, q = np.clip(p, EPS, None), np.clip(q, EPS, None)
    p, q = p / p.sum(), q / q.sum()
    return float(np.sum((q - p) * np.log(q / p)))


def psi(base: pd.Series, cur: pd.Series, categorical: bool) -> tuple[float, float, float]:
    """(psi, baseline mean, current mean)."""
    if categorical:
        b, c = base.astype(str), cur.astype(str)          # None -> "None", its own category
        cats = sorted(set(b) | set(c))
        p = b.value_counts(normalize=True).reindex(cats, fill_value=0).to_numpy()
        q = c.value_counts(normalize=True).reindex(cats, fill_value=0).to_numpy()
        share = lambda s: float((s == "True").mean()) if s.isin(["True", "False", "None"]).all() else None  # noqa: E731
        return _psi(p, q), share(b), share(c)             # booleans: share True; signup_source: no mean
    b, c = base.astype(float), cur.astype(float)
    # Right-closed bins on the interior training deciles: (-inf, q10], (q10, q20], ..., (q90, inf). Ties merge, so a
    # zero-heavy count (declines, pauses) still keeps "0" and "more than 0" apart.
    inner = np.unique(np.nanquantile(b, np.linspace(0.1, 0.9, 9)))
    edges = np.concatenate([[-np.inf], inner, [np.inf]])
    p = pd.cut(b.dropna(), edges, right=True).value_counts(normalize=True, sort=False).to_numpy()
    q = pd.cut(c.dropna(), edges, right=True).value_counts(normalize=True, sort=False).to_numpy()
    return _psi(p, q), float(b.mean()), float(c.mean())


# Importance = mean |SHAP| on held-out rows (as 03 computes it for the model card).
shap_abs = champion.contributions(held.sample(n=min(50_000, len(held)), random_state=15)).abs().mean()
importance = (shap_abs / shap_abs.sum()).sort_values(ascending=False)
top = list(importance.index[:TOP_K])

feat_rows = []
for f in FEATURES:
    v, bm, cm = psi(train[f], score[f], f in CATEGORICAL)
    feat_rows.append({"feature": f, "label": LABELS[f], "importance": float(importance[f]),
                      "importance_rank": int(list(importance.index).index(f) + 1), "is_top": f in top,
                      "psi": v, "baseline_mean": bm, "current_mean": cm,
                      "drift": "high" if v > PSI_RETRAIN else "moderate" if v > PSI_WATCH else "low"})
score_psi, _, _ = psi(pd.Series(held.churn_prob), pd.Series(champion.predict(None, score).churn_prob.to_numpy()), False)
feats = pd.DataFrame(feat_rows).sort_values("psi", ascending=False).reset_index(drop=True)
print(feats[["feature", "importance_rank", "psi", "baseline_mean", "current_mean", "drift"]].to_string())
print(f"model score PSI: {score_psi:.4f}")

# COMMAND ----------
# 4. Status by the retrain rule

top_feats = feats[feats.is_top]
worst_top = top_feats.iloc[0]
drifted_top = top_feats[top_feats.psi > PSI_RETRAIN]
reasons = []
if len(drifted_top):
    reasons.append("PSI above " + str(PSI_RETRAIN) + " on " + ", ".join(
        f"{r.label.lower()} ({r.psi:.2f})" for r in drifted_top.itertuples()))
if auc_drop > AUC_DROP_RETRAIN:
    reasons.append(f"AUC dropped {auc_drop:.3f} from training")
if reasons:
    status = "retrain"
elif (top_feats.psi > PSI_WATCH).any() or auc_drop > AUC_DROP_WATCH:
    status = "watch"
    reasons.append(f"top-feature PSI up to {worst_top.psi:.2f}; AUC change {-auc_drop:+.3f}")
else:
    status = "ok"
    reasons.append(f"top-feature PSI at most {worst_top.psi:.2f}; AUC change {-auc_drop:+.3f}")
status_reason = "; ".join(reasons)
print(status.upper(), "-", status_reason)

# COMMAND ----------
# 5. Outcomes loop: the holdout tests behind each card's save rate (same filter and formula as 03)

ho = spark.table(src("offer_holdout_results")).toPandas()  # noqa: F821
plan = spark.table(fq("save_plan")).select("card_id", "save_rate").toPandas().set_index("card_id")
used = ho[(ho.phase == "pre_cancel") & (ho.n_treated > 0) & (ho.n_holdout > 0)].copy()
used["treated_rate"] = used.saved_treated / used.n_treated
used["holdout_rate"] = used.saved_holdout / used.n_holdout
used["lift"] = used.treated_rate - used.holdout_rate
out_rows = []
for card, offer_type in CARD_OFFER.items():
    g = used[used.offer_type == offer_type]
    plan_rate = float(plan.save_rate.get(card, np.nan))
    for r in g.itertuples():
        out_rows.append({"card_id": card, "offer_type": offer_type, "test_id": r.test_id,
                         "run_date": pd.Timestamp(r.run_date).date(), "n_treated": int(r.n_treated),
                         "n_holdout": int(r.n_holdout), "treated_rate": float(r.treated_rate),
                         "holdout_rate": float(r.holdout_rate), "lift": float(r.lift),
                         "card_save_rate_from_tests": float(g.lift.mean()), "plan_save_rate": plan_rate})
outcomes = pd.DataFrame(out_rows)
check = outcomes.groupby("card_id").agg(tests=("test_id", "count"), from_tests=("card_save_rate_from_tests", "first"),
                                         plan=("plan_save_rate", "first"))
print(check.to_string())
mismatch = check[(check.from_tests - check.plan).abs() > 1e-9]
if len(mismatch):
    print("WARNING: save_plan rates don't match the holdout tests (re-run 03_churn):\n", mismatch)

# COMMAND ----------
# Write the three tables

now = dt.datetime.now(dt.timezone.utc)
summary = {
    "model_name": MODEL_NAME, "champion_version": version, "trained_at": trained_at, "auc_train": auc_train,
    "auc_recent": recent["auc"], "recent_window": recent["snapshot_date"], "recent_window_n": recent["n"],
    "auc_drop": auc_drop, "auc_by_window": [(w["snapshot_date"], w["n"], w["auc"]) for w in windows],
    "scoring_snapshot": pd.Timestamp(score.snapshot_date.max()).date(),
    "n_scored": int(len(score)), "score_psi": score_psi, "top_features": [LABELS[f] for f in top],
    "max_top_psi": float(worst_top.psi), "max_top_psi_feature": worst_top.label,
    "n_top_features_drifted": int(len(drifted_top)), "status": status, "status_reason": status_reason,
    "retrain_rule": RETRAIN_RULE, "psi_retrain": PSI_RETRAIN, "auc_drop_retrain": AUC_DROP_RETRAIN,
    "challenger_version": str(max(int(v.version) for v in challengers)) if challengers else None,
    "monitored_at": now}
spark.createDataFrame([tuple(summary.values())], (  # noqa: F821
    "model_name STRING, champion_version STRING, trained_at TIMESTAMP, auc_train DOUBLE, auc_recent DOUBLE, "
    "recent_window DATE, recent_window_n INT, auc_drop DOUBLE, "
    "auc_by_window ARRAY<STRUCT<snapshot_date: DATE, n: INT, auc: DOUBLE>>, scoring_snapshot DATE, n_scored BIGINT, "
    "score_psi DOUBLE, top_features ARRAY<STRING>, max_top_psi DOUBLE, max_top_psi_feature STRING, "
    "n_top_features_drifted INT, status STRING, status_reason STRING, retrain_rule STRING, psi_retrain DOUBLE, "
    "auc_drop_retrain DOUBLE, challenger_version STRING, monitored_at TIMESTAMP")).write.mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable(fq("churn_monitoring"))
spark.createDataFrame([tuple(r.values()) for r in sorted(feat_rows, key=lambda r: -r["psi"])], (  # noqa: F821
    "feature STRING, label STRING, importance DOUBLE, importance_rank INT, is_top BOOLEAN, psi DOUBLE, "
    "baseline_mean DOUBLE, current_mean DOUBLE, drift STRING")).write.mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable(fq("churn_monitoring_features"))
spark.createDataFrame([tuple(r.values()) for r in out_rows], (  # noqa: F821
    "card_id STRING, offer_type STRING, test_id STRING, run_date DATE, n_treated INT, n_holdout INT, "
    "treated_rate DOUBLE, holdout_rate DOUBLE, lift DOUBLE, card_save_rate_from_tests DOUBLE, "
    "plan_save_rate DOUBLE")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("churn_monitoring_outcomes"))
for t, c in {"churn_monitoring": f"Churn model monitoring (08_monitor): champion, AUC at training vs the latest "
                                 f"labelled window, drift, status by the retrain rule. {RETRAIN_RULE}",
             "churn_monitoring_features": "PSI per feature, training snapshots vs the post-game scoring snapshot "
                                          "(training deciles as bins). drift: high > 0.2, moderate > 0.1.",
             "churn_monitoring_outcomes": "The past pre-cancel holdout tests behind each save-plan card's save rate "
                                          "(mean treated-minus-holdout lift)."}.items():
    spark.sql(f"COMMENT ON TABLE {fq(t)} IS '{c.replace(chr(39), chr(39) * 2)}'")  # noqa: F821

# COMMAND ----------
# Validation (structure only; the numbers are reported as found)

n = {t: spark.table(fq(t)).count() for t in ("churn_monitoring", "churn_monitoring_features",  # noqa: F821
                                               "churn_monitoring_outcomes")}
print(n)
assert n["churn_monitoring"] == 1 and n["churn_monitoring_features"] == len(FEATURES)
assert n["churn_monitoring_outcomes"] > 0 and set(outcomes.card_id) == set(CARD_OFFER)
result = {k: (str(v) if isinstance(v, (dt.date, dt.datetime)) else v) for k, v in summary.items()
          if k not in ("auc_by_window",)}
result["psi"] = {r.feature: round(r.psi, 4) for r in feats.itertuples()}
result["outcomes"] = check.round(4).to_dict("index")
print(json.dumps(result, indent=1, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
