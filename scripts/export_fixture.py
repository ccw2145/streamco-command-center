"""Build app/fixture.json: every screen payload, the recorded Ask run, Genie follow-ups and each number's SQL.

    python scripts/export_fixture.py                  # from the real tables in cindy_wu_catalog.streamco

The export runs every query below on the warehouse, records one live Ask investigation and the two Genie
follow-ups, and writes "illustrative": false. It stops, without writing, if any query fails or returns nothing,
or if the output contains a term listed in BLOCKED_TERMS (comma-separated, optional).
Run from the repo root with app/.venv/bin/python (needs databricks-sdk and the my-workspace profile).
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))
from backend.config import ET, FIXTURE_PATH, get_settings  # noqa: E402

TIER = "Sports"
METRO_NAMES = {"NYC": "New York", "LAX": "Los Angeles", "CHI": "Chicago", "DFW": "Dallas-Fort Worth", "HOU": "Houston",
               "ATL": "Atlanta", "WAS": "Washington", "PHL": "Philadelphia", "MIA": "Miami", "BOS": "Boston",
               "PHX": "Phoenix", "SEA": "Seattle", "SFO": "San Francisco", "DEN": "Denver", "MSP": "Minneapolis"}
OPS_SHIFT_ET = "20:26"   # from the incident log: ops moved the failing viewers to another CDN (no view carries it)
ARPU_MONTHLY, RETENTION_MONTHS = 11.99, 12
PRIMARY_QUESTION = "Why did Sports cancellations jump after the game?"
FOLLOWUPS = [("What about Originals?", ["Did Originals cancellations go up too?"]),
             ("Show cancels by state", ["Which states had the most cancellations?"])]

# ---------------------------------------------------------------- queries (bare names resolve in streamco)
Q = {
    "sources": "SELECT source_id, display_name, system_type, prod_connector, bronze_tables, row_count "
               "FROM ref_source_systems",
    "teams": "SELECT team, count, definition, governed_count, governed_definition FROM gold_team_cancel_counts",
    "governed": "SELECT MEASURE(cancels) AS cancels FROM mv_streamco_metrics WHERE day = :day AND tier = :tier",
    "recon": "SELECT tier, subscriber_live_cancels, ledger_cancels, pct_diff, status FROM gold_reconciliation_daily "
             "WHERE day = :day AND tier = :tier",
    "peak": "SELECT MAX(viewers) AS peak_viewers FROM (SELECT minute_ts, SUM(concurrent_sessions) AS viewers "
            "FROM gold_cdn_region_minute WHERE minute_ts BETWEEN :t0 AND :t1 GROUP BY minute_ts)",
    "forecast_peak": "SELECT MAX(viewers) AS forecast_peak FROM (SELECT ts, SUM(yhat) AS viewers FROM forecast_viewers "
                     "WHERE ts BETWEEN :t0 AND :t1 GROUP BY ts)",
    "expected": "SELECT day, tier, actual, yhat, lo, hi FROM gold_cancel_expected WHERE day = :day AND tier = :tier",
    "model": "SELECT model_name, version, auc, n_scored, n_at_risk, projected_cancels_30d, top_drivers "
             "FROM churn_model_card ORDER BY version DESC LIMIT 1",
    "plan": "SELECT card_id, title, offer, n_subscribers, expected_cancels, save_rate, saved_point, saved_lo, "
            "saved_hi, cost_usd, revenue_protected_usd FROM save_plan",
    "alert": "SELECT alert_id, detected_ts, cdn, metro, device_family, app_version, observed, baseline, "
             "persistence_min, affected_sessions, resolved_ts FROM gold_anomaly_alerts ORDER BY detected_ts LIMIT 1",
    "flags": "SELECT metro, cdn, util_at_peak, flagged FROM forecast_flags ORDER BY util_at_peak DESC",
    "actual_series": "SELECT timestamp_seconds(FLOOR(unix_timestamp(minute_ts) / 300) * 300) AS t, MAX(viewers) AS "
                     "actual FROM (SELECT minute_ts, SUM(concurrent_sessions) AS viewers FROM gold_cdn_region_minute "
                     "WHERE minute_ts BETWEEN :t0 AND :t1 GROUP BY minute_ts) GROUP BY 1 ORDER BY 1",
    "forecast_series": "SELECT timestamp_seconds(FLOOR(unix_timestamp(ts) / 300) * 300) AS t, MAX(yhat) AS yhat, "
                       "MAX(lo) AS lo, MAX(hi) AS hi FROM (SELECT ts, SUM(yhat) AS yhat, SUM(yhat_lo) AS lo, "
                       "SUM(yhat_hi) AS hi FROM forecast_viewers WHERE ts BETWEEN :t0 AND :t1 GROUP BY ts) "
                       "GROUP BY 1 ORDER BY 1",
    "drivers": "SELECT driver, n_cancels, pct, evidence_type, confidence FROM fn_cancel_drivers(:day, :tier) "
               "WHERE driver IS NOT NULL AND driver <> 'baseline'",
    "samples": "SELECT card_id, first_name, subscriber_id, metro, tier, device, tenure_months, what_happened, "
               "fav_genre, recent_titles, watch_next FROM ref_sample_subscribers",
    "messages": "SELECT m.subscriber_id, m.card_id, m.subject, m.body, m.watch_next, m.agent_note, m.model "
                "FROM personalized_messages m JOIN ref_sample_subscribers s ON m.subscriber_id = s.subscriber_id",
    "run": "SELECT n_messages, cost_usd, model, run_at FROM personalization_run ORDER BY run_at DESC LIMIT 1",
    "masking": "SELECT masked_text, transcript_text, n_redactions FROM v_support_showcase LIMIT 1",
}
# The drawer's usage headline is a labelled seed. A live usage row is only meaningful for the app's own service
# principal (run as a person, system.ai_gateway.usage returns all of that person's traffic), so it isn't exported.
OPTIONAL = {"usage"}   # the drawer shows a labelled seed without it
# One Truth: the core entity, data quality from the two pipelines' event logs, and lineage of the governed number.
# Save: model monitoring (notebooks/08_monitor), the holdout tests behind each save rate, retrieved policy passages.
_FQ = get_settings().fq   # validated identifiers (config._ident)
Q.update({
    "subscribers": "SELECT count(*) AS subscribers FROM v_subscribers",
    "entity": " UNION ALL ".join(
        f"SELECT '{name}' AS system, count(DISTINCT x.subscriber_id) AS subscriber_ids, "
        f"count(DISTINCT x.subscriber_id) - count(DISTINCT a.subscriber_id) AS unmatched_ids "
        f"FROM {table} x LEFT JOIN silver_accounts a ON a.subscriber_id = x.subscriber_id"
        for name, table in (("Subscriptions", "silver_subscription_events"), ("Billing", "silver_billing_attempts"),
                            ("Support", "silver_support_cases"), ("Payments", "silver_processor_events"),
                            ("Telemetry", "silver_sessions"))),
    # Latest full refresh of each pipeline (the run that loaded all the data; a later incremental update only sees new
    # rows): every expectation's passed/failed rows and its action (DROP or ALLOW = warn).
    "dq": (f"WITH ev AS (SELECT * FROM event_log(TABLE({_FQ}.silver_accounts)) "
           f"UNION ALL SELECT * FROM event_log(TABLE({_FQ}.gold_cancel_events))), "
           "upd AS (SELECT origin.pipeline_name AS pipeline, max_by(origin.update_id, timestamp) AS update_id, "
           "max(timestamp) AS run_ts FROM ev WHERE event_type = 'create_update' "
           "AND details:create_update.full_refresh::boolean GROUP BY 1), "
           "cur AS (SELECT ev.*, upd.pipeline, upd.run_ts FROM ev JOIN upd ON ev.origin.update_id = upd.update_id), "
           "res AS (SELECT pipeline, explode(from_json(details:flow_progress.data_quality.expectations, "
           "'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')) AS e "
           "FROM cur WHERE event_type = 'flow_progress' AND details:flow_progress.status = 'COMPLETED'), "
           "act AS (SELECT DISTINCT a.name, a.action FROM (SELECT explode(from_json(details:dataset_definition.expectations, "
           "'ARRAY<STRUCT<name: STRING, action: STRING>>')) AS a FROM cur WHERE event_type = 'dataset_definition')) "
           "SELECT u.pipeline, res.e.name AS rule, regexp_replace(res.e.dataset, '^.*[.]', '') AS dataset, act.action, "
           "res.e.passed_records AS passed, res.e.failed_records AS failed, u.run_ts "
           "FROM upd u LEFT JOIN res ON res.pipeline = u.pipeline LEFT JOIN act ON act.name = res.e.name "
           "ORDER BY u.pipeline DESC, failed DESC, rule"),
    "lineage": ("SELECT DISTINCT source_table_name AS source, target_table_name AS target FROM system.access.table_lineage "
                f"WHERE target_table_catalog = '{get_settings().catalog}' AND target_table_schema = '{get_settings().schema}' "
                f"AND source_table_catalog = '{get_settings().catalog}' AND source_table_schema = '{get_settings().schema}' "
                "AND event_time >= current_timestamp() - INTERVAL 30 DAYS"),
    "monitoring": ("SELECT champion_version, trained_at, auc_train, auc_recent, recent_window, recent_window_n, auc_drop, "
                   "auc_by_window, scoring_snapshot, n_scored, score_psi, top_features, max_top_psi, max_top_psi_feature, "
                   "n_top_features_drifted, status, status_reason, retrain_rule, psi_retrain, auc_drop_retrain, "
                   "challenger_version, monitored_at FROM churn_monitoring"),
    "monitoring_features": ("SELECT label, feature, importance_rank, is_top, psi, baseline_mean, current_mean, drift "
                            "FROM churn_monitoring_features ORDER BY is_top DESC, psi DESC"),
    "outcomes": ("SELECT card_id, test_id, run_date, n_treated, n_holdout, treated_rate, holdout_rate, lift, "
                 "card_save_rate_from_tests, plan_save_rate FROM churn_monitoring_outcomes ORDER BY card_id, run_date"),
    # Written by notebooks/07_knowledge (the best-scoring offer-policy passage per plan card). Optional.
    "citations": ("SELECT card_id, doc_title, passage, score FROM knowledge_citations "
                  "QUALIFY row_number() OVER (PARTITION BY card_id ORDER BY score DESC) = 1"),
})
# Lineage, the event log, monitoring and citations are optional: the screens show their cue only when the rows exist.
OPTIONAL |= {"dq", "lineage", "monitoring", "monitoring_features", "outcomes", "citations"}

# How each team's count reconciles to the governed one, for the same day as gold_team_cancel_counts.
Q["bridge"] = """
WITH d AS (SELECT max(day) AS day FROM gold_cancel_events WHERE tier = 'Sports'),
ev AS (SELECT * FROM silver_subscription_events WHERE tier = 'Sports'),
clicks AS (SELECT e.subscriber_id FROM ev e CROSS JOIN d WHERE e.event_type = 'cancel_request'
           AND to_date(from_utc_timestamp(e.event_ts, 'America/New_York')) = d.day),
gov AS (SELECT g.* FROM gold_cancel_events g JOIN d USING (day) WHERE g.tier = 'Sports'),
nc AS (SELECT c.subscriber_id FROM clicks c LEFT ANTI JOIN gov USING (subscriber_id)),
ncx AS (SELECT nc.subscriber_id, max(CASE WHEN e.event_type = 'cancel' THEN 1 ELSE 0 END) AS cancelled_later
        FROM nc LEFT JOIN ev e ON e.subscriber_id = nc.subscriber_id GROUP BY 1)
SELECT (SELECT count(*) FROM ncx WHERE cancelled_later = 0) AS clicked_stayed,
       (SELECT count(*) FROM ncx WHERE cancelled_later = 1) AS clicked_reversed,
       (SELECT count(*) FROM gov g LEFT ANTI JOIN clicks c USING (subscriber_id)) AS no_click,
       (SELECT count(*) FROM gov g LEFT ANTI JOIN clicks c USING (subscriber_id)
         WHERE g.cancel_type = 'involuntary_billing') AS no_click_payment,
       (SELECT count_if(NOT care_touched) FROM gov) AS no_care_contact
"""
OPTIONAL.add("bridge")
# The governed number's path when system.access.table_lineage has no rows, from data/pipelines (labelled as such).
LINEAGE_FROM_DEFINITIONS = {
    "mv_streamco_metrics": {"gold_subscriber_daily"},
    "gold_subscriber_daily": {"gold_cancel_events", "silver_subscription_events", "silver_accounts"},
    "gold_cancel_events": {"silver_subscription_events", "silver_accounts", "silver_billing_attempts",
                           "silver_processor_events", "silver_support_cases", "silver_sessions"},
    "silver_subscription_events": {"bronze_mysql_subscriptions"}, "silver_accounts": {"bronze_mysql_accounts"},
    "silver_billing_attempts": {"bronze_pg_billing_attempts"}, "silver_processor_events": {"bronze_processor_events"},
    "silver_support_cases": {"bronze_sfdc_support_cases"}, "silver_sessions": {"bronze_session_events"},
}

# --placeholder save: these inputs come from the planted targets until notebooks 03_churn and 05_personalize finish.

# metric_id -> (query, label, tables, system of record)
PROVENANCE = {
    "source_systems": ("sources", "Rows landed from each source system", ["ref_source_systems"], "Unity Catalog"),
    "team_cancels_product": ("teams", "Product's cancel count", ["gold_team_cancel_counts"], "Product analytics"),
    "team_cancels_finance": ("teams", "Finance's cancel count", ["gold_team_cancel_counts"], "Billing ledger"),
    "team_cancels_care": ("teams", "Care's cancel count", ["gold_team_cancel_counts"], "Support cases"),
    "cancels_governed": ("governed", f"{TIER} cancellations, governed definition", ["mv_streamco_metrics"],
                         "Unity Catalog metric view"),
    "recon_sports": ("recon", "Reconciliation with the billing ledger", ["gold_reconciliation_daily"], "Billing ledger"),
    "peak_viewers": ("peak", "Peak concurrent viewers", ["gold_cdn_region_minute"], "Playback telemetry"),
    "forecast_peak": ("forecast_peak", "Forecast peak viewers", ["forecast_viewers"], "viewer_forecast model"),
    "cancels_pct_over": ("expected", "Cancellations vs expected", ["gold_cancel_expected"], "Unity Catalog metric view"),
    "subs_at_risk": ("model", "Subscribers at risk", ["churn_model_card", "churn_scores"], "churn_propensity model"),
    "churn_model_auc": ("model", "Churn model accuracy (AUC)", ["churn_model_card"], "churn_propensity model"),
    "revenue_at_risk": ("model", "Revenue at risk if no action", ["churn_model_card"], "churn_propensity model"),
    "plan_saved_total": ("plan", "Subscribers the plan saves", ["save_plan"], "Save plan"),
    "plan_cost": ("plan", "What the plan costs", ["save_plan"], "Save plan"),
    "plan_revenue_protected": ("plan", "Revenue the plan protects", ["save_plan"], "Save plan"),
    "hot_cell_utilization": ("flags", "Forecast utilization at the halftime peak", ["forecast_flags"],
                             "viewer_forecast model"),
    "affected_sessions": ("alert", "Sessions in the failing cell", ["gold_anomaly_alerts"], "Playback telemetry"),
    "personalization_run": ("run", "Personalized messages generated", ["personalization_run"], "AI Functions"),
    **{f"cancel_driver_{d}": ("drivers", f"Cancel driver: {d.replace('_', ' ')}", ["fn_cancel_drivers",
                                                                                  "gold_cancel_decomposition"],
                              "Unity Catalog function") for d in ("playback", "payment", "event_only", "unexplained")},
}

PROVENANCE.update({
    "subscriber_entity": ("entity", "One subscriber ID across the five systems",
                          ["silver_accounts", "silver_subscription_events", "silver_billing_attempts",
                           "silver_support_cases", "silver_processor_events", "silver_sessions"], "Unity Catalog"),
    "data_quality": ("dq", "Data quality rules, latest full pipeline runs", ["event_log(streamco-data-stream)",
                                                                        "event_log(streamco-data-batch)"],
                     "Lakeflow pipeline event log"),
    "model_monitoring": ("monitoring_features", "Churn model drift: training vs the post-game snapshot",
                         ["churn_monitoring", "churn_monitoring_features"], "notebooks/08_monitor"),
    "save_rate_sources": ("outcomes", "The past holdout tests behind each save rate",
                          ["churn_monitoring_outcomes", "offer_holdout_results"], "Offer holdout tests"),
})

STATIC = {
    "badges": [{"id": "pii_masked", "label": "PII masked",
                "detail": "Email, phone and card details are masked by governed views in Unity Catalog."},
               {"id": "kids_filtered", "label": "Kids profiles filtered",
                "detail": "Kids-only accounts are removed by the governed views before any query or model sees them."}],
    "layers": [{"id": "bronze", "label": "Bronze", "summary": "Raw, as landed"},
               {"id": "silver", "label": "Silver", "summary": "Cleaned, joined, quality-checked"},
               {"id": "gold", "label": "Gold", "summary": "Business tables"}],
    "measures": ["cancels", "cancel_rate", "active_subs", "buffering_ratio"],
    "team_short": {"Product analytics": "Product", "Finance": "Finance", "Customer care": "Care"},
    "card_who": {"payment": "Signed up at halftime, payment timed out", "playback": "Buffered during halftime",
                 "onboarding": "Joined for the game, still new"},
    "card_policy": {
        "payment": "Involuntary (payment) cancellations receive a free payment retry. No discount or credit is offered.",
        "playback": "Subscribers with a verified playback failure may receive a one-month service credit and access "
                    "to the replay.",
        "onboarding": "New subscribers may receive welcome messages and title suggestions. No discount is offered."},
    "driver_labels": {"rebuffer_min_30d": "Buffering minutes", "decline_no_retry": "Failed payment, no retry",
                      "event_only_signup": "Joined for the event", "engagement_trend": "Watching less"},
    "lanes": [
        {"lane": "subscribers", "label": "Subscriber numbers", "tools": ["Genie Agent: StreamCo Subscriber Insights"],
         "owner_system": "Metric view"},
        {"lane": "playback", "label": "Viewing quality", "tools": ["gold_anomaly_alerts", "gold_playback_failures_by_device"],
         "owner_system": "Event stream"},
        {"lane": "billing", "label": "Payments", "tools": ["gold_billing_failures"], "owner_system": "Billing (PostgreSQL)"},
        {"lane": "care", "label": "Customer contacts", "tools": ["gold_contact_reasons"],
         "owner_system": "Salesforce Service Cloud"},
        {"lane": "knowledge", "label": "Policies and support chats", "tools": ["AI Search: knowledge_idx"],
         "owner_system": "Policy docs + support transcripts"},
        {"lane": "baseline", "label": "Expected vs unexplained", "tools": ["fn_cancel_drivers"],
         "owner_system": "Metric view + ledger"}],
}

HIW = {
    "truth": [
        ("Five systems, one copy of the truth", "Subscriptions, billing, support, the payment processor and playback "
         "telemetry land in bronze tables, are cleaned in silver and modelled in gold, all in Unity Catalog. In "
         "production they connect through Lakeflow Connect or Zerobus Ingest.",
         ["Lakeflow Connect", "Zerobus Ingest", "Lakeflow Declarative Pipelines", "Unity Catalog"]),
        ("One governed definition", "Product, Finance and Care each counted cancellations their own way. The metric "
         "view defines 'cancels' once, and every screen, the Genie Agent and the Ask agent read that definition.",
         ["Metric Views", "Unity Catalog"]),
        ("Quality checked on the way in", "Each pipeline carries data quality rules (Lakeflow expectations). Rows "
         "that break a hard rule are dropped; rows that break a warning rule are kept and counted. The counts come "
         "from the pipelines' event log, and the governed number is reconciled to the billing ledger.",
         ["Lakeflow expectations", "Pipeline event log"]),
        ("Lineage you can click", "Unity Catalog records which tables each table reads, so the governed number "
         "traces back from the metric view through gold, silver and bronze to the five sources.",
         ["Unity Catalog lineage", "Catalog Explorer"]),
        ("Governed where the data lives", "Every screen, Genie and the agent read governed views that mask email, "
         "phone and address and drop kids-only accounts, so no query, model or prompt can skip them. Only members "
         "of the cc_privileged group see unmasked values.",
         ["Unity Catalog", "Dynamic views", "Account groups"])],
    "forecast": [
        ("The forecast flagged Dallas before kickoff", "A LightGBM viewership forecast, registered in Unity Catalog, "
         "predicted the halftime peak by city and delivery network, and the one cell that would run hot.",
         ["MLflow", "Unity Catalog model registry", "Serverless jobs"]),
        ("The alert is a rule, not AI", "A cell alerts when buffering stays far above its normal range for three "
         "minutes in a row. The alert text is a template filled from the alert row, so it can't drift from the data.",
         ["Lakeflow Declarative Pipelines"]),
        ("A replay of the real night", "The chart replays recorded viewers against the forecast issued before kickoff.",
         [])],
    "ask": [
        ("Six lines of investigation, in parallel", "Lane 1 is the Genie Agent over the metric view; lanes 2-4 and 6 "
         "query governed views and the fn_cancel_drivers function. The answer is written only from what they returned.",
         ["Agent Bricks", "Agent Runtime", "Genie Agents", "Unity Catalog functions"]),
        ("Your documents, cited", "Lane 5 searches StreamCo's policy documents and the masked support transcripts "
         "with AI Search. The answer quotes the payment retry policy and counts the support conversations behind "
         "the payment driver. Transcripts are masked and kids-only accounts are left out before anything is indexed.",
         ["AI Search", "Unity Gateway", "Unity Catalog governed views"]),
        ("Follow-up questions go straight to Genie", "Genie answers from the certified metric definitions and shows "
         "the SQL it ran. When two sources disagree, it says so instead of guessing.", ["Genie Agents", "Metric Views"]),
        ("Every step is traced", "Each tool call, its SQL and its timing are recorded as an MLflow trace.",
         ["MLflow tracing"])],
    "save": [
        ("The churn model", "One model, registered in Unity Catalog with the alias champion, scores every eligible "
         "subscriber from viewing quality, payment and engagement features.",
         ["MLflow", "Unity Catalog model registry"]),
        ("Monitored, and retrained by rule", "After every scoring run, a monitoring job compares accuracy on the "
         "latest labelled month with accuracy at training, and measures drift (PSI) on the model's top features "
         "between the training months and the post-game snapshot. When the rule trips, a challenger is trained and "
         "replaces the champion only if it scores better on the same held-out month.",
         ["MLflow", "Unity Catalog model registry", "Lakeflow Jobs"]),
        ("Learning from outcomes", "Save rates come from past holdout tests (treated minus holdout), with an 80% range. "
         "Every new test the plan runs is added to the same table, so the next plan is sized from what actually "
         "worked.", ["Delta tables"]),
        ("A different message for every subscriber", "ai_query writes one message per at-risk subscriber in a single "
         "batch through Unity Gateway. Personal data is masked and kids profiles are filtered before the model sees "
         "anything. Regenerate writes a new one live, with the same prompt and rules.",
         ["AI Functions", "Unity Gateway", "Unity Catalog governed views"]),
        ("Served from Lakebase", "Churn scores and messages are synced from Unity Catalog into Lakebase, the "
         "Postgres database the app and support tools read one subscriber at a time, in milliseconds. Each card shows "
         "its subscriber's live row.", ["Lakebase", "Lakebase synced tables", "Unity Catalog"]),
        ("Approval, then action", "Nothing is sent until a person approves. The approval is written to an "
         "app-owned table in Lakebase.", ["Lakebase"])],
    "governance": [
        ("One front door for every AI call", "Every model call in this demo, from the Ask agent, Genie and the "
         "personalized messages, goes through Unity Gateway with budgets, rate limits and usage logs.",
         ["Unity Gateway", "System tables"]),
        ("Masked before AI sees it", "Every screen, model and prompt reads through governed views that mask personal "
         "data and drop kids profiles. The prompt is never the control. In production the same rules move onto the "
         "tables as column masks and row filters.", ["Unity Catalog governed views"])],
}


def panels(screen: str) -> list[dict]:
    return [{"title": t, "body": b, "products": p} for t, b, p in HIW[screen]]


# ---------------------------------------------------------------- helpers
def params() -> dict:
    s = get_settings()
    c = s.clock()
    return {"day": s.day_after, "tier": TIER, "t0": c["t0"], "t1": c["end"], "model": s.gateway_model}


def used_params(stmt: str) -> dict:
    return {k: v for k, v in params().items() if f":{k}" in stmt}


def et(ts) -> dt.datetime:
    t = ts if isinstance(ts, dt.datetime) else dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    return (t if t.tzinfo else t.replace(tzinfo=dt.UTC)).astimezone(ET)


def label_clock(t: dt.datetime) -> str:
    return t.strftime("%-I:%M %p")


def compact(n: float) -> str:
    if n >= 1e6:
        return f"{n / 1e6:.1f}M".replace(".0M", "M")
    if n >= 1e4:
        return f"{n / 1e3:.1f}k".replace(".0k", "k")
    return f"{n:,.0f}"


def approx(n: float) -> str:
    return f"~{round(n, -2):,.0f}" if n >= 1000 else f"~{n:,.0f}"


def jsonable(rows: list[dict]) -> tuple[list[str], list[list]]:
    cols = list(rows[0]) if rows else []
    return cols, [[v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else v for v in r.values()] for r in rows]


# ---------------------------------------------------------------- builders (rows -> screen payloads)
def build_entity(r: dict) -> dict | None:
    rows, subs = r.get("entity") or [], r.get("subscribers") or []
    if not rows or not subs:
        return None
    unmatched = sum(x["unmatched_ids"] or 0 for x in rows)
    return {"name": "subscriber", "key": "subscriber_id", "count": subs[0]["subscribers"], "systems": len(rows),
            "unmatched_ids": unmatched, "metric_id": "subscriber_entity",
            "label": (f"one subscriber_id joins all {len(rows)} systems \u00b7 {compact(subs[0]['subscribers'])} "
                      f"subscribers \u00b7 " + ("every ID matched" if unmatched == 0 else f"{unmatched:,} unmatched IDs"))}


def build_quality(r: dict) -> dict | None:
    """Expectation results from the pipelines' latest updates. Rows failing a DROP rule are quarantined (dropped);
    rows failing an ALLOW rule are kept and counted as warnings."""
    rules = [x for x in r.get("dq") or [] if x.get("rule")]
    if not rules:
        return None
    by_ds: dict[str, list[int]] = {}
    for x in rules:
        d = by_ds.setdefault(x["dataset"], [0, 0])
        d[0] = max(d[0], x["passed"] + x["failed"])
        d[1] += x["failed"]                      # an upper bound when two rules fail on one dataset
    rows = sum(d[0] for d in by_ds.values())
    failed_rows = sum(min(d[1], d[0]) for d in by_ds.values())
    dropped = sum(x["failed"] for x in rules if x["action"] == "DROP")
    warned = sum(x["failed"] for x in rules if x["action"] != "DROP")
    pct = (rows - failed_rows) / rows if rows else 1.0
    pct_label = f"{int(pct * 1000) / 10:.1f}%"   # floor, so a failing rule never shows as 100%
    failing = [{"rule": x["rule"], "dataset": x["dataset"], "action": x["action"], "failed": x["failed"],
                "checked": x["passed"] + x["failed"]} for x in rules if x["failed"]]
    pipelines = sorted({x["pipeline"] for x in r["dq"]})
    return {"n_rules": len(rules), "rows_checked": rows, "pct_passed": pct, "quarantined": dropped,
            "warned": warned, "failing": failing, "pipelines": pipelines,
            "run_ts": max(str(x["run_ts"]) for x in r["dq"]), "metric_id": "data_quality",
            "status": "ok" if not failing else "warn",
            "label": (f"{len(rules)} quality rules \u00b7 {pct_label} of rows passed \u00b7 {dropped:,} quarantined"
                      + (f" \u00b7 {warned:,} flagged" if warned else "")),
            "detail": ("; ".join(f"{f['rule']} ({'warning, rows kept' if f['action'] != 'DROP' else 'rows dropped'}): "
                                 f"{f['failed']:,} of {f['checked']:,} {f['dataset']} rows" for f in failing)
                       or "Every rule passed on the latest run.")}


def build_lineage(r: dict) -> dict:
    """The governed cancel count's upstream path, from system.access.table_lineage (else the pipeline code)."""
    up: dict[str, set] = {}
    for e in r.get("lineage") or []:
        up.setdefault(e["target"], set()).add(e["source"])
    basis = "Unity Catalog lineage (system.access.table_lineage)"
    if "gold_subscriber_daily" not in up.get("mv_streamco_metrics", set()):
        up, basis = LINEAGE_FROM_DEFINITIONS, "pipeline definitions (no lineage rows were found)"
    seen, todo = set(), ["gold_subscriber_daily"]      # the cancels measure reads gold_subscriber_daily only
    while todo:
        t = todo.pop()
        if t not in seen:
            seen.add(t)
            todo += [x for x in up.get(t, ()) if x.startswith(("gold_", "silver_", "bronze_"))]
    layer = lambda p: sorted(x for x in seen if x.startswith(p))  # noqa: E731
    bronze = set(layer("bronze_"))
    sources = [s["system_type"].replace(" (SaaS)", "") for s in r.get("sources") or []
               if bronze & set(s["bronze_tables"])]
    url = None
    try:
        from backend.databricks import workspace

        w = workspace()
        c, sc = get_settings().catalog, get_settings().schema
        url = f"{w.config.host.rstrip('/')}/explore/data/{c}/{sc}/mv_streamco_metrics?o={w.get_workspace_id()}&activeTab=lineage"
    except Exception as exc:  # noqa: BLE001
        print(f"  no Catalog Explorer link ({str(exc)[:80]})")
    return {"basis": basis, "url": url, "hops": [
        {"layer": "Sources", "tables": sources, "basis": "pipeline definition (Auto Loader over the landing volume)"},
        {"layer": "Bronze", "tables": layer("bronze_"), "basis": basis.split(" (")[0]},
        {"layer": "Silver", "tables": layer("silver_"), "basis": basis.split(" (")[0]},
        {"layer": "Gold", "tables": layer("gold_"), "basis": basis.split(" (")[0]},
        {"layer": "Metric view", "tables": ["mv_streamco_metrics"], "basis": basis.split(" (")[0]}]}


def build_monitoring(r: dict) -> dict | None:
    if not r.get("monitoring"):
        return None
    m = r["monitoring"][0]
    as_list = lambda v: v if isinstance(v, list) else json.loads(v) if v else []  # noqa: E731
    feats = [{"feature": f["feature"], "label": f["label"], "rank": f["importance_rank"], "is_top": f["is_top"],
              "psi": f["psi"], "baseline_mean": f["baseline_mean"], "current_mean": f["current_mean"],
              "drift": f["drift"]} for f in r.get("monitoring_features") or []]
    titles = {c["card_id"]: c["title"] for c in r.get("plan") or []}
    outcomes = {}
    for o in r.get("outcomes") or []:
        card = outcomes.setdefault(o["card_id"], {"card_id": o["card_id"], "title": titles.get(o["card_id"], o["card_id"]),
                                                  "save_rate": o["card_save_rate_from_tests"],
                                                  "plan_save_rate": o["plan_save_rate"], "tests": []})
        card["tests"].append({"test_id": o["test_id"], "run_date": str(o["run_date"]), "n_treated": o["n_treated"],
                              "n_holdout": o["n_holdout"], "treated_rate": o["treated_rate"],
                              "holdout_rate": o["holdout_rate"], "lift": o["lift"]})
    return {"version": str(m["champion_version"]), "trained_at": str(m["trained_at"]), "auc_train": m["auc_train"],
            "auc_recent": m["auc_recent"], "recent_window": str(m["recent_window"]),
            "recent_window_n": m["recent_window_n"], "auc_drop": m["auc_drop"],
            "auc_by_window": [{"window": str(w["snapshot_date"]), "n": int(w["n"]), "auc": float(w["auc"])}
                              for w in as_list(m["auc_by_window"])],
            "scoring_snapshot": str(m["scoring_snapshot"]), "score_psi": m["score_psi"],
            "max_top_psi": m["max_top_psi"], "max_top_psi_feature": m["max_top_psi_feature"],
            "status": m["status"], "status_reason": m["status_reason"], "retrain_rule": m["retrain_rule"],
            "retrain_short": (f"retrains when a top-5 feature drifts (PSI > {m['psi_retrain']:g}) or AUC drops "
                              f"> {m['auc_drop_retrain']:g}"),
            "challenger_version": m["challenger_version"], "monitored_at": str(m["monitored_at"]),
            "features": feats, "outcomes": list(outcomes.values()), "metric_id": "model_monitoring"}


def build_bridge(short: str, delta: int, b: dict | None) -> str | None:
    """One line: why this team's count differs from the governed one."""
    if short == "Finance":
        return f"Counts the billing ledger: {abs(delta):,} cancels hadn't posted to it yet." if delta < 0 else None
    if b is None:
        return None
    if short == "Product":
        return (f"Counts cancel clicks: {b['clicked_stayed'] + b['clicked_reversed']:,} stayed or came back within 24 hours, "
                f"and {b['no_click']:,} left without a click, {b['no_click_payment']:,} of them on a failed payment.")
    if short == "Care":
        return f"Counts only cancels with a support contact: {b['no_care_contact']:,} cancelled without contacting Care."
    return None


def build_truth(r: dict) -> dict:
    e, m = r["expected"][0], r["model"][0]
    plan = r["plan"]
    saved = sum(c["saved_point"] for c in plan)
    pct_over = e["actual"] / e["yhat"] - 1
    recon = r["recon"][0]
    governed = r["governed"][0]["cancels"]
    teams = [{"team": t["team"], "short": STATIC["team_short"].get(t["team"], t["team"]), "count": t["count"],
              "definition": t["definition"], "metric_id": f"team_cancels_{STATIC['team_short'].get(t['team'], t['team']).lower()}"}
             for t in r["teams"]]
    order = ["Product", "Finance", "Care"]
    teams.sort(key=lambda t: order.index(t["short"]) if t["short"] in order else 9)
    for t in teams:
        t["delta"] = t["count"] - governed
        t["bridge"] = build_bridge(t["short"], t["delta"], (r.get("bridge") or [None])[0])
    return {
        "story": (f"A record night: {compact(r['peak'][0]['peak_viewers'])} people streaming at the peak. The next "
                  f"morning, {TIER} cancellations came in {pct_over * 100:.0f}% above normal. Before anyone decides "
                  f"what to do, every team needs the same number."),
        "badges": STATIC["badges"],
        "sources": [{"source_id": s["source_id"], "name": s["display_name"],
                     "system_type": s["system_type"].replace(" (SaaS)", ""), "prod_connector": s["prod_connector"],
                     "bronze_tables": s["bronze_tables"], "row_count": s["row_count"]} for s in r["sources"]],
        "pipeline": {"layers": STATIC["layers"], "metric_view": {"name": "mv_streamco_metrics",
                                                                  "measures": STATIC["measures"]}},
        "team_counts": {
            "tier": TIER, "teams": teams,
            "governed": {"count": governed, "definition": r["teams"][0]["governed_definition"],
                         "metric_id": "cancels_governed"},
            "reconciliation": {"ledger": recon["ledger_cancels"], "pct_diff": recon["pct_diff"],
                               "status": recon["status"],
                               "label": (f"Reconciled to the billing ledger within {recon['pct_diff'] * 100:.2f}%"
                                         if recon["status"] != "sources_disagree" else "Sources disagree")}},
        "tiles": [
            {"id": "peak_viewers", "label": "Peak viewers", "display": compact(r["peak"][0]["peak_viewers"]),
             "compare_label": "Forecast", "compare_display": compact(r["forecast_peak"][0]["forecast_peak"]),
             "status": "ok", "metric_id": "peak_viewers", "next": {"to": "/forecast", "label": "See the forecast"}},
            {"id": "cancels_vs_expected", "label": "Cancellations vs expected", "display": f"+{pct_over * 100:.0f}%",
             "compare_label": "Actual vs expected", "compare_display": f"{compact(e['actual'])} vs {e['yhat']:,.0f}",
             "status": "alert", "metric_id": "cancels_pct_over", "next": {"to": "/ask", "label": "Ask why"}},
            {"id": "subs_at_risk", "label": "Subscribers at risk", "display": compact(m["n_at_risk"]),
             "compare_label": "Likely to cancel in 30 days", "compare_display": approx(m["projected_cancels_30d"]),
             "status": "alert", "metric_id": "subs_at_risk", "next": {"to": "/save", "label": "See the plan"}},
            {"id": "subs_saved", "label": "Subscribers the plan saves", "display": approx(saved),
             "compare_label": "Range", "compare_display": (f"{round(sum(c['saved_lo'] for c in plan), -2):,.0f}-"
                                                           f"{round(sum(c['saved_hi'] for c in plan), -2):,.0f}"),
             "status": "ok", "metric_id": "plan_saved_total"}],
        "core_entity": build_entity(r),
        "data_quality": build_quality(r),
        "how_it_works": {"panels": panels("truth"), "facts": [
            ["Core entity", "subscriber_id, the key every source system shares"],
            ["Unmasked email, phone, address", "members of the cc_privileged account group only (group pending creation)"],
            ["Kids-only accounts", "filtered for everyone, before any query, model or prompt"]]},
    }


def build_forecast(r: dict) -> dict:
    s = get_settings()
    c = s.clock()
    a = r["alert"][0]
    detected = et(a["detected_ts"])
    onset = detected - dt.timedelta(minutes=a["persistence_min"] - 1)
    resolved = et(a["resolved_ts"])
    h, mi = (int(x) for x in OPS_SHIFT_ET.split(":"))
    shift = dt.datetime.combine(s.game_date, dt.time(h, mi), tzinfo=ET)
    minutes = round((resolved - onset).total_seconds() / 60)
    metro = METRO_NAMES.get(a["metro"], a["metro"])
    flags = r["flags"]
    hot = flags[0]
    others = [f for f in flags if f["metro"] == hot["metro"] and f["cdn"] != hot["cdn"]]
    above = sum(1 for f in flags if f["util_at_peak"] >= 0.9)
    detail = (("The only network cell" if above == 1 else f"One of {above} cells") + " forecast above 90% at the "
              "halftime peak." + (f" {others[0]['cdn']} in the same city was forecast at "
                                   f"{others[0]['util_at_peak'] * 100:.0f}%." if others else ""))
    fc = {et(x["t"]).isoformat(): x for x in r["forecast_series"]}
    series = [{"t": et(x["t"]).isoformat(), "actual": x["actual"], "yhat": fc[k]["yhat"], "lo": fc[k]["lo"],
               "hi": fc[k]["hi"]} for x in r["actual_series"] if (k := et(x["t"]).isoformat()) in fc]
    cohort = f"{a['device_family']} app {a['app_version']}, {metro}, {a['cdn']}"
    return {
        "clock": {k: v.isoformat() for k, v in c.items()},
        "series": series,
        "peak": {"actual": r["peak"][0]["peak_viewers"], "forecast": r["forecast_peak"][0]["forecast_peak"]},
        "callout": {"cdn": hot["cdn"], "metro": hot["metro"], "metro_name": METRO_NAMES.get(hot["metro"], hot["metro"]),
                    "util": hot["util_at_peak"], "detail": detail},
        "alert": {
            "onset_t": onset.isoformat(), "detected_t": detected.isoformat(),
            "minutes_to_detect": a["persistence_min"],
            "headline": (f"{a['device_family']} viewers in {metro.split('-')[0]} on {a['cdn']}: "
                         f"{a['observed'] * 100:.1f}% buffering vs {a['baseline'] * 100:.1f}% normal"),
            "explanation": (f"{a['device_family']} app {a['app_version']} viewers in {metro} on {a['cdn']} started "
                            f"buffering at {label_clock(onset)}. The anomaly rule flagged the cell at "
                            f"{label_clock(detected)}, within {a['persistence_min']} minutes."),
            "cohort_label": cohort, "observed": a["observed"], "baseline": a["baseline"],
            "affected_sessions": a["affected_sessions"],
            "ops": {"t": shift.isoformat(), "resolved_t": resolved.isoformat(), "minutes_to_resolve": minutes,
                    "label": f"Ops shifted traffic at {label_clock(shift)} ({minutes} minutes to resolve)"}},
        "how_it_works": {"panels": panels("forecast"), "facts": [
            ["Forecast model", "cindy_wu_catalog.streamco.viewer_forecast"],
            ["Anomaly rule", f"buffering far above normal for {a['persistence_min']} minutes in a row"],
            ["Alert", a["alert_id"]]]},
    }


def build_save(r: dict) -> dict:
    m = r["model"][0]
    run = r["run"][0]
    cites = {x["card_id"]: x for x in r.get("citations") or [] if (x.get("passage") or "").strip()}
    cards = [{**c, "who": STATIC["card_who"].get(c["card_id"], ""),
              "policy": cites[c["card_id"]]["passage"].strip() if c["card_id"] in cites
              else STATIC["card_policy"].get(c["card_id"], ""),
              "policy_source": cites[c["card_id"]]["doc_title"] if c["card_id"] in cites else None}
             for c in sorted(r["plan"], key=lambda c: ["payment", "playback", "onboarding"].index(c["card_id"])
                             if c["card_id"] in ("payment", "playback", "onboarding") else 9)]
    msgs = {x["subscriber_id"]: x for x in r["messages"]}
    samples = []
    for sm in r["samples"]:
        msg = msgs.get(sm["subscriber_id"])
        if not msg:
            continue
        samples.append({"sample_id": sm["first_name"].lower(), "subscriber_id": sm["subscriber_id"],
                        "card_id": sm["card_id"], "first_name": sm["first_name"],
                        "metro": sm["metro"], "metro_name": METRO_NAMES.get(sm["metro"], sm["metro"]),
                        "tier": sm["tier"], "device": sm["device"], "tenure_months": sm["tenure_months"],
                        "what_happened": sm["what_happened"], "fav_genre": sm["fav_genre"],
                        "recent_titles": list(sm["recent_titles"]),
                        "message": {k: msg[k] for k in ("subject", "body", "watch_next", "agent_note", "model")}})
    drivers = m["top_drivers"] if isinstance(m["top_drivers"], list) else json.loads(m["top_drivers"])
    return {
        "headline": {"at_risk": m["n_at_risk"], "projected_cancels_30d": m["projected_cancels_30d"],
                     "saved_point": sum(c["saved_point"] for c in cards), "saved_lo": sum(c["saved_lo"] for c in cards),
                     "saved_hi": sum(c["saved_hi"] for c in cards), "cost_usd": sum(c["cost_usd"] for c in cards),
                     "revenue_protected_usd": sum(c["revenue_protected_usd"] for c in cards),
                     "revenue_at_risk_usd": round(m["projected_cancels_30d"] * ARPU_MONTHLY * RETENTION_MONTHS)},
        "model_card": {"uc_name": f"{get_settings().fq}.churn_propensity", "alias": "champion",
                       "version": str(m["version"]), "auc": m["auc"], "scored": m["n_scored"],
                       "top_drivers": [STATIC["driver_labels"].get(d, d.replace("_", " ").capitalize()) for d in drivers],
                       "monitoring": build_monitoring(r)},
        "cards": cards,
        "samples": samples,
        "run": {"n_messages": run["n_messages"], "cost_usd": run["cost_usd"], "model": run["model"]},
        "assumptions": f"Revenue assumes ${ARPU_MONTHLY}/month for {RETENTION_MONTHS} months, gross. Ranges are 80% "
                       "ranges from past holdout tests.",
        "how_it_works": {"panels": panels("save")},
    }


def build_governance(r: dict, trace: dict) -> dict:
    mask = r["masking"][0]
    masked, unmasked = mask["masked_text"], mask["transcript_text"]
    usage = None
    return {
        "seeded_note": "Budgets and usage figures are seeded for the demo (system.ai_gateway.usage holds the real "
                       "log).",
        "usage": {"calls": 172, "redactions": 1600,
                  "by_service": [{"service": "Model: gpt-5-6-sol", "calls": 118},
                                 {"service": "Agent tools", "calls": 41}, {"service": "Genie Agent", "calls": 13}],
                  "costs": [{"label": "Ask: one investigation", "usd": 0.30},
                            {"label": f"Personalized messages for {compact(r['run'][0]['n_messages'])}",
                             "usd": r["run"][0]["cost_usd"]}]},
        "live_usage": ({"calls": usage[0]["calls"], "tokens": usage[0]["tokens"] or 0,
                        "label": f"{get_settings().gateway_model}, this identity, last 7 days"}
                       if usage and usage[0]["calls"] else None),
        "budgets": [{"team": t, "monthly_budget_usd": b, "spend_usd": round(b * p), "budget_pct": p, "alert_pct": 0.9,
                     "status": "alert" if p >= 0.9 else "ok"}
                    for t, b, p in (("Commerce Data", 5000, 0.62), ("Media Engineering", 4000, 0.48),
                                    ("Support AI", 2500, 0.91))],
        "masking": {"masked_text": masked, "unmasked_text": unmasked,
                    "n_redactions": mask.get("n_redactions") or masked.count("[")},
        "trace": trace,
        "how_it_works": {"panels": panels("governance")},
    }


def build_ask() -> dict:
    return {"primary_question": PRIMARY_QUESTION, "followup_suggestions": [q for q, _ in FOLLOWUPS],
            "lanes": STATIC["lanes"], "how_it_works": {"panels": panels("ask")}}


def build_provenance(r: dict) -> dict:
    out = {}
    now = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    from backend.routers.ask import show_sql

    for metric_id, (key, label, tables, owner) in PROVENANCE.items():
        if not r.get(key):
            continue                              # an optional source with no rows: its number isn't shown
        cols, rows = jsonable(r[key])
        out[metric_id] = {"label": label, "sql": show_sql(Q[key], used_params(Q[key])), "tables": tables,
                          "owner_system": owner, "columns": cols, "rows": rows[:20], "ran_at": now}
    if "cancels_governed" in out:
        out["cancels_governed"]["lineage"] = build_lineage(r)
    return out


def assemble(r: dict, ask_run: dict, followups: list[dict], trace: dict, *, illustrative: bool, source: str) -> dict:
    return {
        "illustrative": illustrative, "interim": illustrative, "source": source,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "screens": {"truth": build_truth(r), "forecast": build_forecast(r), "ask": build_ask(), "save": build_save(r),
                    "governance": build_governance(r, trace)},
        "ask_run": ask_run, "followups": followups, "provenance": build_provenance(r),
    }


# ---------------------------------------------------------------- real export
def fetch_real(skip: set[str]) -> dict:
    from backend.databricks import run_sql

    rows, failed = {}, []
    for key, stmt in Q.items():
        if key in skip:
            continue
        try:
            rows[key] = run_sql(stmt, used_params(stmt), timeout_s=120)
            if not rows[key] and key not in OPTIONAL:
                failed.append(f"{key}: no rows")
        except Exception as exc:  # noqa: BLE001
            if key in OPTIONAL:
                rows[key] = []
            else:
                failed.append(f"{key}: {str(exc)[:200]}")
        print(f"  {key}: {len(rows.get(key) or [])} rows")
    if failed:
        raise SystemExit("Not writing the fixture; these queries failed:\n  " + "\n  ".join(failed))
    return rows


async def record_ask() -> dict:
    from backend.routers import ask

    trace, start = [], time.monotonic()
    async for event, data in ask.live_events(PRIMARY_QUESTION):
        trace.append({"offset_ms": int((time.monotonic() - start) * 1000), "event": event,
                      "data": json.loads(json.dumps(data, default=str))})
        print(f"  {trace[-1]['offset_ms']:>6} ms {event} {data.get('lane', '')}")
    answer = next(t for t in trace if t["event"] == "answer")["data"]
    narrator = answer.get("source")
    answer["source"] = "recorded"
    if answer.get("declined") or not answer.get("drivers"):
        raise SystemExit("Not writing the fixture: the live Ask run declined or returned no drivers")
    source = f"live run, narrated by {narrator}"
    lane1 = next(t for t in trace if t["event"] == "lane_done" and t["data"]["lane"] == "subscribers")
    if lane1["data"]["status"] != "ok":
        lane1["data"].update(status="ok", **subscriber_lane_from_sql())
        source += "; lane 1 from the metric view (Genie Agent not set up yet)"
    return {"question": PRIMARY_QUESTION, "variants": ["Why did Sports cancellations spike after the Big Game?",
                                                       "What caused the Sports cancel spike?"], "trace": trace,
            "source": source}


def record_followups() -> tuple[list[dict], str]:
    """The two follow-ups from the Genie Agent; if it isn't set up yet, from the same governed SQL."""
    from backend.databricks import genie_ask, run_sql
    from backend.routers.ask import is_declined, show_sql

    try:
        out, conv = [], None
        for q, variants in FOLLOWUPS:
            g = genie_ask(q, conv, timeout_s=120)
            conv = g["conversation_id"]
            out.append({"variants": [q, *variants], "answer": {"question": q, **g,
                                                               "declined": is_declined(g["text"], g["rows"])}})
            print(f"  follow-up {q!r}: {len(g['rows'])} rows, declined={out[-1]['answer']['declined']}")
        return out, "Genie Agent"
    except Exception as exc:  # noqa: BLE001
        print(f"  Genie Agent unavailable ({str(exc)[:120]}); recording the follow-ups from SQL")
    day = get_settings().day_after
    rq = ("SELECT tier, subscriber_live_cancels, ledger_cancels, pct_diff, status FROM gold_reconciliation_daily "
          "WHERE day = :day AND tier = 'Originals'")
    sq = ("SELECT state, MEASURE(cancels) AS cancels FROM mv_streamco_metrics WHERE day = :day AND tier = :tier "
          "GROUP BY state ORDER BY cancels DESC")
    r = run_sql(rq, {"day": day})[0]
    states = run_sql(sq, {"day": day, "tier": TIER})
    disagree = r["status"] == "sources_disagree"
    text0 = (f"I can't give a confident Originals cancellation number for yesterday. The subscriber system shows "
             f"{r['subscriber_live_cancels']:,} and the Finance ledger shows {r['ledger_cancels']:,}, a "
             f"{r['pct_diff'] * 100:.1f}% gap with no known cause, above the 2% tolerance. Treat Originals as "
             "unconfirmed until the two sources are reconciled." if disagree else
             f"Originals cancellations were {r['subscriber_live_cancels']:,} yesterday, within "
             f"{r['pct_diff'] * 100:.1f}% of the Finance ledger.")
    top = states[0]
    text1 = (f"{top['state']} had the most {TIER} cancellations the morning after the game ({top['cancels']:,})"
             + (", driven by Dallas-Fort Worth." if top["state"] == "Texas" else ".")
             + " The other states tracked their usual share of viewers.")
    answers = [(rq, {"day": day}, text0, disagree, [r]), (sq, {"day": day, "tier": TIER}, text1, False, states)]
    out = []
    for (q, variants), (stmt, prm, text, declined, rows) in zip(FOLLOWUPS, answers):
        cols, data = jsonable(rows)
        out.append({"variants": [q, *variants], "answer": {
            "question": q, "conversation_id": "recorded-conversation", "message_id": f"recorded-{len(out) + 1}",
            "text": text, "declined": declined, "sql": show_sql(stmt, prm), "columns": cols, "rows": data,
            "duration_ms": 6400 + 1500 * len(out)}})
    return out, "governed SQL (Genie Agent not set up yet)"


def subscriber_lane_from_sql() -> dict:
    """Lane 1's result from the metric view when the Genie Agent isn't set up yet (same numbers Genie would read)."""
    from backend.databricks import run_sql
    from backend.routers.ask import show_sql

    stmt = ("SELECT e.actual, e.yhat, e.lo, e.hi FROM gold_cancel_expected e WHERE e.day = :day AND e.tier = :tier")
    prm = {"day": get_settings().day_after, "tier": TIER}
    e = run_sql(stmt, prm)[0]
    summary = (f"{TIER} cancellations were {e['actual']:,} vs {e['yhat']:,.0f} expected "
               f"(+{(e['actual'] / e['yhat'] - 1) * 100:.0f}%), outside the {e['lo']:,.0f}-{e['hi']:,.0f} band.")
    return {"summary": summary, "sql_text": show_sql(stmt, prm), "row_count": 1}


def export_real() -> dict:
    os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", find_warehouse())
    print("Querying", get_settings().fq)
    r = fetch_real(set())
    print("Recording one live Ask run")
    ask_run = asyncio.run(record_ask())
    print("Recording the follow-ups")
    followups, followup_source = record_followups()
    trace = {"label": "Ask: why did Sports cancellations jump?", "url": get_settings().mlflow_trace_url or None,
             "steps": sum(1 for t in ask_run["trace"] if t["event"] == "lane_done") + 1,
             "duration_s": round(ask_run["trace"][-1]["offset_ms"] / 1000, 1)}
    out = assemble(r, ask_run, followups, trace, illustrative=False, source=f"{get_settings().fq} (real tables)")
    out["illustrative_parts"] = []
    out["interim"] = False
    out["followups_source"] = followup_source
    out["ask_run_source"] = ask_run.pop("source")
    return out


def find_warehouse() -> str:
    from backend.databricks import workspace

    for w in workspace().warehouses.list():
        if w.name == "ai_query_sql_warehouse":
            return w.id
    raise SystemExit("Set DATABRICKS_WAREHOUSE_ID (no warehouse named ai_query_sql_warehouse)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=FIXTURE_PATH)
    args = ap.parse_args()
    out = export_real()
    text = json.dumps(out, indent=1, default=str)
    for word in filter(None, (t.strip() for t in os.getenv("BLOCKED_TERMS", "").split(","))):
        if word.lower() in text.lower():
            raise SystemExit(f"Not writing the fixture: it contains a blocked term ({word!r})")
    args.out.write_text(text + "\n")
    t = out["screens"]["truth"]
    print(f"Wrote {args.out} (illustrative={out['illustrative']})")
    print("Headline numbers:", json.dumps({
        "governed": t["team_counts"]["governed"]["count"],
        "teams": {x["short"]: x["count"] for x in t["team_counts"]["teams"]},
        "tiles": {x["id"]: f"{x['display']} ({x['compare_display']})" for x in t["tiles"]},
        "hot_cell": f"{out['screens']['forecast']['callout']['cdn']} {out['screens']['forecast']['callout']['metro']} "
                    f"{out['screens']['forecast']['callout']['util']:.0%}",
        "drivers": {d["driver"]: d["pct"] for d in next(x for x in out["ask_run"]["trace"]
                                                        if x["event"] == "answer")["data"]["drivers"]},
        "plan": {k: out["screens"]["save"]["headline"][k] for k in ("at_risk", "saved_point", "cost_usd",
                                                                     "revenue_protected_usd")}}, indent=1))


if __name__ == "__main__":
    main()
