import json
import os

from backend.config import FIXTURE_PATH

BLOCKED = [t.strip().lower() for t in os.getenv("BLOCKED_TERMS", "").split(",") if t.strip()]


def test_fixture_has_no_blocked_terms():
    text = FIXTURE_PATH.read_text().lower()
    assert not [t for t in BLOCKED if t in text]


def test_every_screen_loads(client):
    for name in ("truth", "forecast", "ask", "save", "governance"):
        r = client.get(f"/api/screens/{name}")
        assert r.status_code == 200, name
        assert r.json()["how_it_works"]["panels"]
    assert client.get("/api/screens/nope").status_code == 404


def test_every_clickable_number_has_its_sql(client):
    fx = json.loads(FIXTURE_PATH.read_text())
    truth = fx["screens"]["truth"]
    ids = {t["metric_id"] for t in truth["tiles"]} | {t["metric_id"] for t in truth["team_counts"]["teams"]}
    ids |= {truth["team_counts"]["governed"]["metric_id"], "recon_sports", "hot_cell_utilization",
            "affected_sessions", "subs_at_risk", "churn_model_auc", "plan_saved_total", "plan_revenue_protected",
            "personalization_run"}
    answer = next(t for t in fx["ask_run"]["trace"] if t["event"] == "answer")["data"]
    ids |= {d["metric_id"] for d in answer["drivers"]}
    for metric_id in ids:
        r = client.get(f"/api/provenance/{metric_id}")
        assert r.status_code == 200, metric_id
        assert r.json()["sql"].startswith("SELECT") and r.json()["rows"], metric_id


def test_headline_numbers_add_up():
    fx = json.loads(FIXTURE_PATH.read_text())
    save = fx["screens"]["save"]
    h = save["headline"]
    assert h["saved_point"] == sum(c["saved_point"] for c in save["cards"])
    assert h["revenue_protected_usd"] == sum(c["revenue_protected_usd"] for c in save["cards"])
    answer = next(t for t in fx["ask_run"]["trace"] if t["event"] == "answer")["data"]
    assert abs(sum(d["pct"] for d in answer["drivers"]) - 1) < 0.02
    assert {s["sample_id"] for s in save["samples"]} >= {"marcus", "priya"}


def test_health_reports_recorded_mode(client):
    body = client.get("/api/health").json()
    assert body["live"] is False and body["force_fixture"] is True
