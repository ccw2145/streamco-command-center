import json
from pathlib import Path

import pytest

from backend import databricks as dbx
from backend import lakebase
from backend.config import FIXTURE_PATH
from backend.routers import save

REPO = Path(__file__).resolve().parents[2]


def _sample(sample_id: str):
    s = json.loads(FIXTURE_PATH.read_text())["screens"]["save"]
    sample = next(x for x in s["samples"] if x["sample_id"] == sample_id)
    return sample, next(c for c in s["cards"] if c["card_id"] == sample["card_id"])


def test_approve_keeps_approval_in_memory_when_recorded(client):
    r = client.post("/api/save/approve").json()
    assert r["stored"] == "memory" and r["recorded"] is True
    assert client.post("/api/save/approve").json() == r              # idempotent
    assert client.get("/api/screens/save").json()["approval"] == r
    client.post("/api/reset")
    assert client.get("/api/screens/save").json()["approval"] is None


def test_approve_writes_lakebase_when_live(live_client, monkeypatch):
    calls = []

    def fake_query(stmt, params=None, **k):
        calls.append((stmt, params))
        return [{"approval_id": 7, "approved_at": "2027-02-15T08:00:00Z"}] if stmt.startswith("INSERT") else []

    monkeypatch.setattr(lakebase, "query", fake_query)
    r = live_client.post("/api/save/approve", headers={"X-Forwarded-Email": "cindy@example.com"}).json()
    assert r["stored"] == "lakebase" and r["approval_id"] == 7 and r["approved_by"] == "cindy@example.com"
    assert "CREATE TABLE IF NOT EXISTS streamco_app.approvals" in calls[0][0]
    stmt, params = calls[1]
    assert stmt.startswith("INSERT INTO streamco_app.approvals") and json.loads(params["snapshot"])["cards"]


def test_serving_reads_one_subscriber_from_lakebase(live_client, monkeypatch):
    row = {"subscriber_id": 1, "churn_prob": 0.42, "risk_decile": 10, "top_drivers": ["Low viewing hours"],
           "card_id": "playback", "model_version": "1", "subject": "Hi", "watch_next": "x"}
    monkeypatch.setattr(lakebase, "timed_query", lambda stmt, params=None, **k: ([row], 2.5))
    r = live_client.get("/api/save/serving/marcus").json()
    assert r["source"] == "lakebase" and r["found"] and r["churn_prob"] == 0.42 and r["ms"] == 2.5
    assert live_client.get("/api/save/serving/nobody").status_code == 404


def test_regenerate_recorded_returns_the_saved_message(client):
    r = client.post("/api/save/regenerate/marcus").json()
    assert r["recorded"] is True and r["message"]["body"].startswith("Hi Marcus,")
    assert client.post("/api/save/regenerate/nobody").status_code == 404


def test_regenerate_live_returns_a_checked_message(live_client, monkeypatch):
    sample, _ = _sample("priya")
    good = {**{k: sample["message"][k] for k in save.FIELDS}, "subject": "Your renewal is sorted"}
    monkeypatch.setattr(dbx, "llm_json", lambda messages, **k: good)
    r = live_client.post("/api/save/regenerate/priya").json()
    assert r["recorded"] is False and r["message"]["subject"] == "Your renewal is sorted"


def test_regenerate_falls_back_when_the_message_breaks_a_rule(live_client, monkeypatch):
    sample, _ = _sample("marcus")
    bad = {**{k: sample["message"][k] for k in save.FIELDS}, "body": "Hi Marcus, take 50% off before you cancel."}
    monkeypatch.setattr(dbx, "llm_json", lambda messages, **k: bad)
    r = live_client.post("/api/save/regenerate/marcus").json()
    assert r["recorded"] is True and "checks" in r["note"]


@pytest.mark.parametrize("sample_id", ["marcus", "priya"])
def test_recorded_messages_pass_the_rules(sample_id):
    sample, card = _sample(sample_id)
    v = save.variables(sample, card)
    save.check_message(sample["message"], v)
    with pytest.raises(save.CheckFailed):
        save.check_message({**sample["message"], "body": sample["message"]["body"] + " Enjoy a free month on us."}, v)


def test_prompt_matches_the_batch_prompt_when_present():
    """The app's copy must be the same prompt notebooks/05_personalize uses (once the shared file exists)."""
    shared = [p for p in (REPO / "notebooks").rglob("personalized_message.md")] if (REPO / "notebooks").exists() else []
    shared += [p for p in (REPO / "prompts").glob("personalized_message.md")] if (REPO / "prompts").exists() else []
    if not shared:
        pytest.skip("no shared prompt file yet")
    strip = lambda t: save.re.sub(r"^<!--.*?-->\s*", "", t, flags=save.re.DOTALL).strip()  # noqa: E731
    assert strip(shared[0].read_text()) == strip(save.PROMPT_PATH.read_text())
